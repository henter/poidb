package poidb

import (
	"bufio"
	"fmt"
	"github.com/eapache/go-resiliency/deadline"
	"github.com/henter/poidb/d2"
	"github.com/tidwall/btree"
	"github.com/tidwall/geojson"
	"github.com/tidwall/geojson/geo"
	"github.com/tidwall/geojson/geometry"
	"io"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)


type DB struct {
	mu        sync.RWMutex      // the gatekeeper for all fields
	file      *os.File          // the underlying file
	buf       []byte            // a buffer to write to
	keys      *btree.BTree      // a tree of all item ordered by key
	index     *d2.BoxTree        // poi index
	flushes   int               // a count of the number of disk flushes
	closed    bool              // set when the database has been closed
	config    Config            // the database configuration
	persist   bool              // do we write to disk
	shrinking bool              // when an aof shrink is in-process.
	lastaofsz int               // the size of the last shrink aof size

	commitItems     map[string]*dbItem // details for committing tx.
}

// Config represents database configuration options. These
// options are used to change various behaviors of the database.
type Config struct {
	// SyncPolicy adjusts how often the data is synced to disk.
	// This value can be Never, EverySecond, or Always.
	// The default is EverySecond.
	SyncPolicy SyncPolicy

	// AutoShrinkPercentage is used by the background process to trigger
	// a shrink of the aof file when the size of the file is larger than the
	// percentage of the result of the previous shrunk file.
	// For example, if this value is 100, and the last shrink process
	// resulted in a 100mb file, then the new aof file must be 200mb before
	// a shrink is triggered.
	AutoShrinkPercentage int

	// AutoShrinkMinSize defines the minimum size of the aof file before
	// an automatic shrink can occur.
	AutoShrinkMinSize int

	// AutoShrinkDisabled turns off automatic background shrinking
	AutoShrinkDisabled bool
}

func Open(path string) (*DB, error) {
	db := &DB{}
	// initialize trees and indexes
	db.keys = btree.New(btreeDegrees, nil)
	db.index = &d2.BoxTree{}
	db.commitItems = make(map[string]*dbItem)

	// initialize default configuration
	db.config = Config{
		SyncPolicy:           EverySecond,
		AutoShrinkPercentage: 100,
		AutoShrinkMinSize:    32 * 1024 * 1024,
	}

	// turn off persistence for pure in-memory
	db.persist = path != ":memory:"
	if db.persist {
		var err error
		// hardcoding 0666 as the default mode.
		db.file, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			return nil, err
		}
		// load the database from disk
		if err := db.load(); err != nil {
			// close on error, ignore close error
			_ = db.file.Close()
			return nil, err
		}
	}
	// start the background manager.
	go db.backgroundManager()
	return db, nil
}

func (db *DB) Count() int {
	return db.index.Count()
}

// ReadConfig returns the database configuration.
func (db *DB) ReadConfig(config *Config) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return ErrDatabaseClosed
	}
	*config = db.config
	return nil
}

// SetConfig updates the database configuration.
func (db *DB) SetConfig(config Config) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrDatabaseClosed
	}
	switch config.SyncPolicy {
	default:
		return ErrInvalidSyncPolicy
	case Never, EverySecond, Always:
	}
	db.config = config
	return nil
}

// insertIntoDatabase performs inserts an item in to the database and updates
// all indexes. If a previous item with the same key already exists, that item
// will be replaced with the new one, and return the previous item.
func (db *DB) insertIntoDatabase(item *dbItem) *dbItem {
	var pdbi *dbItem
	prev := db.keys.ReplaceOrInsert(item)
	if prev != nil {
		// A previous item was removed from the keys tree. Let's
		// fully delete this item from all indexes.
		pdbi = prev.(*dbItem)
		db.index.Delete([]float64{item.lng, item.lat}, []float64{item.lng, item.lat}, *item)
	}

	db.index.Insert([]float64{item.lng, item.lat}, []float64{item.lng, item.lat}, *item)
	// we must return the previous item to the caller.
	return pdbi
}


// load reads entries from the append only database file and fills the database.
// The file format uses the Redis append only file format, which is and a series
// of RESP commands. For more information on RESP please read
// http://redis.io/topics/protocol. The only supported RESP commands are DEL and
// SET.
func (db *DB) load() error {
	fi, err := db.file.Stat()
	if err != nil {
		return err
	}
	if err := db.readLoad(db.file, fi.ModTime()); err != nil {
		return err
	}
	pos, err := db.file.Seek(0, 2)
	if err != nil {
		return err
	}
	db.lastaofsz = int(pos)
	return nil
}

// readLoad reads from the reader and loads commands into the database.
// modTime is the modified time of the reader, should be no greater than
// the current time.Now().
func (db *DB) readLoad(rd io.Reader, modTime time.Time) error {
	data := make([]byte, 4096)
	parts := make([]string, 0, 8)
	r := bufio.NewReader(rd)
	for {
		// read a single command.
		// first we should read the number of parts that the of the command
		line, err := r.ReadBytes('\n')
		if err != nil {
			if len(line) > 0 {
				// got an eof but also data. this should be an unexpected eof.
				return io.ErrUnexpectedEOF
			}
			if err == io.EOF {
				break
			}
			return err
		}
		if line[0] != '*' {
			return ErrInvalid
		}
		// convert the string number to and int
		var n int
		if len(line) == 4 && line[len(line)-2] == '\r' {
			if line[1] < '0' || line[1] > '9' {
				return ErrInvalid
			}
			n = int(line[1] - '0')
		} else {
			if len(line) < 5 || line[len(line)-2] != '\r' {
				return ErrInvalid
			}
			for i := 1; i < len(line)-2; i++ {
				if line[i] < '0' || line[i] > '9' {
					return ErrInvalid
				}
				n = n*10 + int(line[i]-'0')
			}
		}
		// read each part of the command.
		parts = parts[:0]
		for i := 0; i < n; i++ {
			// read the number of bytes of the part.
			line, err := r.ReadBytes('\n')
			if err != nil {
				return err
			}
			if line[0] != '$' {
				return ErrInvalid
			}
			// convert the string number to and int
			var n int
			if len(line) == 4 && line[len(line)-2] == '\r' {
				if line[1] < '0' || line[1] > '9' {
					return ErrInvalid
				}
				n = int(line[1] - '0')
			} else {
				if len(line) < 5 || line[len(line)-2] != '\r' {
					return ErrInvalid
				}
				for i := 1; i < len(line)-2; i++ {
					if line[i] < '0' || line[i] > '9' {
						return ErrInvalid
					}
					n = n*10 + int(line[i]-'0')
				}
			}
			// resize the read buffer
			if len(data) < n+2 {
				dataln := len(data)
				for dataln < n+2 {
					dataln *= 2
				}
				data = make([]byte, dataln)
			}
			if _, err = io.ReadFull(r, data[:n+2]); err != nil {
				return err
			}
			if data[n] != '\r' || data[n+1] != '\n' {
				return ErrInvalid
			}
			// copy string
			parts = append(parts, string(data[:n]))
		}
		// finished reading the command

		if len(parts) == 0 {
			continue
		}
		if (parts[0][0] == 's' || parts[0][1] == 'S') &&
			(parts[0][1] == 'e' || parts[0][1] == 'E') &&
			(parts[0][2] == 't' || parts[0][2] == 'T') {
			// SET
			if len(parts) < 3 || len(parts) > 4 {
				return ErrInvalid
			}

			lng, _ := strconv.ParseFloat(parts[2], 64)
			lat, _ := strconv.ParseFloat(parts[3], 64)

			db.insertIntoDatabase(&dbItem{key: parts[1], lng: lng, lat: lat})
		} else if (parts[0][0] == 'd' || parts[0][1] == 'D') &&
			(parts[0][1] == 'e' || parts[0][1] == 'E') &&
			(parts[0][2] == 'l' || parts[0][2] == 'L') {
			// DEL
			if len(parts) != 2 {
				return ErrInvalid
			}
			db.deleteFromDatabase(&dbItem{key: parts[1]})
		} else if (parts[0][0] == 'f' || parts[0][1] == 'F') &&
			strings.ToLower(parts[0]) == "flushdb" {
			db.keys = btree.New(btreeDegrees, nil)
			db.index = &d2.BoxTree{}
		} else {
			return ErrInvalid
		}
	}
	return nil
}
// deleteFromDatabase removes and item from the database and indexes. The input
// item must only have the key field specified thus "&dbItem{key: key}" is all
// that is needed to fully remove the item with the matching key. If an item
// with the matching key was found in the database, it will be removed and
// returned to the caller. A nil return value means that the item was not
// found in the database
func (db *DB) deleteFromDatabase(item *dbItem) *dbItem {
	var pdbi *dbItem
	prev := db.keys.Delete(item)
	if prev != nil {
		pdbi = prev.(*dbItem)
		db.index.Delete([]float64{pdbi.lng, pdbi.lat}, []float64{pdbi.lng, pdbi.lat}, pdbi.key)
	}
	return pdbi
}

// backgroundManager runs continuously in the background and performs various
// operations such as removing expired items and syncing to disk.
func (db *DB) backgroundManager() {
	flushes := 0
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for range t.C {
		var shrink bool
		err := func() error {
			if db.persist && !db.config.AutoShrinkDisabled {
				pos, err := db.file.Seek(0, 1)
				if err != nil {
					return err
				}
				aofsz := int(pos)
				if aofsz > db.config.AutoShrinkMinSize {
					prc := float64(db.config.AutoShrinkPercentage) / 100.0
					shrink = aofsz > db.lastaofsz+int(float64(db.lastaofsz)*prc)
				}
			}
			return nil
		}()
		if err == ErrDatabaseClosed {
			break
		}

		// execute a disk sync, if needed
		if db.persist && db.config.SyncPolicy == EverySecond {
			func() {
				db.mu.Lock()
				defer db.mu.Unlock()
				fmt.Printf("file sync persist %d policy  %d flushed %d != %d\n", db.persist, db.config.SyncPolicy, flushes, db.flushes)
				if flushes != db.flushes {
					err = db.file.Sync()
					if err != nil {
						fmt.Printf("file sync error %s\n", err)
					}
					flushes = db.flushes
				}
			}()
		}

		if shrink {
			if err = db.Shrink(); err != nil {
				if err == ErrDatabaseClosed {
					break
				}
			}
		}
	}
}

// Shrink will make the database file smaller by removing redundant
// log entries. This operation does not block the database.
func (db *DB) Shrink() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrDatabaseClosed
	}
	if !db.persist {
		// The database was opened with ":memory:" as the path.
		// There is no persistence, and no need to do anything here.
		db.mu.Unlock()
		return nil
	}
	if db.shrinking {
		// The database is already in the process of shrinking.
		db.mu.Unlock()
		return ErrShrinkInProcess
	}
	db.shrinking = true
	defer func() {
		db.mu.Lock()
		db.shrinking = false
		db.mu.Unlock()
	}()
	fname := db.file.Name()
	tmpname := fname + ".tmp"
	// the endpos is used to return to the end of the file when we are
	// finished writing all of the current items.
	endpos, err := db.file.Seek(0, 2)
	if err != nil {
		return err
	}
	db.mu.Unlock()
	time.Sleep(time.Second / 4) // wait just a bit before starting
	f, err := os.Create(tmpname)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
		_ = os.RemoveAll(tmpname)
	}()

	// we are going to read items in as chunks as to not hold up the database
	// for too long.
	var buf []byte
	pivot := ""
	done := false
	for !done {
		err := func() error {
			db.mu.RLock()
			defer db.mu.RUnlock()
			if db.closed {
				return ErrDatabaseClosed
			}
			done = true
			var n int
			db.keys.AscendGreaterOrEqual(&dbItem{key: pivot},
				func(item btree.Item) bool {
					dbi := item.(*dbItem)
					// 1000 items or 64MB buffer
					if n > 1000 || len(buf) > 64*1024*1024 {
						pivot = dbi.key
						done = false
						return false
					}
					buf = dbi.writeSetTo(buf)
					n++
					return true
				},
			)
			if len(buf) > 0 {
				if _, err := f.Write(buf); err != nil {
					return err
				}
				buf = buf[:0]
			}
			return nil
		}()
		if err != nil {
			return err
		}
	}
	// We reached this far so all of the items have been written to a new tmp
	// There's some more work to do by appending the new line from the aof
	// to the tmp file and finally swap the files out.
	return func() error {
		// We're wrapping this in a function to get the benefit of a defered
		// lock/unlock.
		db.mu.Lock()
		defer db.mu.Unlock()
		if db.closed {
			return ErrDatabaseClosed
		}
		// We are going to open a new version of the aof file so that we do
		// not change the seek position of the previous. This may cause a
		// problem in the future if we choose to use syscall file locking.
		aof, err := os.Open(fname)
		if err != nil {
			return err
		}
		defer func() { _ = aof.Close() }()
		if _, err := aof.Seek(endpos, 0); err != nil {
			return err
		}
		// Just copy all of the new commands that have occurred since we
		// started the shrink process.
		if _, err := io.Copy(f, aof); err != nil {
			return err
		}
		// Close all files
		if err := aof.Close(); err != nil {
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
		if err := db.file.Close(); err != nil {
			return err
		}
		// Any failures below here is really bad. So just panic.
		if err := os.Rename(tmpname, fname); err != nil {
			panic(err)
		}
		db.file, err = os.OpenFile(fname, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			panic(err)
		}
		pos, err := db.file.Seek(0, 2)
		if err != nil {
			return err
		}
		db.lastaofsz = int(pos)
		return nil
	}()
}

// Close releases all database resources.
// All transactions must be closed before closing the database.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrDatabaseClosed
	}
	db.closed = true
	if db.persist {
		db.file.Sync() // do a sync but ignore the error
		if err := db.file.Close(); err != nil {
			return err
		}
	}
	// Let's release all references to nil. This will help both with debugging
	// late usage panics and it provides a hint to the garbage collector
	db.keys, db.index, db.file = nil, nil, nil
	return nil
}

// Save writes a snapshot of the database to a writer. This operation blocks all
// writes, but not reads. This can be used for snapshots and backups for pure
// in-memory databases using the ":memory:". Database that persist to disk
// can be snapshotted by simply copying the database file.
func (db *DB) Save(wr io.Writer) error {
	var err error
	db.mu.RLock()
	defer db.mu.RUnlock()
	// use a buffered writer and flush every 4MB
	var buf []byte
	// iterated through every item in the database and write to the buffer
	db.keys.Ascend(func(item btree.Item) bool {
		dbi := item.(*dbItem)
		buf = dbi.writeSetTo(buf)
		if len(buf) > 1024*1024*4 {
			// flush when buffer is over 4MB
			_, err = wr.Write(buf)
			if err != nil {
				return false
			}
			buf = buf[:0]
		}
		return true
	})
	if err != nil {
		return err
	}
	// one final flush
	if len(buf) > 0 {
		_, err = wr.Write(buf)
		if err != nil {
			return err
		}
	}
	return nil
}

// Load loads commands from reader. This operation blocks all reads and writes.
// Note that this can only work for fully in-memory databases opened with
// Open(":memory:").
func (db *DB) Load(rd io.Reader) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.persist {
		// cannot load into databases that persist to disk
		return ErrPersistenceActive
	}
	return db.readLoad(rd, time.Now())
}

func (db *DB) Set(key string, lng, lat float64) (prevKey string, replaced bool, err error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db == nil {
		return "", false, ErrInvalid
	}
	item := &dbItem{key: key, lng: lng, lat: lat}
	// Insert the item into the keys tree.
	prev := db.insertIntoDatabase(item)
	if prev != nil {
		prevKey, replaced = prev.key, true
	}

	// For commits we simply assign the item to the map. We use this map to
	// write the entry to disk.
	if db.persist {
		db.commitItems[key] = item
	}

	err = db.Commit()
	return prevKey, replaced, err
}

// Commit writes all changes to disk.
// An error is returned when a write error occurs, or when a Commit() is called
// from a read-only transaction.
func (db *DB) Commit() error {
	if db == nil {
		return ErrInvalid
	}
	var err error
	if db.persist && len(db.commitItems) > 0 {
		db.buf = db.buf[:0]
		// Each committed record is written to disk
		for key, item := range db.commitItems {
			if item == nil {
				db.buf = (&dbItem{key: key}).writeDeleteTo(db.buf)
			} else {
				db.buf = item.writeSetTo(db.buf)
			}
		}
		// Flushing the buffer only once per transaction.
		// If this operation fails then the write did failed and we must
		// rollback.
		if _, err = db.file.Write(db.buf); err != nil {
			//TODO
		}
		if db.config.SyncPolicy == Always {
			err = db.file.Sync()
		}
		// Increment the number of flushes. The background syncing uses this.
		db.flushes++
		db.commitItems = make(map[string]*dbItem)
	}
	return err
}


// Nearby returns the nearest neighbors
func (db *DB) Nearby(
	target geojson.Object,
	cursor Cursor,
	deadline *deadline.Deadline,
	iter func(id string, lng, lat float64) bool,
) bool {
	// First look to see if there's at least one candidate in the circle's
	// outer rectangle. This is a fast-fail operation.
	if circle, ok := target.(*geojson.Circle); ok {
		meters := circle.Meters()
		if meters > 0 {
			center := circle.Center()
			minLat, minLon, maxLat, maxLon := geo.RectFromCenter(center.Y, center.X, meters)
			var exists bool
			db.index.Search(
				[]float64{minLon, minLat},
				[]float64{maxLon, maxLat},
				func(_, _ []float64, itemv interface{}) bool {
					exists = true
					return false
				},
			)
			if !exists {
				// no candidates
				return true
			}
		}
	}
	// do the kNN operation
	alive := true
	center := target.Center()
	var count uint64
	var offset uint64
	if cursor != nil {
		offset = cursor.Offset()
		cursor.Step(offset)
	}
	db.index.Nearby(
		[]float64{center.X, center.Y},
		[]float64{center.X, center.Y},
		func(_, _ []float64, itemv interface{}) bool {
			count++
			if count <= offset {
				return true
			}
			nextStep(count, cursor, deadline)
			item := itemv.(dbItem)
			alive = iter(item.key, item.lng, item.lat)
			return alive
		},
	)
	return alive
}


// yieldStep forces the iterator to yield goroutine every 255 steps.
const yieldStep = 255

// Cursor allows for quickly paging through Scan, Within, Intersects, and Nearby
type Cursor interface {
	Offset() uint64
	Step(count uint64)
}

func nextStep(step uint64, cursor Cursor, deadline *deadline.Deadline) {
	if step&yieldStep == yieldStep {
		runtime.Gosched()
		//deadline.Check()
	}
	if cursor != nil {
		cursor.Step(1)
	}
}

type Item struct {
	Id         string
	X, Y, Dist float64
}

func (db *DB) NearbyWithDist(lng, lat float64, meters float64, limit int) (items []Item) {
	p := geometry.Point{lng, lat}
	target := geojson.NewCircle(p, meters, 64)

	maxDist := target.Haversine()
	db.Nearby(target, nil, nil, func(id string, lng, lat float64) bool {
		dist := target.HaversineTo(geometry.Point{lng, lat})
		if maxDist > 0 && dist > maxDist {
			return false
		}
		items = append(items, Item{
			Id:   id,
			X:    lng,
			Y:    lat,
			Dist: geo.DistanceFromHaversine(dist),
		})
		return len(items) < limit
	})
	sort.Slice(items, func(i, j int) bool {
		return items[i].Dist < items[j].Dist
	})
	return
}
