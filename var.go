package poidb

import "errors"

var (
	// ErrNotFound is returned when an item or index is not in the database.
	ErrNotFound = errors.New("not found")

	// ErrInvalid is returned when the database file is an invalid format.
	ErrInvalid = errors.New("invalid database")

	// ErrDatabaseClosed is returned when the database is closed.
	ErrDatabaseClosed = errors.New("database closed")

	// ErrInvalidSyncPolicy is returned for an invalid SyncPolicy value.
	ErrInvalidSyncPolicy = errors.New("invalid sync policy")

	// ErrShrinkInProcess is returned when a shrink operation is in-process.
	ErrShrinkInProcess = errors.New("shrink is in-process")

	// ErrPersistenceActive is returned when post-loading data from an database
	// not opened with Open(":memory:").
	ErrPersistenceActive = errors.New("persistence active")
)

// Default number of btree degrees
const btreeDegrees = 64

// SyncPolicy represents how often data is synced to disk.
type SyncPolicy int

const (
	// Never is used to disable syncing data to disk.
	// The faster and less safe method.
	Never SyncPolicy = 0
	// EverySecond is used to sync data to disk every second.
	// It's pretty fast and you can lose 1 second of data if there
	// is a disaster.
	// This is the recommended setting.
	EverySecond = 1
	// Always is used to sync data after every write to disk.
	// Slow. Very safe.
	Always = 2
)