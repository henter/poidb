package poidb

import (
	"fmt"
	"github.com/tidwall/btree"
	"strconv"
)


type dbItem struct {
	key string
	lng, lat float64
}

func appendArray(buf []byte, count int) []byte {
	buf = append(buf, '*')
	buf = append(buf, strconv.FormatInt(int64(count), 10)...)
	buf = append(buf, '\r', '\n')
	return buf
}

func appendBulkString(buf []byte, s string) []byte {
	buf = append(buf, '$')
	buf = append(buf, strconv.FormatInt(int64(len(s)), 10)...)
	buf = append(buf, '\r', '\n')
	buf = append(buf, s...)
	buf = append(buf, '\r', '\n')
	return buf
}

// writeSetTo writes an item as a single SET record to the a bufio Writer.
func (dbi *dbItem) writeSetTo(buf []byte) []byte {
	buf = appendArray(buf, 4)
	buf = appendBulkString(buf, "set")
	buf = appendBulkString(buf, dbi.key)
	buf = appendBulkString(buf, fmt.Sprintf("%g", dbi.lng))
	buf = appendBulkString(buf, fmt.Sprintf("%g", dbi.lat))
	return buf
}

// writeSetTo writes an item as a single DEL record to the a bufio Writer.
func (dbi *dbItem) writeDeleteTo(buf []byte) []byte {
	buf = appendArray(buf, 2)
	buf = appendBulkString(buf, "del")
	buf = appendBulkString(buf, dbi.key)
	return buf
}



// Less determines if a b-tree item is less than another. This is required
// for ordering, inserting, and deleting items from a b-tree. It's important
// to note that the ctx parameter is used to help with determine which
// formula to use on an item. Each b-tree should use a different ctx when
// sharing the same item.
func (dbi *dbItem) Less(item btree.Item, ctx interface{}) bool {
	dbi2 := item.(*dbItem)
	return dbi.key < dbi2.key
}

