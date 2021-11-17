package rowindexcodec

import (
	"bytes"
)

// KeyKind is a specific type of key, mainly used to distinguish row/index.
type KeyKind int

const (
	KeyKindUnknown KeyKind = iota
	KeyKindRow
	KeyKindIndex
)

var (
	tablePrefix = []byte{'t'}
	rowPrefix   = []byte("_r")
	indexPrefix = []byte("_i")
)

// GetKeyKind returns the KeyKind that matches the key in the minimalist way.
func GetKeyKind(key []byte) KeyKind {
	// [ TABLE_PREFIX | TABLE_ID | ROW_PREFIX (INDEX_PREFIX) | ROW_ID (INDEX_ID) | ... ]   (name)
	// [      1       |    8     |            2              |         8         | ... ]   (byte)
	if len(key) < 11 {
		return KeyKindUnknown
	}
	if !bytes.HasPrefix(key, tablePrefix) {
		return KeyKindUnknown
	}
	key = key[9:]
	if bytes.HasPrefix(key, rowPrefix) {
		return KeyKindRow
	}
	if bytes.HasPrefix(key, indexPrefix) {
		return KeyKindIndex
	}
	return KeyKindUnknown
}
