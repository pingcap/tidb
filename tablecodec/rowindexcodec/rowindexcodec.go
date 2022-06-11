// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rowindexcodec

import (
	"bytes"
)

// KeyKind is a specific type of key, mainly used to distinguish row/index.
type KeyKind int

const (
	// KeyKindUnknown indicates that this key is unknown type.
	KeyKindUnknown KeyKind = iota
	// KeyKindRow means that this key belongs to row.
	KeyKindRow
	// KeyKindIndex means that this key belongs to index.
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
