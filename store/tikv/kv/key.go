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
// See the License for the specific language governing permissions and
// limitations under the License.

package kv

import (
	"bytes"
	"encoding/hex"
)

// NextKey returns the next key in byte-order.
func NextKey(k []byte) []byte {
	// add 0x0 to the end of key
	buf := make([]byte, len(k)+1)
	copy(buf, k)
	return buf
}

// CmpKey returns the comparison result of two key.
// The result will be 0 if a==b, -1 if a < b, and +1 if a > b.
func CmpKey(k, another []byte) int {
	return bytes.Compare(k, another)
}

// StrKey returns string for key.
func StrKey(k []byte) string {
	return hex.EncodeToString(k)
}

// KeyRange represents a range where StartKey <= key < EndKey.
type KeyRange struct {
	StartKey []byte
	EndKey   []byte
}
