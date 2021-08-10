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

package local

import (
	"encoding/binary"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/codec"
)

// KeyAdapter is used to encode and decode keys.
type KeyAdapter interface {
	// Encode encodes key with rowID and offset. It guarantees the encoded key is in ascending order for comparison.
	// `buf` is used to buffer data to avoid the cost of make slice.
	// Implementations of Encode must not reuse the key for encoding.
	Encode(buf []byte, key []byte, rowID int64, offset int64) []byte

	// Decode decodes the original key. `buf` is used to buffer data to avoid the cost of make slice.
	// Implementations of Decode must not reuse the data for decoding.
	Decode(buf []byte, data []byte) (key []byte, rowID int64, offset int64, err error)

	// EncodedLen returns the encoded key length.
	EncodedLen(key []byte) int
}

func reallocBytes(b []byte, n int) []byte {
	newSize := len(b) + n
	if cap(b) < newSize {
		bs := make([]byte, len(b), newSize)
		copy(bs, b)
		return bs
	}
	return b
}

type noopKeyAdapter struct{}

func (noopKeyAdapter) Encode(buf []byte, key []byte, _ int64, _ int64) []byte {
	return append(buf[:0], key...)
}

func (noopKeyAdapter) Decode(buf []byte, data []byte) (key []byte, rowID int64, offset int64, err error) {
	key = append(buf[:0], data...)
	return
}

func (noopKeyAdapter) EncodedLen(key []byte) int {
	return len(key)
}

var _ KeyAdapter = noopKeyAdapter{}

type duplicateKeyAdapter struct{}

func (duplicateKeyAdapter) Encode(buf []byte, key []byte, rowID int64, offset int64) []byte {
	buf = codec.EncodeBytes(buf[:0], key)
	buf = reallocBytes(buf, 16)
	n := len(buf)
	buf = buf[:n+16]
	binary.BigEndian.PutUint64(buf[n:n+8], uint64(rowID))
	binary.BigEndian.PutUint64(buf[n+8:], uint64(offset))
	return buf
}

func (duplicateKeyAdapter) Decode(buf []byte, data []byte) (key []byte, rowID int64, offset int64, err error) {
	if len(data) < 16 {
		return nil, 0, 0, errors.New("insufficient bytes to decode value")
	}
	_, key, err = codec.DecodeBytes(data[:len(data)-16], buf)
	if err != nil {
		return
	}
	rowID = int64(binary.BigEndian.Uint64(data[len(data)-16 : len(data)-8]))
	offset = int64(binary.BigEndian.Uint64(data[len(data)-8:]))
	return
}

func (duplicateKeyAdapter) EncodedLen(key []byte) int {
	return codec.EncodedBytesLength(len(key)) + 16
}

var _ KeyAdapter = duplicateKeyAdapter{}
