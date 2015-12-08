// Copyright 2015 PingCAP, Inc.
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

package codec

import (
	"encoding/binary"
	"math"

	"github.com/juju/errors"
)

const (
	negNum    byte = 0
	nonNegNum byte = 1
)

func encodeNumSign(v int64) byte {
	if v < 0 {
		return negNum
	}

	return nonNegNum
}

// EncodeInt appends the encoded value to slice b and returns the appended slice.
// EncodeInt guarantees that the encoded value is in ascending order for comparison.
func EncodeInt(b []byte, v int64) []byte {
	var data [9]byte
	data[0] = encodeNumSign(v)
	binary.BigEndian.PutUint64(data[1:], uint64(v))
	return append(b, data[:]...)
}

// EncodeIntDesc appends the encoded value to slice b and returns the appended slice.
// EncodeIntDesc guarantees that the encoded value is in descending order for comparison.
func EncodeIntDesc(b []byte, v int64) []byte {
	var data [9]byte
	data[0] = ^encodeNumSign(v)
	binary.BigEndian.PutUint64(data[1:], uint64(^v))
	return append(b, data[:]...)
}

// DecodeInt decodes value encoded by EncodeInt before.
// It returns the leftover un-decoded slice, decoded value if no error.
func DecodeInt(b []byte) ([]byte, int64, error) {
	if len(b) < 9 {
		return nil, 0, errors.New("insufficient bytes to decode value")
	}

	numSign := b[0]
	v := binary.BigEndian.Uint64(b[1:9])

	if numSign == nonNegNum && v > math.MaxInt64 {
		return nil, 0, errors.Errorf("decoded value %d - %d overflow int64", v, int64(v))
	}

	b = b[9:]
	return b, int64(v), nil
}

// DecodeIntDesc decodes value encoded by EncodeInt before.
// It returns the leftover un-decoded slice, decoded value if no error.
func DecodeIntDesc(b []byte) ([]byte, int64, error) {
	if len(b) < 9 {
		return nil, 0, errors.New("insufficient bytes to decode value")
	}

	data := b[:9]
	numSign := ^data[0]
	v := binary.BigEndian.Uint64(data[1:9])

	v = ^v
	if numSign == nonNegNum && v > math.MaxInt64 {
		return nil, 0, errors.Errorf("decoded value %d - %d overflow int64", v, int64(v))
	}

	b = b[9:]
	return b, int64(v), nil
}

// EncodeUint appends the encoded value to slice b and returns the appended slice.
// EncodeUint guarantees that the encoded value is in ascending order for comparison.
func EncodeUint(b []byte, v uint64) []byte {
	var data [8]byte
	binary.BigEndian.PutUint64(data[:], v)
	return append(b, data[:]...)
}

// EncodeUintDesc appends the encoded value to slice b and returns the appended slice.
// EncodeUintDesc guarantees that the encoded value is in descending order for comparison.
func EncodeUintDesc(b []byte, v uint64) []byte {
	var data [8]byte
	binary.BigEndian.PutUint64(data[:], ^v)
	return append(b, data[:]...)
}

// DecodeUint decodes value encoded by EncodeUint before.
// It returns the leftover un-decoded slice, decoded value if no error.
func DecodeUint(b []byte) ([]byte, uint64, error) {
	if len(b) < 8 {
		return nil, 0, errors.New("insufficient bytes to decode value")
	}

	v := binary.BigEndian.Uint64(b[:8])
	b = b[8:]
	return b, v, nil
}

// DecodeUintDesc decodes value encoded by EncodeInt before.
// It returns the leftover un-decoded slice, decoded value if no error.
func DecodeUintDesc(b []byte) ([]byte, uint64, error) {
	if len(b) < 8 {
		return nil, 0, errors.New("insufficient bytes to decode value")
	}

	data := b[:8]
	v := binary.BigEndian.Uint64(data)
	b = b[8:]
	return b, ^v, nil
}
