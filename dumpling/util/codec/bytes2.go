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
	"github.com/juju/errors"
)

const (
	encGroupSize     = 8
	avgEncGroupCount = 20
	defaultEncMarker = 0xFF
)

// Refer: https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format
// EncodeBytes guarantees the encoded value is in ascending order for comparison,
// encoding with the following rule:
//  [group1][marker1]...[groupN][markerN]
//  group is 8 bytes sclice which maybe padding with 0.
//  marker is 0xFF - appending 0 number
// For example:
//   [] -> [1 0 0 0 0 0 0 0 0 247]
//   [1, 2, 3] -> [1 1 2 3 0 0 0 0 0 250]
//   [1, 2, 3, 0] -> [1 1 2 3 0 0 0 0 0 251]
//   [1, 2, 3, 4, 5, 6, 7, 8] -> [1 1 2 3 4 5 6 7 8 255 0 0 0 0 0 0 0 0 247]
func EncodeBytes2(b []byte, data []byte) []byte {
	// Allocate more space to avoid unnecessary slice growing.
	// Assume that the byte slice size is about 20*8 = 160 bytes.
	dl := len(data)
	bs := reallocBytes(b, dl+avgEncGroupCount)
	for idx := 0; idx <= dl; idx += encGroupSize {
		remain := dl - idx
		padCount := 0
		if remain >= encGroupSize {
			bs = append(bs, data[idx:idx+encGroupSize]...)
		} else {
			padCount = encGroupSize - remain
			bs = append(bs, data[idx:]...)
			bs = append(bs, make([]byte, padCount)...)
		}

		marker := byte(defaultEncMarker - padCount)
		bs = append(bs, marker)
	}

	return bs
}

// DecodeBytes decodes bytes which is encoded by EncodeBytes before,
// returns the leftover bytes and decoded value if no error.
func DecodeBytes2(b []byte) ([]byte, []byte, error) {
	if len(b) < encGroupSize+1 {
		return nil, nil, errors.Errorf("insufficient bytes to decode value")
	}

	var data []byte
	for {
		group := b[:encGroupSize]
		marker := b[encGroupSize]
		b = b[encGroupSize+1:]
		padCount := byte(defaultEncMarker - marker)
		realSize := encGroupSize - padCount
		if realSize < 0 {
			return nil, nil, errors.Errorf("invalid marker byte, 0x%x", marker)
		}
		data = append(data, group[:realSize]...)
		if marker != defaultEncMarker {
			break
		}
	}

	return b, data, nil
}

// EncodeBytesDesc first encodes bytes using EncodeBytes, then bitwise reverses
// encoded value to guarentee the encoded value is in descending order for comparison,
// The encoded value is >= SmallestNoneNilValue and < InfiniteValue.
func EncodeBytesDesc2(b []byte, data []byte) []byte {
	n := len(b)
	b = EncodeBytes2(b, data)
	reverseBytes(b[n:])
	return b
}

// DecodeBytesDesc decodes bytes which is encoded by EncodeBytesDesc before,
// returns the leftover bytes and decoded value if no error.
func DecodeBytesDesc2(b []byte) ([]byte, []byte, error) {
	remain, data, err := DecodeBytes2(b)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	reverseBytes(data)
	return remain, data, nil
}
