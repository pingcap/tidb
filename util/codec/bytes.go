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
	"bytes"
	"runtime"
	"unsafe"

	"github.com/juju/errors"
)

const (
	encGroupSize     = 8
	avgEncGroupCount = 20
	encMarker        = byte(0xFF)
	encPad           = byte(0x0)
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
func EncodeBytes(b []byte, data []byte) []byte {
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

		marker := encMarker - byte(padCount)
		bs = append(bs, marker)
	}

	return bs
}

func decodeBytes(b []byte, reverse bool) ([]byte, []byte, error) {
	if len(b) < encGroupSize+1 {
		return nil, nil, errors.Errorf("insufficient bytes to decode value")
	}

	data := []byte{}
	for {
		groupBytes := b[:encGroupSize+1]
		if reverse {
			reverseBytes(groupBytes)
		}

		group := groupBytes[:encGroupSize]
		marker := groupBytes[encGroupSize]

		// Check validity of marker.
		padCount := encMarker - marker
		realGroupSize := encGroupSize - padCount
		if realGroupSize < 0 {
			return nil, nil, errors.Errorf("invalid marker byte, group %v", group)
		}

		data = append(data, group[:realGroupSize]...)
		b = b[encGroupSize+1:]

		if marker != encMarker {
			// Check validity of padding bytes.
			if bytes.Count(group[realGroupSize:], []byte{encPad}) != int(padCount) {
				return nil, nil, errors.Errorf("invalid padding byte, group %v", group)
			}

			break
		}
	}

	return b, data, nil
}

// DecodeBytes decodes bytes which is encoded by EncodeBytes before,
// returns the leftover bytes and decoded value if no error.
func DecodeBytes(b []byte) ([]byte, []byte, error) {
	return decodeBytes(b, false)
}

// EncodeBytesDesc first encodes bytes using EncodeBytes, then bitwise reverses
// encoded value to guarentee the encoded value is in descending order for comparison,
// The encoded value is >= SmallestNoneNilValue and < InfiniteValue.
func EncodeBytesDesc(b []byte, data []byte) []byte {
	n := len(b)
	b = EncodeBytes(b, data)
	reverseBytes(b[n:])
	return b
}

// DecodeBytesDesc decodes bytes which is encoded by EncodeBytesDesc before,
// returns the leftover bytes and decoded value if no error.
func DecodeBytesDesc(b []byte) ([]byte, []byte, error) {
	return decodeBytes(b, true)
}

// See https://golang.org/src/crypto/cipher/xor.go
const wordSize = int(unsafe.Sizeof(uintptr(0)))
const supportsUnaligned = runtime.GOARCH == "386" || runtime.GOARCH == "amd64"

func fastReverseBytes(b []byte) {
	n := len(b)
	w := n / wordSize
	if w > 0 {
		bw := *(*[]uintptr)(unsafe.Pointer(&b))
		for i := 0; i < w; i++ {
			bw[i] = ^bw[i]
		}
	}

	for i := w * wordSize; i < n; i++ {
		b[i] = ^b[i]
	}
}

func safeReverseBytes(b []byte) {
	for i := range b {
		b[i] = ^b[i]
	}
}

func reverseBytes(b []byte) {
	if supportsUnaligned {
		fastReverseBytes(b)
		return
	}

	safeReverseBytes(b)
}

// like realloc.
func reallocBytes(b []byte, n int) []byte {
	if cap(b) < n {
		bs := make([]byte, len(b), len(b)+n)
		copy(bs, b)
		return bs
	}

	// slice b has capability to store n bytes
	return b
}
