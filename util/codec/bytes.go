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
	"encoding/binary"
	"runtime"
	"unsafe"

	"github.com/juju/errors"
)

const (
	encGroupSize = 8
	encMarker    = byte(0xFF)
	encPad       = byte(0x0)
)

var zeroBytes = []byte{0, 0, 0, 0, 0, 0, 0, 0}

// writeAscendingBytes guarantees the encoded value is in ascending order for comparison,
// encoding with the following rule:
//  [group1][marker1]...[groupN][markerN]
//  group is 8 bytes slice which is padding with 0.
//  marker is `0xFF - padding 0 count`
// For example:
//   [] -> [0, 0, 0, 0, 0, 0, 0, 0, 247]
//   [1, 2, 3] -> [1, 2, 3, 0, 0, 0, 0, 0, 250]
//   [1, 2, 3, 0] -> [1, 2, 3, 0, 0, 0, 0, 0, 251]
//   [1, 2, 3, 4, 5, 6, 7, 8] -> [1, 2, 3, 4, 5, 6, 7, 8, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247]
// Refer: https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format
func writeAscendingBytes(b *bytes.Buffer, data []byte) {
	// Allocate more space to avoid unnecessary slice growing.
	// Assume that the byte slice size is about `(len(data) / encGroupSize + 1) * (encGroupSize + 1)` bytes,
	// that is `(len(data) / 8 + 1) * 9` in our implement.
	dLen := len(data)
	growSize := (dLen/encGroupSize + 1) * (encGroupSize + 1)
	b.Grow(growSize)
	for idx := 0; idx <= dLen; idx += encGroupSize {
		remain := dLen - idx
		padCount := 0
		if remain >= encGroupSize {
			b.Write(data[idx : idx+encGroupSize])
		} else {
			padCount = encGroupSize - remain
			b.Write(data[idx:])
			b.Write(zeroBytes[:padCount])
		}

		b.WriteByte(encMarker - byte(padCount))
	}
}

func readComparableBytes(b *bytes.Buffer, reverse bool) ([]byte, error) {
	data := make([]byte, 0, b.Len())
	for {
		if b.Len() < encGroupSize+1 {
			return nil, errors.New("insufficient bytes to decode value")
		}

		groupBytes := b.Next(encGroupSize + 1)
		if reverse {
			reverseBytes(groupBytes)
		}

		group := groupBytes[:encGroupSize]
		marker := groupBytes[encGroupSize]

		// Check validity of marker.
		padCount := encMarker - marker
		realGroupSize := encGroupSize - padCount
		if padCount > encGroupSize {
			return nil, errors.Errorf("invalid marker byte, group bytes %q", groupBytes)
		}

		data = append(data, group[:realGroupSize]...)

		if marker != encMarker {
			// Check validity of padding bytes.
			if bytes.Count(group[realGroupSize:], []byte{encPad}) != int(padCount) {
				return nil, errors.Errorf("invalid padding byte, group bytes %q", groupBytes)
			}

			break
		}
	}

	return data, nil
}

func (ascEncoder) WriteSingleByte(b *bytes.Buffer, v byte) {
	b.WriteByte(v)
}

func (ascEncoder) ReadSingleByte(b *bytes.Buffer) (byte, error) {
	return b.ReadByte()
}

func (ascEncoder) WriteBytes(b *bytes.Buffer, v []byte) {
	writeAscendingBytes(b, v)
}

func (ascEncoder) ReadBytes(b *bytes.Buffer) ([]byte, error) {
	return readComparableBytes(b, false)
}

func (descEncoder) WriteSingleByte(b *bytes.Buffer, v byte) {
	b.WriteByte(^v)
}

func (descEncoder) ReadSingleByte(b *bytes.Buffer) (byte, error) {
	v, err := b.ReadByte()
	return ^v, errors.Trace(err)
}

func (descEncoder) WriteBytes(b *bytes.Buffer, v []byte) {
	n := b.Len()
	writeAscendingBytes(b, v)
	reverseBytes(b.Bytes()[n:])
}

func (descEncoder) ReadBytes(b *bytes.Buffer) ([]byte, error) {
	return readComparableBytes(b, true)
}

func (compactEncoder) WriteSingleByte(b *bytes.Buffer, v byte) {
	b.WriteByte(v)
}

func (compactEncoder) ReadSingleByte(b *bytes.Buffer) (byte, error) {
	return b.ReadByte()
}

func (e compactEncoder) WriteBytes(b *bytes.Buffer, v []byte) {
	b.Grow(binary.MaxVarintLen64 + len(v))
	e.WriteInt(b, int64(len(v)))
	b.Write(v)
}

func (e compactEncoder) ReadBytes(b *bytes.Buffer) ([]byte, error) {
	n, err := e.ReadInt(b)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if b.Len() < int(n) {
		return nil, errors.Errorf("insufficient bytes to decode value, expected length: %v", n)
	}
	return b.Next(int(n)), nil
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
