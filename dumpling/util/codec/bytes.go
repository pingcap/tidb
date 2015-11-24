// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

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

// EncodeBytes encodes the slice value using an escape based encoding,
// with the following rule:
//  \x00 -> \x00\xFF
//  \xFF -> \xFF\x00 if the first byte is \xFF
// EncodeBytes will append \x00\x01 at the end of the encoded value to
// indicate the termination.
// EncodeBytes guarantees the encoded value is in ascending order for comparison,
// The encoded value is >= SmallestNoneNilValue and < InfiniteValue.
func EncodeBytes(b []byte, data []byte) []byte {
	// Allocate more space to avoid unnecessary slice growing
	bs := reallocBytes(b, len(data)+20)
	if len(data) > 0 && data[0] == 0xFF {
		// we must escape 0xFF here to guarantee encoded value < InfiniteValue \xFF\xFF.
		bs = append(bs, 0xFF, 0x00)
		data = data[1:]
	}

	for {
		// find 0x00 and escape it.
		i := bytes.IndexByte(data, 0x00)
		if i == -1 {
			break
		}
		bs = append(bs, data[:i]...)
		bs = append(bs, 0x00, 0xFF)
		data = data[i+1:]
	}
	bs = append(bs, data...)
	return append(bs, 0x00, 0x01)
}

// DecodeBytes decodes bytes which is encoded by EncodeBytes before,
// returns the leftover bytes and decoded value if no error.
func DecodeBytes(b []byte) ([]byte, []byte, error) {
	return decodeBytes(b, 0xFF, 0x00, 0x01)
}

func decodeBytes(b []byte, escapeFirst byte, escape byte, term byte) ([]byte, []byte, error) {
	if len(b) < 2 {
		return nil, nil, errors.Errorf("insufficient bytes to decode value")
	}

	var r []byte

	if b[0] == escapeFirst {
		if b[1] != ^escapeFirst {
			return nil, nil, errors.Errorf("invalid escape byte, must 0x%x, but 0x%x", ^escapeFirst, b[1])
		}
		r = append(r, escapeFirst)
		b = b[2:]
	}

	for {
		i := bytes.IndexByte(b, escape)
		if i == -1 {
			return nil, nil, errors.Errorf("invalid termination in bytes")
		}

		if i+1 >= len(b) {
			return nil, nil, errors.Errorf("malformed escaped bytes")
		}

		if b[i+1] == term {
			if r == nil {
				r = b[:i]
			} else {
				r = append(r, b[:i]...)
			}

			return b[i+2:], r, nil
		}

		if b[i+1] != ^escape {
			return nil, nil, errors.Errorf("invalid escape byte, must 0x%x, but got 0x%0x", ^escape, b[i+1])
		}

		// here mean we have \x00 in origin slice, so realloc a large buffer
		// to avoid reallocation again, the final decoded slice length is < len(b) certainly.
		// TODO: we can record the escape offset and then do the alloc + copy in the end.
		r = reallocBytes(r, len(b))
		r = append(r, b[:i]...)
		r = append(r, escape)
		b = b[i+2:]
	}
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
	var (
		r   []byte
		err error
	)
	b, r, err = decodeBytes(b, 0x00, 0xFF, ^byte(0x01))
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	reverseBytes(r)
	return b, r, nil
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
