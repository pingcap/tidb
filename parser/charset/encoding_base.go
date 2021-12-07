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

package charset

import (
	"bytes"
	"fmt"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"golang.org/x/text/encoding"
	"golang.org/x/text/transform"
)

var errInvalidCharacterString = terror.ClassParser.NewStd(mysql.ErrInvalidCharacterString)

// EncodingBase defines some generic functions.
type EncodingBase struct {
	enc encoding.Encoding
}

// Validate checks whether bytes are valid in current charset.
func (b EncodingBase) Validate(dest, src []byte) (result []byte, nSrc int, ok bool) {
	if len(src) == 0 {
		return src, 0, true
	}
	var buf [4]byte
	transformer := b.enc.NewEncoder()
	// First check tries to avoid unnecessary memory allocation.
	invalidPos := -1
	for i, w := 0, 0; i < len(src); i += w {
		w = len(EncodingUTF8Impl.Peek(src[i:]))
		_, _, err := transformer.Transform(buf[:], src[i:i+w], true)
		if err != nil {
			invalidPos = i
			break
		}
	}
	if invalidPos == -1 {
		// First check passed.
		return src, len(src), true
	}
	if len(dest) < len(src) {
		dest = make([]byte, 0, len(src))
	}
	dest = dest[:0]
	// Second check replaces invalid chars with '?'.
	for i, w := 0, 0; i < len(src); i += w {
		w = len(EncodingUTF8Impl.Peek(src[i:]))
		_, _, err := transformer.Transform(buf[:], src[i:i+w], true)
		if err != nil {
			dest = append(dest, '?')
			continue
		}
		dest = append(dest, src[i:i+w]...)
	}
	return dest, invalidPos, false
}

func (b EncodingBase) transform(transformer transform.Transformer, dest, src []byte,
	peek func([]byte) []byte, name string) ([]byte, int, error) {
	if len(src) == 0 {
		return src, 0, nil
	}
	isDecoding := &peek != &encodingUTF8Peek
	if len(dest) < len(src) {
		dest = make([]byte, len(src)*2)
	}
	var destOffset, srcOffset int
	var encodingErr error
	var consumed int
	for {
		srcNextLen := len(peek(src[srcOffset:]))
		nDest, nSrc, err := transformer.Transform(dest[destOffset:], src[srcOffset:srcOffset+srcNextLen], false)
		if err == transform.ErrShortDst {
			newDest := make([]byte, len(dest)*2)
			copy(newDest, dest)
			dest = newDest
		} else if err != nil || (isDecoding && beginWithReplacementChar(dest[destOffset:destOffset+nDest])) {
			if encodingErr == nil {
				encodingErr = generateEncodingErr(name, src[srcOffset:srcOffset+srcNextLen])
				consumed = srcOffset
			}
			dest[destOffset] = byte('?')
			nDest, nSrc = 1, srcNextLen // skip the source bytes that cannot be decoded normally.
		}
		destOffset += nDest
		srcOffset += nSrc
		// The source bytes are exhausted.
		if srcOffset >= len(src) {
			return dest[:destOffset], consumed, encodingErr
		}
	}
}

// replacementBytes are bytes for the replacement rune 0xfffd.
var replacementBytes = []byte{0xEF, 0xBF, 0xBD}

// beginWithReplacementChar check if dst has the prefix '0xEFBFBD'.
func beginWithReplacementChar(dst []byte) bool {
	return bytes.HasPrefix(dst, replacementBytes)
}

// generateEncodingErr generates an invalid string in charset error.
func generateEncodingErr(name string, invalidBytes []byte) error {
	arg := fmt.Sprintf("%X", invalidBytes)
	return errInvalidCharacterString.GenWithStackByArgs(name, arg)
}
