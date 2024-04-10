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
	go_unicode "unicode"

	"golang.org/x/text/encoding"
)

// EncodingASCIIImpl is the instance of encodingASCII
var EncodingASCIIImpl = &encodingASCII{encodingBase{enc: encoding.Nop}}

func init() {
	EncodingASCIIImpl.self = EncodingASCIIImpl
}

// encodingASCII is the ASCII encoding.
type encodingASCII struct {
	encodingBase
}

// Name implements Encoding interface.
func (*encodingASCII) Name() string {
	return CharsetASCII
}

// Tp implements Encoding interface.
func (*encodingASCII) Tp() EncodingTp {
	return EncodingTpASCII
}

// Peek implements Encoding interface.
func (*encodingASCII) Peek(src []byte) []byte {
	if len(src) == 0 {
		return src
	}
	return src[:1]
}

// IsValid implements Encoding interface.
func (*encodingASCII) IsValid(src []byte) bool {
	srcLen := len(src)
	for i := 0; i < srcLen; i++ {
		if src[i] > go_unicode.MaxASCII {
			return false
		}
	}
	return true
}

// Transform implements Encoding interface.
func (e *encodingASCII) Transform(dest *bytes.Buffer, src []byte, op Op) ([]byte, error) {
	if e.IsValid(src) {
		return src, nil
	}
	return e.encodingBase.Transform(dest, src, op)
}

// Foreach implements Encoding interface.
func (*encodingASCII) Foreach(src []byte, _ Op, fn func(from, to []byte, ok bool) bool) {
	for i, w := 0, 0; i < len(src); i += w {
		w = 1
		ok := true
		if src[i] > go_unicode.MaxASCII {
			w = len(EncodingUTF8Impl.Peek(src[i:]))
			ok = false
		}
		if !fn(src[i:i+w], src[i:i+w], ok) {
			return
		}
	}
}
