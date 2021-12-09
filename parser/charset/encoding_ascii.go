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
	go_unicode "unicode"

	"golang.org/x/text/encoding"
)

// EncodingASCIIImpl is the instance of EncodingASCII
var EncodingASCIIImpl = &EncodingASCII{EncodingBase{enc: encoding.Nop}}

func init() {
	EncodingASCIIImpl.self = EncodingASCIIImpl
}

// EncodingASCII is the ASCII encoding.
type EncodingASCII struct {
	EncodingBase
}

// Name implements Encoding interface.
func (e *EncodingASCII) Name() string {
	return CharsetASCII
}

// Peek implements Encoding interface.
func (e *EncodingASCII) Peek(src []byte) []byte {
	if len(src) == 0 {
		return src
	}
	return src[:1]
}

func (e *EncodingASCII) Transform(dest, src []byte, op Op, opt TruncateOpt, cOpt CollectOpt) ([]byte, error) {
	if op == OpToUTF8 {
		// ASCII is a subset of utf-8.
		return src, nil
	}
	if IsValid(e, src) {
		return src, nil
	}
	return e.EncodingBase.Transform(dest, src, op, opt, cOpt)
}

func (e *EncodingASCII) Foreach(src []byte, op Op, fn func(from, to []byte, ok bool) bool) {
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
