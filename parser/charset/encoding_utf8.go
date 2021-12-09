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
	"unicode/utf8"

	"golang.org/x/text/encoding"
)

// EncodingUTF8Impl is the instance of EncodingUTF8.
var EncodingUTF8Impl = &EncodingUTF8{EncodingBase{enc: encoding.Nop}}

// EncodingUTF8MB3StrictImpl is the instance of EncodingUTF8MB3Strict.
var EncodingUTF8MB3StrictImpl = &EncodingUTF8MB3Strict{
	EncodingUTF8{
		EncodingBase{
			enc: encoding.Nop,
		},
	},
}

func init() {
	EncodingUTF8Impl.self = EncodingUTF8Impl
	EncodingUTF8MB3StrictImpl.self = EncodingUTF8MB3StrictImpl
}

// EncodingUTF8 is TiDB's default encoding.
type EncodingUTF8 struct {
	EncodingBase
}

// Name implements Encoding interface.
func (e *EncodingUTF8) Name() string {
	return CharsetUTF8MB4
}

// Peek implements Encoding interface.
func (e *EncodingUTF8) Peek(src []byte) []byte {
	nextLen := 4
	if len(src) == 0 || src[0] < 0x80 {
		nextLen = 1
	} else if src[0] < 0xe0 {
		nextLen = 2
	} else if src[0] < 0xf0 {
		nextLen = 3
	}
	if len(src) < nextLen {
		return src
	}
	return src[:nextLen]
}

// Transform implements Encoding interface.
func (e *EncodingUTF8) Transform(dest, src []byte, op Op, opt TruncateOpt, cOpt CollectOpt) ([]byte, error) {
	if IsValid(e, src) {
		return src, nil
	}
	return e.EncodingBase.Transform(dest, src, op, opt, cOpt)
}

// Foreach implements Encoding interface.
func (e *EncodingUTF8) Foreach(src []byte, op Op, fn func(from, to []byte, ok bool) bool) {
	var rv rune
	for i, w := 0, 0; i < len(src); i += w {
		rv, w = utf8.DecodeRune(src[i:])
		meetErr := rv == utf8.RuneError && w == 1
		if !fn(src[i:i+w], src[i:i+w], !meetErr) {
			return
		}
	}
}

// EncodingUTF8MB3Strict is the strict mode of EncodingUTF8MB3.
// MB4 characters are considered invalid.
type EncodingUTF8MB3Strict struct {
	EncodingUTF8
}

// Foreach implements Encoding interface.
func (e *EncodingUTF8MB3Strict) Foreach(src []byte, op Op, fn func(srcCh, dstCh []byte, ok bool) bool) {
	for i, w := 0, 0; i < len(src); i += w {
		var rv rune
		rv, w = utf8.DecodeRune(src[i:])
		meetErr := (rv == utf8.RuneError && w == 1) || w > 3
		if !fn(src[i:i+w], src[i:i+w], !meetErr) {
			return
		}
	}
}

// Transform implements Encoding interface.
func (e *EncodingUTF8MB3Strict) Transform(dest, src []byte, op Op, opt TruncateOpt, cOpt CollectOpt) ([]byte, error) {
	if IsValid(e, src) {
		return src, nil
	}
	return e.EncodingBase.Transform(dest, src, op, opt, cOpt)
}
