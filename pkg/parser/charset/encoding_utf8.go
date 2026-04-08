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
	"unicode/utf8"

	"golang.org/x/text/encoding"
)

// EncodingUTF8Impl is the instance of encodingUTF8.
var EncodingUTF8Impl = &encodingUTF8{encodingBase{enc: encoding.Nop}}

// EncodingUTF8MB3StrictImpl is the instance of encodingUTF8MB3Strict.
var EncodingUTF8MB3StrictImpl = &encodingUTF8MB3Strict{
	encodingUTF8{
		encodingBase{
			enc: encoding.Nop,
		},
	},
}

func init() {
	EncodingUTF8Impl.self = EncodingUTF8Impl
	EncodingUTF8MB3StrictImpl.self = EncodingUTF8MB3StrictImpl
}

// encodingUTF8 is TiDB's default encoding.
type encodingUTF8 struct {
	encodingBase
}

// Name implements Encoding interface.
func (*encodingUTF8) Name() string {
	return CharsetUTF8MB4
}

// Tp implements Encoding interface.
func (*encodingUTF8) Tp() EncodingTp {
	return EncodingTpUTF8
}

// Peek implements Encoding interface.
func (*encodingUTF8) Peek(src []byte) []byte {
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

func (*encodingUTF8) MbLen(bs string) int {
	_, size := utf8.DecodeRuneInString(bs)
	if size <= 1 {
		return 0
	}
	return size
}

// IsValid implements Encoding interface.
func (e *encodingUTF8) IsValid(src []byte) bool {
	if utf8.Valid(src) {
		return true
	}
	return e.encodingBase.IsValid(src)
}

// Transform implements Encoding interface.
func (e *encodingUTF8) Transform(dest *bytes.Buffer, src []byte, op Op) ([]byte, error) {
	if e.IsValid(src) {
		return src, nil
	}
	return e.encodingBase.Transform(dest, src, op)
}

// Foreach implements Encoding interface.
func (*encodingUTF8) Foreach(src []byte, _ Op, fn func(from, to []byte, ok bool) bool) {
	var rv rune
	for i, w := 0, 0; i < len(src); i += w {
		rv, w = utf8.DecodeRune(src[i:])
		meetErr := rv == utf8.RuneError && w == 1
		if !fn(src[i:i+w], src[i:i+w], !meetErr) {
			return
		}
	}
}

// encodingUTF8MB3Strict is the strict mode of EncodingUTF8MB3.
// MB4 characters are considered invalid.
type encodingUTF8MB3Strict struct {
	encodingUTF8
}

// IsValid implements Encoding interface.
func (e *encodingUTF8MB3Strict) IsValid(src []byte) bool {
	return e.encodingBase.IsValid(src)
}

// Foreach implements Encoding interface.
func (*encodingUTF8MB3Strict) Foreach(src []byte, _ Op, fn func(srcCh, dstCh []byte, ok bool) bool) {
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
func (e *encodingUTF8MB3Strict) Transform(dest *bytes.Buffer, src []byte, op Op) ([]byte, error) {
	if e.IsValid(src) {
		return src, nil
	}
	return e.encodingBase.Transform(dest, src, op)
}
