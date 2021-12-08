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
	"strings"
	"unicode/utf8"
)

// EncodingUTF8Impl is the instance of EncodingUTF8.
var EncodingUTF8Impl = &EncodingUTF8{}

// EncodingUTF8MB3StrictImpl is the instance of EncodingUTF8MB3Strict.
var EncodingUTF8MB3StrictImpl = &EncodingUTF8MB3Strict{}

// EncodingUTF8 is TiDB's default encoding.
type EncodingUTF8 struct{}

// Name implements Encoding interface.
func (e *EncodingUTF8) Name() string {
	return CharsetUTF8MB4
}

// encodingUTF8Peek is used to check function identity.
var encodingUTF8Peek = EncodingUTF8Impl.Peek

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

// Encode implements Encoding interface.
func (e *EncodingUTF8) Encode(_, src []byte) ([]byte, int, error) {
	return src, len(src), nil
}

// EncodeString implements Encoding interface.
func (e *EncodingUTF8) EncodeString(_ []byte, src string) (result string, nSrc int, err error) {
	return src, len(src), nil
}

// Decode implements Encoding interface.
func (e *EncodingUTF8) Decode(dest, src []byte) ([]byte, int, error) {
	return decode(dest, src, e.Name(), true)
}

// DecodeString implements Encoding interface.
func (e *EncodingUTF8) DecodeString(dest []byte, src string) (string, int, error) {
	return decodeString(dest, src, e.Name(), true)
}

// replaceIllegal replaces all the illegal UTF8 characters.
func (e *EncodingUTF8) ReplaceIllegal(dest, src []byte) []byte {
	return replaceIllegal(dest, src, true)
}

// Validate implements Encoding interface.
func (e *EncodingUTF8) Validate(src []byte) (int, bool) {
	return validate(src, true)
}

// ToUpper implements Encoding interface.
func (e *EncodingUTF8) ToUpper(src string) string {
	return strings.ToUpper(src)
}

// ToLower implements Encoding interface.
func (e *EncodingUTF8) ToLower(src string) string {
	return strings.ToLower(src)
}

// EncodingUTF8MB3Strict is the strict mode of EncodingUTF8MB3.
// MB4 characters are considered invalid.
type EncodingUTF8MB3Strict struct {
	EncodingUTF8
}

// Validate implements Encoding interface.
func (e *EncodingUTF8MB3Strict) Validate(src []byte) (int, bool) {
	return validate(src, false)
}

// ReplaceIllegal replaces all the illegal UTF8 characters.
func (e *EncodingUTF8MB3Strict) ReplaceIllegal(dest, src []byte) []byte {
	return replaceIllegal(dest, src, false)
}

// Decode implements Encoding interface.
func (e *EncodingUTF8MB3Strict) Decode(dest, src []byte) ([]byte, int, error) {
	return decode(dest, src, e.Name(), false)
}

// DecodeString implements Encoding interface.
func (e *EncodingUTF8MB3Strict) DecodeString(dest []byte, src string) (string, int, error) {
	return decodeString(dest, src, e.Name(), false)
}

func decodeString(dest []byte, src string, name string, mb4IsLegal bool) (string, int, error) {
	srcBytes := Slice(src)
	nSrc, ok := validate(srcBytes, mb4IsLegal)
	if !ok {
		ret := replaceIllegal(dest, srcBytes, mb4IsLegal)
		err := generateEncodingErr(name, srcBytes[nSrc:])
		return string(ret), nSrc, err
	}
	return src, len(src), nil
}

func decode(dest, src []byte, name string, mb4IsLegal bool) ([]byte, int, error) {
	nSrc, ok := validate(src, mb4IsLegal)
	if !ok {
		ret := replaceIllegal(dest, src, mb4IsLegal)
		err := generateEncodingErr(name, src[nSrc:])
		return ret, nSrc, err
	}
	return src, len(src), nil
}

func validate(src []byte, mb4IsLegal bool) (int, bool) {
	if len(src) == 0 {
		return 0, true
	}
	if mb4IsLegal && utf8.Valid(src) {
		// Quick check passed.
		return len(src), true
	}
	for i, w := 0, 0; i < len(src); i += w {
		var rv rune
		rv, w = utf8.DecodeRune(src[i:])
		if (rv == utf8.RuneError && w == 1) || (w > 3 && !mb4IsLegal) {
			return i, false
		}
	}
	return len(src), true
}

func replaceIllegal(dest, src []byte, mb4IsLegal bool) []byte {
	if len(dest) < len(src) {
		dest = make([]byte, 0, len(src))
	}
	dest = dest[:0]
	for i, w := 0, 0; i < len(src); i += w {
		var rv rune
		rv, w = utf8.DecodeRune(src[i:])
		if (rv == utf8.RuneError && w == 1) || (w > 3 && !mb4IsLegal) {
			dest = append(dest, '?')
			continue
		}
		dest = append(dest, src[i:i+w]...)
	}
	return dest
}
