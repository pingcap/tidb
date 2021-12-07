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
// EncodingUTF8MB3Impl is the instance of EncodingUTF8MB3.
var EncodingUTF8MB3Impl = &EncodingUTF8MB3{}
// EncodingUTF8MB3StrictImpl is the instance of EncodingUTF8MB3Strict.
var EncodingUTF8MB3StrictImpl = &EncodingUTF8MB3Strict{}

// EncodingUTF8 is TiDB's default encoding.
type EncodingUTF8 struct {}

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
	result, nSrc, ok := e.Validate(dest, src)
	if !ok {
		return result, nSrc, generateEncodingErr(e.Name(), src[nSrc:])
	}
	return src, len(src), nil
}

// DecodeString implements Encoding interface.
func (e *EncodingUTF8) DecodeString(dest []byte, src string) (string, int, error) {
	srcBytes := Slice(src)
	result, nSrc, ok := e.Validate(dest, srcBytes)
	if !ok {
		return string(result), nSrc, generateEncodingErr(e.Name(), srcBytes[nSrc:])
	}
	return src, len(src), nil
}

// Validate implements Encoding interface.
func (e *EncodingUTF8) Validate(dest, src[] byte) ([]byte, int, bool) {
	return e.validateUTF8(dest, src, true, true)
}

// validateUTF8 considers the mb3 case.
func (e *EncodingUTF8) validateUTF8(dest, src []byte, isUTF8MB4, checkMB4Value bool) ([]byte, int, bool) {
	if len(src) == 0 {
		return src, 0, true
	}
	if isUTF8MB4 && utf8.Valid(src) {
		// Quick check passed.
		return src, len(src), true
	}
	doMB4CharCheck := !isUTF8MB4 && checkMB4Value
	// The first check.
	invalidPos := -1
	for i, w := 0, 0; i < len(src); i += w {
		var rv rune
		rv, w = utf8.DecodeRune(src[i:])
		if (rv == utf8.RuneError && w == 1) || (w > 3 && doMB4CharCheck) {
			invalidPos = i
			break
		}
	}
	if invalidPos == -1 {
		return src, len(src), true
	}
	// The second check replaces invalid characters.
	if len(dest) < len(src) {
		dest = make([]byte, 0, len(src))
	}
	dest = dest[:0]
	for i, w := 0, 0; i < len(src); i += w {
		var rv rune
		rv, w = utf8.DecodeRune(src[i:])
		if (rv == utf8.RuneError && w == 1) || (w > 3 && doMB4CharCheck) {
			dest = append(dest, '?')
			continue
		}
		dest = append(dest, src[i:i+w]...)
	}
	return dest, invalidPos, false
}

// ToUpper implements Encoding interface.
func (e *EncodingUTF8) ToUpper(src string) string {
	return strings.ToUpper(src)
}

// ToLower implements Encoding interface.
func (e *EncodingUTF8) ToLower(src string) string {
	return strings.ToLower(src)
}

// EncodingUTF8MB3 is the utf8 charset.
type EncodingUTF8MB3 struct {
	EncodingUTF8
}

// Name implements Encoding interface.
func (e *EncodingUTF8MB3) Name() string {
	return CharsetUTF8
}

// Validate implements Encoding interface.
func (e *EncodingUTF8MB3) Validate(dest, src []byte) ([]byte, int, bool) {
	return e.validateUTF8(dest, src, false, false)
}

// Decode implements Encoding interface.
func (e *EncodingUTF8MB3) Decode(dest, src []byte) ([]byte, int, error) {
	result, nSrc, ok := e.Validate(dest, src)
	if !ok {
		return result, nSrc, generateEncodingErr(e.Name(), src[nSrc:])
	}
	return src, len(src), nil
}

// DecodeString implements Encoding interface.
func (e *EncodingUTF8MB3) DecodeString(dest []byte, src string) (string, int, error) {
	srcBytes := Slice(src)
	result, nSrc, ok := e.Validate(dest, srcBytes)
	if !ok {
		return string(result), nSrc, generateEncodingErr(e.Name(), srcBytes[nSrc:])
	}
	return src, len(src), nil
}

// EncodingUTF8MB3Strict is the strict mode of EncodingUTF8MB3.
// MB4 characters are considered invalid.
type EncodingUTF8MB3Strict struct {
	EncodingUTF8MB3
}

// Validate implements Encoding interface.
func (e *EncodingUTF8MB3Strict) Validate(dest, src[] byte) ([]byte, int, bool) {
	return e.validateUTF8(dest, src, false, true)
}

// Decode implements Encoding interface.
func (e *EncodingUTF8MB3Strict) Decode(dest, src []byte) ([]byte, int, error) {
	result, nSrc, ok := e.Validate(dest, src)
	if !ok {
		return result, nSrc, generateEncodingErr(e.Name(), src[nSrc:])
	}
	return src, len(src), nil
}

// DecodeString implements Encoding interface.
func (e *EncodingUTF8MB3Strict) DecodeString(dest []byte, src string) (string, int, error) {
	srcBytes := Slice(src)
	result, nSrc, ok := e.Validate(dest, srcBytes)
	if !ok {
		return string(result), nSrc, generateEncodingErr(e.Name(), srcBytes[nSrc:])
	}
	return src, len(src), nil
}
