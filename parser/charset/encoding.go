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
	"reflect"
	"strings"
	"unicode"
	"unsafe"

	"github.com/cznic/mathutil"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"golang.org/x/text/encoding"
	"golang.org/x/text/transform"
)

var errInvalidCharacterString = terror.ClassParser.NewStd(mysql.ErrInvalidCharacterString)

type EncodingLabel string

// Format trim and change the label to lowercase.
func Format(label string) EncodingLabel {
	return EncodingLabel(strings.ToLower(strings.Trim(label, "\t\n\r\f ")))
}

// Formatted is used when the label is already trimmed and it is lowercase.
func Formatted(label string) EncodingLabel {
	return EncodingLabel(label)
}

// Encoding provide a interface to encode/decode a string with specific encoding.
type Encoding struct {
	enc         encoding.Encoding
	name        string
	charLength  func([]byte) int
	specialCase unicode.SpecialCase
}

// enabled indicates whether the non-utf8 encoding is used.
func (e *Encoding) enabled() bool {
	return e != UTF8Encoding
}

// Name returns the name of the current encoding.
func (e *Encoding) Name() string {
	return e.name
}

// CharLength returns the next character length in bytes.
func (e *Encoding) CharLength(bs []byte) int {
	return e.charLength(bs)
}

// NewEncoding creates a new Encoding.
func NewEncoding(label string) *Encoding {
	if len(label) == 0 {
		return UTF8Encoding
	}

	if e, exist := encodingMap[Format(label)]; exist {
		return e
	}
	return UTF8Encoding
}

// Encode convert bytes from utf-8 charset to a specific charset.
func (e *Encoding) Encode(dest, src []byte) ([]byte, error) {
	if !e.enabled() {
		return src, nil
	}
	return e.transform(e.enc.NewEncoder(), dest, src, false)
}

// EncodeString convert a string from utf-8 charset to a specific charset.
func (e *Encoding) EncodeString(src string) (string, error) {
	if !e.enabled() {
		return src, nil
	}
	bs, err := e.transform(e.enc.NewEncoder(), nil, Slice(src), false)
	return string(bs), err
}

// EncodeFirstChar convert first code point of bytes from utf-8 charset to a specific charset.
func (e *Encoding) EncodeFirstChar(dest, src []byte) ([]byte, error) {
	srcNextLen := e.nextCharLenInSrc(src, false)
	srcEnd := mathutil.Min(srcNextLen, len(src))
	if !e.enabled() {
		return src[:srcEnd], nil
	}
	return e.transform(e.enc.NewEncoder(), dest, src[:srcEnd], false)
}

// EncodeInternal convert bytes from utf-8 charset to a specific charset, we actually do not do the real convert, just find the inconvertible character and use ? replace.
// The code below is equivalent to
//		expr, _ := e.Encode(dest, src)
//		ret, _ := e.Decode(nil, expr)
//		return ret
func (e *Encoding) EncodeInternal(dest, src []byte) []byte {
	if !e.enabled() {
		return src
	}
	if dest == nil {
		dest = make([]byte, 0, len(src))
	}
	var srcOffset int

	var buf [4]byte
	transformer := e.enc.NewEncoder()
	for srcOffset < len(src) {
		length := UTF8Encoding.CharLength(src[srcOffset:])
		_, _, err := transformer.Transform(buf[:], src[srcOffset:srcOffset+length], true)
		if err != nil {
			dest = append(dest, byte('?'))
		} else {
			dest = append(dest, src[srcOffset:srcOffset+length]...)
		}
		srcOffset += length
	}

	return dest
}

// Decode convert bytes from a specific charset to utf-8 charset.
func (e *Encoding) Decode(dest, src []byte) ([]byte, error) {
	if !e.enabled() {
		return src, nil
	}
	return e.transform(e.enc.NewDecoder(), dest, src, true)
}

// DecodeString convert a string from a specific charset to utf-8 charset.
func (e *Encoding) DecodeString(src string) (string, error) {
	if !e.enabled() {
		return src, nil
	}
	bs, err := e.transform(e.enc.NewDecoder(), nil, Slice(src), true)
	return string(bs), err
}

func (e *Encoding) transform(transformer transform.Transformer, dest, src []byte, isDecoding bool) ([]byte, error) {
	if len(dest) < len(src) {
		dest = make([]byte, len(src)*2)
	}
	if len(src) == 0 {
		return src, nil
	}
	var destOffset, srcOffset int
	var encodingErr error
	for {
		srcNextLen := e.nextCharLenInSrc(src[srcOffset:], isDecoding)
		srcEnd := mathutil.Min(srcOffset+srcNextLen, len(src))
		nDest, nSrc, err := transformer.Transform(dest[destOffset:], src[srcOffset:srcEnd], false)
		if err == transform.ErrShortDst {
			dest = enlargeCapacity(dest)
		} else if err != nil || isDecoding && beginWithReplacementChar(dest[destOffset:destOffset+nDest]) {
			if encodingErr == nil {
				encodingErr = e.generateErr(src[srcOffset:], srcNextLen)
			}
			dest[destOffset] = byte('?')
			nDest, nSrc = 1, srcNextLen // skip the source bytes that cannot be decoded normally.
		}
		destOffset += nDest
		srcOffset += nSrc
		// The source bytes are exhausted.
		if srcOffset >= len(src) {
			return dest[:destOffset], encodingErr
		}
	}
}

func (e *Encoding) nextCharLenInSrc(srcRest []byte, isDecoding bool) int {
	if isDecoding {
		if e.charLength != nil {
			return e.charLength(srcRest)
		}
		return len(srcRest)
	}
	return UTF8Encoding.CharLength(srcRest)
}

func enlargeCapacity(dest []byte) []byte {
	newDest := make([]byte, len(dest)*2)
	copy(newDest, dest)
	return newDest
}

func (e *Encoding) generateErr(srcRest []byte, srcNextLen int) error {
	cutEnd := mathutil.Min(srcNextLen, len(srcRest))
	invalidBytes := fmt.Sprintf("%X", string(srcRest[:cutEnd]))
	return errInvalidCharacterString.GenWithStackByArgs(e.name, invalidBytes)
}

// replacementBytes are bytes for the replacement rune 0xfffd.
var replacementBytes = []byte{0xEF, 0xBF, 0xBD}

// beginWithReplacementChar check if dst has the prefix '0xEFBFBD'.
func beginWithReplacementChar(dst []byte) bool {
	return bytes.HasPrefix(dst, replacementBytes)
}

// Slice converts string to slice without copy.
// Use at your own risk.
func Slice(s string) (b []byte) {
	pBytes := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	pString := (*reflect.StringHeader)(unsafe.Pointer(&s))
	pBytes.Data = pString.Data
	pBytes.Len = pString.Len
	pBytes.Cap = pString.Len
	return
}
