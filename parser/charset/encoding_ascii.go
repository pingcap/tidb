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
	go_unicode "unicode"

	"golang.org/x/text/encoding"
)

// EncodingASCIIImpl is the instance of EncodingASCII.
var EncodingASCIIImpl = &EncodingASCII{
	EncodingBase{enc: encoding.Nop},
}

// EncodingASCII is the ASCII encoding.
type EncodingASCII struct {
	EncodingBase
}

// ToUpper implements Encoding interface.
func (e *EncodingASCII) ToUpper(src string) string {
	return strings.ToUpper(src)
}

// ToLower implements Encoding interface.
func (e *EncodingASCII) ToLower(src string) string {
	return strings.ToLower(src)
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

// Validate implements Encoding interface.
func (e *EncodingASCII) Validate(src []byte) (nSrc int, ok bool) {
	for i := 0; i < len(src); i++ {
		if src[i] > go_unicode.MaxASCII {
			return i, false
		}
	}
	return len(src), true
}

// ReplaceIllegal implements Encoding interface.
func (e *EncodingASCII) ReplaceIllegal(dest, src []byte) (result []byte) {
	if len(dest) < len(src) {
		dest = make([]byte, 0, len(src))
	}
	dest = dest[:0]
	for i, w := 0, 0; i < len(src); i += w {
		w = 1
		if src[i] > go_unicode.MaxASCII {
			w = len(EncodingUTF8Impl.Peek(src[i:]))
			dest = append(dest, '?')
			continue
		}
		dest = append(dest, src[i:i+w]...)
	}
	return dest
}

// Encode implements Encoding interface.
func (e *EncodingASCII) Encode(dest, src []byte) (result []byte, nSrc int, err error) {
	return e.transform(e.enc.NewEncoder(), dest, src, encodingUTF8Peek, e.Name())
}

// EncodeString implements Encoding interface.
func (e *EncodingASCII) EncodeString(dest []byte, src string) (result string, nSrc int, err error) {
	var r []byte
	r, nSrc, err = e.transform(e.enc.NewEncoder(), dest, Slice(src), encodingUTF8Peek, e.Name())
	return string(r), nSrc, err
}

// Decode implements Encoding interface.
func (e *EncodingASCII) Decode(dest, src []byte) ([]byte, int, error) {
	nSrc, ok := e.Validate(src)
	var err error
	if !ok {
		ret := e.ReplaceIllegal(dest, src)
		err = generateEncodingErr(e.Name(), src[nSrc:])
		return ret, nSrc, err
	}
	return src, len(src), nil
}

// DecodeString implements Encoding interface.
func (e *EncodingASCII) DecodeString(dest []byte, src string) (string, int, error) {
	srcBytes := Slice(src)
	nSrc, ok := e.Validate(srcBytes)
	var err error
	if !ok {
		ret := e.ReplaceIllegal(dest, srcBytes)
		err = generateEncodingErr(e.Name(), srcBytes[nSrc:])
		return string(ret), nSrc, err
	}
	return src, len(src), nil
}
