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

	"golang.org/x/text/encoding/charmap"
)

// EncodingLatin1Impl is the instance of EncodingLatin1.
var EncodingLatin1Impl = &EncodingLatin1 {
	EncodingBase{enc: charmap.Windows1252},
}

// EncodingLatin1LegacyImpl is the instance of EncodingLatin1Legacy.
// TiDB uses this implementation for latin1 charset because of the backward compatibility.
var EncodingLatin1LegacyImpl Encoding = &EncodingLatin1Legacy{}

type EncodingLatin1 struct {
	EncodingBase
}

// ToUpper implements Encoding interface.
func (e *EncodingLatin1) ToUpper(src string) string {
	return strings.ToUpper(src)
}

// ToLower implements Encoding interface.
func (e *EncodingLatin1) ToLower(src string) string {
	return strings.ToLower(src)
}

// Name implements Encoding interface.
func (e *EncodingLatin1) Name() string {
	return CharsetLatin1
}

// Peek implements Encoding interface.
func (e *EncodingLatin1) Peek(src []byte) []byte {
	if len(src) == 0 {
		return src
	}
	return src[:1]
}

// Validate implements Encoding interface.
func (e *EncodingLatin1) Validate(dest, src []byte) (result []byte, nSrc int, ok bool) {
	return e.EncodingBase.Validate(dest, src)
}

// Encode implements Encoding interface.
func (e *EncodingLatin1) Encode(dest, src []byte) (result []byte, nSrc int, err error) {
	return e.transform(e.enc.NewEncoder(), dest, src, encodingUTF8Peek, e.Name())
}

// EncodeString implements Encoding interface.
func (e *EncodingLatin1) EncodeString(dest []byte, src string) (result string, nSrc int, err error) {
	var r []byte
	r, nSrc, err = e.transform(e.enc.NewEncoder(), dest, Slice(src), encodingUTF8Peek, e.Name())
	return string(r), nSrc, err
}

// Decode implements Encoding interface.
func (e *EncodingLatin1) Decode(dest, src []byte) (result []byte, nSrc int, err error) {
	return e.transform(e.enc.NewDecoder(), dest, src, e.Peek, e.Name())
}

// DecodeString implements Encoding interface.
func (e *EncodingLatin1) DecodeString(dest []byte, src string) (result string, nSrc int, err error) {
	var r []byte
	r, nSrc, err = e.transform(e.enc.NewDecoder(), dest, Slice(src), e.Peek, e.Name())
	return string(r), nSrc, err
}

// EncodingLatin1Legacy compatibles with latin1 in old version TiDB.
type EncodingLatin1Legacy struct {
	EncodingUTF8
}

// Name implements Encoding interface.
func (e *EncodingLatin1Legacy) Name() string {
	return CharsetLatin1
}

// Peek implements Encoding interface.
func (e *EncodingLatin1Legacy) Peek(src []byte) []byte {
	if len(src) == 0 {
		return src
	}
	return src[:1]
}

// Decode implements Encoding interface.
func (e *EncodingLatin1Legacy) Decode(_, src []byte) (result []byte, nSrc int, err error) {
	return src, len(src), nil
}

// DecodeString implements Encoding interface.
func (e *EncodingLatin1Legacy) DecodeString(_ []byte, src string) (result string, nSrc int, err error) {
	return src, len(src), nil
}
