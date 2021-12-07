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
	"reflect"
	"unsafe"
)

// Make sure all of them implement Encoding interface.
var (
	_ Encoding = &EncodingUTF8{}
	_ Encoding = &EncodingASCII{}
	_ Encoding = &EncodingLatin1{}
	_ Encoding = &EncodingLatin1Legacy{}
	_ Encoding = &EncodingBin{}
	_ Encoding = &EncodingGBK{}
)

// Encoding provide encode/decode functions for a string with a specific charset.
type Encoding interface {
	EncodingOperation
	// Name is the name of the encoding.
	Name() string
	// Peek returns a next char byte slice.
	Peek(src []byte) []byte
	// Validate checks whether a utf-8 string is valid in current charset.
	// It returns src if the string is valid, or replace the invalid character with '?' in utf-8.
	Validate(dest, src []byte) (result []byte, nSrc int, ok bool)
	// Encode converts bytes from utf-8 charset to current charset.
	Encode(dest, src[]byte) (result []byte, nSrc int, err error)
	// EncodeString converts a string from utf-8 charset to current charset.
	EncodeString(dest []byte, src string) (result string, nSrc int, err error)
	// Decode converts bytes from current charset to utf-8 charset.
	Decode(dest, src[]byte) (result []byte, nSrc int, err error)
	// DecodeString converts a string from current charset to utf-8 charset.
	DecodeString(dest []byte, src string) (result string, nSrc int, err error)
}

type EncodingOperation interface {
	ToUpper(src string) string
	ToLower(src string) string
}

// FindEncoding finds the encoding according to charset.
func FindEncoding(charset string) Encoding {
	if len(charset) == 0 {
		return EncodingUTF8Impl
	}
	if e, exist := encodingMap[charset]; exist {
		return e
	}
	return EncodingUTF8Impl
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
