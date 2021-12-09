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

// Make sure all of them implement Encoding interface.
var (
	_ Encoding = &EncodingUTF8{}
	_ Encoding = &EncodingUTF8MB3Strict{}
	_ Encoding = &EncodingASCII{}
	_ Encoding = &EncodingLatin1{}
	_ Encoding = &EncodingBin{}
	_ Encoding = &EncodingGBK{}
)

// Encoding provide encode/decode functions for a string with a specific charset.
type Encoding interface {
	// Name is the name of the encoding.
	Name() string
	// Peek returns a next char byte slice.
	Peek(src []byte) []byte
	Foreach(src []byte, op Op, fn func(from, to []byte, ok bool) bool)
	Transform(dest, src []byte, op Op, opt TruncateOpt, cOpt CollectOpt) ([]byte, error)
	ToUpper(src string) string
	ToLower(src string) string
}

type Op bool

const (
	OpFromUTF8 Op = false
	OpToUTF8   Op = true
)

type TruncateOpt int8

const (
	TruncateOptTrim TruncateOpt = iota
	TruncateOptReplace
)

type CollectOpt int8

const (
	CollectOptFrom CollectOpt = iota
	CollectOptTo
)

// IsSupportedEncoding checks if the charset is fully supported.
func IsSupportedEncoding(charset string) bool {
	_, ok := encodingMap[charset]
	return ok
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

var encodingMap = map[string]Encoding{
	CharsetUTF8MB4: EncodingUTF8Impl,
	CharsetUTF8:    EncodingUTF8Impl,
	CharsetGBK:     EncodingGBKImpl,
	CharsetLatin1:  EncodingLatin1Impl,
	CharsetBin:     EncodingBinImpl,
	CharsetASCII:   EncodingASCIIImpl,
}

func IsValid(e Encoding, src []byte) bool {
	isValid := true
	e.Foreach(src, OpFromUTF8, func(from, to []byte, ok bool) bool {
		isValid = ok
		return ok
	})
	return isValid
}

func IsValidString(e Encoding, str string) bool {
	return IsValid(e, Slice(str))
}

func ReplaceIllegal(e Encoding, src []byte) []byte {
	ret, _ := e.Transform(nil, src, OpFromUTF8, TruncateOptReplace, CollectOptFrom)
	return ret
}

func ReplaceIllegalBuf(e Encoding, src, buf []byte) []byte {
	ret, _ := e.Transform(buf, src, OpFromUTF8, TruncateOptReplace, CollectOptFrom)
	return ret
}

func ReplaceIllegalString(e Encoding, str string) string {
	return string(ReplaceIllegal(e, Slice(str)))
}

func FromUTF8(e Encoding, src []byte) ([]byte, error) {
	return e.Transform(nil, src, OpFromUTF8, TruncateOptTrim, CollectOptTo)
}

func FromUTF8Buf(e Encoding, src, buf []byte) ([]byte, error) {
	return e.Transform(buf, src, OpFromUTF8, TruncateOptTrim, CollectOptTo)
}

func FromUTF8String(e Encoding, str string) (string, error) {
	bs, err := FromUTF8(e, Slice(str))
	return string(bs), err
}

func FromUTF8Replace(e Encoding, src []byte) ([]byte, error) {
	return e.Transform(nil, src, OpFromUTF8, TruncateOptReplace, CollectOptTo)
}

func FromUTF8CountValidBytes(e Encoding, src []byte) int {
	nSrc := 0
	e.Foreach(src, OpFromUTF8, func(from, to []byte, ok bool) bool {
		if ok {
			nSrc += len(from)
		}
		return ok
	})
	return nSrc
}

func FromUTF8ReplaceBuf(e Encoding, src, buf []byte) ([]byte, error) {
	return e.Transform(buf, src, OpFromUTF8, TruncateOptReplace, CollectOptTo)
}

func FromUTF8StringReplace(e Encoding, str string) (string, error) {
	bs, err := FromUTF8Replace(e, Slice(str))
	return string(bs), err
}

func ToUTF8(e Encoding, src []byte) ([]byte, error) {
	return e.Transform(nil, src, OpToUTF8, TruncateOptTrim, CollectOptTo)
}

func ToUTF8Buf(e Encoding, src, buf []byte) ([]byte, error) {
	return e.Transform(buf, src, OpToUTF8, TruncateOptTrim, CollectOptTo)
}

func ToUTF8String(e Encoding, str string) (string, error) {
	bs, err := ToUTF8(e, Slice(str))
	return string(bs), err
}

func ToUTF8Replace(e Encoding, src []byte) ([]byte, error) {
	return e.Transform(nil, src, OpToUTF8, TruncateOptReplace, CollectOptTo)
}

func ToUTF8ReplaceBuf(e Encoding, src, buf []byte) ([]byte, error) {
	return e.Transform(buf, src, OpToUTF8, TruncateOptReplace, CollectOptTo)
}

func ToUTF8StringReplace(e Encoding, str string) (string, error) {
	bs, err := ToUTF8Replace(e, Slice(str))
	return string(bs), err
}

func ToUTF8CountValidBytes(e Encoding, src []byte) int {
	nSrc := 0
	e.Foreach(src, OpToUTF8, func(from, to []byte, ok bool) bool {
		if ok {
			nSrc += len(from)
		}
		return ok
	})
	return nSrc
}
