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

import "bytes"

// Make sure all of them implement Encoding interface.
var (
	_ Encoding = &encodingUTF8{}
	_ Encoding = &encodingUTF8MB3Strict{}
	_ Encoding = &encodingASCII{}
	_ Encoding = &encodingLatin1{}
	_ Encoding = &encodingBin{}
	_ Encoding = &encodingGBK{}
	_ Encoding = &encodingGB18030{}
)

// IsSupportedEncoding checks if the charset is fully supported.
func IsSupportedEncoding(charset string) bool {
	_, ok := encodingMap[charset]
	return ok
}

// FindEncodingTakeUTF8AsNoop finds the encoding according to the charset
// except that utf-8 is treated as no-operation encoding. This is used to
// reduce the overhead of utf-8 validation in some cases.
func FindEncodingTakeUTF8AsNoop(charset string) Encoding {
	enc := FindEncoding(charset)
	if enc.Tp() == EncodingTpUTF8 {
		return EncodingBinImpl
	}
	return enc
}

// FindEncoding finds the encoding according to charset.
func FindEncoding(charset string) Encoding {
	if len(charset) == 0 {
		return EncodingBinImpl
	}
	if e, exist := encodingMap[charset]; exist {
		return e
	}
	return EncodingBinImpl
}

var encodingMap = map[string]Encoding{
	CharsetUTF8MB4: EncodingUTF8Impl,
	CharsetUTF8:    EncodingUTF8Impl,
	CharsetGBK:     EncodingGBKImpl,
	CharsetLatin1:  EncodingLatin1Impl,
	CharsetBin:     EncodingBinImpl,
	CharsetASCII:   EncodingASCIIImpl,
	CharsetGB18030: EncodingGB18030Impl,
}

// Encoding provide encode/decode functions for a string with a specific charset.
type Encoding interface {
	// Name is the name of the encoding.
	Name() string
	// Tp is the type of the encoding.
	Tp() EncodingTp
	// Peek returns the next char.
	Peek(src []byte) []byte
	// MbLen returns multiple byte length, if the next character is single byte, return 0.
	MbLen(string) int
	// IsValid checks whether the utf-8 bytes can be convert to valid string in current encoding.
	IsValid(src []byte) bool
	// Foreach iterates the characters in current encoding.
	Foreach(src []byte, op Op, fn func(from, to []byte, ok bool) bool)
	// Transform map the bytes in src to dest according to Op.
	// **the caller should initialize the dest if it wants to avoid memory alloc every time,
	//   or else it will always make a new one**
	//
	// **the returned array may be the alias of `src`, edit the returned array on your own risk**
	Transform(dest *bytes.Buffer, src []byte, op Op) ([]byte, error)
	// ToUpper change a string to uppercase.
	ToUpper(src string) string
	// ToLower change a string to lowercase.
	ToLower(src string) string
}

// EncodingTp is the type of the encoding.
type EncodingTp int8

//revive:disable
const (
	EncodingTpNone EncodingTp = iota
	EncodingTpUTF8
	EncodingTpUTF8MB3Strict
	EncodingTpASCII
	EncodingTpLatin1
	EncodingTpBin
	EncodingTpGBK
	EncodingTpGB18030
)

//revive:enable

// Op is used by Encoding.Transform.
type Op int16

const (
	opFromUTF8 Op = 1 << iota
	opToUTF8
	opTruncateTrim
	opTruncateReplace
	opCollectFrom
	opCollectTo
	opSkipError
)

//revive:disable
const (
	// OpReplaceNoErr is used to replace invalid bytes with '?'.
	OpReplaceNoErr  = opFromUTF8 | opTruncateReplace | opCollectFrom | opSkipError
	OpReplace       = opFromUTF8 | opTruncateReplace | opCollectFrom
	OpEncode        = opFromUTF8 | opTruncateTrim | opCollectTo
	OpEncodeNoErr   = OpEncode | opSkipError
	OpEncodeReplace = opFromUTF8 | opTruncateReplace | opCollectTo
	OpDecode        = opToUTF8 | opTruncateTrim | opCollectTo
	OpDecodeNoErr   = OpDecode | opSkipError
	OpDecodeReplace = opToUTF8 | opTruncateReplace | opCollectTo
)

//revive:enable

// CountValidBytes counts the first valid bytes in src that
// can be encoded to the current encoding.
func CountValidBytes(e Encoding, src []byte) int {
	nSrc := 0
	e.Foreach(src, opFromUTF8, func(from, _ []byte, ok bool) bool {
		if ok {
			nSrc += len(from)
		}
		return ok
	})
	return nSrc
}

// CountValidBytesDecode counts the first valid bytes in src that
// can be decoded to utf-8.
func CountValidBytesDecode(e Encoding, src []byte) int {
	nSrc := 0
	e.Foreach(src, opToUTF8, func(from, _ []byte, ok bool) bool {
		if ok {
			nSrc += len(from)
		}
		return ok
	})
	return nSrc
}
