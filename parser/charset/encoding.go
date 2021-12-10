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

// Encoding provide encode/decode functions for a string with a specific charset.
type Encoding interface {
	// Name is the name of the encoding.
	Name() string
	// Peek returns the next char.
	Peek(src []byte) []byte
	// Foreach iterates the characters in in current encoding.
	Foreach(src []byte, op Op, fn func(from, to []byte, ok bool) bool)
	// Transform map the bytes in src to dest according to Op.
	Transform(dest, src []byte, op Op) ([]byte, error)
	// ToUpper change a string to uppercase.
	ToUpper(src string) string
	// ToLower change a string to lowercase.
	ToLower(src string) string
}

// Op is used by Encoding.Transform.
type Op int16

const (
	OpFromUTF8 Op = iota << 1
	OpToUTF8
	OpTruncateTrim
	OpTruncateReplace
	OpCollectFrom
	OpCollectTo
	OpSkipError
)

const (
	OpReplace       = OpFromUTF8 | OpTruncateReplace | OpCollectFrom | OpSkipError
	OpEncode        = OpFromUTF8 | OpTruncateTrim | OpCollectTo
	OpEncodeReplace = OpFromUTF8 | OpTruncateReplace | OpCollectTo
	OpDecode        = OpToUTF8 | OpTruncateTrim | OpCollectTo
	OpDecodeReplace = OpToUTF8 | OpTruncateReplace | OpCollectTo
)

// IsValid checks whether the bytes is valid in current encoding.
func IsValid(e Encoding, src []byte) bool {
	isValid := true
	e.Foreach(src, OpFromUTF8, func(from, to []byte, ok bool) bool {
		isValid = ok
		return ok
	})
	return isValid
}

// IsValidString is a string version of IsValid.
func IsValidString(e Encoding, str string) bool {
	return IsValid(e, Slice(str))
}

// CountValidBytes counts the first valid bytes in src that
// can be encode to the current encoding.
func CountValidBytes(e Encoding, src []byte) int {
	nSrc := 0
	e.Foreach(src, OpFromUTF8, func(from, to []byte, ok bool) bool {
		if ok {
			nSrc += len(from)
		}
		return ok
	})
	return nSrc
}

// CountValidBytesDecode counts the first valid bytes in src that
// can be decode to utf-8.
func CountValidBytesDecode(e Encoding, src []byte) int {
	nSrc := 0
	e.Foreach(src, OpToUTF8, func(from, to []byte, ok bool) bool {
		if ok {
			nSrc += len(from)
		}
		return ok
	})
	return nSrc
}
