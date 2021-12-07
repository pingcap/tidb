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

	"golang.org/x/text/encoding"
)

// EncodingBinImpl is the instance of EncodingBin.
var EncodingBinImpl = &EncodingBin {
	EncodingBase{enc: encoding.Nop},
}

// EncodingBin is the binary encoding.
type EncodingBin struct {
	EncodingBase
}

// ToUpper implements Encoding interface.
func (e *EncodingBin) ToUpper(src string) string {
	return strings.ToUpper(src)
}

// ToLower implements Encoding interface.
func (e *EncodingBin) ToLower(src string) string {
	return strings.ToLower(src)
}

// Name implements Encoding interface.
func (e *EncodingBin) Name() string {
	return CharsetBin
}

// Peek implements Encoding interface.
func (e *EncodingBin) Peek(src []byte) []byte {
	if len(src) == 0 {
		return src
	}
	return src[:1]
}

// Validate implements Encoding interface.
func (e *EncodingBin) Validate(_, src []byte) (result []byte, nSrc int, ok bool) {
	return src, len(src), true
}

// Encode implements Encoding interface.
func (e *EncodingBin) Encode(_, src []byte) (result []byte, nSrc int, err error) {
	return src, len(src), nil
}

// EncodeString implements Encoding interface.
func (e *EncodingBin) EncodeString(_ []byte, src string) (result string, nSrc int, err error) {
	return src, len(src), nil
}

// Decode implements Encoding interface.
func (e *EncodingBin) Decode(_, src []byte) (result []byte, nSrc int, err error) {
	return src, len(src), nil
}

// DecodeString implements Encoding interface.
func (e *EncodingBin) DecodeString(_ []byte, src string) (result string, nSrc int, err error) {
	return src, len(src), nil
}
