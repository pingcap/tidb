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

// EncodingLatin1Impl is the instance of EncodingLatin1.
// TiDB uses utf8 implementation for latin1 charset because of the backward compatibility.
var EncodingLatin1Impl Encoding = &EncodingLatin1{}

// EncodingLatin1 compatibles with latin1 in old version TiDB.
type EncodingLatin1 struct {
	EncodingUTF8
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

// ReplaceIllegal implements Encoding interface.
func (e *EncodingLatin1) ReplaceIllegal(_, src []byte) []byte {
	return src
}

// Decode implements Encoding interface.
func (e *EncodingLatin1) Decode(_, src []byte) (result []byte, nSrc int, err error) {
	return src, len(src), nil
}

// DecodeString implements Encoding interface.
func (e *EncodingLatin1) DecodeString(_ []byte, src string) (result string, nSrc int, err error) {
	return src, len(src), nil
}
