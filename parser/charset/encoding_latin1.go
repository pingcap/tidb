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
	"golang.org/x/text/encoding"
)

// EncodingLatin1Impl is the instance of encodingLatin1.
// TiDB uses utf8 implementation for latin1 charset because of the backward compatibility.
var EncodingLatin1Impl = &encodingLatin1{encodingUTF8{encodingBase{enc: encoding.Nop}}}

func init() {
	EncodingLatin1Impl.self = EncodingLatin1Impl
}

// encodingLatin1 compatibles with latin1 in old version TiDB.
type encodingLatin1 struct {
	encodingUTF8
}

// Name implements Encoding interface.
func (e *encodingLatin1) Name() string {
	return CharsetLatin1
}

// Peek implements Encoding interface.
func (e *encodingLatin1) Peek(src []byte) []byte {
	if len(src) == 0 {
		return src
	}
	return src[:1]
}

// IsValid implements Encoding interface.
func (e *encodingLatin1) IsValid(src []byte) bool {
	return true
}

// Tp implements Encoding interface.
func (e *encodingLatin1) Tp() EncodingTp {
	return EncodingTpLatin1
}

func (e *encodingLatin1) Transform(dest *bytes.Buffer, src []byte, op Op) ([]byte, error) {
	return src, nil
}
