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

// EncodingBinImpl is the instance of encodingBin.
var EncodingBinImpl = &encodingBin{encodingBase{enc: encoding.Nop}}

func init() {
	EncodingBinImpl.self = EncodingBinImpl
}

// encodingBin is the binary encoding.
type encodingBin struct {
	encodingBase
}

// Name implements Encoding interface.
func (*encodingBin) Name() string {
	return CharsetBin
}

// Tp implements Encoding interface.
func (*encodingBin) Tp() EncodingTp {
	return EncodingTpBin
}

// Peek implements Encoding interface.
func (*encodingBin) Peek(src []byte) []byte {
	if len(src) == 0 {
		return src
	}
	return src[:1]
}

// IsValid implements Encoding interface.
func (*encodingBin) IsValid(_ []byte) bool {
	return true
}

// Foreach implements Encoding interface.
func (*encodingBin) Foreach(src []byte, _ Op, fn func(from, to []byte, ok bool) bool) {
	for i := 0; i < len(src); i++ {
		if !fn(src[i:i+1], src[i:i+1], true) {
			return
		}
	}
}

func (*encodingBin) Transform(_ *bytes.Buffer, src []byte, _ Op) ([]byte, error) {
	return src, nil
}
