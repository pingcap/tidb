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
	"fmt"
	"reflect"
	"strings"
	"unsafe"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"golang.org/x/text/encoding"
	"golang.org/x/text/transform"
)

var errInvalidCharacterString = terror.ClassParser.NewStd(mysql.ErrInvalidCharacterString)

// EncodingBase defines some generic functions.
type EncodingBase struct {
	enc  encoding.Encoding
	self Encoding
}

func (b EncodingBase) ToUpper(src string) string {
	return strings.ToUpper(src)
}

func (b EncodingBase) ToLower(src string) string {
	return strings.ToLower(src)
}

func (b EncodingBase) Transform(dest, src []byte, op Op) (result []byte, err error) {
	if dest == nil {
		dest = make([]byte, len(src))
	}
	dest = dest[:0]
	b.self.Foreach(src, op, func(from, to []byte, ok bool) bool {
		if !ok {
			if err == nil && (op&OpSkipError == 0) {
				err = generateEncodingErr(b.self.Name(), from)
			}
			if op&OpTruncateTrim != 0 {
				return false
			}
			if op&OpTruncateReplace != 0 {
				dest = append(dest, '?')
				return true
			}
		}
		if op&OpCollectFrom != 0 {
			dest = append(dest, from...)
		} else if op&OpCollectTo != 0 {
			dest = append(dest, to...)
		}
		return true
	})
	return dest, err
}

func (b EncodingBase) Foreach(src []byte, op Op, fn func(from, to []byte, ok bool) bool) {
	var tfm transform.Transformer
	var peek func([]byte) []byte
	if op&OpFromUTF8 != 0 {
		tfm = b.enc.NewEncoder()
		peek = EncodingUTF8Impl.Peek
	} else {
		tfm = b.enc.NewDecoder()
		peek = b.self.Peek
	}
	var buf [4]byte
	for i, w := 0, 0; i < len(src); i += w {
		w = len(peek(src[i:]))
		nDst, _, err := tfm.Transform(buf[:], src[i:i+w], false)
		meetErr := err != nil || (op&OpToUTF8 != 0 && beginWithReplacementChar(buf[:nDst]))
		if !fn(src[i:i+w], buf[:nDst], !meetErr) {
			return
		}
	}
}

// replacementBytes are bytes for the replacement rune 0xfffd.
var replacementBytes = []byte{0xEF, 0xBF, 0xBD}

// beginWithReplacementChar check if dst has the prefix '0xEFBFBD'.
func beginWithReplacementChar(dst []byte) bool {
	return bytes.HasPrefix(dst, replacementBytes)
}

// generateEncodingErr generates an invalid string in charset error.
func generateEncodingErr(name string, invalidBytes []byte) error {
	arg := fmt.Sprintf("%X", invalidBytes)
	return errInvalidCharacterString.FastGenByArgs(name, arg)
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
