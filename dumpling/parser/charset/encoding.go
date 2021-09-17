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
	"strings"

	"github.com/cznic/mathutil"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"golang.org/x/text/encoding"
	"golang.org/x/text/transform"
)

const encodingLegacy = "utf-8" // utf-8 encoding is compatible with old default behavior.

var errInvalidCharacterString = terror.ClassParser.NewStd(mysql.ErrInvalidCharacterString)

type EncodingLabel string

// Format trim and change the label to lowercase.
func Format(label string) EncodingLabel {
	return EncodingLabel(strings.ToLower(strings.Trim(label, "\t\n\r\f ")))
}

// Formatted is used when the label is already trimmed and it is lowercase.
func Formatted(label string) EncodingLabel {
	return EncodingLabel(label)
}

// Encoding provide a interface to encode/decode a string with specific encoding.
type Encoding struct {
	enc        encoding.Encoding
	name       string
	charLength func([]byte) int
}

// Enabled indicates whether the non-utf8 encoding is used.
func (e *Encoding) Enabled() bool {
	return e.enc != nil && e.charLength != nil
}

// Name returns the name of the current encoding.
func (e *Encoding) Name() string {
	return e.name
}

// NewEncoding creates a new Encoding.
func NewEncoding(label string) *Encoding {
	if len(label) == 0 {
		return &Encoding{}
	}
	e, name := Lookup(label)
	if e != nil && name != encodingLegacy {
		return &Encoding{
			enc:        e,
			name:       name,
			charLength: FindNextCharacterLength(name),
		}
	}
	return &Encoding{name: name}
}

// UpdateEncoding updates to a new Encoding.
func (e *Encoding) UpdateEncoding(label EncodingLabel) {
	enc, name := lookup(label)
	e.name = name
	if enc != nil && name != encodingLegacy {
		e.enc = enc
		e.charLength = FindNextCharacterLength(name)
	} else {
		e.enc = nil
		e.charLength = nil
	}
}

// Encode convert bytes from utf-8 charset to a specific charset.
func (e *Encoding) Encode(dest, src []byte) ([]byte, error) {
	return e.transform(e.enc.NewEncoder(), dest, src, false)
}

// Decode convert bytes from a specific charset to utf-8 charset.
func (e *Encoding) Decode(dest, src []byte) ([]byte, error) {
	return e.transform(e.enc.NewDecoder(), dest, src, true)
}

func (e *Encoding) transform(transformer transform.Transformer, dest, src []byte, isDecoding bool) ([]byte, error) {
	if len(dest) < len(src) {
		dest = make([]byte, len(src)*2)
	}
	var destOffset, srcOffset int
	var encodingErr error
	for {
		srcNextLen := e.nextCharLenInSrc(src[srcOffset:], isDecoding)
		srcEnd := mathutil.Min(srcOffset+srcNextLen, len(src))
		nDest, nSrc, err := transformer.Transform(dest[destOffset:], src[srcOffset:srcEnd], false)
		if err == transform.ErrShortDst {
			dest = enlargeCapacity(dest)
		} else if err != nil || isDecoding && beginWithReplacementChar(dest[destOffset:destOffset+nDest]) {
			if encodingErr == nil {
				encodingErr = e.generateErr(src[srcOffset:], srcNextLen)
			}
			dest[destOffset] = byte('?')
			nDest, nSrc = 1, srcNextLen // skip the source bytes that cannot be decoded normally.
		}
		destOffset += nDest
		srcOffset += nSrc
		// The source bytes are exhausted.
		if srcOffset >= len(src) {
			return dest[:destOffset], encodingErr
		}
	}
}

func (e *Encoding) nextCharLenInSrc(srcRest []byte, isDecoding bool) int {
	if isDecoding && e.charLength != nil {
		return e.charLength(srcRest)
	}
	return len(srcRest)
}

func enlargeCapacity(dest []byte) []byte {
	newDest := make([]byte, len(dest)*2)
	copy(newDest, dest)
	return newDest
}

func (e *Encoding) generateErr(srcRest []byte, srcNextLen int) error {
	cutEnd := mathutil.Min(srcNextLen, len(srcRest))
	invalidBytes := fmt.Sprintf("%X", string(srcRest[:cutEnd]))
	return errInvalidCharacterString.GenWithStackByArgs(e.name, invalidBytes)
}

// replacementBytes are bytes for the replacement rune 0xfffd.
var replacementBytes = []byte{0xEF, 0xBF, 0xBD}

// beginWithReplacementChar check if dst has the prefix '0xEFBFBD'.
func beginWithReplacementChar(dst []byte) bool {
	return bytes.HasPrefix(dst, replacementBytes)
}
