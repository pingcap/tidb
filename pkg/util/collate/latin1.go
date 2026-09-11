// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collate

import (
	"unicode/utf8"

	"github.com/pingcap/tidb/pkg/util/stringutil"
	"golang.org/x/text/encoding/charmap"
)

// invalidCharWeight is the weight assigned to characters that cannot be encoded
// in Latin-1. It corresponds to the '?' character.
//
// TiDB treated [Latin-1 charset as the same as UTF-8][18955] for compatibility
// with mistakes back in v4.0 days, when "new collation" was not a thing and
// everything is assumed compatible with `utf8mb4`. This means `_latin1'中文'` is
// and will forever be legal, even if it makes no sense in MySQL proper.
//
// Here in this collator, we are going to treat `_latin1'X'` to be the same as
// `CONVERT(_utf8mb4'X' USING latin1)`. So all non-Windows-1252 characters will
// be mapped to '?' during comparison.
//
// [18955]: https://github.com/pingcap/tidb/issues/18955
const invalidCharWeight = 0x3f

type latin1ByteWeightTable [256]uint8

func (wt *latin1ByteWeightTable) sortKey(r rune) byte {
	if b, ok := charmap.Windows1252.EncodeRune(r); ok {
		return wt[b]
	}
	return invalidCharWeight
}

type latin1Collator struct {
	weights *latin1ByteWeightTable
}

// latin1SwedishCIByteWeightTable is the weight table for latin1_swedish_ci.
//
// It performs mappings so that all valid ISO-8859-1 letters that only differ by
// case are equal (e.g. 'x'='X', 'ø'='Ø'), and follows Swedish ordering rules
// i.e. 'A' < ... < 'Z' < 'Å' < 'Ä' < 'Ö', and the other accented letters are
// treated as the same as their base letters (e.g. 'É'='E'), and additionally
// 'Æ'='Ä' and 'Ü'='Y'.
//
// Valid Windows-1252 but invalid ISO-8859-1 characters ('œ', 'š', 'ž', 'Ÿ')
// are considered not coalescable, following the behavior in MySQL v8.4.
var latin1SwedishCIByteWeightTable = latin1ByteWeightTable{
	0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F,
	0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F,
	0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2A, 0x2B, 0x2C, 0x2D, 0x2E, 0x2F,
	0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3A, 0x3B, 0x3C, 0x3D, 0x3E, 0x3F,
	0x40, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x49, 0x4A, 0x4B, 0x4C, 0x4D, 0x4E, 0x4F,
	0x50, 0x51, 0x52, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58, 0x59, 0x5A, 0x5B, 0x5C, 0x5D, 0x5E, 0x5F,
	0x60, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x49, 0x4A, 0x4B, 0x4C, 0x4D, 0x4E, 0x4F,
	0x50, 0x51, 0x52, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58, 0x59, 0x5A, 0x7B, 0x7C, 0x7D, 0x7E, 0x7F,
	0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8A, 0x8B, 0x8C, 0x8D, 0x8E, 0x8F,
	0x90, 0x91, 0x92, 0x93, 0x94, 0x95, 0x96, 0x97, 0x98, 0x99, 0x9A, 0x9B, 0x9C, 0x9D, 0x9E, 0x9F,
	0xA0, 0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7, 0xA8, 0xA9, 0xAA, 0xAB, 0xAC, 0xAD, 0xAE, 0xAF,
	0xB0, 0xB1, 0xB2, 0xB3, 0xB4, 0xB5, 0xB6, 0xB7, 0xB8, 0xB9, 0xBA, 0xBB, 0xBC, 0xBD, 0xBE, 0xBF,
	0x41, 0x41, 0x41, 0x41, 0x5C, 0x5B, 0x5C, 0x43, 0x45, 0x45, 0x45, 0x45, 0x49, 0x49, 0x49, 0x49,
	0x44, 0x4E, 0x4F, 0x4F, 0x4F, 0x4F, 0x5D, 0xD7, 0xD8, 0x55, 0x55, 0x55, 0x59, 0x59, 0xDE, 0xDF,
	0x41, 0x41, 0x41, 0x41, 0x5C, 0x5B, 0x5C, 0x43, 0x45, 0x45, 0x45, 0x45, 0x49, 0x49, 0x49, 0x49,
	0x44, 0x4E, 0x4F, 0x4F, 0x4F, 0x4F, 0x5D, 0xF7, 0xD8, 0x55, 0x55, 0x55, 0x59, 0x59, 0xDE, 0xFF,
}

var _ Collator = (*latin1Collator)(nil)

func (c *latin1Collator) Compare(a, b string) int {
	return compareCommon(a, b, func(r rune) uint32 {
		return uint32(c.weights.sortKey(r))
	})
}

func (c *latin1Collator) Key(str string) []byte {
	return c.KeyWithoutTrimRightSpace(truncateTailingSpace(str))
}

func (c *latin1Collator) ImmutableKey(str string) []byte {
	return c.KeyWithoutTrimRightSpace(truncateTailingSpace(str))
}

func (c *latin1Collator) KeyWithoutTrimRightSpace(str string) []byte {
	key := make([]byte, 0, len(str))
	for i := 0; i < len(str); {
		r, rLen := utf8.DecodeRuneInString(str[i:])
		if r == utf8.RuneError && rLen == 1 {
			return key
		}
		key = append(key, c.weights.sortKey(r))
		i += rLen
	}
	return key
}

func (c *latin1Collator) Pattern() WildcardPattern {
	return &latin1Pattern{weights: c.weights}
}

func (c *latin1Collator) Clone() Collator {
	return &latin1Collator{weights: c.weights}
}

func (*latin1Collator) MaxKeyLen(s string) int {
	return len(s)
}

type latin1Pattern struct {
	weights  *latin1ByteWeightTable
	patRunes []rune
	patTypes []byte
}

func (p *latin1Pattern) Compile(patternStr string, escape byte) {
	p.patRunes, p.patTypes = stringutil.CompilePattern(patternStr, escape)
}

func (p *latin1Pattern) DoMatch(str string) bool {
	return stringutil.DoMatchCustomized(str, p.patRunes, p.patTypes, func(a, b rune) bool {
		return p.weights.sortKey(a) == p.weights.sortKey(b)
	})
}
