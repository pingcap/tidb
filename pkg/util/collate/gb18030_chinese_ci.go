// Copyright 2024 PingCAP, Inc.
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
	_ "embed"
	"encoding/binary"
	"unicode/utf8"

	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/stringutil"
)

//go:embed gb18030_weight.data
var gb18030WeightData []byte

const (
	// Unicode code points up to U+10FFFF can be encoded as GB18030.
	gb18030MaxCodePoint = 0x10FFFF
)

type gb18030ChineseCICollator struct {
}

// Clone implements Collator interface.
func (*gb18030ChineseCICollator) Clone() Collator {
	return new(gb18030ChineseCICollator)
}

// Compare implements Collator interface.
func (*gb18030ChineseCICollator) Compare(a, b string) int {
	a = truncateTailingSpace(a)
	b = truncateTailingSpace(b)

	r1, r2 := rune(0), rune(0)
	ai, bi := 0, 0
	r1Len, r2Len := 0, 0
	for ai < len(a) && bi < len(b) {
		r1, r1Len = utf8.DecodeRune(hack.Slice(a[ai:]))
		r2, r2Len = utf8.DecodeRune(hack.Slice(b[bi:]))

		if r1 == utf8.RuneError || r2 == utf8.RuneError {
			return 0
		}

		ai = ai + r1Len
		bi = bi + r2Len

		cmp := int(gb18030ChineseCISortKey(r1)) - int(gb18030ChineseCISortKey(r2))
		if cmp != 0 {
			return sign(cmp)
		}
	}
	return sign((len(a) - ai) - (len(b) - bi))
}

// Key implements Collator interface.
func (g *gb18030ChineseCICollator) Key(str string) []byte {
	return g.KeyWithoutTrimRightSpace(truncateTailingSpace(str))
}

// KeyWithoutTrimRightSpace implement Collator interface.
func (*gb18030ChineseCICollator) KeyWithoutTrimRightSpace(str string) []byte {
	buf := make([]byte, 0, len(str)*2)
	i, rLen := 0, 0
	r := rune(0)
	for i < len(str) {
		r, rLen = utf8.DecodeRune(hack.Slice(str[i:]))

		if r == utf8.RuneError {
			return buf
		}

		i = i + rLen
		u32 := gb18030ChineseCISortKey(r)
		if u32 > 0xFFFFFF {
			buf = append(buf, byte(u32>>24))
		}
		if u32 > 0xFFFF {
			buf = append(buf, byte(u32>>16))
		}
		if u32 > 0xFF {
			buf = append(buf, byte(u32>>8))
		}
		buf = append(buf, byte(u32))
	}
	return buf
}

// Pattern implements Collator interface.
func (*gb18030ChineseCICollator) Pattern() WildcardPattern {
	return &gb18030ChineseCIPattern{}
}

type gb18030ChineseCIPattern struct {
	patChars []rune
	patTypes []byte
}

// Compile implements WildcardPattern interface.
func (p *gb18030ChineseCIPattern) Compile(patternStr string, escape byte) {
	p.patChars, p.patTypes = stringutil.CompilePatternInner(patternStr, escape)
}

// DoMatch implements WildcardPattern interface.
func (p *gb18030ChineseCIPattern) DoMatch(str string) bool {
	return stringutil.DoMatchCustomized(str, p.patChars, p.patTypes, func(a, b rune) bool {
		return gb18030ChineseCISortKey(a) == gb18030ChineseCISortKey(b)
	})
}

func gb18030ChineseCISortKey(r rune) uint32 {
	if r > gb18030MaxCodePoint {
		return 0x3F
	}

	return binary.LittleEndian.Uint32(gb18030WeightData[4*r : 4*r+4])
}
