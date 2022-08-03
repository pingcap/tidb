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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collate

import "github.com/pingcap/tidb/util/stringutil"

type gbkChineseCICollator struct {
}

// Compare implements Collator interface.
func (g *gbkChineseCICollator) Compare(a, b string) int {
	a = truncateTailingSpace(a)
	b = truncateTailingSpace(b)

	r1, r2 := rune(0), rune(0)
	ai, bi := 0, 0
	for ai < len(a) && bi < len(b) {
		r1, ai = decodeRune(a, ai)
		r2, bi = decodeRune(b, bi)

		cmp := int(gbkChineseCISortKey(r1)) - int(gbkChineseCISortKey(r2))
		if cmp != 0 {
			return sign(cmp)
		}
	}
	return sign((len(a) - ai) - (len(b) - bi))
}

// Key implements Collator interface.
func (g *gbkChineseCICollator) Key(str string) []byte {
	return g.KeyWithoutTrimRightSpace(truncateTailingSpace(str))
}

// KeyWithoutTrimRightSpace implement Collator interface.
func (g *gbkChineseCICollator) KeyWithoutTrimRightSpace(str string) []byte {
	buf := make([]byte, 0, len(str)*2)
	i := 0
	r := rune(0)
	for i < len(str) {
		r, i = decodeRune(str, i)
		u16 := gbkChineseCISortKey(r)
		if u16 > 0xFF {
			buf = append(buf, byte(u16>>8))
		}
		buf = append(buf, byte(u16))
	}
	return buf
}

// Pattern implements Collator interface.
func (g *gbkChineseCICollator) Pattern() WildcardPattern {
	return &gbkChineseCIPattern{}
}

type gbkChineseCIPattern struct {
	patChars []rune
	patTypes []byte
}

// Compile implements WildcardPattern interface.
func (p *gbkChineseCIPattern) Compile(patternStr string, escape byte) {
	p.patChars, p.patTypes = stringutil.CompilePatternInner(patternStr, escape)
}

// DoMatch implements WildcardPattern interface.
func (p *gbkChineseCIPattern) DoMatch(str string) bool {
	return stringutil.DoMatchInner(str, p.patChars, p.patTypes, func(a, b rune) bool {
		return gbkChineseCISortKey(a) == gbkChineseCISortKey(b)
	})
}

func gbkChineseCISortKey(r rune) uint16 {
	if r > 0xFFFF {
		return 0x3F
	}

	return gbkChineseCISortKeyTable[r]
}
