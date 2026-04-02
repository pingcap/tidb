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

import (
	"unicode/utf8"

	"github.com/pingcap/tidb/pkg/util/stringutil"
)

type gbkChineseCICollator struct {
}

// Compare implements Collator interface.
func (*gbkChineseCICollator) Compare(a, b string) int {
	return compareCommon(a, b, gbkChineseCISortKey)
}

// Key implements Collator interface.
func (g *gbkChineseCICollator) Key(str string) []byte {
	return g.KeyWithoutTrimRightSpace(truncateTailingSpace(str))
}

// ImmutableKey implement Collator interface.
func (g *gbkChineseCICollator) ImmutableKey(str string) []byte {
	return g.KeyWithoutTrimRightSpace(truncateTailingSpace(str))
}

// KeyWithoutTrimRightSpace implement Collator interface.
func (*gbkChineseCICollator) KeyWithoutTrimRightSpace(str string) []byte {
	buf := make([]byte, 0, len(str)*2)
	i, rLen := 0, 0
	r := rune(0)
	for i < len(str) {
		// When the byte sequence is not a valid UTF-8 encoding of a rune, Golang returns RuneError('�') and size 1.
		// See https://pkg.go.dev/unicode/utf8#DecodeRune for more details.
		// Here we check both the size and rune to distinguish between invalid byte sequence and valid '�'.
		r, rLen = utf8.DecodeRuneInString(str[i:])
		invalid := r == utf8.RuneError && rLen == 1
		if invalid {
			return buf
		}

		i = i + rLen
		u16 := gbkChineseCISortKey(r)
		if u16 > 0xFF {
			buf = append(buf, byte(u16>>8))
		}
		buf = append(buf, byte(u16))
	}
	return buf
}

// MaxKeyLen implements Collator interface.
func (*gbkChineseCICollator) MaxKeyLen(s string) int {
	return utf8.RuneCountInString(s) * 2
}

// Pattern implements Collator interface.
func (*gbkChineseCICollator) Pattern() WildcardPattern {
	return &gbkChineseCIPattern{}
}

// Clone implements Collator interface.
func (*gbkChineseCICollator) Clone() Collator {
	return new(gbkChineseCICollator)
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
	return stringutil.DoMatchCustomized(str, p.patChars, p.patTypes, func(a, b rune) bool {
		return gbkChineseCISortKey(a) == gbkChineseCISortKey(b)
	})
}

func gbkChineseCISortKey(r rune) uint32 {
	if r > 0xFFFF {
		return 0x3F
	}

	return uint32(gbkChineseCISortKeyTable[r])
}
