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
)

type latin1ByteWeightTable [256]uint8

type latin1Collator struct {
	weights *latin1ByteWeightTable
}

var _ Collator = (*latin1Collator)(nil)

func (c *latin1Collator) Compare(a, b string) int {
	return compareCommon(a, b, func(r rune) uint32 {
		if r <= 0xFF {
			return uint32(c.weights[byte(r)])
		}
		return uint32(r)
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
		if r <= 0xFF {
			key = append(key, c.weights[byte(r)])
			i += rLen
			continue
		}
		key = append(key, str[i:i+rLen]...)
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
		if a <= 0xFF && b <= 0xFF {
			return p.weights[byte(a)] == p.weights[byte(b)]
		}
		return a == b
	})
}
