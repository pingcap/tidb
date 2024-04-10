// Copyright 2023 PingCAP, Inc.
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
	"github.com/pingcap/tidb/pkg/util/collate/ucadata"
	"github.com/pingcap/tidb/pkg/util/stringutil"
)

//go:generate go run ./ucaimpl/main.go -- unicode_0900_ai_ci_generated.go

type unicode0900Impl struct {
}

func (unicode0900Impl) Preprocess(s string) string {
	return s
}

func (unicode0900Impl) GetWeight(r rune) (first, second uint64) {
	return convertRuneUnicodeCI0900(r)
}

func (unicode0900Impl) Pattern() WildcardPattern {
	return &unicode0900AICIPattern{}
}

func convertRuneUnicodeCI0900(r rune) (first, second uint64) {
	if int(r) > len(ucadata.DUCET0900Table.MapTable4) {
		return uint64(r>>15) + 0xFBC0 + (uint64((r&0x7FFF)|0x8000) << 16), 0
	}

	first = ucadata.DUCET0900Table.MapTable4[r]
	if first == ucadata.LongRune8 {
		return ucadata.DUCET0900Table.LongRuneMap[r][0], ucadata.DUCET0900Table.LongRuneMap[r][1]
	}
	return first, 0
}

type unicode0900AICIPattern struct {
	patChars []rune
	patTypes []byte
}

// Compile implements WildcardPattern interface.
func (p *unicode0900AICIPattern) Compile(patternStr string, escape byte) {
	p.patChars, p.patTypes = stringutil.CompilePatternInner(patternStr, escape)
}

// DoMatch implements WildcardPattern interface.
func (p *unicode0900AICIPattern) DoMatch(str string) bool {
	return stringutil.DoMatchCustomized(str, p.patChars, p.patTypes, func(a, b rune) bool {
		aFirst, aSecond := convertRuneUnicodeCI0900(a)
		bFirst, bSecond := convertRuneUnicodeCI0900(b)

		return aFirst == bFirst && aSecond == bSecond
	})
}
