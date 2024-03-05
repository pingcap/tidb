// Copyright 2020 PingCAP, Inc.
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

const (
	// magic number indicate weight has 2 uint64, should get from `longRuneMap`
	longRune uint64 = 0xFFFD
)

//go:generate go run ./ucaimpl/main.go -- unicode_0400_ci_generated.go

type unicode0400Impl struct {
}

func (unicode0400Impl) Preprocess(s string) string {
	return truncateTailingSpace(s)
}

func (unicode0400Impl) GetWeight(r rune) (first, second uint64) {
	if r > 0xFFFF {
		return 0xFFFD, 0
	}
	if ucadata.DUCET0400Table.MapTable4[r] == longRune {
		return ucadata.DUCET0400Table.LongRuneMap[r][0], ucadata.DUCET0400Table.LongRuneMap[r][1]
	}
	return ucadata.DUCET0400Table.MapTable4[r], 0
}

func (unicode0400Impl) Pattern() WildcardPattern {
	return &unicodePattern{}
}

type unicodePattern struct {
	patChars []rune
	patTypes []byte
}

// Compile implements WildcardPattern interface.
func (p *unicodePattern) Compile(patternStr string, escape byte) {
	p.patChars, p.patTypes = stringutil.CompilePatternInner(patternStr, escape)
}

// DoMatch implements WildcardPattern interface.
func (p *unicodePattern) DoMatch(str string) bool {
	return stringutil.DoMatchCustomized(str, p.patChars, p.patTypes, func(a, b rune) bool {
		if a > 0xFFFF || b > 0xFFFF {
			return a == b
		}

		ar, br := ucadata.DUCET0400Table.MapTable4[a], ucadata.DUCET0400Table.MapTable4[b]
		if ar != br {
			return false
		}

		if ar == longRune {
			return a == b
		}

		return true
	})
}
