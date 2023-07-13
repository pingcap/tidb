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
	"strings"

	"github.com/pingcap/tidb/util/stringutil"
)

type binCollator struct {
}

// Compare implement Collator interface.
func (bc *binCollator) Compare(a, b string) int {
	return strings.Compare(a, b)
}

// Key implement Collator interface.
func (bc *binCollator) Key(str string) []byte {
	return []byte(str)
}

// KeyWithoutTrimRightSpace implement Collator interface.
func (bc *binCollator) KeyWithoutTrimRightSpace(str string) []byte {
	return []byte(str)
}

// Pattern implements Collator interface.
func (bc *binCollator) Pattern() WildcardPattern {
	return &binPattern{}
}

type binPaddingCollator struct {
}

func (bpc *binPaddingCollator) Compare(a, b string) int {
	return strings.Compare(truncateTailingSpace(a), truncateTailingSpace(b))
}

func (bpc *binPaddingCollator) Key(str string) []byte {
	return []byte(truncateTailingSpace(str))
}

// KeyWithoutTrimRightSpace implement Collator interface.
func (bpc *binPaddingCollator) KeyWithoutTrimRightSpace(str string) []byte {
	return []byte(str)
}

// Pattern implements Collator interface.
// Notice that trailing spaces are significant.
func (bpc *binPaddingCollator) Pattern() WildcardPattern {
	return &binPattern{}
}

type binPattern struct {
	patChars []rune
	patTypes []byte
}

// Compile implements WildcardPattern interface.
func (p *binPattern) Compile(patternStr string, escape byte) {
	p.patChars, p.patTypes = stringutil.CompilePattern(patternStr, escape)
}

// DoMatch implements WildcardPattern interface.
func (p *binPattern) DoMatch(str string) bool {
	return stringutil.DoMatch(str, p.patChars, p.patTypes)
}
