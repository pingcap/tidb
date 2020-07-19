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
// See the License for the specific language governing permissions and
// limitations under the License.

package collate

import (
	_ "unicode/utf8"

	_ "github.com/pingcap/tidb/util/stringutil"
)

type unicodeCICollator struct {
}

// Compare implement Collator interface.
func (uc *unicodeCICollator) Compare(a, b string) int {
	return 0
}

// Key implements Collator interface.
func (uc *unicodeCICollator) Key(str string) []byte {
	return []byte{}
}

// Pattern implements Collator interface.
func (uc *unicodeCICollator) Pattern() WildcardPattern {
	return &unicodePattern{}
}

// unicodePattern implement use binaryPattern temporary
type unicodePattern struct {
	patChars []rune
	patTypes []byte
}

// Compile implements WildcardPattern interface.
func (p *unicodePattern) Compile(patternStr string, escape byte) {

}

// Compile implements WildcardPattern interface.
func (p *unicodePattern) DoMatch(str string) bool {
	return false
}
