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
	"bytes"

	"github.com/pingcap/tidb/util/hack"
	"golang.org/x/text/encoding"
)

// gbkBinCollator is collator for gbk_bin.
type gbkBinCollator struct {
	e *encoding.Encoder
}

// Compare implement Collator interface.
func (g *gbkBinCollator) Compare(a, b string) int {
	a = truncateTailingSpace(a)
	b = truncateTailingSpace(b)

	// compare the character one by one.
	for len(a) > 0 && len(b) > 0 {
		aLen, bLen := runeLen(a[0]), runeLen(b[0])
		aGbk, err := g.e.Bytes(hack.Slice(a[:aLen]))
		// if convert error happened, we use '?'(0x3F) replace it.
		// It should not happen.
		if err != nil {
			aGbk = []byte{0x3F}
		}
		bGbk, err := g.e.Bytes(hack.Slice(b[:bLen]))
		if err != nil {
			bGbk = []byte{0x3F}
		}

		compare := bytes.Compare(aGbk, bGbk)
		if compare != 0 {
			return compare
		}
		a = a[aLen:]
		b = b[bLen:]
	}

	return sign(len(a) - len(b))
}

// Key implement Collator interface.
func (g *gbkBinCollator) Key(str string) []byte {
	return g.KeyWithoutTrimRightSpace(truncateTailingSpace(str))
}

// KeyWithoutTrimRightSpace implement Collator interface.
func (g *gbkBinCollator) KeyWithoutTrimRightSpace(str string) []byte {
	buf := make([]byte, 0, len(str))
	for len(str) > 0 {
		l := runeLen(str[0])
		gbk, err := g.e.Bytes(hack.Slice(str[:l]))
		if err != nil {
			buf = append(buf, byte('?'))
		} else {
			buf = append(buf, gbk...)
		}
		str = str[l:]
	}

	return buf
}

// Pattern implements Collator interface.
func (g *gbkBinCollator) Pattern() WildcardPattern {
	return &gbkBinPattern{}
}

// use binPattern directly, they are totally same.
type gbkBinPattern struct {
	binPattern
}
