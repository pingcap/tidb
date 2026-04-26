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
	"bytes"
	"unicode/utf8"

	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/util/hack"
	"golang.org/x/text/encoding"
)

// Each of these runes is 3 bytes in utf8. But since some characters is translated in customGB18030Encoder,
// when we meet these runes, we should use 4 bytes to represent them.
var fourBytesRune = map[rune]struct{}{
	'\ue78d': {}, '\ue78e': {}, '\ue78f': {}, '\ue790': {}, '\ue791': {}, '\ue792': {},
	'\ue793': {}, '\ue794': {}, '\ue795': {}, '\ue796': {}, '\ue7c7': {}, '\ue81e': {},
	'\ue826': {}, '\ue82b': {}, '\ue82c': {}, '\ue832': {}, '\ue843': {}, '\ue854': {},
	'\ue864': {},
}

// gb18030BinCollator is collator for gb18030_bin.
type gb18030BinCollator struct {
	e *encoding.Encoder
}

// Clone implements Collator interface.
func (*gb18030BinCollator) Clone() Collator {
	return &gb18030BinCollator{charset.NewCustomGB18030Encoder()}
}

// Compare implement Collator interface.
func (g *gb18030BinCollator) Compare(a, b string) int {
	a = truncateTailingSpace(a)
	b = truncateTailingSpace(b)

	// compare the character one by one.
	for len(a) > 0 && len(b) > 0 {
		aLen, bLen := min(len(a), runeLen(a[0])), min(len(b), runeLen(b[0]))
		aGb18030, err := g.e.Bytes(hack.Slice(a[:aLen]))
		// if convert error happened, we use '?'(0x3F) replace it.
		// It should not happen.
		if err != nil {
			aGb18030 = []byte{0x3F}
		}
		bGb18030, err := g.e.Bytes(hack.Slice(b[:bLen]))
		if err != nil {
			bGb18030 = []byte{0x3F}
		}

		compare := bytes.Compare(aGb18030, bGb18030)
		if compare != 0 {
			return compare
		}
		a = a[aLen:]
		b = b[bLen:]
	}

	return sign(len(a) - len(b))
}

// Key implement Collator interface.
func (g *gb18030BinCollator) Key(str string) []byte {
	return g.KeyWithoutTrimRightSpace(truncateTailingSpace(str))
}

// KeyWithoutTrimRightSpace implement Collator interface.
func (g *gb18030BinCollator) KeyWithoutTrimRightSpace(str string) []byte {
	buf := make([]byte, 0, len(str))
	for len(str) > 0 {
		l := min(len(str), runeLen(str[0]))
		r, _ := utf8.DecodeRuneInString(str[:l])
		var buf1 []byte
		if _, ok := fourBytesRune[r]; ok {
			buf1 = make([]byte, 4)
		} else {
			buf1 = make([]byte, l)
		}
		copy(buf1, str[:l])
		gb18030, err := g.e.Bytes(buf1)
		if err != nil {
			buf = append(buf, byte('?'))
		} else {
			buf = append(buf, gb18030...)
		}
		str = str[l:]
	}

	return buf
}

// Pattern implements Collator interface.
func (*gb18030BinCollator) Pattern() WildcardPattern {
	return &gb18030BinPattern{}
}

// use binPattern directly, they are totally same.
type gb18030BinPattern struct {
	binPattern
}
