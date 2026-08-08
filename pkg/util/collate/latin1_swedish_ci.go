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
	"cmp"

	"github.com/pingcap/tidb/pkg/util/stringutil"
)

// latin1SwedishCICollator is the collator for latin1_swedish_ci.
//
// Unlike the utf8mb4 collators this one is byte-oriented rather than rune-oriented.
// MySQL defines latin1_swedish_ci as a 256-entry weight table indexed by the cp1252
// byte value (strings/ctype-latin1.c, sort_order_latin1), and TiDB's latin1 charset
// stores whatever bytes the client sent: charset.EncodingLatin1Impl uses encoding.Nop
// and a no-op Transform. Staying byte-oriented therefore matches both MySQL and the
// existing latin1_bin collator for the same column.
//
// The consequence is that a multi-byte UTF-8 sequence stored in a latin1 column is
// weighted byte by byte rather than as one character. That is the same treatment
// latin1_bin already gives it, so it is not a new divergence, but it does mean the
// collation is only MySQL-faithful for genuinely cp1252-encoded data.
//
// Because every input byte maps to exactly one weight byte, the sort key has the same
// length as the (space-trimmed) input and index key sizes are unchanged relative to
// latin1_bin. The sort key is not the raw data though, so latin1_swedish_ci columns
// need restored data in indexes - see types.NeedRestoredDataWithCollate.
type latin1SwedishCICollator struct{}

// Compare implements Collator interface.
func (*latin1SwedishCICollator) Compare(a, b string) int {
	a = truncateTailingSpace(a)
	b = truncateTailingSpace(b)
	for i := 0; i < len(a) && i < len(b); i++ {
		if c := cmp.Compare(latin1SwedishCISortOrder[a[i]], latin1SwedishCISortOrder[b[i]]); c != 0 {
			return c
		}
	}
	return cmp.Compare(len(a), len(b))
}

// Key implements Collator interface.
func (c *latin1SwedishCICollator) Key(str string) []byte {
	return c.KeyWithoutTrimRightSpace(truncateTailingSpace(str))
}

// ImmutableKey implements Collator interface.
func (c *latin1SwedishCICollator) ImmutableKey(str string) []byte {
	return c.KeyWithoutTrimRightSpace(truncateTailingSpace(str))
}

// KeyWithoutTrimRightSpace implements Collator interface.
func (*latin1SwedishCICollator) KeyWithoutTrimRightSpace(str string) []byte {
	buf := make([]byte, 0, len(str))
	for i := range len(str) {
		buf = append(buf, latin1SwedishCISortOrder[str[i]])
	}
	return buf
}

// MaxKeyLen implements Collator interface.
func (*latin1SwedishCICollator) MaxKeyLen(s string) int {
	return len(s)
}

// Pattern implements Collator interface.
func (*latin1SwedishCICollator) Pattern() WildcardPattern {
	return &latin1SwedishCIPattern{}
}

// Clone implements Collator interface.
func (*latin1SwedishCICollator) Clone() Collator {
	return new(latin1SwedishCICollator)
}

type latin1SwedishCIPattern struct {
	patChars []byte
	patTypes []byte
}

// Compile implements WildcardPattern interface.
func (p *latin1SwedishCIPattern) Compile(patternStr string, escape byte) {
	p.patChars, p.patTypes = stringutil.CompilePatternBinary(patternStr, escape)
}

// DoMatch implements WildcardPattern interface.
func (p *latin1SwedishCIPattern) DoMatch(str string) bool {
	return stringutil.DoMatchCustomizedBinary(str, p.patChars, p.patTypes, func(a, b byte) bool {
		return latin1SwedishCISortOrder[a] == latin1SwedishCISortOrder[b]
	})
}

// latin1SwedishCISortOrder is MySQL's sort_order_latin1, transcribed verbatim from
// strings/ctype-latin1.cc in mysql-server. That is the table my_charset_latin1
// (collation id 8, latin1_swedish_ci) hands to my_collation_8bit_simple_ci_handler.
//
// The entries carrying the actual Swedish semantics, and the ones to check first if a
// comparison looks wrong: Å (0xC5) -> 0x5B, Ä/Æ (0xC4/0xC6) -> 0x5C, Ö (0xD6) -> 0x5D.
// These sit just above 'Z' (0x5A), so the Swedish letters sort after the ASCII
// alphabet rather than folding into A and O.
//
// Several accented characters deliberately do not fold to a base letter, which is
// easy to get wrong by reasoning from Unicode case folding instead of from this table:
// Ü (0xDC) and Ý (0xDD) both fold to 'Y', but ÿ (0xFF) keeps weight 0xFF; ß (0xDF)
// keeps weight 0xDF and is therefore NOT equal to 's' or "ss" (that expansion belongs
// to latin1_german2_ci, which uses the separate sort_order_latin1_de table); and
// Þ/þ and Ø/ø fold to each other but to no letter.
var latin1SwedishCISortOrder = [256]byte{
	// 0x00 - 0x3F: control characters, punctuation and digits keep their own value.
	0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F,
	0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F,
	0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2A, 0x2B, 0x2C, 0x2D, 0x2E, 0x2F,
	0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3A, 0x3B, 0x3C, 0x3D, 0x3E, 0x3F,
	// 0x40 - 0x5F: uppercase ASCII, unchanged.
	0x40, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x49, 0x4A, 0x4B, 0x4C, 0x4D, 0x4E, 0x4F,
	0x50, 0x51, 0x52, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58, 0x59, 0x5A, 0x5B, 0x5C, 0x5D, 0x5E, 0x5F,
	// 0x60 - 0x7F: lowercase ASCII folds onto uppercase.
	0x60, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x49, 0x4A, 0x4B, 0x4C, 0x4D, 0x4E, 0x4F,
	0x50, 0x51, 0x52, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58, 0x59, 0x5A, 0x7B, 0x7C, 0x7D, 0x7E, 0x7F,
	// 0x80 - 0xBF: cp1252 extras and symbols keep their own value.
	0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8A, 0x8B, 0x8C, 0x8D, 0x8E, 0x8F,
	0x90, 0x91, 0x92, 0x93, 0x94, 0x95, 0x96, 0x97, 0x98, 0x99, 0x9A, 0x9B, 0x9C, 0x9D, 0x9E, 0x9F,
	0xA0, 0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7, 0xA8, 0xA9, 0xAA, 0xAB, 0xAC, 0xAD, 0xAE, 0xAF,
	0xB0, 0xB1, 0xB2, 0xB3, 0xB4, 0xB5, 0xB6, 0xB7, 0xB8, 0xB9, 0xBA, 0xBB, 0xBC, 0xBD, 0xBE, 0xBF,
	// 0xC0 - 0xDF: accented uppercase letters.
	// À Á Â Ã fold to A; Ä Å Æ take the Swedish weights; Ç -> C; È..Ë -> E; Ì..Ï -> I.
	0x41, 0x41, 0x41, 0x41, 0x5C, 0x5B, 0x5C, 0x43, 0x45, 0x45, 0x45, 0x45, 0x49, 0x49, 0x49, 0x49,
	// Ð -> D; Ñ -> N; Ò..Õ -> O; Ö -> 0x5D; × and Ø keep their own value;
	// Ù..Û -> U; Ü and Ý -> Y; Þ and ß keep their own value.
	0x44, 0x4E, 0x4F, 0x4F, 0x4F, 0x4F, 0x5D, 0xD7, 0xD8, 0x55, 0x55, 0x55, 0x59, 0x59, 0xDE, 0xDF,
	// 0xE0 - 0xFF: accented lowercase letters, mirroring 0xC0 - 0xDF except for
	// ÷ (0xF7) and ÿ (0xFF), which keep their own value.
	0x41, 0x41, 0x41, 0x41, 0x5C, 0x5B, 0x5C, 0x43, 0x45, 0x45, 0x45, 0x45, 0x49, 0x49, 0x49, 0x49,
	0x44, 0x4E, 0x4F, 0x4F, 0x4F, 0x4F, 0x5D, 0xF7, 0xD8, 0x55, 0x55, 0x55, 0x59, 0x59, 0xDE, 0xFF,
}
