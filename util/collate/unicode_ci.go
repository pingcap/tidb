package collate

import (
	"strings"

	"github.com/pingcap/tidb/util/stringutil"
)

// unicodeCICollator implementment use binaryCollator temporary
type unicodeCICollator struct {
}

// Compare implement Collator interface.
func (uc *unicodeCICollator) Compare(a, b string) int {
	return strings.Compare(a, b)
}

// Key implements Collator interface.
func (uc *unicodeCICollator) Key(str string) []byte {
	return []byte(str)
}

// Pattern implements Collator interface.
func (uc *unicodeCICollator) Pattern() WildcardPattern {
	return &unicodePattern{}
}

// unicodePattern implementment use binaryParttern temporary
type unicodePattern struct {
	patChars []byte
	patTypes []byte
}

// Compile implements WildcardPattern interface.
func (p *unicodePattern) Compile(patternStr string, escape byte) {
	p.patChars, p.patTypes = stringutil.CompilePattern(patternStr, escape)
}

// Compile implements WildcardPattern interface.
func (p *unicodePattern) DoMatch(str string) bool {
	return stringutil.DoMatch(str, p.patChars, p.patTypes)
}
