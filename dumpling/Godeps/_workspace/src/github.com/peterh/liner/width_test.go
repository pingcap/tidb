package liner

import (
	"strconv"
	"testing"
)

func accent(in []rune) []rune {
	var out []rune
	for _, r := range in {
		out = append(out, r)
		out = append(out, '\u0301')
	}
	return out
}

type testCase struct {
	s      []rune
	glyphs int
}

var testCases = []testCase{
	{[]rune("query"), 5},
	{[]rune("私"), 2},
	{[]rune("hello世界"), 9},
}

func TestCountGlyphs(t *testing.T) {
	for _, testCase := range testCases {
		count := countGlyphs(testCase.s)
		if count != testCase.glyphs {
			t.Errorf("ASCII count incorrect. %d != %d", count, testCase.glyphs)
		}
		count = countGlyphs(accent(testCase.s))
		if count != testCase.glyphs {
			t.Errorf("Accent count incorrect. %d != %d", count, testCase.glyphs)
		}
	}
}

func compare(a, b []rune, name string, t *testing.T) {
	if len(a) != len(b) {
		t.Errorf(`"%s" != "%s" in %s"`, string(a), string(b), name)
		return
	}
	for i := range a {
		if a[i] != b[i] {
			t.Errorf(`"%s" != "%s" in %s"`, string(a), string(b), name)
			return
		}
	}
}

func TestPrefixGlyphs(t *testing.T) {
	for _, testCase := range testCases {
		for i := 0; i <= len(testCase.s); i++ {
			iter := strconv.Itoa(i)
			out := getPrefixGlyphs(testCase.s, i)
			compare(out, testCase.s[:i], "ascii prefix "+iter, t)
			out = getPrefixGlyphs(accent(testCase.s), i)
			compare(out, accent(testCase.s[:i]), "accent prefix "+iter, t)
		}
		out := getPrefixGlyphs(testCase.s, 999)
		compare(out, testCase.s, "ascii prefix overflow", t)
		out = getPrefixGlyphs(accent(testCase.s), 999)
		compare(out, accent(testCase.s), "accent prefix overflow", t)

		out = getPrefixGlyphs(testCase.s, -3)
		if len(out) != 0 {
			t.Error("ascii prefix negative")
		}
		out = getPrefixGlyphs(accent(testCase.s), -3)
		if len(out) != 0 {
			t.Error("accent prefix negative")
		}
	}
}

func TestSuffixGlyphs(t *testing.T) {
	for _, testCase := range testCases {
		for i := 0; i <= len(testCase.s); i++ {
			iter := strconv.Itoa(i)
			out := getSuffixGlyphs(testCase.s, i)
			compare(out, testCase.s[len(testCase.s)-i:], "ascii suffix "+iter, t)
			out = getSuffixGlyphs(accent(testCase.s), i)
			compare(out, accent(testCase.s[len(testCase.s)-i:]), "accent suffix "+iter, t)
		}
		out := getSuffixGlyphs(testCase.s, 999)
		compare(out, testCase.s, "ascii suffix overflow", t)
		out = getSuffixGlyphs(accent(testCase.s), 999)
		compare(out, accent(testCase.s), "accent suffix overflow", t)

		out = getSuffixGlyphs(testCase.s, -3)
		if len(out) != 0 {
			t.Error("ascii suffix negative")
		}
		out = getSuffixGlyphs(accent(testCase.s), -3)
		if len(out) != 0 {
			t.Error("accent suffix negative")
		}
	}
}
