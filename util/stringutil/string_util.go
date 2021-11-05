// Copyright 2015 PingCAP, Inc.
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

package stringutil

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util/hack"
)

// ErrSyntax indicates that a value does not have the right syntax for the target type.
var ErrSyntax = errors.New("invalid syntax")

// UnquoteChar decodes the first character or byte in the escaped string
// or character literal represented by the string s.
// It returns four values:
//
// 1) value, the decoded Unicode code point or byte value;
// 2) multibyte, a boolean indicating whether the decoded character requires a multibyte UTF-8 representation;
// 3) tail, the remainder of the string after the character; and
// 4) an error that will be nil if the character is syntactically valid.
//
// The second argument, quote, specifies the type of literal being parsed
// and therefore which escaped quote character is permitted.
// If set to a single quote, it permits the sequence \' and disallows unescaped '.
// If set to a double quote, it permits \" and disallows unescaped ".
// If set to zero, it does not permit either escape and allows both quote characters to appear unescaped.
// Different with strconv.UnquoteChar, it permits unnecessary backslash.
func UnquoteChar(s string, quote byte) (value []byte, tail string, err error) {
	// easy cases
	switch c := s[0]; {
	case c == quote:
		err = errors.Trace(ErrSyntax)
		return
	case c >= utf8.RuneSelf:
		r, size := utf8.DecodeRuneInString(s)
		if r == utf8.RuneError {
			value = append(value, c)
			return value, s[1:], nil
		}
		value = append(value, string(r)...)
		return value, s[size:], nil
	case c != '\\':
		value = append(value, c)
		return value, s[1:], nil
	}
	// hard case: c is backslash
	if len(s) <= 1 {
		err = errors.Trace(ErrSyntax)
		return
	}
	c := s[1]
	s = s[2:]
	switch c {
	case 'b':
		value = append(value, '\b')
	case 'n':
		value = append(value, '\n')
	case 'r':
		value = append(value, '\r')
	case 't':
		value = append(value, '\t')
	case 'Z':
		value = append(value, '\032')
	case '0':
		value = append(value, '\000')
	case '_', '%':
		value = append(value, '\\')
		value = append(value, c)
	case '\\':
		value = append(value, '\\')
	case '\'', '"':
		value = append(value, c)
	default:
		value = append(value, c)
	}
	tail = s
	return
}

// Unquote interprets s as a single-quoted, double-quoted,
// or backquoted Go string literal, returning the string value
// that s quotes. For example: test=`"\"\n"` (hex: 22 5c 22 5c 6e 22)
// should be converted to `"\n` (hex: 22 0a).
func Unquote(s string) (t string, err error) {
	n := len(s)
	if n < 2 {
		return "", errors.Trace(ErrSyntax)
	}
	quote := s[0]
	if quote != s[n-1] {
		return "", errors.Trace(ErrSyntax)
	}
	s = s[1 : n-1]
	if quote != '"' && quote != '\'' {
		return "", errors.Trace(ErrSyntax)
	}
	// Avoid allocation. No need to convert if there is no '\'
	if strings.IndexByte(s, '\\') == -1 && strings.IndexByte(s, quote) == -1 {
		return s, nil
	}
	buf := make([]byte, 0, 3*len(s)/2) // Try to avoid more allocations.
	for len(s) > 0 {
		mb, ss, err := UnquoteChar(s, quote)
		if err != nil {
			return "", errors.Trace(err)
		}
		s = ss
		buf = append(buf, mb...)
	}
	return string(buf), nil
}

const (
	// PatMatch is the enumeration value for per-character match.
	PatMatch = iota + 1
	// PatOne is the enumeration value for '_' match.
	PatOne
	// PatAny is the enumeration value for '%' match.
	PatAny
)

// CompilePatternBytes is a adapter for `CompilePatternInner`, `pattern` can only be an ascii string.
func CompilePatternBytes(pattern string, escape byte) (patChars, patTypes []byte) {
	patWeights, patTypes := CompilePatternInner(pattern, escape)
	patChars = []byte(string(patWeights))

	return patChars, patTypes
}

// CompilePattern is a adapter for `CompilePatternInner`, `pattern` can be any unicode string.
func CompilePattern(pattern string, escape byte) (patWeights []rune, patTypes []byte) {
	return CompilePatternInner(pattern, escape)
}

// CompilePatternInner handles escapes and wild cards convert pattern characters and
// pattern types.
func CompilePatternInner(pattern string, escape byte) (patWeights []rune, patTypes []byte) {
	runes := []rune(pattern)
	escapeRune := rune(escape)
	lenRunes := len(runes)
	patWeights = make([]rune, lenRunes)
	patTypes = make([]byte, lenRunes)
	patLen := 0
	for i := 0; i < lenRunes; i++ {
		var tp byte
		var r = runes[i]
		switch r {
		case escapeRune:
			tp = PatMatch
			if i < lenRunes-1 {
				i++
				r = runes[i]
			}
		case '_':
			// %_ => _%
			if patLen > 0 && patTypes[patLen-1] == PatAny {
				tp = PatAny
				r = '%'
				patWeights[patLen-1], patTypes[patLen-1] = '_', PatOne
			} else {
				tp = PatOne
			}
		case '%':
			// %% => %
			if patLen > 0 && patTypes[patLen-1] == PatAny {
				continue
			}
			tp = PatAny
		default:
			tp = PatMatch
		}
		patWeights[patLen] = r
		patTypes[patLen] = tp
		patLen++
	}
	patWeights = patWeights[:patLen]
	patTypes = patTypes[:patLen]
	return
}

func matchRune(a, b rune) bool {
	return a == b
	// We may reuse below code block when like function go back to case insensitive.
	/*
		if a == b {
			return true
		}
		if a >= 'a' && a <= 'z' && a-caseDiff == b {
			return true
		}
		return a >= 'A' && a <= 'Z' && a+caseDiff == b
	*/
}

// CompileLike2Regexp convert a like `lhs` to a regular expression
func CompileLike2Regexp(str string) string {
	patChars, patTypes := CompilePattern(str, '\\')
	var result []rune
	for i := 0; i < len(patChars); i++ {
		switch patTypes[i] {
		case PatMatch:
			result = append(result, patChars[i])
		case PatOne:
			result = append(result, '.')
		case PatAny:
			result = append(result, '.', '*')
		}
	}
	return string(result)
}

// DoMatchBytes is a adapter for `DoMatchInner`, `str` can only be an ascii string.
func DoMatchBytes(str string, patChars, patTypes []byte) bool {
	return DoMatchInner(str, []rune(string(patChars)), patTypes, matchRune)
}

// DoMatch is a adapter for `DoMatchInner`, `str` can be any unicode string.
func DoMatch(str string, patChars []rune, patTypes []byte) bool {
	return DoMatchInner(str, patChars, patTypes, matchRune)
}

// DoMatchInner matches the string with patChars and patTypes.
// The algorithm has linear time complexity.
// https://research.swtch.com/glob
func DoMatchInner(str string, patWeights []rune, patTypes []byte, matcher func(a, b rune) bool) bool {
	// TODO(bb7133): it is possible to get the rune one by one to avoid the cost of get them as a whole.
	runes := []rune(str)
	lenRunes := len(runes)
	var rIdx, pIdx, nextRIdx, nextPIdx int
	for pIdx < len(patWeights) || rIdx < lenRunes {
		if pIdx < len(patWeights) {
			switch patTypes[pIdx] {
			case PatMatch:
				if rIdx < lenRunes && matcher(runes[rIdx], patWeights[pIdx]) {
					pIdx++
					rIdx++
					continue
				}
			case PatOne:
				if rIdx < lenRunes {
					pIdx++
					rIdx++
					continue
				}
			case PatAny:
				// Try to match at sIdx.
				// If that doesn't work out,
				// restart at sIdx+1 next.
				nextPIdx = pIdx
				nextRIdx = rIdx + 1
				pIdx++
				continue
			}
		}
		// Mismatch. Maybe restart.
		if 0 < nextRIdx && nextRIdx <= lenRunes {
			pIdx = nextPIdx
			rIdx = nextRIdx
			continue
		}
		return false
	}
	// Matched all of pattern to all of name. Success.
	return true
}

// IsExactMatch return true if no wildcard character
func IsExactMatch(patTypes []byte) bool {
	for _, pt := range patTypes {
		if pt != PatMatch {
			return false
		}
	}
	return true
}

// Copy deep copies a string.
func Copy(src string) string {
	return string(hack.Slice(src))
}

// StringerFunc defines string func implement fmt.Stringer.
type StringerFunc func() string

// String implements fmt.Stringer
func (l StringerFunc) String() string {
	return l()
}

// MemoizeStr returns memoized version of stringFunc.
func MemoizeStr(l func() string) fmt.Stringer {
	return StringerFunc(func() string {
		return l()
	})
}

// StringerStr defines a alias to normal string.
// implement fmt.Stringer
type StringerStr string

// String implements fmt.Stringer
func (i StringerStr) String() string {
	return string(i)
}

// Escape the identifier for pretty-printing.
// For instance, the identifier "foo `bar`" will become "`foo ``bar```".
// The sqlMode controls whether to escape with backquotes (`) or double quotes
// (`"`) depending on whether mysql.ModeANSIQuotes is enabled.
func Escape(str string, sqlMode mysql.SQLMode) string {
	var quote string
	if sqlMode&mysql.ModeANSIQuotes != 0 {
		quote = `"`
	} else {
		quote = "`"
	}
	return quote + strings.Replace(str, quote, quote+quote, -1) + quote
}

// BuildStringFromLabels construct config labels into string by following format:
// "keyA=valueA,keyB=valueB"
func BuildStringFromLabels(labels map[string]string) string {
	if len(labels) < 1 {
		return ""
	}
	s := make([]string, 0, len(labels))
	for k := range labels {
		s = append(s, k)
	}
	sort.Strings(s)
	r := new(bytes.Buffer)
	// visit labels by sorted key in order to make sure that result should be consistency
	for _, key := range s {
		r.WriteString(fmt.Sprintf("%s=%s,", key, labels[key]))
	}
	returned := r.String()
	return returned[:len(returned)-1]
}

// GetTailSpaceCount returns the number of tailed spaces.
func GetTailSpaceCount(str string) int64 {
	length := len(str)
	for length > 0 && str[length-1] == ' ' {
		length--
	}
	return int64(len(str) - length)
}
