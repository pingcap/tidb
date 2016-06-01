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
// See the License for the specific language governing permissions and
// limitations under the License.

package stringutil

import (
	"bytes"
	"strings"
	"unicode/utf8"

	"github.com/juju/errors"
)

// ErrSyntax indicates that a value does not have the right syntax for the target type.
var ErrSyntax = errors.New("invalid syntax")

// See: https://dev.mysql.com/doc/refman/5.7/en/string-literals.html#character-escape-sequences
const validEscapeChars = `0'"bntrz\\%_`

// RemoveUselessBackslash removes backslashs which could be ignored in the string literal.
// See: https://dev.mysql.com/doc/refman/5.7/en/string-literals.html
// " Each of these sequences begins with a backslash ("\"), known as the escape character.
// MySQL recognizes the escape sequences shown in Table 9.1, "Special Character Escape Sequences".
// For all other escape sequences, backslash is ignored. That is, the escaped character is
// interpreted as if it was not escaped. For example, "\x" is just "x". These sequences are case sensitive.
// For example, "\b" is interpreted as a backspace, but "\B" is interpreted as "B"."
func RemoveUselessBackslash(s string) string {
	var (
		buf bytes.Buffer
		i   = 0
	)
	for i < len(s)-1 {
		if s[i] != '\\' {
			buf.WriteByte(s[i])
			i++
			continue
		}
		next := s[i+1]
		if strings.IndexByte(validEscapeChars, next) != -1 {
			buf.WriteByte(s[i])
		}
		buf.WriteByte(next)
		i += 2
	}
	if i == len(s)-1 {
		buf.WriteByte(s[i])
	}
	return buf.String()
}

// Reverse returns its argument string reversed rune-wise left to right.
func Reverse(s string) string {
	r := []rune(s)
	for i, j := 0, len(r)-1; i < len(r)/2; i, j = i+1, j-1 {
		r[i], r[j] = r[j], r[i]
	}
	return string(r)
}

// UnquoteChar decodes the first character or byte in the escaped string
// or character literal represented by the string s.
// It returns four values:
//
//1) value, the decoded Unicode code point or byte value;
//2) multibyte, a boolean indicating whether the decoded character requires a multibyte UTF-8 representation;
//3) tail, the remainder of the string after the character; and
//4) an error that will be nil if the character is syntactically valid.
//
// The second argument, quote, specifies the type of literal being parsed
// and therefore which escaped quote character is permitted.
// If set to a single quote, it permits the sequence \' and disallows unescaped '.
// If set to a double quote, it permits \" and disallows unescaped ".
// If set to zero, it does not permit either escape and allows both quote characters to appear unescaped.
// Diffrent with strconv.UnquoteChar, it permits unnecessary backslash.
func UnquoteChar(s string, quote byte) (value rune, multibyte bool, tail string, err error) {
	// easy cases
	switch c := s[0]; {
	case c == quote && (quote == '\'' || quote == '"'):
		err = errors.Trace(ErrSyntax)
		return
	case c >= utf8.RuneSelf:
		r, size := utf8.DecodeRuneInString(s)
		return r, true, s[size:], nil
	case c != '\\':
		return rune(s[0]), false, s[1:], nil
	}
	// hard case: c is backslash
	if len(s) <= 1 {
		err = errors.Trace(ErrSyntax)
		return
	}
	c := s[1]
	s = s[2:]
	switch c {
	case 'a':
		value = '\a'
	case 'b':
		value = '\b'
	case 'f':
		value = '\f'
	case 'n':
		value = '\n'
	case 'r':
		value = '\r'
	case 't':
		value = '\t'
	case 'v':
		value = '\v'
	case 'x', 'u', 'U':
		n := 0
		switch c {
		case 'x':
			n = 2
		case 'u':
			n = 4
		case 'U':
			n = 8
		}
		var v rune
		if len(s) < n {
			err = errors.Trace(ErrSyntax)
			return
		}
		for j := 0; j < n; j++ {
			x, ok := unhex(s[j])
			if !ok {
				err = errors.Trace(ErrSyntax)
				return
			}
			v = v<<4 | x
		}
		s = s[n:]
		if c == 'x' {
			// single-byte string, possibly not UTF-8
			value = v
			break
		}
		if v > utf8.MaxRune {
			err = errors.Trace(ErrSyntax)
			return
		}
		value = v
		multibyte = true
	case '0', '1', '2', '3', '4', '5', '6', '7':
		v := rune(c) - '0'
		if len(s) < 2 {
			err = errors.Trace(ErrSyntax)
			return
		}
		for j := 0; j < 2; j++ { // one digit already; two more
			x := rune(s[j]) - '0'
			if x < 0 || x > 7 {
				err = errors.Trace(ErrSyntax)
				return
			}
			v = (v << 3) | x
		}
		s = s[2:]
		if v > 255 {
			err = errors.Trace(ErrSyntax)
			return
		}
		value = v
	case '\\':
		value = '\\'
	case '\'', '"':
		value = rune(c)
	default:
		err = errors.Trace(ErrSyntax)
		return
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
	if quote == '`' {
		if strings.IndexByte(s, '`') != -1 {
			return "", errors.Trace(ErrSyntax)
		}
		return s, nil
	}
	if quote != '"' && quote != '\'' {
		return "", errors.Trace(ErrSyntax)
	}
	// Avoid allocation. No need to convert if there is no '\'
	if strings.IndexByte(s, '\\') == -1 && strings.IndexByte(s, quote) == -1 {
		switch quote {
		case '"':
			return s, nil
		case '\'':
			r, size := utf8.DecodeRuneInString(s)
			if size == len(s) && (r != utf8.RuneError || size != 1) {
				return s, nil
			}
		}
	}
	var runeTmp [utf8.UTFMax]byte
	buf := make([]byte, 0, 3*len(s)/2) // Try to avoid more allocations.
	for len(s) > 0 {
		c, multibyte, ss, err := UnquoteChar(s, quote)
		if err != nil {
			return "", errors.Trace(err)
		}
		s = ss
		if c < utf8.RuneSelf || !multibyte {
			buf = append(buf, byte(c))
		} else {
			n := utf8.EncodeRune(runeTmp[:], c)
			buf = append(buf, runeTmp[:n]...)
		}
	}
	return string(buf), nil
}

func unhex(b byte) (v rune, ok bool) {
	c := rune(b)
	switch {
	case '0' <= c && c <= '9':
		return c - '0', true
	case 'a' <= c && c <= 'f':
		return c - 'a' + 10, true
	case 'A' <= c && c <= 'F':
		return c - 'A' + 10, true
	}
	return
}
