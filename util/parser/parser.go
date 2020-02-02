package parser

import (
	"strconv"
	"unicode"

	"github.com/pingcap/errors"
)

var (
	// ErrPatternNotMatch represents an error that patterns doesn't match.
	ErrPatternNotMatch = errors.New("Pattern not match")
)

// Match matches the `pat` at least `times`, and returns the match, the rest and the error
func Match(buf string, pat func(byte) bool, times int) (string, string, error) {
	var i int
	for i = 0; i < len(buf) && pat(buf[i]); i++ {
	}
	if i < times {
		return "", buf, ErrPatternNotMatch
	}
	return buf[:i], buf[i:], nil
}

// MatchOne matches only one time with pat
func MatchOne(buf string, pat func(byte) bool) (string, error) {
	if len(buf) == 0 || !pat(buf[0]) {
		return buf, ErrPatternNotMatch
	}
	return buf[1:], nil
}

// AnyPunct matches an arbitrary punctuation
func AnyPunct(buf string) (string, error) {
	return MatchOne(buf, func(b byte) bool {
		return unicode.IsPunct(rune(b))
	})
}

// AnyChar matches an arbitrary character
func AnyChar(buf string) (string, error) {
	return MatchOne(buf, func(byte) bool {
		return true
	})
}

// Char matches a character: c
func Char(buf string, c byte) (string, error) {
	return MatchOne(buf, func(x byte) bool {
		return x == c
	})
}

// Space matches at least `times` spaces
func Space(buf string, times int) (string, error) {
	_, rest, err := Match(buf, func(c byte) bool {
		return unicode.IsSpace(rune(c))
	}, times)
	return rest, err
}

// Space0 matches at least 0 space.
func Space0(buf string) string {
	rest, err := Space(buf, 0)
	if err != nil {
		panic(err)
	}
	return rest
}

// Digit matches at least `times` digits
func Digit(buf string, times int) (string, string, error) {
	return Match(buf, func(c byte) bool {
		return unicode.IsDigit(rune(c))
	}, times)
}

// Number matches a series of digits and convert it to an int
func Number(str string) (int, string, error) {
	digits, rest, err := Digit(str, 1)
	if err != nil {
		return 0, str, err
	}
	num, err := strconv.Atoi(digits)
	return num, rest, err
}
