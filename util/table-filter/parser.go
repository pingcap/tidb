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

package filter

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/pingcap/errors"
)

type tableRulesParser struct {
	rules []tableRule
	matcherParser
}

func (p *tableRulesParser) parse(line string, canImport bool) error {
	line = strings.Trim(line, " \t")
	if len(line) == 0 {
		return nil
	}

	positive := true
	switch line[0] {
	case '#':
		return nil
	case '!':
		positive = false
		line = line[1:]
	case '@':
		if !canImport {
			// FIXME: should we relax this?
			return p.errorf("importing filter files recursively is not allowed")
		}
		// FIXME: can't deal with file names which ends in spaces (perhaps not a big deal)
		return p.importFile(line[1:], p.parse)
	}

	var sm, tm matcher

	sm, line, err := p.parsePattern(line, true)
	if err != nil {
		return err
	}
	if len(line) == 0 {
		return p.errorf("missing table pattern")
	}
	if line[0] != '.' {
		return p.errorf("syntax error: missing '.' between schema and table patterns")
	}

	tm, line, err = p.parsePattern(line[1:], true)
	if err != nil {
		return err
	}
	if len(line) != 0 {
		return p.errorf("syntax error: stray characters after table pattern")
	}

	p.rules = append(p.rules, tableRule{
		schema:   sm,
		table:    tm,
		positive: positive,
	})
	return nil
}

type columnRulesParser struct {
	rules []columnRule
	matcherParser
}

func (p *columnRulesParser) parse(line string, canImport bool) error {
	line = strings.Trim(line, " \t")
	if len(line) == 0 {
		return nil
	}

	positive := true
	switch line[0] {
	case '#':
		return nil
	case '!':
		positive = false
		line = line[1:]
	case '@':
		if !canImport {
			return p.errorf("importing filter files recursively is not allowed")
		}
		return p.importFile(line[1:], p.parse)
	}

	var cm matcher

	cm, line, err := p.parsePattern(line, false)
	if err != nil {
		return err
	}

	if len(line) != 0 {
		return p.errorf("syntax error: stray characters after column pattern")
	}

	p.rules = append(p.rules, columnRule{
		// Column is not case-sensitive on any platform, nor are column aliases.
		// So we always match in lowercase.
		column:   cm.toLower(),
		positive: positive,
	})
	return nil
}

type matcherParser struct {
	fileName string
	lineNum  int64
}

func (p *matcherParser) wrapErrorFormat(format string) string {
	return fmt.Sprintf("at %s:%d: %s", strings.ReplaceAll(p.fileName, "%", "%%"), p.lineNum, format)
}

func (p *matcherParser) errorf(format string, args ...interface{}) error {
	return errors.Errorf(p.wrapErrorFormat(format), args...)
}

func (p *matcherParser) annotatef(err error, format string, args ...interface{}) error {
	return errors.Annotatef(err, p.wrapErrorFormat(format), args...)
}

var (
	regexpRegexp        = regexp.MustCompile(`^/(?:\\.|[^/])+/`)
	doubleQuotedRegexp  = regexp.MustCompile(`^"(?:""|[^"])+"`)
	backquotedRegexp    = regexp.MustCompile("^`(?:``|[^`])+`")
	wildcardRangeRegexp = regexp.MustCompile(`^\[!?(?:\\[^0-9a-zA-Z]|[^\\\]])+\]`)
)

func (p *matcherParser) newRegexpMatcher(pat string) (matcher, error) {
	m, err := newRegexpMatcher(pat)
	if err != nil {
		return nil, p.annotatef(err, "invalid pattern")
	}
	return m, nil
}

func (p *matcherParser) parsePattern(line string, needsDotSeparator bool) (matcher, string, error) {
	if len(line) == 0 {
		return nil, "", p.errorf("syntax error: missing pattern")
	}

	switch line[0] {
	case '/':
		// a regexp pattern
		loc := regexpRegexp.FindStringIndex(line)
		if len(loc) < 2 {
			return nil, "", p.errorf("syntax error: incomplete regexp")
		}
		m, err := p.newRegexpMatcher(line[1 : loc[1]-1])
		if err != nil {
			return nil, "", err
		}
		return m, line[loc[1]:], nil

	case '"':
		// a double-quoted pattern
		loc := doubleQuotedRegexp.FindStringIndex(line)
		if len(loc) < 2 {
			return nil, "", p.errorf("syntax error: incomplete quoted identifier")
		}
		name := strings.ReplaceAll(line[1:loc[1]-1], `""`, `"`)
		return stringMatcher(name), line[loc[1]:], nil

	case '`':
		// a backquoted pattern
		loc := backquotedRegexp.FindStringIndex(line)
		if len(loc) < 2 {
			return nil, "", p.errorf("syntax error: incomplete quoted identifier")
		}
		name := strings.ReplaceAll(line[1:loc[1]-1], "``", "`")
		return stringMatcher(name), line[loc[1]:], nil

	default:
		// wildcard or literal string.
		return p.parseWildcardPattern(line, needsDotSeparator)
	}
}

func isASCIIAlphanumeric(b byte) bool {
	return '0' <= b && b <= '9' || 'a' <= b && b <= 'z' || 'A' <= b && b <= 'Z'
}

func (p *matcherParser) parseWildcardPattern(line string, needsDotSeparator bool) (matcher, string, error) {
	var (
		literalStringBuilder   strings.Builder
		wildcardPatternBuilder strings.Builder
		isLiteralString        = true
		i                      = 0
	)
	literalStringBuilder.Grow(len(line))
	wildcardPatternBuilder.Grow(len(line) + 6)
	wildcardPatternBuilder.WriteString("(?s)^")

parseLoop:
	for i < len(line) {
		c := line[i]
		switch c {
		case '\\':
			// escape character
			if i == len(line)-1 {
				return nil, "", p.errorf(`syntax error: cannot place \ at end of line`)
			}
			esc := line[i+1]
			if isASCIIAlphanumeric(esc) {
				return nil, "", p.errorf(`cannot escape a letter or number (\%c), it is reserved for future extension`, esc)
			}
			if isLiteralString {
				literalStringBuilder.WriteByte(esc)
			}
			if esc < 0x80 {
				wildcardPatternBuilder.WriteByte('\\')
			}
			wildcardPatternBuilder.WriteByte(esc)

			i += 2

		case '.':
			if needsDotSeparator {
				// table separator, end now.
				break parseLoop
			}
			return nil, "", p.errorf("unexpected special character '%c'", c)
		case '*':
			// wildcard
			isLiteralString = false
			wildcardPatternBuilder.WriteString(".*")
			i++

		case '?':
			isLiteralString = false
			wildcardPatternBuilder.WriteByte('.')
			i++

		case '[':
			// range of characters
			isLiteralString = false
			rangeLoc := wildcardRangeRegexp.FindStringIndex(line[i:])
			if len(rangeLoc) < 2 {
				return nil, "", p.errorf("syntax error: failed to parse character class")
			}
			end := i + rangeLoc[1]
			switch line[i+1] {
			case '!':
				wildcardPatternBuilder.WriteString("[^")
				wildcardPatternBuilder.WriteString(line[i+2 : end])
			case '^': // `[^` is not special in a glob pattern. escape it.
				wildcardPatternBuilder.WriteString(`[\^`)
				wildcardPatternBuilder.WriteString(line[i+2 : end])
			default:
				wildcardPatternBuilder.WriteString(line[i:end])
			}
			i = end

		default:
			if c == '$' || c == '_' || isASCIIAlphanumeric(c) || c >= 0x80 {
				literalStringBuilder.WriteByte(c)
				wildcardPatternBuilder.WriteByte(c)
				i++
			} else {
				return nil, "", p.errorf("unexpected special character '%c'", c)
			}
		}
	}

	line = line[i:]
	if isLiteralString {
		return stringMatcher(literalStringBuilder.String()), line, nil
	}
	wildcardPatternBuilder.WriteByte('$')
	m, err := p.newRegexpMatcher(wildcardPatternBuilder.String())
	if err != nil {
		return nil, "", err
	}
	return m, line, nil
}

func (p *matcherParser) importFile(fileName string, parseMatcher func(string, bool) error) error {
	file, err := os.Open(fileName)
	if err != nil {
		return p.annotatef(err, "cannot open filter file")
	}
	defer file.Close()

	oldFileName, oldLineNum := p.fileName, p.lineNum
	p.fileName, p.lineNum = fileName, 1

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if err := parseMatcher(scanner.Text(), false); err != nil {
			return err
		}
		p.lineNum++
	}

	p.fileName, p.lineNum = oldFileName, oldLineNum

	if err := scanner.Err(); err != nil {
		return p.annotatef(err, "cannot read filter file")
	}
	return nil
}
