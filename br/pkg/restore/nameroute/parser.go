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

package nameroute

import (
	"fmt"
	"unicode"
	"unicode/utf8"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

// ParseRules parses rename specifications in either schema or table form.
// A specification has the form "source:target" or
// "source_schema.source_table:target_schema.target_table". Identifiers may be
// quoted with MySQL backticks; a backtick inside a quoted identifier is escaped
// by doubling it.
func ParseRules(specs []string) ([]Rule, error) {
	rules := make([]Rule, 0, len(specs))
	for i, spec := range specs {
		parser := specParser{input: spec}
		rule, err := parser.parseRule()
		if err != nil {
			return nil, fmt.Errorf("invalid rename rule %d %q: %w", i+1, spec, err)
		}
		rules = append(rules, rule)
	}
	return rules, nil
}

type specParser struct {
	input string
	pos   int
}

func (p *specParser) parseRule() (Rule, error) {
	p.skipSpaces()
	source, err := p.parseObjectName()
	if err != nil {
		return Rule{}, fmt.Errorf("invalid source: %w", err)
	}
	p.skipSpaces()
	if !p.consume(':') {
		return Rule{}, fmt.Errorf("expected ':' at byte %d", p.pos)
	}
	p.skipSpaces()
	target, err := p.parseObjectName()
	if err != nil {
		return Rule{}, fmt.Errorf("invalid target: %w", err)
	}
	p.skipSpaces()
	if p.pos != len(p.input) {
		return Rule{}, fmt.Errorf("unexpected character %q at byte %d", p.input[p.pos], p.pos)
	}
	if source.IsTable() != target.IsTable() {
		return Rule{}, fmt.Errorf("source and target must both name a schema or both name a table")
	}
	return Rule{Source: source, Target: target}, nil
}

func (p *specParser) parseObjectName() (ObjectName, error) {
	schema, err := p.parseIdentifier(mysql.MaxDatabaseNameLength)
	if err != nil {
		return ObjectName{}, err
	}
	p.skipSpaces()
	if !p.consume('.') {
		return ObjectName{Schema: ast.NewCIStr(schema)}, nil
	}
	p.skipSpaces()
	table, err := p.parseIdentifier(mysql.MaxTableNameLength)
	if err != nil {
		return ObjectName{}, err
	}
	return ObjectName{Schema: ast.NewCIStr(schema), Table: ast.NewCIStr(table)}, nil
}

func (p *specParser) parseIdentifier(maxLength int) (string, error) {
	if p.pos >= len(p.input) {
		return "", fmt.Errorf("expected identifier at byte %d", p.pos)
	}
	if p.input[p.pos] == '`' {
		return p.parseQuotedIdentifier(maxLength)
	}

	start := p.pos
	for p.pos < len(p.input) {
		r, size := utf8.DecodeRuneInString(p.input[p.pos:])
		if r == utf8.RuneError && size == 1 {
			return "", fmt.Errorf("identifier contains invalid UTF-8 at byte %d", p.pos)
		}
		if r == '.' || r == ':' || unicode.IsSpace(r) {
			break
		}
		if !isUnquotedIdentifierRune(r) {
			return "", fmt.Errorf("character %q at byte %d must be inside backticks", r, p.pos)
		}
		p.pos += size
	}
	if p.pos == start {
		return "", fmt.Errorf("expected identifier at byte %d", p.pos)
	}
	identifier := p.input[start:p.pos]
	if err := validateIdentifier(identifier, maxLength); err != nil {
		return "", err
	}
	return identifier, nil
}

func (p *specParser) parseQuotedIdentifier(maxLength int) (string, error) {
	start := p.pos
	p.pos++
	result := make([]byte, 0, 16)
	for p.pos < len(p.input) {
		if p.input[p.pos] == '`' {
			if p.pos+1 < len(p.input) && p.input[p.pos+1] == '`' {
				result = append(result, '`')
				p.pos += 2
				continue
			}
			p.pos++
			identifier := string(result)
			if err := validateIdentifier(identifier, maxLength); err != nil {
				return "", err
			}
			return identifier, nil
		}
		_, size := utf8.DecodeRuneInString(p.input[p.pos:])
		if size == 1 && p.input[p.pos] >= utf8.RuneSelf {
			return "", fmt.Errorf("identifier contains invalid UTF-8 at byte %d", p.pos)
		}
		result = append(result, p.input[p.pos:p.pos+size]...)
		p.pos += size
	}
	return "", fmt.Errorf("unterminated quoted identifier starting at byte %d", start)
}

func (p *specParser) skipSpaces() {
	for p.pos < len(p.input) {
		r, size := utf8.DecodeRuneInString(p.input[p.pos:])
		if !unicode.IsSpace(r) {
			return
		}
		p.pos += size
	}
}

func (p *specParser) consume(ch byte) bool {
	if p.pos >= len(p.input) || p.input[p.pos] != ch {
		return false
	}
	p.pos++
	return true
}

func isUnquotedIdentifierRune(r rune) bool {
	if r >= utf8.RuneSelf {
		return !unicode.IsSpace(r) && !unicode.IsControl(r)
	}
	return r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '$'
}

func validateIdentifier(identifier string, maxLength int) error {
	if identifier == "" {
		return fmt.Errorf("identifier must not be empty")
	}
	if !utf8.ValidString(identifier) {
		return fmt.Errorf("identifier contains invalid UTF-8")
	}
	for _, r := range identifier {
		if r == 0 {
			return fmt.Errorf("identifier must not contain NUL")
		}
	}
	if utf8.RuneCountInString(identifier) > maxLength {
		return fmt.Errorf("identifier %q exceeds the maximum length of %d characters", identifier, maxLength)
	}
	return nil
}
