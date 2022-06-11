// Copyright 2022 PingCAP, Inc.
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

import "strings"

// ColumnFilter is a structure to check if a column should be included for processing.
type ColumnFilter interface {
	// MatchColumn checks if a column can be processed after applying the columnFilter.
	MatchColumn(column string) bool
}

// columnFilter is a concrete implementation of ColumnFilter.
type columnFilter []columnRule

// ParseColumnFilter a columnFilter from a list of serialized columnFilter rules.
// Column is not case-sensitive on any platform, nor are column aliases.
// So the parsed columnFilter is case-insensitive.
func ParseColumnFilter(args []string) (ColumnFilter, error) {
	p := columnRulesParser{
		make([]columnRule, 0, len(args)),
		matcherParser{
			fileName: "<cmdline>",
			lineNum:  1,
		},
	}

	for _, arg := range args {
		if err := p.parse(arg, true); err != nil {
			return nil, err
		}
	}

	reverse(p.rules)

	return columnFilter(p.rules), nil
}

// MatchColumn checks if a column can be processed after applying the columnFilter `f`.
// Column is not case-sensitive on any platform, nor are column aliases.
// So we always match in lowercase.
// See also: https://dev.mysql.com/doc/refman/5.7/en/identifier-case-sensitivity.html
func (f columnFilter) MatchColumn(column string) bool {
	lowercaseColumn := strings.ToLower(column)
	for _, rule := range f {
		if rule.column.matchString(lowercaseColumn) {
			return rule.positive
		}
	}
	return false
}
