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
	"strings"
)

// Filter is a structure to check if a table should be included for processing.
type Filter interface {
	// MatchTable checks if a table can be processed after applying the tableFilter.
	MatchTable(schema string, table string) bool
	// MatchSchema checks if a schema can be processed after applying the tableFilter.
	MatchSchema(schema string) bool
	// toLower changes the tableFilter to compare with case-insensitive strings.
	toLower() Filter
}

// tableFilter is a concrete implementation of Filter.
type tableFilter []tableRule

// Parse a tableFilter from a list of serialized tableFilter rules. The parsed tableFilter is
// case-sensitive by default.
func Parse(args []string) (Filter, error) {
	p := tableRulesParser{
		make([]tableRule, 0, len(args)),
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

	return tableFilter(p.rules), nil
}

// CaseInsensitive returns a new tableFilter which is the case-insensitive version of
// the input tableFilter.
func CaseInsensitive(f Filter) Filter {
	return loweredFilter{wrapped: f.toLower()}
}

// MatchTable checks if a table can be processed after applying the tableFilter `f`.
func (f tableFilter) MatchTable(schema string, table string) bool {
	for _, rule := range f {
		if rule.schema.matchString(schema) && rule.table.matchString(table) {
			return rule.positive
		}
	}
	return false
}

// MatchSchema checks if a schema can be processed after applying the tableFilter `f`.
func (f tableFilter) MatchSchema(schema string) bool {
	for _, rule := range f {
		if rule.schema.matchString(schema) && (rule.positive || rule.table.matchAllStrings()) {
			return rule.positive
		}
	}
	return false
}

func (f tableFilter) toLower() Filter {
	rules := make([]tableRule, 0, len(f))
	for _, r := range f {
		rules = append(rules, tableRule{
			schema:   r.schema.toLower(),
			table:    r.table.toLower(),
			positive: r.positive,
		})
	}
	return tableFilter(rules)
}

type loweredFilter struct {
	wrapped Filter
}

func (f loweredFilter) MatchTable(schema string, table string) bool {
	return f.wrapped.MatchTable(strings.ToLower(schema), strings.ToLower(table))
}

func (f loweredFilter) MatchSchema(schema string) bool {
	return f.wrapped.MatchSchema(strings.ToLower(schema))
}

func (f loweredFilter) toLower() Filter {
	return f
}

type allFilter struct{}

func (allFilter) MatchTable(string, string) bool {
	return true
}

func (allFilter) MatchSchema(string) bool {
	return true
}

func (f allFilter) toLower() Filter {
	return f
}

// All creates a tableFilter which matches everything.
func All() Filter {
	return allFilter{}
}
