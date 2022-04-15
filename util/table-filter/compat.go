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

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// Table represents a qualified table name.
type Table struct {
	// Schema is the name of the schema (database) containing this table.
	Schema string `toml:"db-name" json:"db-name" yaml:"db-name"`
	// Name is the unqualified table name.
	Name string `toml:"tbl-name" json:"tbl-name" yaml:"tbl-name"`
}

// nolint:unused
func (t *Table) lessThan(u *Table) bool {
	return t.Schema < u.Schema || t.Schema == u.Schema && t.Name < u.Name
}

// String implements the fmt.Stringer interface.
func (t *Table) String() string {
	if len(t.Name) > 0 {
		return fmt.Sprintf("`%s`.`%s`", t.Schema, t.Name)
	}
	return fmt.Sprintf("`%s`", t.Schema)
}

// Clone clones a new filter.Table
func (t *Table) Clone() *Table {
	return &Table{
		Schema: t.Schema,
		Name:   t.Name,
	}
}

// MySQLReplicationRules is a set of rules based on MySQL's replication tableFilter.
type MySQLReplicationRules struct {
	// DoTables is an allowlist of tables.
	DoTables []*Table `json:"do-tables" toml:"do-tables" yaml:"do-tables"`
	// DoDBs is an allowlist of schemas.
	DoDBs []string `json:"do-dbs" toml:"do-dbs" yaml:"do-dbs"`

	// IgnoreTables is a blocklist of tables.
	IgnoreTables []*Table `json:"ignore-tables" toml:"ignore-tables" yaml:"ignore-tables"`
	// IgnoreDBs is a blocklist of schemas.
	IgnoreDBs []string `json:"ignore-dbs" toml:"ignore-dbs" yaml:"ignore-dbs"`
}

// ToLower convert all entries to lowercase
// Deprecated: use `filter.CaseInsensitive` instead.
func (r *MySQLReplicationRules) ToLower() {
	if r == nil {
		return
	}

	for _, table := range r.DoTables {
		table.Name = strings.ToLower(table.Name)
		table.Schema = strings.ToLower(table.Schema)
	}
	for _, table := range r.IgnoreTables {
		table.Name = strings.ToLower(table.Name)
		table.Schema = strings.ToLower(table.Schema)
	}
	for i, db := range r.IgnoreDBs {
		r.IgnoreDBs[i] = strings.ToLower(db)
	}
	for i, db := range r.DoDBs {
		r.DoDBs[i] = strings.ToLower(db)
	}
}

type schemasFilter struct {
	schemas map[string]struct{}
}

func (f schemasFilter) MatchTable(schema string, table string) bool {
	return f.MatchSchema(schema)
}

func (f schemasFilter) MatchSchema(schema string) bool {
	_, ok := f.schemas[schema]
	return ok
}

func (f schemasFilter) toLower() Filter {
	loweredSchemas := make(map[string]struct{}, len(f.schemas))
	for schema := range f.schemas {
		loweredSchemas[strings.ToLower(schema)] = struct{}{}
	}
	return schemasFilter{schemas: loweredSchemas}
}

// NewSchemasFilter creates a tableFilter which only accepts a list of schemas.
func NewSchemasFilter(schemas ...string) Filter {
	schemaMap := make(map[string]struct{}, len(schemas))
	for _, schema := range schemas {
		schemaMap[schema] = struct{}{}
	}
	return schemasFilter{schemas: schemaMap}
}

type tablesFilter struct {
	schemas map[string]map[string]struct{}
}

func (f tablesFilter) MatchTable(schema string, table string) bool {
	t, ok := f.schemas[schema]
	if !ok {
		return false
	}
	_, ok = t[table]
	return ok
}

func (f tablesFilter) MatchSchema(schema string) bool {
	_, ok := f.schemas[schema]
	return ok
}

func (f tablesFilter) toLower() Filter {
	loweredSchemas := make(map[string]map[string]struct{}, len(f.schemas))
	for schema, tables := range f.schemas {
		loweredSchema := strings.ToLower(schema)
		loweredTables, ok := loweredSchemas[loweredSchema]
		if !ok {
			loweredTables = make(map[string]struct{}, len(tables))
		}
		for table := range tables {
			loweredTables[strings.ToLower(table)] = struct{}{}
		}
		loweredSchemas[loweredSchema] = loweredTables
	}
	return tablesFilter{schemas: loweredSchemas}
}

// NewTablesFilter creates a tableFilter which only accepts a list of tables.
func NewTablesFilter(tables ...Table) Filter {
	schemas := make(map[string]map[string]struct{})
	for _, table := range tables {
		tbls, ok := schemas[table.Schema]
		if !ok {
			tbls = make(map[string]struct{})
		}
		tbls[table.Name] = struct{}{}
		schemas[table.Schema] = tbls
	}
	return tablesFilter{schemas: schemas}
}

// bothFilter is a tableFilter which passes if both filters in the field passes.
type bothFilter struct {
	a Filter
	b Filter
}

func (f *bothFilter) MatchTable(schema string, table string) bool {
	return f.a.MatchTable(schema, table) && f.b.MatchTable(schema, table)
}

func (f *bothFilter) MatchSchema(schema string) bool {
	return f.a.MatchSchema(schema) && f.b.MatchSchema(schema)
}

func (f *bothFilter) toLower() Filter {
	return &bothFilter{
		a: f.a.toLower(),
		b: f.b.toLower(),
	}
}

var legacyWildcardReplacer = strings.NewReplacer(
	`\*`, ".*",
	`\?`, ".",
	`\[!`, "[^",
	`\[`, "[",
	`\]`, "]",
)

func matcherFromLegacyPattern(pattern string) (matcher, error) {
	if len(pattern) == 0 {
		return nil, errors.New("pattern cannot be empty")
	}
	if pattern[0] == '~' {
		// this is a regexp pattern.
		return newRegexpMatcher(pattern[1:])
	}

	if !strings.ContainsAny(pattern, "?*[") {
		// this is a literal string.
		return stringMatcher(pattern), nil
	}

	// this is a wildcard.
	pattern = "(?s)^" + legacyWildcardReplacer.Replace(regexp.QuoteMeta(pattern)) + "$"
	return newRegexpMatcher(pattern)
}

// ParseMySQLReplicationRules constructs up to 2 filters from the MySQLReplicationRules.
// Tables have to pass *both* filters to be processed.
func ParseMySQLReplicationRules(rules *MySQLReplicationRules) (Filter, error) {
	if rules == nil {
		return All(), nil
	}
	schemas := rules.DoDBs
	positive := true
	rulesLen := len(schemas)
	if rulesLen == 0 {
		schemas = rules.IgnoreDBs
		positive = false
		rulesLen = len(schemas) + 1
	}

	schemaRules := make([]tableRule, 0, rulesLen)
	for _, schema := range schemas {
		m, err := matcherFromLegacyPattern(schema)
		if err != nil {
			return nil, err
		}
		schemaRules = append(schemaRules, tableRule{
			schema:   m,
			table:    trueMatcher{},
			positive: positive,
		})
	}
	if !positive {
		schemaRules = append(schemaRules, tableRule{
			schema:   trueMatcher{},
			table:    trueMatcher{},
			positive: true,
		})
	}

	tables := rules.DoTables
	positive = true
	rulesLen = len(tables)
	if len(tables) == 0 {
		tables = rules.IgnoreTables
		positive = false
		rulesLen = len(tables) + 1
	}

	tableRules := make([]tableRule, 0, rulesLen)
	for _, table := range tables {
		sm, err := matcherFromLegacyPattern(table.Schema)
		if err != nil {
			return nil, err
		}
		tm, err := matcherFromLegacyPattern(table.Name)
		if err != nil {
			return nil, err
		}
		tableRules = append(tableRules, tableRule{
			schema:   sm,
			table:    tm,
			positive: positive,
		})
	}
	if !positive {
		tableRules = append(tableRules, tableRule{
			schema:   trueMatcher{},
			table:    trueMatcher{},
			positive: true,
		})
	}

	return &bothFilter{a: tableFilter(schemaRules), b: tableFilter(tableRules)}, nil
}
