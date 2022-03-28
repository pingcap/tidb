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

package router

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/pingcap/errors"
	selector "github.com/pingcap/tidb/util/table-rule-selector"
)

// TableRule is a rule to route schema/table to target schema/table
// pattern format refers 'pkg/table-rule-selector'
type TableRule struct {
	SchemaPattern   string           `json:"schema-pattern" toml:"schema-pattern" yaml:"schema-pattern"`
	TablePattern    string           `json:"table-pattern" toml:"table-pattern" yaml:"table-pattern"`
	TargetSchema    string           `json:"target-schema" toml:"target-schema" yaml:"target-schema"`
	TargetTable     string           `json:"target-table" toml:"target-table" yaml:"target-table"`
	TableExtractor  *TableExtractor  `json:"extract-table,omitempty" toml:"extract-table,omitempty" yaml:"extract-table,omitempty"`
	SchemaExtractor *SchemaExtractor `json:"extract-schema,omitempty" toml:"extract-schema,omitempty" yaml:"extract-schema,omitempty"`
	SourceExtractor *SourceExtractor `json:"extract-source,omitempty" toml:"extract-source,omitempty" yaml:"extract-source,omitempty"`
}

// TableExtractor extracts table name to column
type TableExtractor struct {
	TargetColumn string `json:"target-column" toml:"target-column" yaml:"target-column"`
	TableRegexp  string `json:"table-regexp" toml:"table-regexp" yaml:"table-regexp"`
	regexp       *regexp.Regexp
}

// SchemaExtractor extracts schema name to column
type SchemaExtractor struct {
	TargetColumn string `json:"target-column" toml:"target-column" yaml:"target-column"`
	SchemaRegexp string `json:"schema-regexp" toml:"schema-regexp" yaml:"schema-regexp"`
	regexp       *regexp.Regexp
}

// SourceExtractor extracts source name to column
type SourceExtractor struct {
	TargetColumn string `json:"target-column" toml:"target-column" yaml:"target-column"`
	SourceRegexp string `json:"source-regexp" toml:"source-regexp" yaml:"source-regexp"`
	regexp       *regexp.Regexp
}

// Valid checks validity of rule
func (t *TableRule) Valid() error {
	if len(t.SchemaPattern) == 0 {
		return errors.New("schema pattern of table route rule should not be empty")
	}

	if len(t.TargetSchema) == 0 {
		return errors.New("target schema of table route rule should not be empty")
	}

	if t.TableExtractor != nil {
		tableRe := t.TableExtractor.TableRegexp
		re, err := regexp.Compile(tableRe)
		if err != nil {
			return fmt.Errorf("table extractor table regexp illegal %s", tableRe)
		}
		if len(t.TableExtractor.TargetColumn) == 0 {
			return errors.New("table extractor target column cannot be empty")
		}
		t.TableExtractor.regexp = re
	}
	if t.SchemaExtractor != nil {
		schemaRe := t.SchemaExtractor.SchemaRegexp
		re, err := regexp.Compile(schemaRe)
		if err != nil {
			return fmt.Errorf("schema extractor schema regexp illegal %s", schemaRe)
		}
		if len(t.SchemaExtractor.TargetColumn) == 0 {
			return errors.New("schema extractor target column cannot be empty")
		}
		t.SchemaExtractor.regexp = re
	}
	if t.SourceExtractor != nil {
		sourceRe := t.SourceExtractor.SourceRegexp
		re, err := regexp.Compile(sourceRe)
		if err != nil {
			return fmt.Errorf("source extractor source regexp illegal %s", sourceRe)
		}
		if len(t.SourceExtractor.TargetColumn) == 0 {
			return errors.New("source extractor target column cannot be empty")
		}
		t.SourceExtractor.regexp = re
	}
	return nil
}

// ToLower covert schema/table parttern to lower case
func (t *TableRule) ToLower() {
	t.SchemaPattern = strings.ToLower(t.SchemaPattern)
	t.TablePattern = strings.ToLower(t.TablePattern)
}

// Table routes schema/table to target schema/table by given route rules
type Table struct {
	selector.Selector

	caseSensitive bool
}

// NewTableRouter returns a table router
func NewTableRouter(caseSensitive bool, rules []*TableRule) (*Table, error) {
	r := &Table{
		Selector:      selector.NewTrieSelector(),
		caseSensitive: caseSensitive,
	}

	for _, rule := range rules {
		if err := r.AddRule(rule); err != nil {
			return nil, errors.Annotatef(err, "initial rule %+v in table router", rule)
		}
	}

	return r, nil
}

// AddRule adds a rule into table router
func (r *Table) AddRule(rule *TableRule) error {
	err := rule.Valid()
	if err != nil {
		return errors.Trace(err)
	}
	if !r.caseSensitive {
		rule.ToLower()
	}

	err = r.Insert(rule.SchemaPattern, rule.TablePattern, rule, selector.Insert)
	if err != nil {
		return errors.Annotatef(err, "add rule %+v into table router", rule)
	}

	return nil
}

// UpdateRule updates rule
func (r *Table) UpdateRule(rule *TableRule) error {
	err := rule.Valid()
	if err != nil {
		return errors.Trace(err)
	}
	if !r.caseSensitive {
		rule.ToLower()
	}

	err = r.Insert(rule.SchemaPattern, rule.TablePattern, rule, selector.Replace)
	if err != nil {
		return errors.Annotatef(err, "update rule %+v into table router", rule)
	}

	return nil
}

// RemoveRule removes a rule from table router
func (r *Table) RemoveRule(rule *TableRule) error {
	if !r.caseSensitive {
		rule.ToLower()
	}

	err := r.Remove(rule.SchemaPattern, rule.TablePattern)
	if err != nil {
		return errors.Annotatef(err, "remove rule %+v from table router", rule)
	}

	return nil
}

// Route routes schema/table to target schema/table
// don't support to route schema/table to multiple schema/table
func (r *Table) Route(schema, table string) (string, string, error) {
	schemaL, tableL := schema, table
	if !r.caseSensitive {
		schemaL, tableL = strings.ToLower(schema), strings.ToLower(table)
	}

	rules := r.Match(schemaL, tableL)
	var (
		schemaRules = make([]*TableRule, 0, len(rules))
		tableRules  = make([]*TableRule, 0, len(rules))
	)
	// classify rules into schema level rules and table level
	// table level rules have highest priority
	for i := range rules {
		rule, ok := rules[i].(*TableRule)
		if !ok {
			return "", "", errors.NotValidf("table route rule %+v", rules[i])
		}

		if len(rule.TablePattern) == 0 {
			schemaRules = append(schemaRules, rule)
		} else {
			tableRules = append(tableRules, rule)
		}
	}

	var (
		targetSchema string
		targetTable  string
	)
	if len(table) == 0 || len(tableRules) == 0 {
		if len(schemaRules) > 1 {
			return "", "", errors.NotSupportedf("`%s`.`%s` matches %d schema route rules which is more than one.\nThe first two rules are %+v, %+v.\nIt's", schema, table, len(schemaRules), schemaRules[0], schemaRules[1])
		}

		if len(schemaRules) == 1 {
			targetSchema, targetTable = schemaRules[0].TargetSchema, schemaRules[0].TargetTable
		}
	} else {
		if len(tableRules) > 1 {
			return "", "", errors.NotSupportedf("`%s`.`%s` matches %d table route rules which is more than one.\nThe first two rules are %+v, %+v.\nIt's", schema, table, len(tableRules), tableRules[0], tableRules[1])
		}

		targetSchema, targetTable = tableRules[0].TargetSchema, tableRules[0].TargetTable
	}

	if len(targetSchema) == 0 {
		targetSchema = schema
	}

	if len(targetTable) == 0 {
		targetTable = table
	}

	return targetSchema, targetTable, nil
}

// ExtractVal match value via regexp
func (t *TableRule) extractVal(s string, ext interface{}) string {
	var params []string
	switch e := ext.(type) {
	case *TableExtractor:
		params = e.regexp.FindStringSubmatch(s)
	case *SchemaExtractor:
		params = e.regexp.FindStringSubmatch(s)
	case *SourceExtractor:
		params = e.regexp.FindStringSubmatch(s)
	}
	var val strings.Builder
	for idx, param := range params {
		if idx > 0 {
			val.WriteString(param)
		}
	}
	return val.String()
}

// FetchExtendColumn get extract rule, return extracted cols and extracted vals.
func (r *Table) FetchExtendColumn(schema, table, source string) ([]string, []string) {
	var cols []string
	var vals []string
	rules := r.Match(schema, table)
	var (
		schemaRules = make([]*TableRule, 0, len(rules))
		tableRules  = make([]*TableRule, 0, len(rules))
	)
	for i := range rules {
		rule, ok := rules[i].(*TableRule)
		if !ok {
			return cols, vals
		}
		if len(rule.TablePattern) == 0 {
			schemaRules = append(schemaRules, rule)
		} else {
			tableRules = append(tableRules, rule)
		}
	}
	if len(tableRules) == 0 && len(schemaRules) == 0 {
		return cols, vals
	}
	var rule *TableRule
	// table level rules have highest priority
	if len(tableRules) == 0 {
		rule = schemaRules[0]
	} else {
		rule = tableRules[0]
	}

	if rule.TableExtractor != nil {
		cols = append(cols, rule.TableExtractor.TargetColumn)
		vals = append(vals, rule.extractVal(table, rule.TableExtractor))
	}

	if rule.SchemaExtractor != nil {
		cols = append(cols, rule.SchemaExtractor.TargetColumn)
		vals = append(vals, rule.extractVal(schema, rule.SchemaExtractor))
	}

	if rule.SourceExtractor != nil {
		cols = append(cols, rule.SourceExtractor.TargetColumn)
		vals = append(vals, rule.extractVal(source, rule.SourceExtractor))
	}
	return cols, vals
}
