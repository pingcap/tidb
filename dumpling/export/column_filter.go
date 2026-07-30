// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"encoding/json"
	"os"
	"sort"
	"strings"

	"github.com/pingcap/errors"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
)

// ColumnFilterConfig stores table matchers and column filter rules used to project data output.
type ColumnFilterConfig struct {
	Filters []ColumnFilterRule `json:"columnFilters"`
}

// ColumnFilterRule maps a set of table matchers to a set of column filter rules.
type ColumnFilterRule struct {
	Matcher []string `json:"matcher"`
	Columns []string `json:"columns"`

	tableFilter filter.Filter
}

// ParseColumnFilterFile parses a JSON column filter file.
func ParseColumnFilterFile(path string, caseSensitive bool) (*ColumnFilterConfig, error) {
	if strings.TrimSpace(path) == "" {
		return nil, nil
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.Annotatef(err, "failed to read --column-filter-file %s", path)
	}
	var columnFilter ColumnFilterConfig
	if err := json.Unmarshal(content, &columnFilter); err != nil {
		return nil, errors.Annotatef(err, "failed to parse --column-filter-file %s", path)
	}
	if err := columnFilter.compile(caseSensitive); err != nil {
		return nil, err
	}
	return &columnFilter, nil
}

func (c *ColumnFilterConfig) compile(caseSensitive bool) error {
	if len(c.Filters) == 0 {
		return errors.New("--column-filter-file requires at least one column filter")
	}
	for i := range c.Filters {
		rule := &c.Filters[i]
		if len(rule.Matcher) == 0 {
			return errors.Errorf("--column-filter-file filter %d requires at least one matcher", i)
		}
		tableFilter, err := filter.Parse(rule.Matcher)
		if err != nil {
			return errors.Annotatef(err, "failed to parse --column-filter-file filter %d matcher", i)
		}
		if !caseSensitive {
			tableFilter = filter.CaseInsensitive(tableFilter)
		}
		rule.tableFilter = tableFilter
		if _, err = filter.ParseColumnFilter(activeColumnRules(rule.Columns)); err != nil {
			return errors.Annotatef(err, "failed to parse --column-filter-file filter %d columns", i)
		}
	}
	return nil
}

func (c *ColumnFilterConfig) applyToColumns(database, table string, sourceColumns []string) ([]string, error) {
	columnRules := c.matchColumnRules(database, table)
	if len(columnRules) == 0 {
		return sourceColumns, nil
	}

	columnFilter, err := filter.ParseColumnFilter(columnRules)
	if err != nil {
		return nil, errors.Annotatef(
			err,
			"failed to parse --column-filter-file columns for table `%s`.`%s`",
			escapeString(database),
			escapeString(table),
		)
	}
	if err = validatePositiveColumnRules(database, table, columnRules, sourceColumns); err != nil {
		return nil, err
	}

	filteredColumns := make([]string, 0, len(sourceColumns))
	for _, column := range sourceColumns {
		if !columnFilter.MatchColumn(column) {
			continue
		}
		filteredColumns = append(filteredColumns, column)
	}
	if len(sourceColumns) > 0 && len(filteredColumns) == 0 {
		return nil, errors.Errorf(
			"--column-filter-file selects no writable columns from table `%s`.`%s`",
			escapeString(database),
			escapeString(table),
		)
	}

	return filteredColumns, nil
}

func (c *ColumnFilterConfig) matchColumnRules(database, table string) []string {
	var columnRules []string
	for _, rule := range c.Filters {
		if !rule.tableFilter.MatchTable(database, table) {
			continue
		}
		columnRules = append(columnRules, rule.Columns...)
	}
	return activeColumnRules(columnRules)
}

func activeColumnRules(columnRules []string) []string {
	activeRules := make([]string, 0, len(columnRules))
	for _, rule := range columnRules {
		rule = strings.Trim(rule, " \t")
		if rule == "" || rule[0] == '#' {
			continue
		}
		activeRules = append(activeRules, normalizeColumnRule(rule))
	}
	return activeRules
}

func normalizeColumnRule(rule string) string {
	if rule == "" {
		return rule
	}
	if rule[0] == '!' {
		if len(rule) == 1 {
			return rule
		}
		return "!" + normalizeColumnPattern(rule[1:])
	}
	return normalizeColumnPattern(rule)
}

func normalizeColumnPattern(pattern string) string {
	if pattern == "" {
		return pattern
	}
	switch pattern[0] {
	case '/', '"', '`', '@':
		return pattern
	}
	if strings.ContainsAny(pattern, `*?[\`) {
		return pattern
	}
	return quoteColumnPattern(pattern)
}

func quoteColumnPattern(column string) string {
	return `"` + strings.ReplaceAll(column, `"`, `""`) + `"`
}

func validatePositiveColumnRules(database, table string, columnRules, sourceColumns []string) error {
	if len(sourceColumns) == 0 {
		return nil
	}

	unmatched := make([]string, 0)
	for _, rule := range columnRules {
		if !isPositiveColumnRule(rule) {
			continue
		}
		matched, err := columnRuleMatches(rule, sourceColumns)
		if err != nil {
			return err
		}
		if !matched {
			unmatched = append(unmatched, rule)
		}
	}
	sort.Strings(unmatched)
	if len(unmatched) == 0 {
		return nil
	}
	return errors.Errorf(
		"included column rules %s do not match writable columns of table `%s`.`%s`",
		strings.Join(unmatched, ","),
		escapeString(database),
		escapeString(table),
	)
}

func isPositiveColumnRule(rule string) bool {
	if rule == "" {
		return false
	}
	switch rule[0] {
	case '!', '@':
		return false
	default:
		return true
	}
}

func columnRuleMatches(rule string, sourceColumns []string) (bool, error) {
	columnFilter, err := filter.ParseColumnFilter([]string{rule})
	if err != nil {
		return false, errors.Trace(err)
	}
	for _, column := range sourceColumns {
		if columnFilter.MatchColumn(column) {
			return true, nil
		}
	}
	return false, nil
}
