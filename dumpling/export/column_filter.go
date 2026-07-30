// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"os"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
)

// ColumnFilterConfig stores table matchers and column filter rules used to project data output.
type ColumnFilterConfig struct {
	Filters []ColumnFilterRule `toml:"filters"`
}

// ColumnFilterRule maps a set of table matchers to a set of column filter rules.
type ColumnFilterRule struct {
	Matcher []string `toml:"matcher"`
	Columns []string `toml:"columns"`

	tableFilter filter.Filter
	columnRules filter.ColumnFilterRules
}

// ParseColumnFilterFile parses a TOML column filter file.
func ParseColumnFilterFile(path string, caseSensitive bool) (*ColumnFilterConfig, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.Annotatef(err, "failed to read --column-filter-file %s", path)
	}
	var columnFilter ColumnFilterConfig
	if _, err := toml.Decode(string(content), &columnFilter); err != nil {
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
		columnRules, err := filter.ParseColumnFilter(rule.Columns)
		if err != nil {
			return errors.Annotatef(err, "failed to parse --column-filter-file filter %d columns", i)
		}
		rule.tableFilter = tableFilter
		rule.columnRules = columnRules
	}
	return nil
}

func (c *ColumnFilterConfig) applyToColumns(database, table string, sourceColumns []string) ([]string, error) {
	columnRules := c.matchColumnRules(database, table)
	if len(columnRules) == 0 {
		return sourceColumns, nil
	}

	filteredColumns := make([]string, 0, len(sourceColumns))
	for _, column := range sourceColumns {
		if !columnRules.MatchColumn(column) {
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

func (c *ColumnFilterConfig) matchColumnRules(database, table string) filter.ColumnFilterRules {
	var columnRules filter.ColumnFilterRules
	for i := len(c.Filters) - 1; i >= 0; i-- {
		rule := c.Filters[i]
		if !rule.tableFilter.MatchTable(database, table) {
			continue
		}
		columnRules = append(columnRules, rule.columnRules...)
	}
	return columnRules
}
