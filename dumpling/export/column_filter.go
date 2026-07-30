// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"os"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
)

type columnFilterConfig struct {
	Filters []columnFilterRule `toml:"filters"`
}

type columnFilterRule struct {
	Matcher []string `toml:"matcher"`
	Columns []string `toml:"columns"`

	tableFilter filter.Filter
	columnRules filter.ColumnFilterRules
}

func parseColumnFilterConfig(path string, caseSensitive bool) (columnFilterConfig, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return columnFilterConfig{}, errors.Annotatef(err, "failed to read --column-filter-file %s", path)
	}
	var columnFilter columnFilterConfig
	if _, err := toml.Decode(string(content), &columnFilter); err != nil {
		return columnFilterConfig{}, errors.Annotatef(err, "failed to parse --column-filter-file %s", path)
	}
	if err := columnFilter.compile(caseSensitive); err != nil {
		return columnFilterConfig{}, err
	}
	return columnFilter, nil
}

func (c *columnFilterConfig) compile(caseSensitive bool) error {
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

func (c *columnFilterConfig) applyToColumns(database, table string, sourceColumns []string) ([]string, error) {
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
	if len(filteredColumns) == 0 {
		return nil, errors.Errorf(
			"--column-filter-file selects no writable columns from table `%s`.`%s`",
			escapeString(database),
			escapeString(table),
		)
	}

	return filteredColumns, nil
}

func (c *columnFilterConfig) matchColumnRules(database, table string) filter.ColumnFilterRules {
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
