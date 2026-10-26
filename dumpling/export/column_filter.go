// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"os"
	"strings"

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
	md, err := toml.Decode(string(content), &columnFilter)
	if err != nil {
		return columnFilterConfig{}, errors.Annotatef(err, "failed to parse --column-filter-file %s", path)
	}
	if err := validateColumnFilterKeys(md, flagColumnFilterFile); err != nil {
		return columnFilterConfig{}, err
	}
	if err := columnFilter.compileForOption(caseSensitive, flagColumnFilterFile); err != nil {
		return columnFilterConfig{}, err
	}
	return columnFilter, nil
}

func (c *columnFilterConfig) compileForOption(caseSensitive bool, flagName string) error {
	if len(c.Filters) == 0 {
		return errors.Errorf("--%s requires at least one column filter", flagName)
	}

	for i := range c.Filters {
		rule := &c.Filters[i]
		if len(rule.Matcher) == 0 {
			return errors.Errorf("--%s filter %d requires at least one matcher", flagName, i)
		}
		if len(rule.Columns) == 0 {
			return errors.Errorf("--%s filter %d requires at least one column rule", flagName, i)
		}
		tableFilter, err := filter.Parse(rule.Matcher)
		if err != nil {
			return errors.Annotatef(err, "failed to parse --%s filter %d matcher", flagName, i)
		}
		if !caseSensitive {
			tableFilter = filter.CaseInsensitive(tableFilter)
		}
		columnRules, err := filter.ParseColumnFilterRules(rule.Columns)
		if err != nil {
			return errors.Annotatef(err, "failed to parse --%s filter %d columns", flagName, i)
		}
		rule.tableFilter = tableFilter
		rule.columnRules = columnRules
	}
	return nil
}

func parseColumnFilterArgs(args []string, caseSensitive bool) (columnFilterConfig, error) {
	columnFilter := columnFilterConfig{
		Filters: make([]columnFilterRule, 0, len(args)),
	}
	for i, arg := range args {
		var wrapper struct {
			Filter columnFilterRule `toml:"filter"`
		}
		// Decode a single inline rule by wrapping it as a one-field TOML document.
		md, err := toml.Decode("filter = "+arg, &wrapper)
		if err != nil {
			return columnFilterConfig{}, errors.Annotatef(err, "failed to parse --column-filter %d", i)
		}
		if err := validateColumnFilterKeys(md, flagColumnFilter); err != nil {
			return columnFilterConfig{}, err
		}
		columnFilter.Filters = append(columnFilter.Filters, wrapper.Filter)
	}
	if err := columnFilter.compileForOption(caseSensitive, flagColumnFilter); err != nil {
		return columnFilterConfig{}, err
	}
	return columnFilter, nil
}

func validateColumnFilterKeys(md toml.MetaData, flagName string) error {
	undecoded := md.Undecoded()
	if len(undecoded) == 0 {
		return nil
	}

	keys := make([]string, 0, len(undecoded))
	for _, key := range undecoded {
		keys = append(keys, key.String())
	}
	return errors.Errorf("--%s contains unknown TOML keys: %s", flagName, strings.Join(keys, ", "))
}

func (c *columnFilterConfig) applyToColumns(database, table string, sourceColumns []string) ([]string, []int, error) {
	columnRules, matched := c.matchColumnRules(database, table)

	selectedIndexes := make([]int, 0, len(sourceColumns))
	if !matched {
		for i := range sourceColumns {
			selectedIndexes = append(selectedIndexes, i)
		}
		return sourceColumns, selectedIndexes, nil
	}

	selectedColumns := make([]string, 0, len(sourceColumns))
	for i, column := range sourceColumns {
		if columnRules.MatchColumn(column) {
			selectedColumns = append(selectedColumns, column)
			selectedIndexes = append(selectedIndexes, i)
		}
	}
	if len(selectedColumns) == 0 {
		return nil, nil, errors.Errorf(
			"column filter selects no writable columns from table `%s`.`%s`",
			escapeString(database),
			escapeString(table),
		)
	}

	return selectedColumns, selectedIndexes, nil
}

func (c *columnFilterConfig) matchColumnRules(database, table string) (filter.ColumnFilterRules, bool) {
	var columnRules filter.ColumnFilterRules
	matched := false
	// Later TOML [[filters]] entries take precedence over earlier ones.
	for i := len(c.Filters) - 1; i >= 0; i-- {
		rule := c.Filters[i]
		if rule.tableFilter.MatchTable(database, table) {
			matched = true
			columnRules = append(columnRules, rule.columnRules...)
		}
	}
	return columnRules, matched
}
