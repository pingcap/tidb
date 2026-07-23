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

// ColumnSelectorMode controls how column selector matches are applied.
type ColumnSelectorMode string

const (
	// ColumnSelectorModeInclude exports only the union of matched selector columns.
	ColumnSelectorModeInclude ColumnSelectorMode = "INCLUDE"
	// ColumnSelectorModeExclude exports all columns except the union of matched selector columns.
	ColumnSelectorModeExclude ColumnSelectorMode = "EXCLUDE"
)

// ColumnSelectors stores table matchers and columns used to project data output.
type ColumnSelectors struct {
	Mode      ColumnSelectorMode `json:"mode"`
	Selectors []ColumnSelector   `json:"columnSelectors"`
}

// ColumnSelector maps a set of table matchers to a set of column names.
type ColumnSelector struct {
	Matcher []string `json:"matcher"`
	Columns []string `json:"columns"`

	tableFilter filter.Filter
	columnSet   map[string]struct{}
}

// ParseColumnSelectorsFile parses a JSON column selectors file.
func ParseColumnSelectorsFile(path string, caseSensitive bool) (*ColumnSelectors, error) {
	if strings.TrimSpace(path) == "" {
		return nil, nil
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.Annotatef(err, "failed to read --column-selectors-file %s", path)
	}
	var selectors ColumnSelectors
	if err := json.Unmarshal(content, &selectors); err != nil {
		return nil, errors.Annotatef(err, "failed to parse --column-selectors-file %s", path)
	}
	if err := selectors.compile(caseSensitive); err != nil {
		return nil, err
	}
	return &selectors, nil
}

func (s *ColumnSelectors) compile(caseSensitive bool) error {
	mode := ColumnSelectorMode(strings.ToUpper(strings.TrimSpace(string(s.Mode))))
	switch mode {
	case ColumnSelectorModeInclude, ColumnSelectorModeExclude:
		s.Mode = mode
	default:
		return errors.Errorf("--column-selectors-file mode must be INCLUDE or EXCLUDE, got %q", s.Mode)
	}
	if len(s.Selectors) == 0 {
		return errors.New("--column-selectors-file requires at least one column selector")
	}
	for i := range s.Selectors {
		selector := &s.Selectors[i]
		if len(selector.Matcher) == 0 {
			return errors.Errorf("--column-selectors-file selector %d requires at least one matcher", i)
		}
		if s.Mode == ColumnSelectorModeInclude && len(selector.Columns) == 0 {
			return errors.Errorf("--column-selectors-file selector %d requires at least one column in INCLUDE mode", i)
		}
		tableFilter, err := filter.Parse(selector.Matcher)
		if err != nil {
			return errors.Annotatef(err, "failed to parse --column-selectors-file selector %d matcher", i)
		}
		if !caseSensitive {
			tableFilter = filter.CaseInsensitive(tableFilter)
		}
		selector.tableFilter = tableFilter
		selector.columnSet = make(map[string]struct{}, len(selector.Columns))
		for _, column := range selector.Columns {
			normalizedColumn := strings.ToLower(strings.TrimSpace(column))
			if normalizedColumn == "" {
				return errors.Errorf("--column-selectors-file selector %d contains an empty column name", i)
			}
			if _, ok := selector.columnSet[normalizedColumn]; ok {
				return errors.Errorf("--column-selectors-file selector %d contains duplicate column %q", i, column)
			}
			selector.columnSet[normalizedColumn] = struct{}{}
		}
	}
	return nil
}

func (s *ColumnSelectors) applyToColumns(database, table string, sourceColumns []string) ([]string, error) {
	selectedColumns := s.matchColumns(database, table)
	if len(selectedColumns) == 0 {
		return sourceColumns, nil
	}

	var matchedIncludedColumns map[string]struct{}
	if s.Mode == ColumnSelectorModeInclude {
		matchedIncludedColumns = make(map[string]struct{}, len(selectedColumns))
	}
	filteredColumns := make([]string, 0, len(sourceColumns))
	for _, column := range sourceColumns {
		if !s.shouldIncludeColumn(selectedColumns, column) {
			continue
		}
		if s.Mode == ColumnSelectorModeInclude {
			matchedIncludedColumns[strings.ToLower(column)] = struct{}{}
		}
		filteredColumns = append(filteredColumns, column)
	}
	if s.Mode == ColumnSelectorModeInclude {
		unmatched := unmatchedColumns(selectedColumns, matchedIncludedColumns)
		if len(unmatched) > 0 {
			return nil, errors.Errorf(
				"included columns %s do not exist in writable columns of table `%s`.`%s`",
				strings.Join(unmatched, ","),
				escapeString(database),
				escapeString(table),
			)
		}
	}
	if len(sourceColumns) > 0 && len(filteredColumns) == 0 {
		return nil, errors.Errorf(
			"--column-selectors-file selects no writable columns from table `%s`.`%s`",
			escapeString(database),
			escapeString(table),
		)
	}

	return filteredColumns, nil
}

func (s *ColumnSelectors) matchColumns(database, table string) map[string]struct{} {
	selectedColumns := make(map[string]struct{})
	for _, selector := range s.Selectors {
		if !selector.tableFilter.MatchTable(database, table) {
			continue
		}
		for column := range selector.columnSet {
			selectedColumns[column] = struct{}{}
		}
	}
	return selectedColumns
}

func (s *ColumnSelectors) shouldIncludeColumn(selectedColumns map[string]struct{}, column string) bool {
	_, ok := selectedColumns[strings.ToLower(column)]
	if s.Mode == ColumnSelectorModeInclude {
		return ok
	}
	return !ok
}

func unmatchedColumns(selectedColumns, matched map[string]struct{}) []string {
	unmatched := make([]string, 0)
	for column := range selectedColumns {
		if _, ok := matched[column]; !ok {
			unmatched = append(unmatched, column)
		}
	}
	sort.Strings(unmatched)
	return unmatched
}
