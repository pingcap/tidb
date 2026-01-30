package audit

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/stringutil"
	tablefilter "github.com/pingcap/tidb/pkg/util/table-filter"
	"go.uber.org/zap"
)

var (
	// SchemaName is the database of audit log tables
	SchemaName = "mysql"
	// TableNameAuditLogFilters is the table of audit log filters
	TableNameAuditLogFilters = fmt.Sprintf("%s.%s", SchemaName, "audit_log_filters")
	// TableNameAuditLogFilterRules is the table of audit log filter rules
	TableNameAuditLogFilterRules = fmt.Sprintf("%s.%s", SchemaName, "audit_log_filter_rules")
)

func checkTableAccess(db, tbl, _ string, _ mysql.PrivilegeType, sem bool) []string {
	if db != SchemaName {
		return nil
	}

	fullTblName := strings.ToLower(fmt.Sprintf("%s.%s", db, tbl))
	if fullTblName != TableNameAuditLogFilters && fullTblName != TableNameAuditLogFilterRules {
		return nil
	}

	return requirePrivileges(sem)
}

var (
	// CreateFilterTableSQL creates audit log filter table
	CreateFilterTableSQL = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    	FILTER_NAME VARCHAR(128),
		CONTENT     TEXT,
    	PRIMARY KEY(FILTER_NAME)
    )`, TableNameAuditLogFilters)
	// InsertFilterSQL inserts a filter
	InsertFilterSQL = fmt.Sprintf(
		"INSERT INTO %s (FILTER_NAME, CONTENT) VALUES (%%?, %%?)", TableNameAuditLogFilters)
	// ReplaceFilterSQL replaces a filter
	ReplaceFilterSQL = fmt.Sprintf(
		"REPLACE INTO %s (FILTER_NAME, CONTENT) VALUES (%%?, %%?)", TableNameAuditLogFilters)
	// SelectFilterByNameSQL queries a filter by name
	SelectFilterByNameSQL = fmt.Sprintf(
		"SELECT FILTER_NAME, CONTENT FROM %s WHERE FILTER_NAME = %%?", TableNameAuditLogFilters)
	// DeleteFilterByNameSQL deletes a filter by name
	DeleteFilterByNameSQL = fmt.Sprintf(
		"DELETE FROM %s WHERE FILTER_NAME = %%?", TableNameAuditLogFilters)
	// SelectFilterListSQL queries all filters
	SelectFilterListSQL = fmt.Sprintf(
		"SELECT FILTER_NAME, CONTENT FROM %s", TableNameAuditLogFilters)

	// CreateFilterRuleTableSQL creates audit log filter rule table
	CreateFilterRuleTableSQL = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		USER        VARCHAR(64),
    	FILTER_NAME VARCHAR(128),
    	ENABLED     TINYINT(4),
    	PRIMARY KEY(FILTER_NAME, USER)
	)`, TableNameAuditLogFilterRules)
	// InsertFilterRuleSQL inerts a new filter rule
	InsertFilterRuleSQL = fmt.Sprintf(
		"INSERT INTO %s (USER, FILTER_NAME, ENABLED) VALUES (%%?, %%?, 1)", TableNameAuditLogFilterRules)
	// ReplaceFilterRuleSQL replaces a filter rule
	ReplaceFilterRuleSQL = fmt.Sprintf(
		"REPLACE INTO %s (USER, FILTER_NAME, ENABLED) VALUES (%%?, %%?, 1)", TableNameAuditLogFilterRules)
	// DeleteFilterRuleSQL deletes a filter rule
	DeleteFilterRuleSQL = fmt.Sprintf(
		"DELETE FROM %s WHERE (USER, FILTER_NAME) = (%%?, %%?)", TableNameAuditLogFilterRules)
	// SelectFilterRuleListSQL queries all filter rules
	SelectFilterRuleListSQL = fmt.Sprintf(
		"SELECT USER, FILTER_NAME, ENABLED FROM %s", TableNameAuditLogFilterRules)
	// SelectFilterRuleByUserAndFilterSQL queries a filter by user and filter name
	SelectFilterRuleByUserAndFilterSQL = fmt.Sprintf(
		"SELECT USER, FILTER_NAME FROM %s WHERE (USER, FILTER_NAME) = (%%?, %%?)", TableNameAuditLogFilterRules)
	// SelectFilterRuleByFilterSQL queries filters by filter name
	SelectFilterRuleByFilterSQL = fmt.Sprintf(
		"SELECT USER, FILTER_NAME FROM %s WHERE FILTER_NAME = %%?", TableNameAuditLogFilterRules)
	// UpdateFilterRuleEnabledSQL update a filter by user and filter name
	UpdateFilterRuleEnabledSQL = fmt.Sprintf(
		"UPDATE %s SET ENABLED = %%? WHERE (USER, FILTER_NAME) = (%%?, %%?)", TableNameAuditLogFilterRules)
)

// filter function names
const (
	FuncAuditLogCreateFilter = "audit_log_create_filter"
	FuncAuditLogRemoveFilter = "audit_log_remove_filter"
	FuncAuditLogCreateRule   = "audit_log_create_rule"
	FuncAuditLogRemoveRule   = "audit_log_remove_rule"
	FuncAuditLogEnableRule   = "audit_log_enable_rule"
	FuncAuditLogDisableRule  = "audit_log_disable_rule"
)

type filterFunc func(entry *LogEntry) bool

var trueFilter = filterFunc(func(_ *LogEntry) bool { return true })
var falseFilter = filterFunc(func(_ *LogEntry) bool { return false })

func valuesMatchSet[T any, E any](values []T, set []E, include bool, match func(T, E) bool) bool {
	if set == nil {
		return true
	}
	if include && len(values) == 0 {
		return false
	}
	for _, v := range values {
		for _, item := range set {
			if match(v, item) {
				return include
			}
		}
	}
	return !include
}

func eventClassMatch(v, m EventClass) bool {
	return v == m
}

func tableFilterMatch(v stmtctx.TableEntry, f tablefilter.Filter) bool {
	return f.MatchTable(v.DB, v.Table)
}

func statusCodeMatch(v int, codes []int) bool {
	if codes == nil {
		return true
	}
	for _, c := range codes {
		if v == c {
			return true
		}
	}
	return false
}

// FilterSpec is only exported for test
type FilterSpec struct {
	Classes        []string `json:"class,omitempty"`
	ClassesExclude []string `json:"class_excl,omitempty"`
	Tables         []string `json:"table,omitempty"`
	TablesExclude  []string `json:"table_excl,omitempty"`
	StatusCodes    []int    `json:"status_code,omitempty"`
}

// Validate checks whether the filter spec is valid
func (f *FilterSpec) Validate() error {
	for _, c := range f.Classes {
		if _, ok := getEventClass(c); !ok {
			return errors.Errorf("invalid event class name '%s'", c)
		}
	}
	for _, c := range f.ClassesExclude {
		if _, ok := getEventClass(c); !ok {
			return errors.Errorf("invalid event class name '%s'", c)
		}
	}
	for _, t := range f.Tables {
		if _, err := parseTableFilter(t); err != nil {
			return err
		}
	}
	for _, t := range f.TablesExclude {
		if _, err := parseTableFilter(t); err != nil {
			return err
		}
	}
	for _, c := range f.StatusCodes {
		if c != StatusCodeFailed && c != StatusCodeSuccess {
			return errors.Errorf("invalid status code '%d'", c)
		}
	}
	return nil
}

// LogFilter is only exported for test
type LogFilter struct {
	Name   string       `json:"-"`
	Filter []FilterSpec `json:"filter,omitempty"`
}

func newLogFilterFormJSON(name string, data []byte) (*LogFilter, error) {
	var f LogFilter
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, err
	}
	f.Name = name
	return &f, nil
}

// Normalize normalizes the filter
func (f *LogFilter) Normalize() (*LogFilter, error) {
	newFilter := *f
	for i := range newFilter.Filter {
		spec := &newFilter.Filter[i]
		for j, c := range spec.Classes {
			class, ok := getEventClass(c)
			if !ok {
				return nil, errors.Errorf("invalid event class: '%s'", c)
			}
			spec.Classes[j] = class.String()
		}
		for j, c := range spec.ClassesExclude {
			class, ok := getEventClass(c)
			if !ok {
				return nil, errors.Errorf("invalid event class: '%s'", c)
			}
			spec.ClassesExclude[j] = class.String()
		}
	}
	return &newFilter, nil
}

// ToJSON converts the filter to JSON string
func (f *LogFilter) ToJSON() (string, error) {
	bs, err := json.Marshal(f)
	if err != nil {
		return "", err
	}
	return string(bs), nil
}

// Validate checks whether the filter is valid
func (f *LogFilter) Validate() error {
	if f.Name == "" {
		return errors.New("filter name should not be empty")
	}
	for _, spec := range f.Filter {
		if err := spec.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// CreateFilterFunc creates a filter function from the filter
func (f *LogFilter) CreateFilterFunc() filterFunc {
	if len(f.Filter) == 0 {
		return trueFilter
	}

	classes := make([][]EventClass, len(f.Filter))
	exclClasses := make([][]EventClass, len(f.Filter))
	tableFilters := make([][]tablefilter.Filter, len(f.Filter))
	exclTableFilters := make([][]tablefilter.Filter, len(f.Filter))
	for i, spec := range f.Filter {
		classes[i] = f.getEventClassFilterList(spec.Classes)
		exclClasses[i] = f.getEventClassFilterList(spec.ClassesExclude)
		tableFilters[i] = f.getTableFilterList(spec.Tables)
		exclTableFilters[i] = f.getTableFilterList(spec.TablesExclude)
	}

	return func(entry *LogEntry) bool {
		statusCode := getStatusCode(entry.Err)
		for i, spec := range f.Filter {
			if valuesMatchSet(entry.Classes, classes[i], true, eventClassMatch) &&
				valuesMatchSet(entry.Classes, exclClasses[i], false, eventClassMatch) &&
				valuesMatchSet(entry.Tables, tableFilters[i], true, tableFilterMatch) &&
				valuesMatchSet(entry.Tables, exclTableFilters[i], false, tableFilterMatch) &&
				statusCodeMatch(statusCode, spec.StatusCodes) {
				return true
			}
		}
		return false
	}
}

func (f *LogFilter) getEventClassFilterList(classes []string) []EventClass {
	if classes == nil {
		return nil
	}

	class := make([]EventClass, 0, len(classes))
	for _, s := range classes {
		if c, ok := string2class[strings.ToUpper(s)]; ok {
			class = append(class, c)
		} else {
			logutil.BgLogger().Error(
				"error occurs when loading audit log filter, parse event class failed",
				zap.String("filter", f.Name),
				zap.String("eventClass", s),
			)
		}
	}
	return class
}

func (f *LogFilter) getTableFilterList(tables []string) []tablefilter.Filter {
	if len(tables) == 0 {
		return nil
	}

	tblFilters := make([]tablefilter.Filter, 0, len(tables))
	for _, tbl := range tables {
		tblFilter, err := parseTableFilter(tbl)
		if err != nil {
			logutil.BgLogger().Error(
				"error occurs when loading audit log filter, parse table filter failed",
				zap.String("filter", f.Name),
				zap.String("tableFilter", tbl),
				zap.Error(err),
			)
			continue
		}
		tblFilters = append(tblFilters, tblFilter)
	}
	return tblFilters
}

// LogFilterRule is a bundle that contains a filter
type LogFilterRule struct {
	User       string
	FilterName string
	Enabled    bool
	Filter     *LogFilter
}

// CreateFilterFunc creates a filter function from the rule
func (r *LogFilterRule) CreateFilterFunc() filterFunc {
	if r.Filter == nil || !r.Enabled {
		return falseFilter
	}

	fn := r.Filter.CreateFilterFunc()
	_, userPattern, hostPattern, err := normalizeUserAndHost(r.User)
	if err != nil {
		logutil.BgLogger().Error(
			"error occurs when loading audit log filter, parse user filter failed",
			zap.String("filter", r.FilterName),
			zap.String("userFilter", r.User),
			zap.Error(err),
		)
		return falseFilter
	}
	userPatWeights, userPatTypes := stringutil.CompilePattern(userPattern, '\\')
	hostPatWeights, hostPatTypes := stringutil.CompilePattern(hostPattern, '\\')

	return func(entry *LogEntry) bool {
		if !stringutil.DoMatch(entry.User, userPatWeights, userPatTypes) {
			return false
		}

		switch entry.Host {
		case "localhost", "127.0.0.1":
			if !stringutil.DoMatch("localhost", hostPatWeights, hostPatTypes) &&
				!stringutil.DoMatch("127.0.0.1", hostPatWeights, hostPatTypes) {
				return false
			}
		default:
			if !stringutil.DoMatch(entry.Host, hostPatWeights, hostPatTypes) {
				return false
			}
		}
		return fn(entry)
	}
}

// LogFilterRuleBundle is a bundle that contains some rules
type LogFilterRuleBundle struct {
	rules     []*LogFilterRule
	ruleFuncs []filterFunc
}

// NewLogFilterRuleBundle creates a new filter rule bundle
func NewLogFilterRuleBundle(rules []*LogFilterRule) *LogFilterRuleBundle {
	ruleFuncs := make([]filterFunc, len(rules))
	for i, rule := range rules {
		ruleFuncs[i] = rule.CreateFilterFunc()
	}

	return &LogFilterRuleBundle{
		rules:     rules,
		ruleFuncs: ruleFuncs,
	}
}

// Filter filters a log entry
func (b *LogFilterRuleBundle) Filter(entry *LogEntry) *LogEntry {
	if entry == nil || b == nil {
		return entry
	}

	for _, fn := range b.ruleFuncs {
		if fn(entry) {
			return entry
		}
	}
	return nil
}

func listFilters(ctx context.Context, exec sqlexec.SQLExecutor) ([]*LogFilter, error) {
	rows, err := executeSQL(ctx, exec, SelectFilterListSQL)
	if err != nil {
		return nil, err
	}

	filters := make([]*LogFilter, 0, len(rows))
	for _, row := range rows {
		f, err := newLogFilterFormJSON(row.GetString(0), row.GetBytes(1))
		if err != nil {
			return nil, err
		}
		filters = append(filters, f)
	}
	return filters, nil
}

// ListFilterRules lists all filter rules from system tables
func ListFilterRules(ctx context.Context, exec sqlexec.SQLExecutor) ([]*LogFilterRule, error) {
	filters, err := listFilters(ctx, exec)
	if err != nil {
		return nil, err
	}

	filterMap := make(map[string]*LogFilter, len(filters))
	for _, f := range filters {
		filterMap[f.Name] = f
	}

	rows, err := executeSQL(ctx, exec, SelectFilterRuleListSQL)
	if err != nil {
		return nil, err
	}

	rules := make([]*LogFilterRule, 0, len(rows))
	for _, row := range rows {
		user := row.GetString(0)
		filterName := row.GetString(1)
		filter, ok := filterMap[filterName]
		if !ok {
			logutil.BgLogger().Warn(
				"cannot find filter when listFilterRules",
				zap.String("filter", filterName),
				zap.String("user", user),
			)
			continue
		}
		rules = append(rules, &LogFilterRule{
			User:       user,
			FilterName: filterName,
			Enabled:    row.GetInt64(2) == 1,
			Filter:     filter,
		})
	}
	return rules, nil
}
