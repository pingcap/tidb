// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package plans

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/column"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/stmt"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/format"
	"github.com/pingcap/tidb/util/types"
)

var (
	_ plan.Plan = (*ShowPlan)(nil)
)

// ShowPlan is used for show statements
type ShowPlan struct {
	Target     int
	DBName     string
	TableName  string
	ColumnName string
	Flag       int
	Full       bool
	// Used by SHOW VARIABLES
	GlobalScope bool
	Pattern     *expression.PatternLike
	Where       expression.Expression
	rows        []*plan.Row
	cursor      int
	User        string // ShowGrants need to know username.
}

func (s *ShowPlan) isColOK(c *column.Col) bool {
	// support `desc tableName columnName`
	// TODO: columnName can be a regular
	if s.ColumnName == "" {
		return true
	}

	if strings.EqualFold(s.ColumnName, c.Name.L) {
		return true
	}

	return false
}

// Explain implements plan.Plan Explain interface.
func (s *ShowPlan) Explain(w format.Formatter) {
	// TODO: finish this
}

// GetFields implements plan.Plan GetFields interface.
func (s *ShowPlan) GetFields() []*field.ResultField {
	var (
		names []string
		types []byte
	)

	switch s.Target {
	case stmt.ShowEngines:
		names = []string{"Engine", "Support", "Comment", "Transactions", "XA", "Savepoints"}
	case stmt.ShowDatabases:
		names = []string{"Database"}
	case stmt.ShowTables:
		names = []string{fmt.Sprintf("Tables_in_%s", s.DBName)}
		if s.Full {
			names = append(names, "Table_type")
		}
	case stmt.ShowTableStatus:
		names = []string{"Name", "Engine", "Version", "Row_format", "Rows", "Avg_row_length",
			"Data_length", "Max_data_length", "Index_length", "Data_free", "Auto_increment",
			"Create_time", "Update_time", "Check_time", "Collation", "Checksum",
			"Create_options", "Comment"}
		types = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeLonglong,
			mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeLonglong,
			mysql.TypeDatetime, mysql.TypeDatetime, mysql.TypeDatetime, mysql.TypeVarchar, mysql.TypeVarchar,
			mysql.TypeVarchar, mysql.TypeVarchar}
	case stmt.ShowColumns:
		names = column.ColDescFieldNames(s.Full)
	case stmt.ShowWarnings:
		names = []string{"Level", "Code", "Message"}
		types = []byte{mysql.TypeVarchar, mysql.TypeLong, mysql.TypeVarchar}
	case stmt.ShowCharset:
		names = []string{"Charset", "Description", "Default collation", "Maxlen"}
		types = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong}
	case stmt.ShowVariables:
		names = []string{"Variable_name", "Value"}
	case stmt.ShowStatus:
		names = []string{"Variable_name", "Value"}
	case stmt.ShowCollation:
		names = []string{"Collation", "Charset", "Id", "Default", "Compiled", "Sortlen"}
		types = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong,
			mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong}
	case stmt.ShowCreateTable:
		names = []string{"Table", "Create Table"}
	case stmt.ShowGrants:
		names = []string{fmt.Sprintf("Grants for %s", s.User)}
	case stmt.ShowTriggers:
		names = []string{"Trigger", "Event", "Table", "Statement", "Timing", "Created",
			"sql_mode", "Definer", "character_set_client", "collation_connection", "Database Collation"}
		types = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar,
			mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar}
	}
	fields := make([]*field.ResultField, 0, len(names))
	for i, name := range names {
		f := &field.ResultField{Name: name}
		if types == nil || types[i] == 0 {
			// use varchar as the default return column type
			f.Col.Tp = mysql.TypeVarchar
		} else {
			f.Col.Tp = types[i]
		}

		fields = append(fields, f)
	}

	return fields
}

// Filter implements plan.Plan Filter interface.
func (s *ShowPlan) Filter(ctx context.Context, expr expression.Expression) (plan.Plan, bool, error) {
	return s, false, nil
}

// Next implements plan.Plan Next interface.
func (s *ShowPlan) Next(ctx context.Context) (row *plan.Row, err error) {
	if s.rows == nil {
		if err := s.fetchAll(ctx); err != nil {
			return nil, errors.Trace(err)
		}
	}
	if s.cursor == len(s.rows) {
		return
	}
	row = s.rows[s.cursor]
	s.cursor++
	return
}

func (s *ShowPlan) fetchAll(ctx context.Context) error {
	switch s.Target {
	case stmt.ShowEngines:
		return s.fetchShowEngines(ctx)
	case stmt.ShowDatabases:
		return s.fetchShowDatabases(ctx)
	case stmt.ShowTables:
		return s.fetchShowTables(ctx)
	case stmt.ShowTableStatus:
		return s.fetchShowTableStatus(ctx)
	case stmt.ShowColumns:
		return s.fetchShowColumns(ctx)
	case stmt.ShowWarnings:
		// empty result
	case stmt.ShowCharset:
		return s.fetchShowCharset(ctx)
	case stmt.ShowVariables:
		return s.fetchShowVariables(ctx)
	case stmt.ShowStatus:
		return s.fetchShowStatus(ctx)
	case stmt.ShowCollation:
		return s.fetchShowCollation(ctx)
	case stmt.ShowCreateTable:
		return s.fetchShowCreateTable(ctx)
	case stmt.ShowGrants:
		return s.fetchShowGrants(ctx)
	case stmt.ShowTriggers:
		return s.fetchShowTriggers(ctx)
	}
	return nil
}

// Close implements plan.Plan Close interface.
func (s *ShowPlan) Close() error {
	s.rows = nil
	s.cursor = 0
	return nil
}

func (s *ShowPlan) evalCondition(ctx context.Context, m map[interface{}]interface{}) (bool, error) {
	var cond expression.Expression
	if s.Pattern != nil {
		cond = s.Pattern
	} else if s.Where != nil {
		cond = s.Where
	}

	if cond == nil {
		return true, nil
	}
	return expression.EvalBoolExpr(ctx, cond, m)
}

func (s *ShowPlan) getTable(ctx context.Context) (table.Table, error) {
	is := sessionctx.GetDomain(ctx).InfoSchema()
	dbName := model.NewCIStr(s.DBName)
	if !is.SchemaExists(dbName) {
		// MySQL returns no such table here if database doesn't exist.
		return nil, errors.Trace(mysql.NewErr(mysql.ErrNoSuchTable, s.DBName, s.TableName))
	}
	tbName := model.NewCIStr(s.TableName)
	tb, err := is.TableByName(dbName, tbName)
	if err != nil {
		return nil, errors.Trace(mysql.NewErr(mysql.ErrNoSuchTable, s.DBName, s.TableName))
	}
	return tb, nil
}

func (s *ShowPlan) fetchShowColumns(ctx context.Context) error {
	tb, err := s.getTable(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	cols := tb.Cols()

	for _, col := range cols {
		if !s.isColOK(col) {
			continue
		}

		desc := column.NewColDesc(col)

		// The FULL keyword causes the output to include the column collation and comments,
		// as well as the privileges you have for each column.
		row := &plan.Row{}
		if s.Full {
			row.Data = []interface{}{
				desc.Field,
				desc.Type,
				desc.Collation,
				desc.Null,
				desc.Key,
				desc.DefaultValue,
				desc.Extra,
				desc.Privileges,
				desc.Comment,
			}
		} else {
			row.Data = []interface{}{
				desc.Field,
				desc.Type,
				desc.Null,
				desc.Key,
				desc.DefaultValue,
				desc.Extra,
			}
		}
		s.rows = append(s.rows, row)
	}
	return nil
}

func (s *ShowPlan) fetchShowCollation(ctx context.Context) error {
	collations := charset.GetCollations()
	m := map[interface{}]interface{}{}

	for _, v := range collations {
		if s.Pattern != nil {
			s.Pattern.Expr = expression.Value{Val: v.Name}
		} else if s.Where != nil {
			m[expression.ExprEvalIdentFunc] = func(name string) (interface{}, error) {
				switch {
				case strings.EqualFold(name, "Collation"):
					return v.Name, nil
				case strings.EqualFold(name, "Charset"):
					return v.CharsetName, nil
				case strings.EqualFold(name, "Id"):
					return v.ID, nil
				case strings.EqualFold(name, "Default"):
					if v.IsDefault {
						return "Yes", nil
					}
					return "", nil
				case strings.EqualFold(name, "Compiled"):
					return "Yes", nil
				case strings.EqualFold(name, "Sortlen"):
					// TODO: add sort length in Collation
					return 1, nil
				default:
					return nil, errors.Errorf("unknown field %s", name)
				}
			}
		}

		match, err := s.evalCondition(ctx, m)
		if err != nil {
			return errors.Trace(err)
		}
		if !match {
			continue
		}

		isDefault := ""
		if v.IsDefault {
			isDefault = "Yes"
		}
		row := &plan.Row{Data: []interface{}{v.Name, v.CharsetName, v.ID, isDefault, "Yes", 1}}
		s.rows = append(s.rows, row)
	}
	return nil
}

func (s *ShowPlan) fetchShowTables(ctx context.Context) error {
	is := sessionctx.GetDomain(ctx).InfoSchema()
	dbName := model.NewCIStr(s.DBName)
	if !is.SchemaExists(dbName) {
		return errors.Errorf("Can not find DB: %s", dbName)
	}

	// sort for tables
	var tableNames []string
	for _, v := range is.SchemaTables(dbName) {
		tableNames = append(tableNames, v.TableName().L)
	}

	sort.Strings(tableNames)

	m := map[interface{}]interface{}{}
	for _, v := range tableNames {
		data := []interface{}{v}
		if s.Full {
			// TODO: support "VIEW" later if we have supported view feature.
			// now, just use "BASE TABLE".
			data = append(data, "BASE TABLE")
		}
		// Check like/where clause.
		if s.Pattern != nil {
			s.Pattern.Expr = expression.Value{Val: data[0]}
		} else if s.Where != nil {
			m[expression.ExprEvalIdentFunc] = func(name string) (interface{}, error) {
				if s.Full && strings.EqualFold(name, "Table_type") {
					return data[1], nil
				}
				return nil, errors.Errorf("unknown field %s", name)
			}
		}
		match, err := s.evalCondition(ctx, m)
		if err != nil {
			return errors.Trace(err)
		}
		if !match {
			continue
		}
		s.rows = append(s.rows, &plan.Row{Data: data})
	}
	return nil
}

func (s *ShowPlan) fetchShowTableStatus(ctx context.Context) error {
	is := sessionctx.GetDomain(ctx).InfoSchema()
	dbName := model.NewCIStr(s.DBName)
	if !is.SchemaExists(dbName) {
		return errors.Errorf("Can not find DB: %s", dbName)
	}

	// sort for tables
	var tableNames []string
	for _, v := range is.SchemaTables(dbName) {
		tableNames = append(tableNames, v.TableName().L)
	}

	sort.Strings(tableNames)

	m := map[interface{}]interface{}{}
	for _, v := range tableNames {
		// Check like/where clause.
		if s.Pattern != nil {
			s.Pattern.Expr = expression.Value{Val: v}
		} else if s.Where != nil {
			m[expression.ExprEvalIdentFunc] = func(name string) (interface{}, error) {
				return nil, errors.Errorf("unknown field %s", name)
			}
		}
		match, err := s.evalCondition(ctx, m)
		if err != nil {
			return errors.Trace(err)
		}
		if !match {
			continue
		}
		now := mysql.CurrentTime(mysql.TypeDatetime)
		data := []interface{}{
			v, "InnoDB", "10", "Compact", 100, 100,
			100, 100, 100, 100, 100,
			now, now, now, "utf8_general_ci", "",
			"", "",
		}
		s.rows = append(s.rows, &plan.Row{Data: data})
	}
	return nil
}
func (s *ShowPlan) fetchShowVariables(ctx context.Context) error {
	sessionVars := variable.GetSessionVars(ctx)
	globalVars := variable.GetGlobalVarAccessor(ctx)
	m := map[interface{}]interface{}{}

	for _, v := range variable.SysVars {
		if s.Pattern != nil {
			s.Pattern.Expr = expression.Value{Val: v.Name}
		} else if s.Where != nil {
			m[expression.ExprEvalIdentFunc] = func(name string) (interface{}, error) {
				if strings.EqualFold(name, "Variable_name") {
					return v.Name, nil
				}

				return nil, errors.Errorf("unknown field %s", name)
			}
		}

		match, err := s.evalCondition(ctx, m)
		if err != nil {
			return errors.Trace(err)
		}
		if !match {
			continue
		}

		var value string
		if !s.GlobalScope {
			// Try to get Session Scope variable value first.
			sv, ok := sessionVars.Systems[v.Name]
			if ok {
				value = sv
			} else {
				// If session scope variable is not set, get the global scope value.
				value, err = globalVars.GetGlobalSysVar(ctx, v.Name)
				if err != nil {
					return errors.Trace(err)
				}
			}
		} else {
			value, err = globalVars.GetGlobalSysVar(ctx, v.Name)
			if err != nil {
				return errors.Trace(err)
			}
		}
		row := &plan.Row{Data: []interface{}{v.Name, value}}
		s.rows = append(s.rows, row)
	}
	return nil
}

func (s *ShowPlan) fetchShowStatus(ctx context.Context) error {
	m := map[interface{}]interface{}{}

	statusVars, err := variable.GetStatusVars()
	if err != nil {
		return errors.Trace(err)
	}

	for status, v := range statusVars {
		if s.Pattern != nil {
			s.Pattern.Expr = expression.Value{Val: status}
		} else if s.Where != nil {
			m[expression.ExprEvalIdentFunc] = func(name string) (interface{}, error) {
				if strings.EqualFold(name, "Variable_name") {
					return status, nil
				}

				return nil, errors.Errorf("unknown field %s", name)
			}
		}

		match, err := s.evalCondition(ctx, m)
		if err != nil {
			return errors.Trace(err)
		}
		if !match {
			continue
		}

		if s.GlobalScope && v.Scope == variable.ScopeSession {
			continue
		}

		value, err := types.ToString(v.Value)
		if err != nil {
			return errors.Trace(err)
		}
		row := &plan.Row{Data: []interface{}{status, value}}
		s.rows = append(s.rows, row)
	}

	return nil
}

func (s *ShowPlan) fetchShowCharset(ctx context.Context) error {
	// See: http://dev.mysql.com/doc/refman/5.7/en/show-character-set.html
	descs := charset.GetAllCharsets()
	for _, desc := range descs {
		row := &plan.Row{
			Data: []interface{}{desc.Name, desc.Desc, desc.DefaultCollation, desc.Maxlen},
		}
		s.rows = append(s.rows, row)
	}
	return nil
}

func (s *ShowPlan) fetchShowEngines(ctx context.Context) error {
	// Mock data
	row := &plan.Row{
		Data: []interface{}{"InnoDB", "DEFAULT", "Supports transactions, row-level locking, and foreign keys", "YES", "YES", "YES"},
	}
	s.rows = append(s.rows, row)
	return nil
}

func (s *ShowPlan) fetchShowDatabases(ctx context.Context) error {
	dbs := sessionctx.GetDomain(ctx).InfoSchema().AllSchemaNames()

	// TODO: let information_schema be the first database
	sort.Strings(dbs)

	for _, d := range dbs {
		s.rows = append(s.rows, &plan.Row{Data: []interface{}{d}})
	}
	return nil
}

func (s *ShowPlan) fetchShowCreateTable(ctx context.Context) error {
	tb, err := s.getTable(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	// TODO: let the result more like MySQL.
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("CREATE TABLE `%s` (\n", tb.TableName().O))
	for i, col := range tb.Cols() {
		buf.WriteString(fmt.Sprintf("  `%s` %s", col.Name.O, col.GetTypeDesc()))
		if mysql.HasAutoIncrementFlag(col.Flag) {
			buf.WriteString(" NOT NULL AUTO_INCREMENT")
		} else {
			if mysql.HasNotNullFlag(col.Flag) {
				buf.WriteString(" NOT NULL")
			}
			switch col.DefaultValue {
			case nil:
				buf.WriteString(" DEFAULT NULL")
			case "CURRENT_TIMESTAMP":
				buf.WriteString(" DEFAULT CURRENT_TIMESTAMP")
			default:
				buf.WriteString(fmt.Sprintf(" DEFAULT '%v'", col.DefaultValue))
			}

			if mysql.HasOnUpdateNowFlag(col.Flag) {
				buf.WriteString(" ON UPDATE CURRENT_TIMESTAMP")
			}
		}
		if i != len(tb.Cols())-1 {
			buf.WriteString(",\n")
		}
	}

	if len(tb.Indices()) > 0 {
		buf.WriteString(",\n")
	}

	for i, idx := range tb.Indices() {
		if idx.Primary {
			buf.WriteString("  PRIMARY KEY ")
		} else if idx.Unique {
			buf.WriteString(fmt.Sprintf("  UNIQUE KEY `%s` ", idx.Name.O))
		} else {
			buf.WriteString(fmt.Sprintf("  KEY `%s` ", idx.Name.O))
		}

		cols := make([]string, 0, len(idx.Columns))
		for _, c := range idx.Columns {
			cols = append(cols, c.Name.O)
		}
		buf.WriteString(fmt.Sprintf("(`%s`)", strings.Join(cols, "`,`")))
		if i != len(tb.Indices())-1 {
			buf.WriteString(",\n")
		}
	}

	buf.WriteString("\n")

	buf.WriteString(") ENGINE=InnoDB")
	if s := tb.Meta().Charset; len(s) > 0 {
		buf.WriteString(fmt.Sprintf(" DEFAULT CHARSET=%s", s))
	} else {
		buf.WriteString(" DEFAULT CHARSET=latin1")
	}

	data := []interface{}{
		tb.TableName().O,
		buf.String(),
	}

	s.rows = append(s.rows, &plan.Row{Data: data})

	return nil
}

func (s *ShowPlan) fetchShowGrants(ctx context.Context) error {
	// Get checker
	checker := privilege.GetPrivilegeChecker(ctx)
	if checker == nil {
		return errors.New("Miss privilege checker!")
	}
	gs, err := checker.ShowGrants(ctx, s.User)
	if err != nil {
		return errors.Trace(err)
	}
	for _, g := range gs {
		data := []interface{}{g}
		s.rows = append(s.rows, &plan.Row{Data: data})
	}
	return nil
}

func (s *ShowPlan) fetchShowTriggers(ctx context.Context) error {
	return nil
}
