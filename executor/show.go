// Copyright 2016 PingCAP, Inc.
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

package executor

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/sessionctx/varsutil"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/types"
)

// ShowExec represents a show executor.
type ShowExec struct {
	Tp     ast.ShowStmtType // Databases/Tables/Columns/....
	DBName model.CIStr
	Table  *ast.TableName  // Used for showing columns.
	Column *ast.ColumnName // Used for `desc table column`.
	Flag   int             // Some flag parsed from sql, such as FULL.
	Full   bool
	User   string // Used for show grants.

	// Used by show variables
	GlobalScope bool

	schema *expression.Schema
	ctx    context.Context
	is     infoschema.InfoSchema

	fetched bool
	rows    []*Row
	cursor  int
}

// Schema implements the Executor Schema interface.
func (e *ShowExec) Schema() *expression.Schema {
	return e.schema
}

// Next implements Execution Next interface.
func (e *ShowExec) Next() (*Row, error) {
	if e.rows == nil {
		err := e.fetchAll()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if e.cursor >= len(e.rows) {
		return nil, nil
	}
	row := e.rows[e.cursor]
	e.cursor++
	return row, nil
}

func (e *ShowExec) fetchAll() error {
	switch e.Tp {
	case ast.ShowCharset:
		return e.fetchShowCharset()
	case ast.ShowCollation:
		return e.fetchShowCollation()
	case ast.ShowColumns:
		return e.fetchShowColumns()
	case ast.ShowCreateTable:
		return e.fetchShowCreateTable()
	case ast.ShowCreateDatabase:
		return e.fetchShowCreateDatabase()
	case ast.ShowDatabases:
		return e.fetchShowDatabases()
	case ast.ShowEngines:
		return e.fetchShowEngines()
	case ast.ShowGrants:
		return e.fetchShowGrants()
	case ast.ShowIndex:
		return e.fetchShowIndex()
	case ast.ShowProcedureStatus:
		return e.fetchShowProcedureStatus()
	case ast.ShowStatus:
		return e.fetchShowStatus()
	case ast.ShowTables:
		return e.fetchShowTables()
	case ast.ShowTableStatus:
		return e.fetchShowTableStatus()
	case ast.ShowTriggers:
		return e.fetchShowTriggers()
	case ast.ShowVariables:
		return e.fetchShowVariables()
	case ast.ShowWarnings:
		return e.fetchShowWarnings()
	case ast.ShowProcessList, ast.ShowEvents:
		// empty result
	}
	return nil
}

func (e *ShowExec) fetchShowEngines() error {
	row := &Row{
		Data: types.MakeDatums(
			"InnoDB",
			"DEFAULT",
			"Supports transactions, row-level locking, and foreign keys",
			"YES",
			"YES",
			"YES",
		),
	}
	e.rows = append(e.rows, row)
	return nil
}

func (e *ShowExec) fetchShowDatabases() error {
	dbs := e.is.AllSchemaNames()
	// TODO: let information_schema be the first database
	sort.Strings(dbs)
	for _, d := range dbs {
		e.rows = append(e.rows, &Row{Data: types.MakeDatums(d)})
	}
	return nil
}

func (e *ShowExec) fetchShowTables() error {
	if !e.is.SchemaExists(e.DBName) {
		return errors.Errorf("Can not find DB: %s", e.DBName)
	}
	// sort for tables
	var tableNames []string
	for _, v := range e.is.SchemaTables(e.DBName) {
		tableNames = append(tableNames, v.Meta().Name.O)
	}
	sort.Strings(tableNames)
	for _, v := range tableNames {
		data := types.MakeDatums(v)
		if e.Full {
			// TODO: support "VIEW" later if we have supported view feature.
			// now, just use "BASE TABLE".
			data = append(data, types.NewDatum("BASE TABLE"))
		}
		e.rows = append(e.rows, &Row{Data: data})
	}
	return nil
}

func (e *ShowExec) fetchShowTableStatus() error {
	if !e.is.SchemaExists(e.DBName) {
		return errors.Errorf("Can not find DB: %s", e.DBName)
	}

	// sort for tables
	tables := e.is.SchemaTables(e.DBName)
	sort.Sort(table.Slice(tables))

	for _, t := range tables {
		now := types.CurrentTime(mysql.TypeDatetime)
		data := types.MakeDatums(t.Meta().Name.O, "InnoDB", "10", "Compact", 100, 100, 100, 100, 100, 100, 100,
			now, now, now, "utf8_general_ci", "", "", t.Meta().Comment)
		e.rows = append(e.rows, &Row{Data: data})
	}
	return nil
}

func (e *ShowExec) fetchShowColumns() error {
	tb, err := e.getTable()
	if err != nil {
		return errors.Trace(err)
	}
	cols := tb.Cols()
	for _, col := range cols {
		if e.Column != nil && e.Column.Name.L != col.Name.L {
			continue
		}

		desc := table.NewColDesc(col)

		// The FULL keyword causes the output to include the column collation and comments,
		// as well as the privileges you have for each column.
		row := &Row{}
		if e.Full {
			row.Data = types.MakeDatums(
				desc.Field,
				desc.Type,
				desc.Collation,
				desc.Null,
				desc.Key,
				desc.DefaultValue,
				desc.Extra,
				desc.Privileges,
				desc.Comment,
			)
		} else {
			row.Data = types.MakeDatums(
				desc.Field,
				desc.Type,
				desc.Null,
				desc.Key,
				desc.DefaultValue,
				desc.Extra,
			)
		}
		e.rows = append(e.rows, row)
	}
	return nil
}

func (e *ShowExec) fetchShowIndex() error {
	tb, err := e.getTable()
	if err != nil {
		return errors.Trace(err)
	}
	if tb.Meta().PKIsHandle {
		var pkCol *table.Column
		for _, col := range tb.Cols() {
			if mysql.HasPriKeyFlag(col.Flag) {
				pkCol = col
				break
			}
		}
		data := types.MakeDatums(
			tb.Meta().Name.O, // Table
			0,                // Non_unique
			"PRIMARY",        // Key_name
			1,                // Seq_in_index
			pkCol.Name.O,     // Column_name
			"utf8_bin",       // Colation
			0,                // Cardinality
			nil,              // Sub_part
			nil,              // Packed
			"",               // Null
			"BTREE",          // Index_type
			"",               // Comment
			"",               // Index_comment
		)
		e.rows = append(e.rows, &Row{Data: data})
	}
	for _, idx := range tb.Indices() {
		for i, col := range idx.Meta().Columns {
			nonUniq := 1
			if idx.Meta().Unique {
				nonUniq = 0
			}
			var subPart interface{}
			if col.Length != types.UnspecifiedLength {
				subPart = col.Length
			}
			data := types.MakeDatums(
				tb.Meta().Name.O,  // Table
				nonUniq,           // Non_unique
				idx.Meta().Name.O, // Key_name
				i+1,               // Seq_in_index
				col.Name.O,        // Column_name
				"utf8_bin",        // Colation
				0,                 // Cardinality
				subPart,           // Sub_part
				nil,               // Packed
				"YES",             // Null
				idx.Meta().Tp.String(), // Index_type
				"",                 // Comment
				idx.Meta().Comment, // Index_comment
			)
			e.rows = append(e.rows, &Row{Data: data})
		}
	}
	return nil
}

// See http://dev.mysql.com/doc/refman/5.7/en/show-character-set.html
func (e *ShowExec) fetchShowCharset() error {
	descs := charset.GetAllCharsets()
	for _, desc := range descs {
		row := &Row{
			Data: types.MakeDatums(
				desc.Name,
				desc.Desc,
				desc.DefaultCollation,
				desc.Maxlen,
			),
		}
		e.rows = append(e.rows, row)
	}
	return nil
}

func (e *ShowExec) fetchShowVariables() error {
	sessionVars := e.ctx.GetSessionVars()
	for _, v := range variable.SysVars {
		var err error
		var value string
		if !e.GlobalScope {
			// Try to get Session Scope variable value first.
			value, err = varsutil.GetSessionSystemVar(sessionVars, v.Name)
		} else {
			value, err = varsutil.GetGlobalSystemVar(sessionVars, v.Name)
		}
		if err != nil {
			return errors.Trace(err)
		}
		row := &Row{Data: types.MakeDatums(v.Name, value)}
		e.rows = append(e.rows, row)
	}
	return nil
}

func (e *ShowExec) fetchShowStatus() error {
	statusVars, err := variable.GetStatusVars()
	if err != nil {
		return errors.Trace(err)
	}
	for status, v := range statusVars {
		if e.GlobalScope && v.Scope == variable.ScopeSession {
			continue
		}
		switch v.Value.(type) {
		case []interface{}, nil:
			v.Value = fmt.Sprintf("%v", v.Value)
		}
		value, err := types.ToString(v.Value)
		if err != nil {
			return errors.Trace(err)
		}
		row := &Row{Data: types.MakeDatums(status, value)}
		e.rows = append(e.rows, row)
	}
	return nil
}

func (e *ShowExec) fetchShowCreateTable() error {
	tb, err := e.getTable()
	if err != nil {
		return errors.Trace(err)
	}

	// TODO: let the result more like MySQL.
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("CREATE TABLE `%s` (\n", tb.Meta().Name.O))
	var pkCol *table.Column
	for i, col := range tb.Cols() {
		buf.WriteString(fmt.Sprintf("  `%s` %s", col.Name.O, col.GetTypeDesc()))
		if mysql.HasAutoIncrementFlag(col.Flag) {
			buf.WriteString(" NOT NULL AUTO_INCREMENT")
		} else {
			if mysql.HasNotNullFlag(col.Flag) {
				buf.WriteString(" NOT NULL")
			}
			if !mysql.HasNoDefaultValueFlag(col.Flag) {
				switch col.DefaultValue {
				case nil:
					if !mysql.HasNotNullFlag(col.Flag) {
						if mysql.HasTimestampFlag(col.Flag) {
							buf.WriteString(" NULL")
						}
						buf.WriteString(" DEFAULT NULL")
					}
				case "CURRENT_TIMESTAMP":
					buf.WriteString(" DEFAULT CURRENT_TIMESTAMP")
				default:
					buf.WriteString(fmt.Sprintf(" DEFAULT '%v'", col.DefaultValue))
				}
			}
			if mysql.HasOnUpdateNowFlag(col.Flag) {
				buf.WriteString(" ON UPDATE CURRENT_TIMESTAMP")
			}
		}
		if len(col.Comment) > 0 {
			buf.WriteString(fmt.Sprintf(" COMMENT '%s'", col.Comment))
		}
		if i != len(tb.Cols())-1 {
			buf.WriteString(",\n")
		}
		if tb.Meta().PKIsHandle && mysql.HasPriKeyFlag(col.Flag) {
			pkCol = col
		}
	}

	if pkCol != nil {
		// If PKIsHanle, pk info is not in tb.Indices(). We should handle it here.
		buf.WriteString(",\n")
		buf.WriteString(fmt.Sprintf(" PRIMARY KEY (`%s`)", pkCol.Name.O))
	}

	if len(tb.Indices()) > 0 || len(tb.Meta().ForeignKeys) > 0 {
		buf.WriteString(",\n")
	}

	for i, idx := range tb.Indices() {
		idxInfo := idx.Meta()
		if idxInfo.Primary {
			buf.WriteString("  PRIMARY KEY ")
		} else if idxInfo.Unique {
			buf.WriteString(fmt.Sprintf("  UNIQUE KEY `%s` ", idxInfo.Name.O))
		} else {
			buf.WriteString(fmt.Sprintf("  KEY `%s` ", idxInfo.Name.O))
		}

		cols := make([]string, 0, len(idxInfo.Columns))
		for _, c := range idxInfo.Columns {
			cols = append(cols, c.Name.O)
		}
		buf.WriteString(fmt.Sprintf("(`%s`)", strings.Join(cols, "`,`")))
		if i != len(tb.Indices())-1 {
			buf.WriteString(",\n")
		}
	}

	if len(tb.Indices()) > 0 && len(tb.Meta().ForeignKeys) > 0 {
		buf.WriteString(",\n")
	}

	firstFK := true
	for _, fk := range tb.Meta().ForeignKeys {
		if fk.State != model.StatePublic {
			continue
		}
		if !firstFK {
			buf.WriteString(",\n")
		}
		firstFK = false
		cols := make([]string, 0, len(fk.Cols))
		for _, c := range fk.Cols {
			cols = append(cols, c.O)
		}

		refCols := make([]string, 0, len(fk.RefCols))
		for _, c := range fk.Cols {
			refCols = append(refCols, c.O)
		}

		buf.WriteString(fmt.Sprintf("  CONSTRAINT `%s` FOREIGN KEY (`%s`)", fk.Name.O, strings.Join(cols, "`,`")))
		buf.WriteString(fmt.Sprintf(" REFERENCES `%s` (`%s`)", fk.RefTable.O, strings.Join(refCols, "`,`")))

		if ast.ReferOptionType(fk.OnDelete) != ast.ReferOptionNoOption {
			buf.WriteString(fmt.Sprintf(" ON DELETE %s", ast.ReferOptionType(fk.OnDelete)))
		}

		if ast.ReferOptionType(fk.OnUpdate) != ast.ReferOptionNoOption {
			buf.WriteString(fmt.Sprintf(" ON UPDATE %s", ast.ReferOptionType(fk.OnUpdate)))
		}
	}
	buf.WriteString("\n")

	buf.WriteString(") ENGINE=InnoDB")
	if s := tb.Meta().Charset; len(s) > 0 {
		buf.WriteString(fmt.Sprintf(" DEFAULT CHARSET=%s", s))
	}

	if tb.Meta().AutoIncID > 0 {
		buf.WriteString(fmt.Sprintf(" AUTO_INCREMENT=%d", tb.Meta().AutoIncID))
	}

	if len(tb.Meta().Comment) > 0 {
		buf.WriteString(fmt.Sprintf(" COMMENT='%s'", tb.Meta().Comment))
	}

	data := types.MakeDatums(tb.Meta().Name.O, buf.String())
	e.rows = append(e.rows, &Row{Data: data})
	return nil
}

// Compose show create database result.
func (e *ShowExec) fetchShowCreateDatabase() error {
	db, ok := e.is.SchemaByName(e.DBName)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenByArgs(e.DBName.O)
	}

	var buf bytes.Buffer
	fmt.Fprintf(&buf, "CREATE DATABASE `%s`", db.Name.O)
	if s := db.Charset; len(s) > 0 {
		fmt.Fprintf(&buf, " /* !40100 DEFAULT CHARACTER SET %s */", s)
	}

	data := types.MakeDatums(db.Name.O, buf.String())
	e.rows = append(e.rows, &Row{Data: data})
	return nil
}

func (e *ShowExec) fetchShowCollation() error {
	collations := charset.GetCollations()
	for _, v := range collations {
		isDefault := ""
		if v.IsDefault {
			isDefault = "Yes"
		}
		row := &Row{Data: types.MakeDatums(
			v.Name,
			v.CharsetName,
			v.ID,
			isDefault,
			"Yes",
			1,
		)}
		e.rows = append(e.rows, row)
	}
	return nil
}

func (e *ShowExec) fetchShowGrants() error {
	// Get checker
	checker := privilege.GetPrivilegeChecker(e.ctx)
	if checker == nil {
		return errors.New("miss privilege checker")
	}
	gs, err := checker.ShowGrants(e.ctx, e.User)
	if err != nil {
		return errors.Trace(err)
	}
	for _, g := range gs {
		data := types.MakeDatums(g)
		e.rows = append(e.rows, &Row{Data: data})
	}
	return nil
}

func (e *ShowExec) fetchShowTriggers() error {
	return nil
}

func (e *ShowExec) fetchShowProcedureStatus() error {
	return nil
}

func (e *ShowExec) fetchShowWarnings() error {
	warns := e.ctx.GetSessionVars().StmtCtx.GetWarnings()
	for _, warn := range warns {
		datums := make([]types.Datum, 3)
		datums[0] = types.NewStringDatum("Warning")
		switch x := warn.(type) {
		case *terror.Error:
			sqlErr := x.ToSQLError()
			datums[1] = types.NewIntDatum(int64(sqlErr.Code))
			datums[2] = types.NewStringDatum(sqlErr.Message)
		default:
			datums[1] = types.NewIntDatum(int64(mysql.ErrUnknown))
			datums[2] = types.NewStringDatum(warn.Error())
		}
		e.rows = append(e.rows, &Row{Data: datums})
	}
	return nil
}

func (e *ShowExec) getTable() (table.Table, error) {
	if e.Table == nil {
		return nil, errors.New("table not found")
	}
	tb, ok := e.is.TableByID(e.Table.TableInfo.ID)
	if !ok {
		return nil, errors.Errorf("table %s not found", e.Table.Name)
	}
	return tb, nil
}

// Close implements the Executor Close interface.
func (e *ShowExec) Close() error {
	return nil
}
