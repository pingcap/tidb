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
	"strconv"
	"strings"
	"time"

	"github.com/cznic/mathutil"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/auth"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/format"
	goctx "golang.org/x/net/context"
)

// ShowExec represents a show executor.
type ShowExec struct {
	baseExecutor

	Tp     ast.ShowStmtType // Databases/Tables/Columns/....
	DBName model.CIStr
	Table  *ast.TableName  // Used for showing columns.
	Column *ast.ColumnName // Used for `desc table column`.
	Flag   int             // Some flag parsed from sql, such as FULL.
	Full   bool
	User   *auth.UserIdentity // Used for show grants.

	// GlobalScope is used by show variables
	GlobalScope bool

	is infoschema.InfoSchema

	forChunk bool
	fetched  bool
	rows     []Row
	result   *chunk.Chunk
	cursor   int
}

// Next implements Execution Next interface.
func (e *ShowExec) Next(goCtx goctx.Context) (Row, error) {
	if e.rows == nil {
		e.forChunk = false
		err := e.fetchAll()
		if err != nil {
			return nil, errors.Trace(err)
		}
		for i := 0; e.rows != nil && i < len(e.rows); i++ {
			for j, row := 0, e.rows[i]; j < len(row); j++ {
				if row[j].Kind() != types.KindString {
					continue
				}
				val := row[j].GetString()
				retType := e.Schema().Columns[j].RetType
				if valLen := len(val); retType.Flen < valLen {
					retType.Flen = valLen
				}
			}
		}
	}
	if e.cursor >= len(e.rows) {
		return nil, nil
	}
	row := e.rows[e.cursor]
	e.cursor++
	return row, nil
}

// NextChunk implements the Executor NextChunk interface.
func (e *ShowExec) NextChunk(goCtx goctx.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.result == nil {
		e.result = e.newChunk()
		e.forChunk = true
		err := e.fetchAll()
		if err != nil {
			return errors.Trace(err)
		}
	}
	if e.cursor >= e.result.NumRows() {
		return nil
	}
	numCurBatch := mathutil.Min(e.maxChunkSize, e.result.NumRows()-e.cursor)
	chk.Append(e.result, e.cursor, e.cursor+numCurBatch)
	e.cursor += numCurBatch
	return nil
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
	case ast.ShowProcessList:
		return e.fetchShowProcessList()
	case ast.ShowEvents:
		// empty result
	case ast.ShowStatsMeta:
		return e.fetchShowStatsMeta()
	case ast.ShowStatsHistograms:
		return e.fetchShowStatsHistogram()
	case ast.ShowStatsBuckets:
		return e.fetchShowStatsBuckets()
	case ast.ShowPlugins:
		return e.fetchShowPlugins()
	case ast.ShowProfiles:
		// empty result
	}
	return nil
}

func (e *ShowExec) fetchShowEngines() error {
	e.appendRow([]interface{}{
		"InnoDB",
		"DEFAULT",
		"Supports transactions, row-level locking, and foreign keys",
		"YES",
		"YES",
		"YES",
	})
	return nil
}

func (e *ShowExec) fetchShowDatabases() error {
	dbs := e.is.AllSchemaNames()
	checker := privilege.GetPrivilegeManager(e.ctx)
	// TODO: let information_schema be the first database
	sort.Strings(dbs)
	for _, d := range dbs {
		if checker != nil && !checker.DBIsVisible(d) {
			continue
		}
		e.appendRow([]interface{}{
			d,
		})
	}
	return nil
}

func (e *ShowExec) fetchShowProcessList() error {
	sm := e.ctx.GetSessionManager()
	if sm == nil {
		return nil
	}

	pl := sm.ShowProcessList()
	for _, pi := range pl {
		var t uint64
		if len(pi.Info) != 0 {
			t = uint64(time.Since(pi.Time) / time.Second)
		}

		var info string
		if e.Full {
			info = pi.Info
		} else {
			info = fmt.Sprintf("%.100v", pi.Info)
		}

		e.appendRow([]interface{}{
			pi.ID,
			pi.User,
			pi.Host,
			pi.DB,
			pi.Command,
			t,
			fmt.Sprintf("%d", pi.State),
			info,
		})
	}
	return nil
}

func (e *ShowExec) fetchShowTables() error {
	if !e.is.SchemaExists(e.DBName) {
		return errors.Errorf("Can not find DB: %s", e.DBName)
	}
	checker := privilege.GetPrivilegeManager(e.ctx)
	// sort for tables
	var tableNames []string
	for _, v := range e.is.SchemaTables(e.DBName) {
		// Test with mysql.AllPrivMask means any privilege would be OK.
		// TODO: Should consider column privileges, which also make a table visible.
		if checker != nil && !checker.RequestVerification(e.DBName.O, v.Meta().Name.O, "", mysql.AllPrivMask) {
			continue
		}
		tableNames = append(tableNames, v.Meta().Name.O)
	}
	sort.Strings(tableNames)
	for _, v := range tableNames {
		if e.Full {
			// TODO: support "VIEW" later if we have supported view feature.
			// now, just use "BASE TABLE".
			e.appendRow([]interface{}{v, "BASE TABLE"})
		} else {
			e.appendRow([]interface{}{v})
		}
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
		e.appendRow([]interface{}{t.Meta().Name.O, "InnoDB", 10, "Compact", 100, 100, 100, 100, 100, 100, 100,
			now, now, now, "utf8_general_ci", "", "", t.Meta().Comment})
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
		if e.Full {
			e.appendRow([]interface{}{
				desc.Field,
				desc.Type,
				desc.Collation,
				desc.Null,
				desc.Key,
				desc.DefaultValue,
				desc.Extra,
				desc.Privileges,
				desc.Comment,
			})
		} else {
			e.appendRow([]interface{}{
				desc.Field,
				desc.Type,
				desc.Null,
				desc.Key,
				desc.DefaultValue,
				desc.Extra,
			})
		}
	}
	return nil
}

// TODO: index collation can have values A (ascending) or NULL (not sorted).
// see: https://dev.mysql.com/doc/refman/5.7/en/show-index.html
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
		e.appendRow([]interface{}{
			tb.Meta().Name.O, // Table
			0,                // Non_unique
			"PRIMARY",        // Key_name
			1,                // Seq_in_index
			pkCol.Name.O,     // Column_name
			"A",              // Collation
			0,                // Cardinality
			nil,              // Sub_part
			nil,              // Packed
			"",               // Null
			"BTREE",          // Index_type
			"",               // Comment
			"",               // Index_comment
		})
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
			e.appendRow([]interface{}{
				tb.Meta().Name.O,  // Table
				nonUniq,           // Non_unique
				idx.Meta().Name.O, // Key_name
				i + 1,             // Seq_in_index
				col.Name.O,        // Column_name
				"A",               // Collation
				0,                 // Cardinality
				subPart,           // Sub_part
				nil,               // Packed
				"YES",             // Null
				idx.Meta().Tp.String(), // Index_type
				"",                 // Comment
				idx.Meta().Comment, // Index_comment
			})
		}
	}
	return nil
}

// fetchShowCharset gets all charset information and fill them into e.rows.
// See http://dev.mysql.com/doc/refman/5.7/en/show-character-set.html
func (e *ShowExec) fetchShowCharset() error {
	descs := charset.GetAllCharsets()
	for _, desc := range descs {
		e.appendRow([]interface{}{
			desc.Name,
			desc.Desc,
			desc.DefaultCollation,
			desc.Maxlen,
		})
	}
	return nil
}

func (e *ShowExec) fetchShowVariables() (err error) {
	var (
		value         string
		ok            bool
		sessionVars   = e.ctx.GetSessionVars()
		unreachedVars = make([]string, 0, len(variable.SysVars))
	)
	for _, v := range variable.SysVars {
		if !e.GlobalScope {
			// For a session scope variable,
			// 1. try to fetch value from SessionVars.Systems;
			// 2. if this variable is session-only, fetch value from SysVars
			//		otherwise, fetch the value from table `mysql.Global_Variables`.
			value, ok, err = variable.GetSessionOnlySysVars(sessionVars, v.Name)
		} else {
			// If the scope of a system variable is ScopeNone,
			// it's a read-only variable, so we return the default value of it.
			// Otherwise, we have to fetch the values from table `mysql.Global_Variables` for global variable names.
			value, ok, err = variable.GetScopeNoneSystemVar(v.Name)
		}
		if err != nil {
			return errors.Trace(err)
		}
		if !ok {
			unreachedVars = append(unreachedVars, v.Name)
			continue
		}
		e.appendRow([]interface{}{v.Name, value})
	}
	if len(unreachedVars) != 0 {
		systemVars, err := sessionVars.GlobalVarsAccessor.GetAllSysVars()
		if err != nil {
			return errors.Trace(err)
		}
		for _, varName := range unreachedVars {
			varValue, ok := systemVars[varName]
			if !ok {
				varValue = variable.SysVars[varName].Value
			}
			e.appendRow([]interface{}{varName, varValue})
		}
	}
	return nil
}

func (e *ShowExec) fetchShowStatus() error {
	sessionVars := e.ctx.GetSessionVars()
	statusVars, err := variable.GetStatusVars(sessionVars)
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
		e.appendRow([]interface{}{status, value})
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
	var hasAutoIncID bool
	for i, col := range tb.Cols() {
		if col.State != model.StatePublic {
			continue
		}
		buf.WriteString(fmt.Sprintf("  `%s` %s", col.Name.O, col.GetTypeDesc()))
		if col.IsGenerated() {
			// It's a generated column.
			buf.WriteString(fmt.Sprintf(" GENERATED ALWAYS AS (%s)", col.GeneratedExprString))
			if col.GeneratedStored {
				buf.WriteString(" STORED")
			} else {
				buf.WriteString(" VIRTUAL")
			}
		}
		if mysql.HasAutoIncrementFlag(col.Flag) {
			hasAutoIncID = true
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
					defaultValStr := fmt.Sprintf("%v", col.DefaultValue)
					if col.Tp == mysql.TypeBit {
						defaultValBinaryLiteral := types.BinaryLiteral(defaultValStr)
						buf.WriteString(fmt.Sprintf(" DEFAULT %s", defaultValBinaryLiteral.ToBitLiteralString(true)))
					} else {
						buf.WriteString(fmt.Sprintf(" DEFAULT '%s'", format.OutputFormat(defaultValStr)))
					}
				}
			}
			if mysql.HasOnUpdateNowFlag(col.Flag) {
				buf.WriteString(" ON UPDATE CURRENT_TIMESTAMP")
			}
		}
		if len(col.Comment) > 0 {
			buf.WriteString(fmt.Sprintf(" COMMENT '%s'", format.OutputFormat(col.Comment)))
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
		buf.WriteString(fmt.Sprintf("  PRIMARY KEY (`%s`)", pkCol.Name.O))
	}

	if len(tb.Indices()) > 0 || len(tb.Meta().ForeignKeys) > 0 {
		buf.WriteString(",\n")
	}

	for i, idx := range tb.Indices() {
		idxInfo := idx.Meta()
		if idxInfo.State != model.StatePublic {
			continue
		}
		if idxInfo.Primary {
			buf.WriteString("  PRIMARY KEY ")
		} else if idxInfo.Unique {
			buf.WriteString(fmt.Sprintf("  UNIQUE KEY `%s` ", idxInfo.Name.O))
		} else {
			buf.WriteString(fmt.Sprintf("  KEY `%s` ", idxInfo.Name.O))
		}

		cols := make([]string, 0, len(idxInfo.Columns))
		for _, c := range idxInfo.Columns {
			colInfo := fmt.Sprintf("`%s`", c.Name.String())
			if c.Length != types.UnspecifiedLength {
				colInfo = fmt.Sprintf("%s(%s)", colInfo, strconv.Itoa(c.Length))
			}
			cols = append(cols, colInfo)
		}
		buf.WriteString(fmt.Sprintf("(%s)", strings.Join(cols, ",")))
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
		for _, c := range fk.RefCols {
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
	charsetName := tb.Meta().Charset
	if len(charsetName) == 0 {
		charsetName = charset.CharsetUTF8
	}
	collate := tb.Meta().Collate
	if len(collate) == 0 {
		collate = charset.CollationUTF8
	}
	// Because we only support case sensitive utf8_bin collate, we need to explicitly set the default charset and collation
	// to make it work on MySQL server which has default collate utf8_general_ci.
	buf.WriteString(fmt.Sprintf(" DEFAULT CHARSET=%s COLLATE=%s", charsetName, collate))

	if hasAutoIncID {
		autoIncID, err := tb.Allocator(e.ctx).NextGlobalAutoID(tb.Meta().ID)
		if err != nil {
			return errors.Trace(err)
		}
		// It's campatible with MySQL.
		if autoIncID > 1 {
			buf.WriteString(fmt.Sprintf(" AUTO_INCREMENT=%d", autoIncID))
		}
	}

	if tb.Meta().ShardRowIDBits > 0 {
		buf.WriteString(fmt.Sprintf("/*!90000 SHARD_ROW_ID_BITS=%d */", tb.Meta().ShardRowIDBits))
	}

	if len(tb.Meta().Comment) > 0 {
		buf.WriteString(fmt.Sprintf(" COMMENT='%s'", format.OutputFormat(tb.Meta().Comment)))
	}

	e.appendRow([]interface{}{tb.Meta().Name.O, buf.String()})
	return nil
}

// fetchShowCreateDatabase composes show create database result.
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

	e.appendRow([]interface{}{db.Name.O, buf.String()})
	return nil
}

func (e *ShowExec) fetchShowCollation() error {
	collations := charset.GetCollations()
	for _, v := range collations {
		isDefault := ""
		if v.IsDefault {
			isDefault = "Yes"
		}
		e.appendRow([]interface{}{
			v.Name,
			v.CharsetName,
			v.ID,
			isDefault,
			"Yes",
			1,
		})
	}
	return nil
}

func (e *ShowExec) fetchShowGrants() error {
	// Get checker
	checker := privilege.GetPrivilegeManager(e.ctx)
	if checker == nil {
		return errors.New("miss privilege checker")
	}
	gs, err := checker.ShowGrants(e.ctx, e.User)
	if err != nil {
		return errors.Trace(err)
	}
	for _, g := range gs {
		e.appendRow([]interface{}{g})
	}
	return nil
}

func (e *ShowExec) fetchShowTriggers() error {
	return nil
}

func (e *ShowExec) fetchShowProcedureStatus() error {
	return nil
}

func (e *ShowExec) fetchShowPlugins() error {
	return nil
}

func (e *ShowExec) fetchShowWarnings() error {
	warns := e.ctx.GetSessionVars().StmtCtx.GetWarnings()
	for _, warn := range warns {
		warn = errors.Cause(warn)
		switch x := warn.(type) {
		case *terror.Error:
			sqlErr := x.ToSQLError()
			e.appendRow([]interface{}{"Warning", int64(sqlErr.Code), sqlErr.Message})
		default:
			e.appendRow([]interface{}{"Warning", int64(mysql.ErrUnknown), warn.Error()})
		}
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

func (e *ShowExec) appendRow(row []interface{}) {
	if !e.forChunk {
		e.rows = append(e.rows, types.MakeDatums(row...))
		return
	}
	for i, col := range row {
		if col == nil {
			e.result.AppendNull(i)
			continue
		}
		switch x := col.(type) {
		case nil:
			e.result.AppendNull(i)
		case int:
			e.result.AppendInt64(i, int64(x))
		case int64:
			e.result.AppendInt64(i, x)
		case uint64:
			e.result.AppendUint64(i, x)
		case float64:
			e.result.AppendFloat64(i, x)
		case float32:
			e.result.AppendFloat32(i, x)
		case string:
			e.result.AppendString(i, x)
		case []byte:
			e.result.AppendBytes(i, x)
		case types.BinaryLiteral:
			e.result.AppendBytes(i, x)
		case *types.MyDecimal:
			e.result.AppendMyDecimal(i, x)
		case types.Time:
			e.result.AppendTime(i, x)
		case json.BinaryJSON:
			e.result.AppendJSON(i, x)
		case types.Duration:
			e.result.AppendDuration(i, x)
		case types.Enum:
			e.result.AppendEnum(i, x)
		case types.Set:
			e.result.AppendSet(i, x)
		default:
			e.result.AppendNull(i)
		}
	}
}
