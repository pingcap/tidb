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
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb-tools/pkg/etcd"
	"github.com/pingcap/tidb-tools/pkg/utils"
	"github.com/pingcap/tidb-tools/tidb-binlog/node"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/plugin"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/format"
	"golang.org/x/net/context"
)

var etcdDialTimeout = 5 * time.Second

// ShowExec represents a show executor.
type ShowExec struct {
	baseExecutor

	Tp        ast.ShowStmtType // Databases/Tables/Columns/....
	DBName    model.CIStr
	Table     *ast.TableName  // Used for showing columns.
	Column    *ast.ColumnName // Used for `desc table column`.
	IndexName model.CIStr     // Used for show table regions.
	Flag      int             // Some flag parsed from sql, such as FULL.
	Full      bool
	User      *auth.UserIdentity // Used for show grants.

	// GlobalScope is used by show variables
	GlobalScope bool

	is infoschema.InfoSchema

	result *chunk.Chunk
	cursor int
}

// Next implements the Executor Next interface.
func (e *ShowExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.GrowAndReset(e.maxChunkSize)
	if e.result == nil {
		e.result = e.newFirstChunk()
		err := e.fetchAll()
		if err != nil {
			return errors.Trace(err)
		}
		iter := chunk.NewIterator4Chunk(e.result)
		for colIdx := 0; colIdx < e.Schema().Len(); colIdx++ {
			retType := e.Schema().Columns[colIdx].RetType
			if !types.IsTypeVarchar(retType.Tp) {
				continue
			}
			for row := iter.Begin(); row != iter.End(); row = iter.Next() {
				if valLen := len(row.GetString(colIdx)); retType.Flen < valLen {
					retType.Flen = valLen
				}
			}
		}
	}
	if e.cursor >= e.result.NumRows() {
		return nil
	}
	numCurBatch := mathutil.Min(chk.Capacity(), e.result.NumRows()-e.cursor)
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
	case ast.ShowDrainerStatus:
		return e.fetchShowPumpOrDrainerStatus(node.DrainerNode)
	case ast.ShowEngines:
		return e.fetchShowEngines()
	case ast.ShowGrants:
		return e.fetchShowGrants()
	case ast.ShowIndex:
		return e.fetchShowIndex()
	case ast.ShowProcedureStatus:
		return e.fetchShowProcedureStatus()
	case ast.ShowPumpStatus:
		return e.fetchShowPumpOrDrainerStatus(node.PumpNode)
	case ast.ShowStatus:
		return e.fetchShowStatus()
	case ast.ShowTables:
		return e.fetchShowTables()
	case ast.ShowOpenTables:
		return e.fetchShowOpenTables()
	case ast.ShowTableStatus:
		return e.fetchShowTableStatus()
	case ast.ShowTriggers:
		return e.fetchShowTriggers()
	case ast.ShowVariables:
		return e.fetchShowVariables()
	case ast.ShowWarnings:
		return e.fetchShowWarnings(false)
	case ast.ShowErrors:
		return e.fetchShowWarnings(true)
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
	case ast.ShowStatsHealthy:
		e.fetchShowStatsHealthy()
		return nil
	case ast.ShowPlugins:
		return e.fetchShowPlugins()
	case ast.ShowProfiles:
		// empty result
	case ast.ShowMasterStatus:
		return e.fetchShowMasterStatus()
	case ast.ShowPrivileges:
		return e.fetchShowPrivileges()
	case ast.ShowRegions:
		return e.fetchShowTableRegions()
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
		row := pi.ToRowForShow(e.Full)
		e.appendRow(row)
	}
	return nil
}

func (e *ShowExec) fetchShowOpenTables() error {
	// TiDB has no concept like mysql's "table cache" and "open table"
	// For simplicity, we just return an empty result with the same structure as MySQL's SHOW OPEN TABLES
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
			model.TSConvert2Time(t.Meta().UpdateTS).String(), now, now, "utf8_general_ci", "", createOptions(t.Meta()), t.Meta().Comment})
	}
	return nil
}

func createOptions(tb *model.TableInfo) string {
	if tb.GetPartitionInfo() != nil {
		return "partitioned"
	}
	return ""
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
		var columnDefault interface{}
		if desc.DefaultValue != nil {
			// SHOW COLUMNS result expects string value
			defaultValStr := fmt.Sprintf("%v", desc.DefaultValue)
			// If column is timestamp, and default value is not current_timestamp, should convert the default value to the current session time zone.
			if col.Tp == mysql.TypeTimestamp && defaultValStr != types.ZeroDatetimeStr && !strings.HasPrefix(strings.ToUpper(defaultValStr), strings.ToUpper(ast.CurrentTimestamp)) {
				timeValue, err := table.GetColDefaultValue(e.ctx, col.ToInfo())
				if err != nil {
					return errors.Trace(err)
				}
				defaultValStr = timeValue.GetMysqlTime().String()
			}
			if col.Tp == mysql.TypeBit {
				defaultValBinaryLiteral := types.BinaryLiteral(defaultValStr)
				columnDefault = defaultValBinaryLiteral.ToBitLiteralString(true)
			} else {
				columnDefault = defaultValStr
			}
		}

		// The FULL keyword causes the output to include the column collation and comments,
		// as well as the privileges you have for each column.
		if e.Full {
			e.appendRow([]interface{}{
				desc.Field,
				desc.Type,
				desc.Collation,
				desc.Null,
				desc.Key,
				columnDefault,
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
				columnDefault,
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
		idxInfo := idx.Meta()
		if idxInfo.State != model.StatePublic {
			continue
		}
		for i, col := range idxInfo.Columns {
			nonUniq := 1
			if idx.Meta().Unique {
				nonUniq = 0
			}
			var subPart interface{}
			if col.Length != types.UnspecifiedLength {
				subPart = col.Length
			}
			e.appendRow([]interface{}{
				tb.Meta().Name.O,       // Table
				nonUniq,                // Non_unique
				idx.Meta().Name.O,      // Key_name
				i + 1,                  // Seq_in_index
				col.Name.O,             // Column_name
				"A",                    // Collation
				0,                      // Cardinality
				subPart,                // Sub_part
				nil,                    // Packed
				"YES",                  // Null
				idx.Meta().Tp.String(), // Index_type
				"",                     // Comment
				idx.Meta().Comment,     // Index_comment
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

func (e *ShowExec) fetchShowMasterStatus() error {
	tso := e.ctx.GetSessionVars().TxnCtx.StartTS
	e.appendRow([]interface{}{"tidb-binlog", tso, "", "", ""})
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

func getDefaultCollate(charsetName string) string {
	for _, c := range charset.GetAllCharsets() {
		if strings.EqualFold(c.Name, charsetName) {
			return c.DefaultCollation
		}
	}
	return ""
}

// escape the identifier for pretty-printing.
// For instance, the identifier "foo `bar`" will become "`foo ``bar```".
// The sqlMode controls whether to escape with backquotes (`) or double quotes
// (`"`) depending on whether mysql.ModeANSIQuotes is enabled.
func escape(cis model.CIStr, sqlMode mysql.SQLMode) string {
	var quote string
	if sqlMode&mysql.ModeANSIQuotes != 0 {
		quote = `"`
	} else {
		quote = "`"
	}
	return quote + strings.Replace(cis.O, quote, quote+quote, -1) + quote
}

func (e *ShowExec) fetchShowCreateTable() error {
	tb, err := e.getTable()
	if err != nil {
		return errors.Trace(err)
	}

	sqlMode := e.ctx.GetSessionVars().SQLMode

	// TODO: let the result more like MySQL.
	var buf bytes.Buffer

	tblCharset := tb.Meta().Charset
	if len(tblCharset) == 0 {
		tblCharset = mysql.DefaultCharset
	}
	tblCollate := tb.Meta().Collate
	// Set default collate if collate is not specified.
	if len(tblCollate) == 0 {
		tblCollate = getDefaultCollate(tblCharset)
	}

	fmt.Fprintf(&buf, "CREATE TABLE %s (\n", escape(tb.Meta().Name, sqlMode))
	var pkCol *table.Column
	var hasAutoIncID bool
	for i, col := range tb.Cols() {
		buf.WriteString(fmt.Sprintf("  %s %s", escape(col.Name, sqlMode), col.GetTypeDesc()))
		if col.Charset != "binary" {
			if col.Charset != tblCharset {
				fmt.Fprintf(&buf, " CHARACTER SET %s", col.Charset)
			}
			if col.Collate != tblCollate {
				fmt.Fprintf(&buf, " COLLATE %s", col.Collate)
			} else {
				defcol, err := charset.GetDefaultCollation(col.Charset)
				if err == nil && defcol != col.Collate {
					fmt.Fprintf(&buf, " COLLATE %s", col.Collate)
				}
			}
		}
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
				defaultValue := col.GetDefaultValue()
				switch defaultValue {
				case nil:
					if !mysql.HasNotNullFlag(col.Flag) {
						if col.Tp == mysql.TypeTimestamp {
							buf.WriteString(" NULL")
						}
						buf.WriteString(" DEFAULT NULL")
					}
				case "CURRENT_TIMESTAMP":
					buf.WriteString(" DEFAULT CURRENT_TIMESTAMP")
					if col.Decimal > 0 {
						buf.WriteString(fmt.Sprintf("(%d)", col.Decimal))
					}
				default:
					defaultValStr := fmt.Sprintf("%v", defaultValue)
					// If column is timestamp, and default value is not current_timestamp, should convert the default value to the current session time zone.
					if col.Tp == mysql.TypeTimestamp && defaultValStr != types.ZeroDatetimeStr {
						timeValue, err := table.GetColDefaultValue(e.ctx, col.ToInfo())
						if err != nil {
							return errors.Trace(err)
						}
						defaultValStr = timeValue.GetMysqlTime().String()
					}

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
				buf.WriteString(table.OptionalFsp(&col.FieldType))
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
		buf.WriteString(fmt.Sprintf("  PRIMARY KEY (%s)", escape(pkCol.Name, sqlMode)))
	}

	publicIndices := make([]table.Index, 0, len(tb.Indices()))
	for _, idx := range tb.Indices() {
		if idx.Meta().State == model.StatePublic {
			publicIndices = append(publicIndices, idx)
		}
	}
	if len(publicIndices) > 0 {
		buf.WriteString(",\n")
	}

	for i, idx := range publicIndices {
		idxInfo := idx.Meta()
		if idxInfo.Primary {
			buf.WriteString("  PRIMARY KEY ")
		} else if idxInfo.Unique {
			buf.WriteString(fmt.Sprintf("  UNIQUE KEY %s ", escape(idxInfo.Name, sqlMode)))
		} else {
			buf.WriteString(fmt.Sprintf("  KEY %s ", escape(idxInfo.Name, sqlMode)))
		}

		cols := make([]string, 0, len(idxInfo.Columns))
		for _, c := range idxInfo.Columns {
			colInfo := escape(c.Name, sqlMode)
			if c.Length != types.UnspecifiedLength {
				colInfo = fmt.Sprintf("%s(%s)", colInfo, strconv.Itoa(c.Length))
			}
			cols = append(cols, colInfo)
		}
		buf.WriteString(fmt.Sprintf("(%s)", strings.Join(cols, ",")))
		if i != len(publicIndices)-1 {
			buf.WriteString(",\n")
		}
	}

	buf.WriteString("\n")

	buf.WriteString(") ENGINE=InnoDB")
	// Because we only support case sensitive utf8_bin collate, we need to explicitly set the default charset and collation
	// to make it work on MySQL server which has default collate utf8_general_ci.
	if len(tblCollate) == 0 {
		// If we can not find default collate for the given charset,
		// do not show the collate part.
		fmt.Fprintf(&buf, " DEFAULT CHARSET=%s", tblCharset)
	} else {
		fmt.Fprintf(&buf, " DEFAULT CHARSET=%s COLLATE=%s", tblCharset, tblCollate)
	}

	// Displayed if the compression typed is set.
	if len(tb.Meta().Compression) != 0 {
		buf.WriteString(fmt.Sprintf(" COMPRESSION='%s'", tb.Meta().Compression))
	}

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

<<<<<<< HEAD
	if tb.Meta().ShardRowIDBits > 0 {
		fmt.Fprintf(&buf, "/*!90000 SHARD_ROW_ID_BITS=%d ", tb.Meta().ShardRowIDBits)
		if tb.Meta().PreSplitRegions > 0 {
			fmt.Fprintf(&buf, "PRE_SPLIT_REGIONS=%d ", tb.Meta().PreSplitRegions)
=======
	if tableInfo.AutoIdCache != 0 {
		fmt.Fprintf(buf, " /*T![auto_id_cache] AUTO_ID_CACHE=%d */", tableInfo.AutoIdCache)
	}

	if tableInfo.ShardRowIDBits > 0 {
		fmt.Fprintf(buf, "/*!90000 SHARD_ROW_ID_BITS=%d ", tableInfo.ShardRowIDBits)
		if tableInfo.PreSplitRegions > 0 {
			fmt.Fprintf(buf, "PRE_SPLIT_REGIONS=%d ", tableInfo.PreSplitRegions)
>>>>>>> 1c73dec... ddl: add syntax for setting the cache step of auto id explicitly. (#15409)
		}
		buf.WriteString("*/")
	}

	if len(tb.Meta().Comment) > 0 {
		buf.WriteString(fmt.Sprintf(" COMMENT='%s'", format.OutputFormat(tb.Meta().Comment)))
	}

	// add partition info here.
	appendPartitionInfo(tb.Meta().Partition, &buf)

	e.appendRow([]interface{}{tb.Meta().Name.O, buf.String()})
	return nil
}

func appendPartitionInfo(partitionInfo *model.PartitionInfo, buf *bytes.Buffer) {
	if partitionInfo != nil {
		// this if statement takes care of range columns case
		if partitionInfo.Columns != nil && partitionInfo.Type == model.PartitionTypeRange {
			buf.WriteString(fmt.Sprintf("\nPARTITION BY RANGE COLUMNS("))
			for i, col := range partitionInfo.Columns {
				buf.WriteString(col.L)
				if i < len(partitionInfo.Columns)-1 {
					buf.WriteString(",")
				}
			}
			buf.WriteString(") (\n")
		} else {
			buf.WriteString(fmt.Sprintf("\nPARTITION BY %s ( %s ) (\n", partitionInfo.Type.String(), partitionInfo.Expr))
		}
		for i, def := range partitionInfo.Definitions {
			lessThans := ""
			lessThans = strings.Join(def.LessThan, ",")

			var parDef string
			parDef = fmt.Sprintf("  PARTITION %s VALUES LESS THAN (%s)", def.Name, lessThans)
			if i < len(partitionInfo.Definitions)-1 {
				buf.WriteString(parDef)
				buf.WriteString(",\n")
			} else {
				buf.WriteString(parDef)
				buf.WriteString("\n")
			}
		}
		buf.WriteString(")")
	}
}

// fetchShowCreateDatabase composes show create database result.
func (e *ShowExec) fetchShowCreateDatabase() error {
	db, ok := e.is.SchemaByName(e.DBName)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(e.DBName.O)
	}

	sqlMode := e.ctx.GetSessionVars().SQLMode

	var buf bytes.Buffer
	fmt.Fprintf(&buf, "CREATE DATABASE %s", escape(db.Name, sqlMode))
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

func (e *ShowExec) fetchShowPrivileges() error {
	e.appendRow([]interface{}{"Alter", "Tables", "To alter the table"})
	e.appendRow([]interface{}{"Alter", "Tables", "To alter the table"})
	e.appendRow([]interface{}{"Alter routine", "Functions,Procedures", "To alter or drop stored functions/procedures"})
	e.appendRow([]interface{}{"Create", "Databases,Tables,Indexes", "To create new databases and tables"})
	e.appendRow([]interface{}{"Create routine", "Databases", "To use CREATE FUNCTION/PROCEDURE"})
	e.appendRow([]interface{}{"Create temporary tables", "Databases", "To use CREATE TEMPORARY TABLE"})
	e.appendRow([]interface{}{"Create view", "Tables", "To create new views"})
	e.appendRow([]interface{}{"Create user", "Server Admin", "To create new users"})
	e.appendRow([]interface{}{"Delete", "Tables", "To delete existing rows"})
	e.appendRow([]interface{}{"Drop", "Databases,Tables", "To drop databases, tables, and views"})
	e.appendRow([]interface{}{"Event", "Server Admin", "To create, alter, drop and execute events"})
	e.appendRow([]interface{}{"Execute", "Functions,Procedures", "To execute stored routines"})
	e.appendRow([]interface{}{"File", "File access on server", "To read and write files on the server"})
	e.appendRow([]interface{}{"Grant option", "Databases,Tables,Functions,Procedures", "To give to other users those privileges you possess"})
	e.appendRow([]interface{}{"Index", "Tables", "To create or drop indexes"})
	e.appendRow([]interface{}{"Insert", "Tables", "To insert data into tables"})
	e.appendRow([]interface{}{"Lock tables", "Databases", "To use LOCK TABLES (together with SELECT privilege)"})
	e.appendRow([]interface{}{"Process", "Server Admin", "To view the plain text of currently executing queries"})
	e.appendRow([]interface{}{"Proxy", "Server Admin", "To make proxy user possible"})
	e.appendRow([]interface{}{"References", "Databases,Tables", "To have references on tables"})
	e.appendRow([]interface{}{"Reload", "Server Admin", "To reload or refresh tables, logs and privileges"})
	e.appendRow([]interface{}{"Replication client", "Server Admin", "To ask where the slave or master servers are"})
	e.appendRow([]interface{}{"Replication slave", "Server Admin", "To read binary log events from the master"})
	e.appendRow([]interface{}{"Select", "Tables", "To retrieve rows from table"})
	e.appendRow([]interface{}{"Show databases", "Server Admin", "To see all databases with SHOW DATABASES"})
	e.appendRow([]interface{}{"Show view", "Tables", "To see views with SHOW CREATE VIEW"})
	e.appendRow([]interface{}{"Shutdown", "Server Admin", "To shut down the server"})
	e.appendRow([]interface{}{"Super", "Server Admin", "To use KILL thread, SET GLOBAL, CHANGE MASTER, etc."})
	e.appendRow([]interface{}{"Trigger", "Tables", "To use triggers"})
	e.appendRow([]interface{}{"Create tablespace", "Server Admin", "To create/alter/drop tablespaces"})
	e.appendRow([]interface{}{"Update", "Tables", "To update existing rows"})
	e.appendRow([]interface{}{"Usage", "Server Admin", "No privileges - allow connect only"})
	return nil
}

func (e *ShowExec) fetchShowTriggers() error {
	return nil
}

func (e *ShowExec) fetchShowProcedureStatus() error {
	return nil
}

func (e *ShowExec) fetchShowPlugins() error {
	tiPlugins := plugin.GetAll()
	for _, ps := range tiPlugins {
		for _, p := range ps {
			e.appendRow([]interface{}{p.Name, p.StateValue(), p.Kind.String(), p.Path, p.License, strconv.Itoa(int(p.Version))})
		}
	}
	return nil
}

func (e *ShowExec) fetchShowWarnings(errOnly bool) error {
	warns := e.ctx.GetSessionVars().StmtCtx.GetWarnings()
	for _, w := range warns {
		if errOnly && w.Level != stmtctx.WarnLevelError {
			continue
		}
		warn := errors.Cause(w.Err)
		switch x := warn.(type) {
		case *terror.Error:
			sqlErr := x.ToSQLError()
			e.appendRow([]interface{}{w.Level, int64(sqlErr.Code), sqlErr.Message})
		default:
			e.appendRow([]interface{}{w.Level, int64(mysql.ErrUnknown), warn.Error()})
		}
	}
	return nil
}

// fetchShowPumpOrDrainerStatus gets status of all pumps or drainers and fill them into e.rows.
func (e *ShowExec) fetchShowPumpOrDrainerStatus(kind string) error {
	registry, err := createRegistry(config.GetGlobalConfig().Path)
	if err != nil {
		return errors.Trace(err)
	}

	nodes, _, err := registry.Nodes(context.Background(), node.NodePrefix[kind])
	if err != nil {
		return errors.Trace(err)
	}
	err = registry.Close()
	if err != nil {
		return errors.Trace(err)
	}

	for _, n := range nodes {
		e.appendRow([]interface{}{n.NodeID, n.Addr, n.State, n.MaxCommitTS, utils.TSOToRoughTime(n.UpdateTS).Format(types.TimeFormat)})
	}

	return nil
}

// createRegistry returns an ectd registry
func createRegistry(urls string) (*node.EtcdRegistry, error) {
	ectdEndpoints, err := utils.ParseHostPortAddr(urls)
	if err != nil {
		return nil, errors.Trace(err)
	}
	cli, err := etcd.NewClientFromCfg(ectdEndpoints, etcdDialTimeout, node.DefaultRootPath, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return node.NewEtcdRegistry(cli, etcdDialTimeout), nil
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

func (e *ShowExec) fetchShowTableRegions() error {
	store := e.ctx.GetStore()
	tikvStore, ok := store.(tikv.Storage)
	if !ok {
		return nil
	}
	splitStore, ok := store.(kv.SplitableStore)
	if !ok {
		return nil
	}

	tb, err := e.getTable()
	if err != nil {
		return errors.Trace(err)
	}

	// Get table regions from from pd, not from regionCache, because the region cache maybe outdated.
	var regions []regionMeta
	if len(e.IndexName.L) != 0 {
		indexInfo := tb.Meta().FindIndexByName(e.IndexName.L)
		if indexInfo == nil {
			return plannercore.ErrKeyDoesNotExist.GenWithStackByArgs(e.IndexName, tb.Meta().Name)
		}
		regions, err = getTableIndexRegions(tb, indexInfo, tikvStore, splitStore)
	} else {
		regions, err = getTableRegions(tb, tikvStore, splitStore)
	}

	if err != nil {
		return err
	}
	e.fillRegionsToChunk(regions)
	return nil
}

func getTableRegions(tb table.Table, tikvStore tikv.Storage, splitStore kv.SplitableStore) ([]regionMeta, error) {
	if info := tb.Meta().GetPartitionInfo(); info != nil {
		return getPartitionTableRegions(info, tb.(table.PartitionedTable), tikvStore, splitStore)
	}
	return getPhysicalTableRegions(tb.Meta().ID, tb.Meta(), tikvStore, splitStore, nil)
}

func getTableIndexRegions(tb table.Table, indexInfo *model.IndexInfo, tikvStore tikv.Storage, splitStore kv.SplitableStore) ([]regionMeta, error) {
	if info := tb.Meta().GetPartitionInfo(); info != nil {
		return getPartitionIndexRegions(info, tb.(table.PartitionedTable), indexInfo, tikvStore, splitStore)
	}
	return getPhysicalIndexRegions(tb.Meta().ID, indexInfo, tikvStore, splitStore, nil)
}

func getPartitionTableRegions(info *model.PartitionInfo, tbl table.PartitionedTable, tikvStore tikv.Storage, splitStore kv.SplitableStore) ([]regionMeta, error) {
	regions := make([]regionMeta, 0, len(info.Definitions))
	uniqueRegionMap := make(map[uint64]struct{})
	for _, def := range info.Definitions {
		pid := def.ID
		partition := tbl.GetPartition(pid)
		partition.GetPhysicalID()
		partitionRegions, err := getPhysicalTableRegions(partition.GetPhysicalID(), tbl.Meta(), tikvStore, splitStore, uniqueRegionMap)
		if err != nil {
			return nil, err
		}
		regions = append(regions, partitionRegions...)
	}
	return regions, nil
}

func getPartitionIndexRegions(info *model.PartitionInfo, tbl table.PartitionedTable, indexInfo *model.IndexInfo, tikvStore tikv.Storage, splitStore kv.SplitableStore) ([]regionMeta, error) {
	var regions []regionMeta
	uniqueRegionMap := make(map[uint64]struct{})
	for _, def := range info.Definitions {
		pid := def.ID
		partition := tbl.GetPartition(pid)
		partition.GetPhysicalID()
		partitionRegions, err := getPhysicalIndexRegions(partition.GetPhysicalID(), indexInfo, tikvStore, splitStore, uniqueRegionMap)
		if err != nil {
			return nil, err
		}
		regions = append(regions, partitionRegions...)
	}
	return regions, nil
}

func (e *ShowExec) fillRegionsToChunk(regions []regionMeta) {
	for i := range regions {
		e.result.AppendUint64(0, regions[i].region.Id)
		e.result.AppendString(1, regions[i].start)
		e.result.AppendString(2, regions[i].end)
		e.result.AppendUint64(3, regions[i].leaderID)
		e.result.AppendUint64(4, regions[i].storeID)

		peers := ""
		for i, peer := range regions[i].region.Peers {
			if i > 0 {
				peers += ", "
			}
			peers += strconv.FormatUint(peer.Id, 10)
		}
		e.result.AppendString(5, peers)
		if regions[i].scattering {
			e.result.AppendInt64(6, 1)
		} else {
			e.result.AppendInt64(6, 0)
		}
	}
}
