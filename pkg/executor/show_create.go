// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	parserformat "github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/tidb"
	field_types "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/format"
	"github.com/pingcap/tidb/pkg/util/set"
	"github.com/pingcap/tidb/pkg/util/stringutil"
)

func getDefaultCollate(charsetName string) string {
	ch, err := charset.GetCharsetInfo(charsetName)
	if err != nil {
		// The charset is invalid, return server default.
		return mysql.DefaultCollationName
	}
	return ch.DefaultCollation
}

// ConstructResultOfShowCreateTable constructs the result for show create table.
func ConstructResultOfShowCreateTable(ctx sessionctx.Context, tableInfo *model.TableInfo, allocators autoid.Allocators, buf *bytes.Buffer) (err error) {
	return constructResultOfShowCreateTable(ctx, nil, tableInfo, allocators, buf)
}

func constructResultOfShowCreateTable(ctx sessionctx.Context, dbName *ast.CIStr, tableInfo *model.TableInfo, allocators autoid.Allocators, buf *bytes.Buffer) (err error) {
	if tableInfo.IsView() {
		fetchShowCreateTable4View(ctx, tableInfo, buf)
		return nil
	}
	if tableInfo.IsSequence() {
		ConstructResultOfShowCreateSequence(ctx, tableInfo, buf)
		return nil
	}

	tblCharset := tableInfo.Charset
	if len(tblCharset) == 0 {
		tblCharset = mysql.DefaultCharset
	}
	tblCollate := tableInfo.Collate
	// Set default collate if collate is not specified.
	if len(tblCollate) == 0 {
		tblCollate = getDefaultCollate(tblCharset)
	}

	sqlMode := ctx.GetSessionVars().SQLMode
	tableName := stringutil.Escape(tableInfo.Name.O, sqlMode)
	switch tableInfo.TempTableType {
	case model.TempTableGlobal:
		fmt.Fprintf(buf, "CREATE GLOBAL TEMPORARY TABLE %s (\n", tableName)
	case model.TempTableLocal:
		fmt.Fprintf(buf, "CREATE TEMPORARY TABLE %s (\n", tableName)
	default:
		fmt.Fprintf(buf, "CREATE TABLE %s (\n", tableName)
	}
	var pkCol *model.ColumnInfo
	var hasAutoIncID bool
	needAddComma := false
	for i, col := range tableInfo.Cols() {
		if col == nil || col.Hidden {
			continue
		}
		if needAddComma {
			buf.WriteString(",\n")
		}
		fmt.Fprintf(buf, "  %s %s", stringutil.Escape(col.Name.O, sqlMode), col.GetTypeDesc())
		if field_types.HasCharset(&col.FieldType) {
			if col.GetCharset() != tblCharset {
				fmt.Fprintf(buf, " CHARACTER SET %s", col.GetCharset())
			}
			if col.GetCollate() != tblCollate {
				fmt.Fprintf(buf, " COLLATE %s", col.GetCollate())
			} else {
				defcol, err := charset.GetDefaultCollation(col.GetCharset())
				if err == nil && defcol != col.GetCollate() {
					fmt.Fprintf(buf, " COLLATE %s", col.GetCollate())
				}
			}
		}
		if col.IsGenerated() {
			// It's a generated column.
			fmt.Fprintf(buf, " GENERATED ALWAYS AS (%s)", col.GeneratedExprString)
			if col.GeneratedStored {
				buf.WriteString(" STORED")
			} else {
				buf.WriteString(" VIRTUAL")
			}
		}
		if mysql.HasAutoIncrementFlag(col.GetFlag()) {
			hasAutoIncID = true
			buf.WriteString(" NOT NULL AUTO_INCREMENT")
		} else {
			if mysql.HasNotNullFlag(col.GetFlag()) {
				buf.WriteString(" NOT NULL")
			}
			// default values are not shown for generated columns in MySQL
			if !mysql.HasNoDefaultValueFlag(col.GetFlag()) && !col.IsGenerated() {
				defaultValue := col.GetDefaultValue()
				switch defaultValue {
				case nil:
					if !mysql.HasNotNullFlag(col.GetFlag()) {
						if col.GetType() == mysql.TypeTimestamp {
							buf.WriteString(" NULL")
						}
						buf.WriteString(" DEFAULT NULL")
					}
				case "CURRENT_TIMESTAMP":
					buf.WriteString(" DEFAULT ")
					buf.WriteString(defaultValue.(string))
					if col.GetDecimal() > 0 {
						fmt.Fprintf(buf, "(%d)", col.GetDecimal())
					}
				case "CURRENT_DATE":
					buf.WriteString(" DEFAULT (")
					buf.WriteString(defaultValue.(string))
					if col.GetDecimal() > 0 {
						fmt.Fprintf(buf, "(%d)", col.GetDecimal())
					}
					buf.WriteString(")")
				default:
					defaultValStr := fmt.Sprintf("%v", defaultValue)
					// If column is timestamp, and default value is not current_timestamp, should convert the default value to the current session time zone.
					if defaultValStr != types.ZeroDatetimeStr && col.GetType() == mysql.TypeTimestamp {
						timeValue, err := table.GetColDefaultValue(ctx.GetExprCtx(), col)
						if err != nil {
							return errors.Trace(err)
						}
						defaultValStr = timeValue.GetMysqlTime().String()
					}

					if col.DefaultIsExpr {
						fmt.Fprintf(buf, " DEFAULT (%s)", defaultValStr)
					} else {
						if col.GetType() == mysql.TypeBit {
							defaultValBinaryLiteral := types.BinaryLiteral(defaultValStr)
							fmt.Fprintf(buf, " DEFAULT %s", defaultValBinaryLiteral.ToBitLiteralString(true))
						} else {
							fmt.Fprintf(buf, " DEFAULT '%s'", format.OutputFormat(defaultValStr))
						}
					}
				}
			}
			if mysql.HasOnUpdateNowFlag(col.GetFlag()) {
				buf.WriteString(" ON UPDATE CURRENT_TIMESTAMP")
				buf.WriteString(table.OptionalFsp(&col.FieldType))
			}
		}
		if ddl.IsAutoRandomColumnID(tableInfo, col.ID) {
			s, r := tableInfo.AutoRandomBits, tableInfo.AutoRandomRangeBits
			if r == 0 || r == autoid.AutoRandomRangeBitsDefault {
				fmt.Fprintf(buf, " /*T![auto_rand] AUTO_RANDOM(%d) */", s)
			} else {
				fmt.Fprintf(buf, " /*T![auto_rand] AUTO_RANDOM(%d, %d) */", s, r)
			}
		}
		if len(col.Comment) > 0 {
			fmt.Fprintf(buf, " COMMENT '%s'", format.OutputFormat(col.Comment))
		}
		if i != len(tableInfo.Cols())-1 {
			needAddComma = true
		}
		if tableInfo.PKIsHandle && mysql.HasPriKeyFlag(col.GetFlag()) {
			pkCol = col
		}
	}

	if pkCol != nil {
		// If PKIsHandle, pk info is not in tb.Indices(). We should handle it here.
		buf.WriteString(",\n")
		fmt.Fprintf(buf, "  PRIMARY KEY (%s)", stringutil.Escape(pkCol.Name.O, sqlMode))
		buf.WriteString(" /*T![clustered_index] CLUSTERED */")
	}

	publicIndices := make([]*model.IndexInfo, 0, len(tableInfo.Indices))
	for _, idx := range tableInfo.Indices {
		if idx.State == model.StatePublic {
			publicIndices = append(publicIndices, idx)
		}
	}

	// consider hypo-indexes
	hypoIndexes := ctx.GetSessionVars().HypoIndexes
	if hypoIndexes != nil && dbName != nil {
		schemaName := dbName.L
		tblName := tableInfo.Name.L
		if hypoIndexes[schemaName] != nil && hypoIndexes[schemaName][tblName] != nil {
			hypoIndexList := make([]*model.IndexInfo, 0, len(hypoIndexes[schemaName][tblName]))
			for _, index := range hypoIndexes[schemaName][tblName] {
				hypoIndexList = append(hypoIndexList, index)
			}
			sort.Slice(hypoIndexList, func(i, j int) bool { // to make the result stable
				return hypoIndexList[i].Name.O < hypoIndexList[j].Name.O
			})
			publicIndices = append(publicIndices, hypoIndexList...)
		}
	}
	if len(publicIndices) > 0 {
		buf.WriteString(",\n")
	}

	for i, idxInfo := range publicIndices {
		if idxInfo.Primary {
			buf.WriteString("  PRIMARY KEY ")
		} else if idxInfo.Unique {
			fmt.Fprintf(buf, "  UNIQUE KEY %s ", stringutil.Escape(idxInfo.Name.O, sqlMode))
		} else if idxInfo.VectorInfo != nil {
			fmt.Fprintf(buf, "  VECTOR INDEX %s", stringutil.Escape(idxInfo.Name.O, sqlMode))
		} else if idxInfo.FullTextInfo != nil {
			fmt.Fprintf(buf, "  FULLTEXT INDEX %s", stringutil.Escape(idxInfo.Name.O, sqlMode))
		} else if idxInfo.InvertedInfo != nil {
			fmt.Fprintf(buf, "  COLUMNAR INDEX %s", stringutil.Escape(idxInfo.Name.O, sqlMode))
		} else {
			fmt.Fprintf(buf, "  KEY %s ", stringutil.Escape(idxInfo.Name.O, sqlMode))
		}

		cols := make([]string, 0, len(idxInfo.Columns))
		var colInfo string
		for _, c := range idxInfo.Columns {
			if tableInfo.Columns[c.Offset].Hidden {
				colInfo = fmt.Sprintf("(%s)", tableInfo.Columns[c.Offset].GeneratedExprString)
			} else {
				colInfo = stringutil.Escape(c.Name.O, sqlMode)
				if c.Length != types.UnspecifiedLength {
					colInfo = fmt.Sprintf("%s(%s)", colInfo, strconv.Itoa(c.Length))
				}
			}
			cols = append(cols, colInfo)
		}
		if idxInfo.VectorInfo != nil {
			funcName := model.IndexableDistanceMetricToFnName[idxInfo.VectorInfo.DistanceMetric]
			fmt.Fprintf(buf, "((%s(%s)))", strings.ToUpper(funcName), strings.Join(cols, ","))
		} else {
			fmt.Fprintf(buf, "(%s)", strings.Join(cols, ","))
		}

		if idxInfo.InvertedInfo != nil {
			fmt.Fprintf(buf, " USING INVERTED")
		}
		if idxInfo.FullTextInfo != nil {
			fmt.Fprintf(buf, " WITH PARSER %s", idxInfo.FullTextInfo.ParserType.SQLName())
		}
		if idxInfo.ConditionExprString != "" {
			fmt.Fprintf(buf, " WHERE %s", idxInfo.ConditionExprString)
		}
		if idxInfo.Invisible {
			fmt.Fprintf(buf, ` /*!80000 INVISIBLE */`)
		}
		if idxInfo.Comment != "" {
			fmt.Fprintf(buf, ` COMMENT '%s'`, format.OutputFormat(idxInfo.Comment))
		}
		if idxInfo.Tp == ast.IndexTypeHypo {
			fmt.Fprintf(buf, ` /* HYPO INDEX */`)
		}
		if idxInfo.Primary {
			if tableInfo.HasClusteredIndex() {
				buf.WriteString(" /*T![clustered_index] CLUSTERED */")
			} else {
				buf.WriteString(" /*T![clustered_index] NONCLUSTERED */")
			}
		}
		if idxInfo.Global {
			buf.WriteString(" /*T![global_index] GLOBAL */")
		}
		if i != len(publicIndices)-1 {
			buf.WriteString(",\n")
		}
	}

	// Foreign Keys are supported by data dictionary even though
	// they are not enforced by DDL. This is still helpful to applications.
	for _, fk := range tableInfo.ForeignKeys {
		fmt.Fprintf(buf, ",\n  CONSTRAINT %s FOREIGN KEY ", stringutil.Escape(fk.Name.O, sqlMode))
		colNames := make([]string, 0, len(fk.Cols))
		for _, col := range fk.Cols {
			colNames = append(colNames, stringutil.Escape(col.O, sqlMode))
		}
		fmt.Fprintf(buf, "(%s)", strings.Join(colNames, ","))
		if fk.RefSchema.L != "" && dbName != nil && fk.RefSchema.L != dbName.L {
			fmt.Fprintf(buf, " REFERENCES %s.%s ", stringutil.Escape(fk.RefSchema.O, sqlMode), stringutil.Escape(fk.RefTable.O, sqlMode))
		} else {
			fmt.Fprintf(buf, " REFERENCES %s ", stringutil.Escape(fk.RefTable.O, sqlMode))
		}
		refColNames := make([]string, 0, len(fk.Cols))
		for _, refCol := range fk.RefCols {
			refColNames = append(refColNames, stringutil.Escape(refCol.O, sqlMode))
		}
		fmt.Fprintf(buf, "(%s)", strings.Join(refColNames, ","))
		if ast.ReferOptionType(fk.OnDelete) != 0 {
			fmt.Fprintf(buf, " ON DELETE %s", ast.ReferOptionType(fk.OnDelete).String())
		}
		if ast.ReferOptionType(fk.OnUpdate) != 0 {
			fmt.Fprintf(buf, " ON UPDATE %s", ast.ReferOptionType(fk.OnUpdate).String())
		}
		if fk.Version < model.FKVersion1 {
			buf.WriteString(" /* FOREIGN KEY INVALID */")
		}
	}
	// add check constraints info
	publicConstraints := make([]*model.ConstraintInfo, 0, len(tableInfo.Indices))
	for _, constr := range tableInfo.Constraints {
		if constr.State == model.StatePublic {
			publicConstraints = append(publicConstraints, constr)
		}
	}
	if len(publicConstraints) > 0 {
		buf.WriteString(",\n")
	}
	for i, constrInfo := range publicConstraints {
		fmt.Fprintf(buf, "  CONSTRAINT %s CHECK ((%s))", stringutil.Escape(constrInfo.Name.O, sqlMode), constrInfo.ExprString)
		if !constrInfo.Enforced {
			buf.WriteString(" /*!80016 NOT ENFORCED */")
		}
		if i != len(publicConstraints)-1 {
			buf.WriteString(",\n")
		}
	}

	buf.WriteString("\n")

	buf.WriteString(") ENGINE=InnoDB")
	// We need to explicitly set the default charset and collation
	// to make it work on MySQL server which has default collate utf8_general_ci.
	if len(tblCollate) == 0 || tblCollate == "binary" {
		// If we can not find default collate for the given charset,
		// or the collate is 'binary'(MySQL-5.7 compatibility, see #15633 for details),
		// do not show the collate part.
		fmt.Fprintf(buf, " DEFAULT CHARSET=%s", tblCharset)
	} else {
		fmt.Fprintf(buf, " DEFAULT CHARSET=%s COLLATE=%s", tblCharset, tblCollate)
	}

	// Displayed if the compression typed is set.
	if len(tableInfo.Compression) != 0 {
		fmt.Fprintf(buf, " COMPRESSION='%s'", tableInfo.Compression)
	}

	incrementAllocator := allocators.Get(autoid.AutoIncrementType)
	if hasAutoIncID && incrementAllocator != nil {
		autoIncID, err := incrementAllocator.NextGlobalAutoID()
		if err != nil {
			return errors.Trace(err)
		}

		// It's compatible with MySQL.
		if autoIncID > 1 {
			fmt.Fprintf(buf, " AUTO_INCREMENT=%d", autoIncID)
		}
	}

	if tableInfo.AutoIDCache != 0 {
		fmt.Fprintf(buf, " /*T![auto_id_cache] AUTO_ID_CACHE=%d */", tableInfo.AutoIDCache)
	}

	randomAllocator := allocators.Get(autoid.AutoRandomType)
	if randomAllocator != nil {
		autoRandID, err := randomAllocator.NextGlobalAutoID()
		if err != nil {
			return errors.Trace(err)
		}

		if autoRandID > 1 {
			fmt.Fprintf(buf, " /*T![auto_rand_base] AUTO_RANDOM_BASE=%d */", autoRandID)
		}
	}

	if tableInfo.ShardRowIDBits > 0 {
		fmt.Fprintf(buf, " /*T! SHARD_ROW_ID_BITS=%d ", tableInfo.ShardRowIDBits)
		if tableInfo.PreSplitRegions > 0 {
			fmt.Fprintf(buf, "PRE_SPLIT_REGIONS=%d ", tableInfo.PreSplitRegions)
		}
		buf.WriteString("*/")
	}

	if tableInfo.AutoRandomBits > 0 && tableInfo.PreSplitRegions > 0 {
		fmt.Fprintf(buf, " /*T! PRE_SPLIT_REGIONS=%d */", tableInfo.PreSplitRegions)
	}

	if len(tableInfo.Comment) > 0 {
		fmt.Fprintf(buf, " COMMENT='%s'", format.OutputFormat(tableInfo.Comment))
	}

	if tableInfo.TempTableType == model.TempTableGlobal {
		fmt.Fprintf(buf, " ON COMMIT DELETE ROWS")
	}

	if tableInfo.PlacementPolicyRef != nil {
		fmt.Fprintf(buf, " /*T![placement] PLACEMENT POLICY=%s */", stringutil.Escape(tableInfo.PlacementPolicyRef.Name.String(), sqlMode))
	}

	if tableInfo.TableCacheStatusType == model.TableCacheStatusEnable {
		// This is not meant to be understand by other components, so it's not written as /*T![cached] */
		// For all external components, cached table is just a normal table.
		fmt.Fprintf(buf, " /* CACHED ON */")
	}

	if tableInfo.TTLInfo != nil {
		restoreFlags := parserformat.RestoreStringSingleQuotes | parserformat.RestoreNameBackQuotes | parserformat.RestoreTiDBSpecialComment
		restoreCtx := parserformat.NewRestoreCtx(restoreFlags, buf)

		restoreCtx.WritePlain(" ")
		err = restoreCtx.WriteWithSpecialComments(tidb.FeatureIDTTL, func() error {
			columnName := ast.ColumnName{Name: tableInfo.TTLInfo.ColumnName}
			timeUnit := ast.TimeUnitExpr{Unit: ast.TimeUnitType(tableInfo.TTLInfo.IntervalTimeUnit)}
			restoreCtx.WriteKeyWord("TTL")
			restoreCtx.WritePlain("=")
			restoreCtx.WriteName(columnName.String())
			restoreCtx.WritePlainf(" + INTERVAL %s ", tableInfo.TTLInfo.IntervalExprStr)
			return timeUnit.Restore(restoreCtx)
		})

		if err != nil {
			return err
		}

		restoreCtx.WritePlain(" ")
		err = restoreCtx.WriteWithSpecialComments(tidb.FeatureIDTTL, func() error {
			restoreCtx.WriteKeyWord("TTL_ENABLE")
			restoreCtx.WritePlain("=")
			if tableInfo.TTLInfo.Enable {
				restoreCtx.WriteString("ON")
			} else {
				restoreCtx.WriteString("OFF")
			}
			return nil
		})

		if err != nil {
			return err
		}

		restoreCtx.WritePlain(" ")
		err = restoreCtx.WriteWithSpecialComments(tidb.FeatureIDTTL, func() error {
			restoreCtx.WriteKeyWord("TTL_JOB_INTERVAL")
			restoreCtx.WritePlain("=")
			if len(tableInfo.TTLInfo.JobInterval) == 0 {
				// This only happens when the table is created from 6.5 in which the `tidb_job_interval` is not introduced yet.
				// We use `OldDefaultTTLJobInterval` as the return value to ensure a consistent behavior for the
				// upgrades: v6.5 -> v8.5(or previous version) -> newer version than v8.5.
				restoreCtx.WriteString(model.OldDefaultTTLJobInterval)
			} else {
				restoreCtx.WriteString(tableInfo.TTLInfo.JobInterval)
			}
			return nil
		})

		if err != nil {
			return err
		}
	}

	if tableInfo.Affinity != nil {
		fmt.Fprintf(buf, " /*T![%s] AFFINITY='%s' */", tidb.FeatureIDAffinity, tableInfo.Affinity.Level)
	}

	// add partition info here.
	ddl.AppendPartitionInfo(tableInfo.Partition, buf, sqlMode)
	return nil
}

// ConstructResultOfShowCreateSequence constructs the result for show create sequence.
func ConstructResultOfShowCreateSequence(ctx sessionctx.Context, tableInfo *model.TableInfo, buf *bytes.Buffer) {
	sqlMode := ctx.GetSessionVars().SQLMode
	fmt.Fprintf(buf, "CREATE SEQUENCE %s ", stringutil.Escape(tableInfo.Name.O, sqlMode))
	sequenceInfo := tableInfo.Sequence
	fmt.Fprintf(buf, "start with %d ", sequenceInfo.Start)
	fmt.Fprintf(buf, "minvalue %d ", sequenceInfo.MinValue)
	fmt.Fprintf(buf, "maxvalue %d ", sequenceInfo.MaxValue)
	fmt.Fprintf(buf, "increment by %d ", sequenceInfo.Increment)
	if sequenceInfo.Cache {
		fmt.Fprintf(buf, "cache %d ", sequenceInfo.CacheValue)
	} else {
		buf.WriteString("nocache ")
	}
	if sequenceInfo.Cycle {
		buf.WriteString("cycle ")
	} else {
		buf.WriteString("nocycle ")
	}
	buf.WriteString("ENGINE=InnoDB")
	if len(sequenceInfo.Comment) > 0 {
		fmt.Fprintf(buf, " COMMENT='%s'", format.OutputFormat(sequenceInfo.Comment))
	}
}

func (e *ShowExec) fetchShowCreateSequence() error {
	tbl, err := e.getTable()
	if err != nil {
		return errors.Trace(err)
	}
	tableInfo := tbl.Meta()
	if !tableInfo.IsSequence() {
		return exeerrors.ErrWrongObject.GenWithStackByArgs(e.DBName.O, tableInfo.Name.O, "SEQUENCE")
	}
	var buf bytes.Buffer
	ConstructResultOfShowCreateSequence(e.Ctx(), tableInfo, &buf)
	e.appendRow([]any{tableInfo.Name.O, buf.String()})
	return nil
}

// TestShowClusterConfigKey is the key used to store TestShowClusterConfigFunc.
var TestShowClusterConfigKey stringutil.StringerStr = "TestShowClusterConfigKey"

// TestShowClusterConfigFunc is used to test 'show config ...'.
type TestShowClusterConfigFunc func() ([][]types.Datum, error)

func (e *ShowExec) fetchShowClusterConfigs() error {
	emptySet := set.NewStringSet()
	var confItems [][]types.Datum
	var err error
	if f := e.Ctx().Value(TestShowClusterConfigKey); f != nil {
		confItems, err = f.(TestShowClusterConfigFunc)()
	} else {
		confItems, err = fetchClusterConfig(e.Ctx(), emptySet, emptySet)
	}
	if err != nil {
		return err
	}
	for _, items := range confItems {
		row := make([]any, 0, 4)
		for _, item := range items {
			row = append(row, item.GetString())
		}
		e.appendRow(row)
	}
	return nil
}

func (e *ShowExec) fetchShowCreateTable() error {
	tb, err := e.getTable()
	if err != nil {
		return errors.Trace(err)
	}

	tableInfo := tb.Meta()
	var buf bytes.Buffer
	// TODO: let the result more like MySQL.
	if err = constructResultOfShowCreateTable(e.Ctx(), &e.DBName, tableInfo, tb.Allocators(e.Ctx().GetTableCtx()), &buf); err != nil {
		return err
	}
	if tableInfo.IsView() {
		e.appendRow([]any{tableInfo.Name.O, buf.String(), tableInfo.Charset, tableInfo.Collate})
		return nil
	}

	e.appendRow([]any{tableInfo.Name.O, buf.String()})
	return nil
}

func (e *ShowExec) fetchShowCreateView() error {
	db, ok := e.is.SchemaByName(e.DBName)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(e.DBName.O)
	}

	tb, err := e.getTable()
	if err != nil {
		return errors.Trace(err)
	}

	if !tb.Meta().IsView() {
		return exeerrors.ErrWrongObject.GenWithStackByArgs(db.Name.O, tb.Meta().Name.O, "VIEW")
	}

	var buf bytes.Buffer
	fetchShowCreateTable4View(e.Ctx(), tb.Meta(), &buf)
	e.appendRow([]any{tb.Meta().Name.O, buf.String(), tb.Meta().Charset, tb.Meta().Collate})
	return nil
}

func fetchShowCreateTable4View(ctx sessionctx.Context, tb *model.TableInfo, buf *bytes.Buffer) {
	sqlMode := ctx.GetSessionVars().SQLMode
	fmt.Fprintf(buf, "CREATE ALGORITHM=%s ", tb.View.Algorithm.String())
	if tb.View.Definer.AuthUsername == "" || tb.View.Definer.AuthHostname == "" {
		fmt.Fprintf(buf, "DEFINER=%s@%s ", stringutil.Escape(tb.View.Definer.Username, sqlMode), stringutil.Escape(tb.View.Definer.Hostname, sqlMode))
	} else {
		fmt.Fprintf(buf, "DEFINER=%s@%s ", stringutil.Escape(tb.View.Definer.AuthUsername, sqlMode), stringutil.Escape(tb.View.Definer.AuthHostname, sqlMode))
	}
	fmt.Fprintf(buf, "SQL SECURITY %s ", tb.View.Security.String())
	fmt.Fprintf(buf, "VIEW %s (", stringutil.Escape(tb.Name.O, sqlMode))
	for i, col := range tb.Columns {
		fmt.Fprintf(buf, "%s", stringutil.Escape(col.Name.O, sqlMode))
		if i < len(tb.Columns)-1 {
			fmt.Fprintf(buf, ", ")
		}
	}
	fmt.Fprintf(buf, ") AS %s", tb.View.SelectStmt)
}

// ConstructResultOfShowCreateDatabase constructs the result for show create database.
func ConstructResultOfShowCreateDatabase(ctx sessionctx.Context, dbInfo *model.DBInfo, ifNotExists bool, buf *bytes.Buffer) (err error) {
	sqlMode := ctx.GetSessionVars().SQLMode
	var ifNotExistsStr string
	if ifNotExists {
		ifNotExistsStr = "IF NOT EXISTS "
	}
	fmt.Fprintf(buf, "CREATE DATABASE %s%s", ifNotExistsStr, stringutil.Escape(dbInfo.Name.O, sqlMode))
	if dbInfo.Charset != "" {
		fmt.Fprintf(buf, " /*!40100 DEFAULT CHARACTER SET %s ", dbInfo.Charset)
		defaultCollate, err := charset.GetDefaultCollation(dbInfo.Charset)
		if err != nil {
			return errors.Trace(err)
		}
		if dbInfo.Collate != "" && dbInfo.Collate != defaultCollate {
			fmt.Fprintf(buf, "COLLATE %s ", dbInfo.Collate)
		}
		fmt.Fprint(buf, "*/")
	} else if dbInfo.Collate != "" {
		collInfo, err := collate.GetCollationByName(dbInfo.Collate)
		if err != nil {
			return errors.Trace(err)
		}
		fmt.Fprintf(buf, " /*!40100 DEFAULT CHARACTER SET %s ", collInfo.CharsetName)
		if !collInfo.IsDefault {
			fmt.Fprintf(buf, "COLLATE %s ", dbInfo.Collate)
		}
		fmt.Fprint(buf, "*/")
	}
	// MySQL 5.7 always show the charset info but TiDB may ignore it, which makes a slight difference. We keep this
	// behavior unchanged because it is trivial enough.
	if dbInfo.PlacementPolicyRef != nil {
		// add placement ref info here
		fmt.Fprintf(buf, " /*T![placement] PLACEMENT POLICY=%s */", stringutil.Escape(dbInfo.PlacementPolicyRef.Name.O, sqlMode))
	}
	return nil
}

// ConstructResultOfShowCreatePlacementPolicy constructs the result for show create placement policy.
func ConstructResultOfShowCreatePlacementPolicy(policyInfo *model.PolicyInfo) string {
	return fmt.Sprintf("CREATE PLACEMENT POLICY `%s` %s", policyInfo.Name.O, policyInfo.PlacementSettings.String())
}

// constructResultOfShowCreateResourceGroup constructs the result for show create resource group.
func constructResultOfShowCreateResourceGroup(resourceGroup *model.ResourceGroupInfo) string {
	return fmt.Sprintf("CREATE RESOURCE GROUP `%s` %s", resourceGroup.Name.O, resourceGroup.ResourceGroupSettings.String())
}

// fetchShowCreateDatabase composes show create database result.
func (e *ShowExec) fetchShowCreateDatabase() error {
	checker := privilege.GetPrivilegeManager(e.Ctx())
	if checker != nil && e.Ctx().GetSessionVars().User != nil {
		if !checker.DBIsVisible(e.Ctx().GetSessionVars().ActiveRoles, e.DBName.String()) {
			return e.dbAccessDenied()
		}
	}
	dbInfo, ok := e.is.SchemaByName(e.DBName)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(e.DBName.O)
	}

	var buf bytes.Buffer
	err := ConstructResultOfShowCreateDatabase(e.Ctx(), dbInfo, e.IfNotExists, &buf)
	if err != nil {
		return err
	}
	e.appendRow([]any{dbInfo.Name.O, buf.String()})
	return nil
}

// fetchShowCreatePlacementPolicy composes show create policy result.
func (e *ShowExec) fetchShowCreatePlacementPolicy() error {
	policy, found := e.is.PolicyByName(e.DBName)
	if !found {
		return infoschema.ErrPlacementPolicyNotExists.GenWithStackByArgs(e.DBName.O)
	}
	showCreate := ConstructResultOfShowCreatePlacementPolicy(policy)
	e.appendRow([]any{e.DBName.O, showCreate})
	return nil
}

// fetchShowCreateResourceGroup composes show create resource group result.
func (e *ShowExec) fetchShowCreateResourceGroup() error {
	group, found := e.is.ResourceGroupByName(e.ResourceGroupName)
	if !found {
		return infoschema.ErrResourceGroupNotExists.GenWithStackByArgs(e.ResourceGroupName.O)
	}
	showCreate := constructResultOfShowCreateResourceGroup(group)
	e.appendRow([]any{e.ResourceGroupName.O, showCreate})
	return nil
}

