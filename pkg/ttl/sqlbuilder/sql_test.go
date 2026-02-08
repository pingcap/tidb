// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlbuilder_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/ttl/cache"
	"github.com/pingcap/tidb/pkg/ttl/session"
	"github.com/pingcap/tidb/pkg/ttl/sqlbuilder"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
)

func TestEscape(t *testing.T) {
	tb := &cache.PhysicalTable{
		Schema: pmodel.NewCIStr("testp;\"';123`456"),
		TableInfo: &model.TableInfo{
			Name: pmodel.NewCIStr("tp\"';123`456"),
		},
		KeyColumns: []*model.ColumnInfo{
			{Name: pmodel.NewCIStr("col1\"';123`456"), FieldType: *types.NewFieldType(mysql.TypeString)},
		},
		TTLTimeColumn: &model.ColumnInfo{
			Name:      pmodel.NewCIStr("time\"';123`456"),
			FieldType: *types.NewFieldType(mysql.TypeDatetime),
		},
		PartitionDef: &model.PartitionDefinition{
			Name: pmodel.NewCIStr("p1\"';123`456"),
		},
	}

	buildSelect := func(d []types.Datum) string {
		b := sqlbuilder.NewSQLBuilder(tb, session.TTLJobTypeTTL)

		require.NoError(t, b.WriteSelectColumns(tb.KeyColumns))
		require.NoError(t, b.WriteCommonCondition(tb.KeyColumns, ">", d))
		require.NoError(t, b.WriteExpireCondition(time.UnixMilli(0).In(time.UTC)))
		s, err := b.Build()
		require.NoError(t, err)
		return s
	}

	buildDelete := func(ds ...[]types.Datum) string {
		b := sqlbuilder.NewSQLBuilder(tb, session.TTLJobTypeTTL)

		require.NoError(t, b.WriteDelete())
		require.NoError(t, b.WriteInCondition(tb.KeyColumns, ds...))
		require.NoError(t, b.WriteExpireCondition(time.UnixMilli(0).In(time.UTC)))
		s, err := b.Build()
		require.NoError(t, err)
		return s
	}

	cases := []struct {
		tp  string
		ds  [][]types.Datum
		sql string
	}{
		{
			tp:  "select",
			ds:  [][]types.Datum{d("key1'\";123`456\t\n\r")},
			sql: "SELECT LOW_PRIORITY SQL_NO_CACHE `col1\"';123``456` FROM `testp;\"';123``456`.`tp\"';123``456` PARTITION(`p1\"';123``456`) WHERE `col1\"';123``456` > 'key1\\'\\\";123`456\t\\n\\r' AND `time\"';123``456` < FROM_UNIXTIME(0)",
		},
		{
			tp:  "delete",
			ds:  [][]types.Datum{d("key2'\";123`456\t\n\r")},
			sql: "DELETE LOW_PRIORITY FROM `testp;\"';123``456`.`tp\"';123``456` PARTITION(`p1\"';123``456`) WHERE `col1\"';123``456` IN ('key2\\'\\\";123`456\t\\n\\r') AND `time\"';123``456` < FROM_UNIXTIME(0)",
		},
		{
			tp:  "delete",
			ds:  [][]types.Datum{d("key3'\";123`456\t\n\r"), d("key4'`\"")},
			sql: "DELETE LOW_PRIORITY FROM `testp;\"';123``456`.`tp\"';123``456` PARTITION(`p1\"';123``456`) WHERE `col1\"';123``456` IN ('key3\\'\\\";123`456\t\\n\\r', 'key4\\'`\\\"') AND `time\"';123``456` < FROM_UNIXTIME(0)",
		},
	}

	for _, c := range cases {
		switch c.tp {
		case "select":
			require.Equal(t, 1, len(c.ds))
			require.Equal(t, c.sql, buildSelect(c.ds[0]))
		case "delete":
			require.Equal(t, c.sql, buildDelete(c.ds...))
		default:
			require.FailNow(t, "invalid tp: %s", c.tp)
		}

		p := parser.New()
		stmts, _, err := p.Parse(c.sql, "", "")
		require.Equal(t, 1, len(stmts))
		require.NoError(t, err)

		var tbName *ast.TableName
		var keyColumnName, timeColumnName string
		var values []string
		var timeFunc string
		var timeTS int64
		switch c.tp {
		case "select":
			stmt, ok := stmts[0].(*ast.SelectStmt)
			require.True(t, ok)
			tbName = stmt.From.TableRefs.Left.(*ast.TableSource).Source.(*ast.TableName)
			and := stmt.Where.(*ast.BinaryOperationExpr)
			cond1 := and.L.(*ast.BinaryOperationExpr)
			keyColumnName = cond1.L.(*ast.ColumnNameExpr).Name.Name.O
			values = []string{cond1.R.(ast.ValueExpr).GetValue().(string)}
			cond2 := and.R.(*ast.BinaryOperationExpr)
			timeColumnName = cond2.L.(*ast.ColumnNameExpr).Name.Name.O
			timeFunc = cond2.R.(*ast.FuncCallExpr).FnName.L
			timeTS = cond2.R.(*ast.FuncCallExpr).Args[0].(ast.ValueExpr).GetValue().(int64)
		case "delete":
			stmt, ok := stmts[0].(*ast.DeleteStmt)
			require.True(t, ok)
			tbName = stmt.TableRefs.TableRefs.Left.(*ast.TableSource).Source.(*ast.TableName)
			and := stmt.Where.(*ast.BinaryOperationExpr)
			cond1 := and.L.(*ast.PatternInExpr)
			keyColumnName = cond1.Expr.(*ast.ColumnNameExpr).Name.Name.O
			require.Equal(t, len(c.ds), len(cond1.List))
			values = make([]string, 0, len(c.ds))
			for _, expr := range cond1.List {
				values = append(values, expr.(ast.ValueExpr).GetValue().(string))
			}
			cond2 := and.R.(*ast.BinaryOperationExpr)
			timeColumnName = cond2.L.(*ast.ColumnNameExpr).Name.Name.O
			timeFunc = cond2.R.(*ast.FuncCallExpr).FnName.L
			timeTS = cond2.R.(*ast.FuncCallExpr).Args[0].(ast.ValueExpr).GetValue().(int64)
		default:
			require.FailNow(t, "invalid tp: %s", c.tp)
		}

		require.Equal(t, tb.Schema.O, tbName.Schema.O)
		require.Equal(t, tb.Name.O, tbName.Name.O)
		require.Equal(t, 1, len(tbName.PartitionNames))
		require.Equal(t, tb.PartitionDef.Name.O, tbName.PartitionNames[0].O)
		require.Equal(t, tb.KeyColumns[0].Name.O, keyColumnName)
		require.Equal(t, tb.TTLTimeColumn.Name.O, timeColumnName)
		for i, row := range c.ds {
			require.Equal(t, row[0].GetString(), values[i])
		}
		require.Equal(t, "from_unixtime", timeFunc)
		require.Equal(t, int64(0), timeTS)
	}
}

func TestFormatSQLDatum(t *testing.T) {
	// invalid pk types contains the types that should not exist in primary keys of a TTL table.
	// We do not need to check sqlbuilder.FormatSQLDatum for these types
	invalidPKTypes := []struct {
		types []string
		err   *terror.Error
	}{
		{
			types: []string{"json"},
			err:   dbterror.ErrJSONUsedAsKey,
		},
		{
			types: []string{"blob"},
			err:   dbterror.ErrBlobKeyWithoutLength,
		},
		{
			types: []string{"blob(8)"},
			err:   dbterror.ErrBlobKeyWithoutLength,
		},
		{
			types: []string{"text"},
			err:   dbterror.ErrBlobKeyWithoutLength,
		},
		{
			types: []string{"text(8)"},
			err:   dbterror.ErrBlobKeyWithoutLength,
		},
		{
			types: []string{"int", "json"},
			err:   dbterror.ErrJSONUsedAsKey,
		},
		{
			types: []string{"int", "blob"},
			err:   dbterror.ErrBlobKeyWithoutLength,
		},
		{
			types: []string{"int", "text"},
			err:   dbterror.ErrBlobKeyWithoutLength,
		},
		{
			types: []string{"float"},
			err:   dbterror.ErrUnsupportedPrimaryKeyTypeWithTTL,
		},
		{
			types: []string{"double"},
			err:   dbterror.ErrUnsupportedPrimaryKeyTypeWithTTL,
		},
		{
			types: []string{"int", "float"},
			err:   dbterror.ErrUnsupportedPrimaryKeyTypeWithTTL,
		},
		{
			types: []string{"int", "double"},
			err:   dbterror.ErrUnsupportedPrimaryKeyTypeWithTTL,
		},
	}

	cases := []struct {
		ft     string
		values []any
		hex    bool
	}{
		{
			ft:     "int",
			values: []any{1, 2, 3, -12},
		},
		{
			ft:     "decimal(5, 2)",
			values: []any{"0.3", "128.71", "-245.32"},
		},
		{
			ft: "varchar(32) CHARACTER SET latin1",
			values: []any{
				"aa';delete from t where 1;",
				string([]byte{0xf1, 0xf2}),
				string([]byte{0xf1, 0xf2, 0xf3, 0xf4}),
			},
		},
		{
			ft: "char(32) CHARACTER SET utf8mb4",
			values: []any{
				"demo",
				"\n123",
				"aa';delete from t where 1;",
				"你好👋",
			},
		},
		{
			ft: "varchar(32) CHARACTER SET utf8mb4",
			values: []any{
				"demo",
				"aa';delete from t where 1;",
				"你好👋",
			},
		},
		{
			ft: "varchar(32) CHARACTER SET binary",
			values: []any{
				string([]byte{0xf1, 0xf2, 0xf3, 0xf4}),
				"你好👋",
				"abcdef",
			},
			hex: true,
		},
		{
			ft: "binary(8)",
			values: []any{
				string([]byte{0xf1, 0xf2}),
				string([]byte{0xf1, 0xf2, 0xf3, 0xf4}),
			},
			hex: true,
		},
		{
			ft: "blob",
			values: []any{
				string([]byte{0xf1, 0xf2}),
				string([]byte{0xf1, 0xf2, 0xf3, 0xf4}),
			},
			hex: true,
		},
		{
			ft:     "bit(1)",
			values: []any{0, 1},
			hex:    true,
		},
		{
			ft:     "date",
			values: []any{"2022-01-02", "1900-12-31"},
		},
		{
			ft:     "time",
			values: []any{"00:00", "01:23", "13:51:22"},
		},
		{
			ft:     "datetime",
			values: []any{"2022-01-02 12:11:11", "2022-01-02"},
		},
		{
			ft:     "datetime(6)",
			values: []any{"2022-01-02 12:11:11.123456"},
		},
		{
			ft:     "timestamp",
			values: []any{"2022-01-02 12:11:11", "2022-01-02"},
		},
		{
			ft:     "timestamp(6)",
			values: []any{"2022-01-02 12:11:11.123456"},
		},
		{
			ft:     "enum('e1', 'e2', \"e3'\", 'e4\"', ';你好👋')",
			values: []any{"e1", "e2", "e3'", "e4\"", ";你好👋"},
		},
		{
			ft:     "set('e1', 'e2', \"e3'\", 'e4\"', ';你好👋')",
			values: []any{"", "e1", "e2", "e3'", "e4\"", ";你好👋"},
		},
	}

	store, do := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	for _, c := range invalidPKTypes {
		var sb strings.Builder
		sb.WriteString("create table t(")
		cols := make([]string, 0, len(invalidPKTypes))
		for i, tp := range c.types {
			colName := fmt.Sprintf("pk%d", i)
			cols = append(cols, colName)
			sb.WriteString(colName)
			sb.WriteString(" ")
			sb.WriteString(tp)
			sb.WriteString(", ")
		}
		sb.WriteString("t timestamp, ")
		sb.WriteString("primary key (")
		sb.WriteString(strings.Join(cols, ", "))
		sb.WriteString(")) TTL=`t` + INTERVAL 1 DAY")
		tk.MustGetDBError(sb.String(), c.err)
	}

	// create a table with n columns
	var sb strings.Builder
	sb.WriteString("CREATE TABLE t (id varchar(32) primary key")
	for i, c := range cases {
		_, err := fmt.Fprintf(&sb, ",\n  col%d %s DEFAULT NULL", i, c.ft)
		require.NoError(t, err)
	}
	sb.WriteString("\n);")
	tk.MustExec(sb.String())

	tbl, err := do.InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
	require.NoError(t, err)

	for i, c := range cases {
		for j, v := range c.values {
			tk.MustExec(fmt.Sprintf("insert into t (id, col%d) values ('%d-%d', ?)", i, i, j), v)
		}
	}

	ctx := kv.WithInternalSourceType(context.TODO(), kv.InternalTxnOthers)
	for i, c := range cases {
		for j := range c.values {
			rowID := fmt.Sprintf("%d-%d", i, j)
			colName := fmt.Sprintf("col%d", i)
			exec := tk.Session().GetSQLExecutor()
			selectSQL := fmt.Sprintf("select %s from t where id='%s'", colName, rowID)
			rs, err := exec.ExecuteInternal(ctx, selectSQL)
			require.NoError(t, err, selectSQL)
			rows, err := sqlexec.DrainRecordSet(ctx, rs, 1)
			require.NoError(t, err, selectSQL)
			require.Equal(t, 1, len(rows), selectSQL)
			col := tbl.Meta().FindPublicColumnByName(colName)
			d := rows[0].GetDatum(0, &col.FieldType)
			s, err := sqlbuilder.FormatSQLDatum(d, &col.FieldType)
			require.NoError(t, err)
			tk.MustQuery("select id from t where " + colName + "=" + s).Check(testkit.Rows(rowID))
			if c.hex {
				require.True(t, strings.HasPrefix(s, "x'"), "ft: %s, got: %s", c.ft, s)
			}
		}
	}
}

func TestSoftDeleteScanQueryGeneratorPaging(t *testing.T) {
	idCol := &model.ColumnInfo{Name: ast.NewCIStr("id"), FieldType: *types.NewFieldType(mysql.TypeLonglong), State: model.StatePublic, Offset: 1}
	timeCol := model.NewExtraSoftDeleteTimeColInfo()
	timeCol.State = model.StatePublic
	timeCol.Offset = 0
	tbl := &cache.PhysicalTable{
		Schema: ast.NewCIStr("test"),
		TableInfo: &model.TableInfo{
			Name:    ast.NewCIStr("t"),
			Columns: []*model.ColumnInfo{timeCol, idCol},
		},
		KeyColumns:           []*model.ColumnInfo{idCol},
		KeyColumnTypes:       []*types.FieldType{&idCol.FieldType},
		SoftDeleteTimeColumn: timeCol,
	}

	expire := time.Unix(100, 0).In(time.UTC)
	g, err := sqlbuilder.NewScanQueryGenerator(session.TTLJobTypeSoftDelete, tbl, expire, nil, nil)

	require.NoError(t, err)

	// first batch
	sql1, err := g.NextSQL(nil, 2)
	require.NoError(t, err)
	require.Contains(t, sql1, "ORDER BY `id` ASC")
	require.Contains(t, sql1, "LIMIT 2")

	// full batch, should continue and include continuation key
	row1 := []types.Datum{types.NewDatum(int64(10))}
	row2 := []types.Datum{types.NewDatum(int64(20))}
	sql2, err := g.NextSQL([][]types.Datum{row1, row2}, 2)
	require.NoError(t, err)
	require.Contains(t, sql2, "`_tidb_softdelete_time` < FROM_UNIXTIME(100)")
	require.Contains(t, sql2, "WHERE `id` >")

	// last batch is smaller than limit, generator should be exhausted
	row3 := []types.Datum{types.NewDatum(int64(30))}
	sql3, err := g.NextSQL([][]types.Datum{row3}, 2)
	require.NoError(t, err)
	require.Equal(t, "", sql3)
}

func TestSoftDeleteCleanupSQL(t *testing.T) {
	idCol := &model.ColumnInfo{Name: ast.NewCIStr("id"), FieldType: *types.NewFieldType(mysql.TypeLonglong), State: model.StatePublic, Offset: 1}
	timeCol := model.NewExtraSoftDeleteTimeColInfo()
	timeCol.State = model.StatePublic
	timeCol.Offset = 0
	tbl := &cache.PhysicalTable{
		Schema: ast.NewCIStr("test"),
		TableInfo: &model.TableInfo{
			Name:    ast.NewCIStr("t"),
			Columns: []*model.ColumnInfo{timeCol, idCol},
		},
		KeyColumns:           []*model.ColumnInfo{idCol},
		KeyColumnTypes:       []*types.FieldType{&idCol.FieldType},
		SoftDeleteTimeColumn: timeCol,
	}

	expire := time.Unix(100, 0).In(time.UTC)
	sql, err := sqlbuilder.BuildDeleteSQL(tbl, session.TTLJobTypeSoftDelete, [][]types.Datum{{types.NewDatum(int64(1))}}, expire, 0)
	require.NoError(t, err)
	require.Contains(t, sql, "DELETE LOW_PRIORITY FROM `test`.`t`")
	require.Contains(t, sql, "WHERE `id` IN (1)")
	require.Contains(t, sql, "AND `_tidb_softdelete_time` < FROM_UNIXTIME(100)")
}

func TestSoftDeleteSQLActiveActiveSafety(t *testing.T) {
	expire := time.UnixMilli(100000).In(time.UTC)

	tblInfo := &model.TableInfo{Name: ast.NewCIStr("t"), IsActiveActive: true}
	tbl := &cache.PhysicalTable{
		Schema:    ast.NewCIStr("test"),
		TableInfo: tblInfo,
		KeyColumns: []*model.ColumnInfo{{
			Name:      ast.NewCIStr("id"),
			FieldType: *types.NewFieldType(mysql.TypeLonglong),
		}},
		KeyColumnTypes:       []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)},
		SoftDeleteTimeColumn: model.NewExtraSoftDeleteTimeColInfo(),
	}

	minCheckpointTS := uint64(123)
	checkContains := func(t *testing.T, sql string) {
		require.Contains(t, sql, fmt.Sprintf("%d > IFNULL(_tidb_origin_ts, _tidb_commit_ts)", minCheckpointTS))
		require.Contains(t, sql, "tidb_current_tso() > IFNULL(_tidb_origin_ts, 0)")
	}

	t.Run("Cleanup", func(t *testing.T) {
		sql, err := sqlbuilder.BuildDeleteSQL(tbl, session.TTLJobTypeSoftDelete, [][]types.Datum{{types.NewDatum(int64(1))}}, expire, minCheckpointTS)
		require.NoError(t, err)
		checkContains(t, sql)
	})

	t.Run("Scan", func(t *testing.T) {
		g, err := sqlbuilder.NewScanQueryGenerator(session.TTLJobTypeSoftDelete, tbl, expire, nil, nil)
		require.NoError(t, err)
		g.SetMinCheckpointTS(minCheckpointTS)
		sql, err := g.NextSQL(nil, 10)
		require.NoError(t, err)
		checkContains(t, sql)
	})
}

func TestActiveActiveSafetySQLExecutable(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(fmt.Sprintf("create database if not exists %s", sqlbuilder.TiCDCProgressDB))
	// Use smaller column sizes to avoid key length limitations in tests.
	tk.MustExec(fmt.Sprintf("create table if not exists %s.%s (changefeed_id varchar(64), upstreamID varchar(64), database_name varchar(64), table_name varchar(64), checkpoint_ts bigint unsigned, primary key(changefeed_id, upstreamID, database_name, table_name))", sqlbuilder.TiCDCProgressDB, sqlbuilder.TiCDCProgressTable))

	preparePhysicalTable := func(t *testing.T) *cache.PhysicalTable {
		is := domain.GetDomain(tk.Session()).InfoSchema()
		tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
		require.NoError(t, err)
		pt, err := cache.NewPhysicalTable(ast.NewCIStr("test"), tbl.Meta(), ast.NewCIStr(""), false, true)
		require.NoError(t, err)
		return pt
	}

	run := func(t *testing.T, pt *cache.PhysicalTable, expire time.Time, minCheckpointTS uint64, expectScan bool, expectDeleted bool) {
		g, err := sqlbuilder.NewScanQueryGenerator(session.TTLJobTypeSoftDelete, pt, expire, nil, nil)
		require.NoError(t, err)
		g.SetMinCheckpointTS(minCheckpointTS)
		q, err := g.NextSQL(nil, 10)
		require.NoError(t, err)
		delSQL, err := sqlbuilder.BuildDeleteSQL(pt, session.TTLJobTypeSoftDelete, [][]types.Datum{{types.NewDatum(int64(1))}}, expire, minCheckpointTS)
		require.NoError(t, err)

		tk.MustExec("set @@tidb_translate_softdelete_sql=0")
		tk.MustExec("begin")
		if expectScan {
			tk.MustQuery(q).Check(testkit.Rows("1"))
		} else {
			tk.MustQuery(q).Check(testkit.Rows())
		}
		tk.MustExec(delSQL)
		tk.MustExec("commit")
		if expectDeleted {
			tk.MustQuery("select count(*) from t").Check(testkit.Rows("0"))
		} else {
			tk.MustQuery("select count(*) from t").Check(testkit.Rows("1"))
		}
	}

	t.Run("LocalTombstone", func(t *testing.T) {
		tk.MustExec(fmt.Sprintf("delete from %s.%s", sqlbuilder.TiCDCProgressDB, sqlbuilder.TiCDCProgressTable))
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t(id int primary key, v int) active_active='on' softdelete retention 1 day softdelete_job_enable='ON'")
		tk.MustExec("set @@tidb_translate_softdelete_sql=1")
		tk.MustExec("insert into t (id, v) values (1, 10)")
		tk.MustExec("delete from t where id=1")
		tk.MustExec("set @@tidb_translate_softdelete_sql=0")
		tk.MustQuery("select count(*) from t").Check(testkit.Rows("1"))
		tk.MustQuery("select _tidb_origin_ts is null from t where id=1").Check(testkit.Rows("1"))
		tk.MustExec("update t set _tidb_softdelete_time = _tidb_softdelete_time - interval 7 day")

		pt := preparePhysicalTable(t)
		expire := time.Now().UTC()

		// No progress
		run(t, pt, expire, 0, false, false)
		// Deleted
		tk.MustExec(fmt.Sprintf("insert into %s.%s select 'cf','1','test','t', cast(IFNULL(_tidb_origin_ts, _tidb_commit_ts) as unsigned)+1 from test.t where id=1", sqlbuilder.TiCDCProgressDB, sqlbuilder.TiCDCProgressTable))
		var checkpointTS uint64
		row := tk.MustQuery(fmt.Sprintf("select min(checkpoint_ts) from %s.%s where database_name='test' and table_name='t'", sqlbuilder.TiCDCProgressDB, sqlbuilder.TiCDCProgressTable)).Rows()
		checkpointTS, err := strconv.ParseUint(row[0][0].(string), 10, 64)
		require.NoError(t, err)
		run(t, pt, expire, checkpointTS, true, true)
	})

	t.Run("UpstreamTombstone", func(t *testing.T) {
		tk.MustExec(fmt.Sprintf("delete from %s.%s", sqlbuilder.TiCDCProgressDB, sqlbuilder.TiCDCProgressTable))
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t(id int primary key, v int) active_active='on' softdelete retention 1 day softdelete_job_enable='ON'")

		// Create an "upstream" tombstone by simulating CDC write
		tk.MustExec("set @@tidb_translate_softdelete_sql=0")
		tk.MustExec("insert into t (id, v, _tidb_origin_ts, _tidb_softdelete_time) values (1, 10, tidb_current_tso(), now())")
		tk.MustQuery("select _tidb_origin_ts is null from t where id=1").Check(testkit.Rows("0"))
		tk.MustExec("update t set _tidb_softdelete_time = _tidb_softdelete_time - interval 7 day")

		pt := preparePhysicalTable(t)
		expire := time.Now().UTC()

		// No progress
		tk.MustExec(fmt.Sprintf("insert into %s.%s values ('cf','1','test','t', 1)", sqlbuilder.TiCDCProgressDB, sqlbuilder.TiCDCProgressTable))
		run(t, pt, expire, 1, false, false)
		// Deleted
		tk.MustExec(fmt.Sprintf("delete from %s.%s", sqlbuilder.TiCDCProgressDB, sqlbuilder.TiCDCProgressTable))
		tk.MustExec(fmt.Sprintf("insert into %s.%s select 'cf','1','test','t', cast(IFNULL(_tidb_origin_ts, _tidb_commit_ts) as unsigned)+1 from test.t where id=1", sqlbuilder.TiCDCProgressDB, sqlbuilder.TiCDCProgressTable))
		row := tk.MustQuery(fmt.Sprintf("select min(checkpoint_ts) from %s.%s where database_name='test' and table_name='t'", sqlbuilder.TiCDCProgressDB, sqlbuilder.TiCDCProgressTable)).Rows()
		checkpointTS, err := strconv.ParseUint(row[0][0].(string), 10, 64)
		require.NoError(t, err)
		run(t, pt, expire, checkpointTS, true, true)
	})
}

func TestSQLBuilder(t *testing.T) {
	must := func(err error) {
		require.NoError(t, err)
	}

	mustBuild := func(b *sqlbuilder.SQLBuilder, str string) {
		s, err := b.Build()
		require.NoError(t, err)
		require.Equal(t, str, s)
	}

	var b *sqlbuilder.SQLBuilder

	t1 := &cache.PhysicalTable{
		Schema: pmodel.NewCIStr("test"),
		TableInfo: &model.TableInfo{
			Name: pmodel.NewCIStr("t1"),
		},
		KeyColumns: []*model.ColumnInfo{
			{Name: pmodel.NewCIStr("id"), FieldType: *types.NewFieldType(mysql.TypeVarchar)},
		},
		TTLTimeColumn: &model.ColumnInfo{
			Name:      pmodel.NewCIStr("time"),
			FieldType: *types.NewFieldType(mysql.TypeDatetime),
		},
	}

	t2 := &cache.PhysicalTable{
		Schema: pmodel.NewCIStr("test2"),
		TableInfo: &model.TableInfo{
			Name: pmodel.NewCIStr("t2"),
		},
		KeyColumns: []*model.ColumnInfo{
			{Name: pmodel.NewCIStr("a"), FieldType: *types.NewFieldType(mysql.TypeVarchar)},
			{Name: pmodel.NewCIStr("b"), FieldType: *types.NewFieldType(mysql.TypeInt24)},
		},
		TTLTimeColumn: &model.ColumnInfo{
			Name:      pmodel.NewCIStr("time"),
			FieldType: *types.NewFieldType(mysql.TypeDatetime),
		},
	}

	tp := &cache.PhysicalTable{
		Schema: pmodel.NewCIStr("testp"),
		TableInfo: &model.TableInfo{
			Name: pmodel.NewCIStr("tp"),
		},
		KeyColumns:    t1.KeyColumns,
		TTLTimeColumn: t1.TTLTimeColumn,
		PartitionDef: &model.PartitionDefinition{
			Name: pmodel.NewCIStr("p1"),
		},
	}

	// test build select queries
	b = sqlbuilder.NewSQLBuilder(t1, session.TTLJobTypeTTL)
	must(b.WriteSelectColumns(t1.KeyColumns))
	mustBuild(b, "SELECT LOW_PRIORITY SQL_NO_CACHE `id` FROM `test`.`t1`")

	b = sqlbuilder.NewSQLBuilder(t1, session.TTLJobTypeTTL)
	must(b.WriteSelectColumns(t1.KeyColumns))
	must(b.WriteCommonCondition(t1.KeyColumns, ">", d("a1")))
	mustBuild(b, "SELECT LOW_PRIORITY SQL_NO_CACHE `id` FROM `test`.`t1` WHERE `id` > 'a1'")

	b = sqlbuilder.NewSQLBuilder(t1, session.TTLJobTypeTTL)
	must(b.WriteSelectColumns(t1.KeyColumns))
	must(b.WriteCommonCondition(t1.KeyColumns, ">", d("a1")))
	must(b.WriteCommonCondition(t1.KeyColumns, "<=", d("c3")))
	mustBuild(b, "SELECT LOW_PRIORITY SQL_NO_CACHE `id` FROM `test`.`t1` WHERE `id` > 'a1' AND `id` <= 'c3'")

	b = sqlbuilder.NewSQLBuilder(t1, session.TTLJobTypeTTL)
	must(b.WriteSelectColumns(t1.KeyColumns))
	shLoc, err := time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err)
	must(b.WriteExpireCondition(time.UnixMilli(0).In(shLoc)))
	mustBuild(b, "SELECT LOW_PRIORITY SQL_NO_CACHE `id` FROM `test`.`t1` WHERE `time` < FROM_UNIXTIME(0)")

	b = sqlbuilder.NewSQLBuilder(t1, session.TTLJobTypeTTL)
	must(b.WriteSelectColumns(t1.KeyColumns))
	must(b.WriteCommonCondition(t1.KeyColumns, ">", d("a1")))
	must(b.WriteCommonCondition(t1.KeyColumns, "<=", d("c3")))
	must(b.WriteExpireCondition(time.UnixMilli(0).In(time.UTC)))
	mustBuild(b, "SELECT LOW_PRIORITY SQL_NO_CACHE `id` FROM `test`.`t1` WHERE `id` > 'a1' AND `id` <= 'c3' AND `time` < FROM_UNIXTIME(0)")

	b = sqlbuilder.NewSQLBuilder(t1, session.TTLJobTypeTTL)
	must(b.WriteSelectColumns(t1.KeyColumns))
	must(b.WriteOrderBy(t1.KeyColumns, false))
	mustBuild(b, "SELECT LOW_PRIORITY SQL_NO_CACHE `id` FROM `test`.`t1` ORDER BY `id` ASC")

	b = sqlbuilder.NewSQLBuilder(t1, session.TTLJobTypeTTL)
	must(b.WriteSelectColumns(t1.KeyColumns))
	must(b.WriteOrderBy(t1.KeyColumns, true))
	mustBuild(b, "SELECT LOW_PRIORITY SQL_NO_CACHE `id` FROM `test`.`t1` ORDER BY `id` DESC")

	b = sqlbuilder.NewSQLBuilder(t1, session.TTLJobTypeTTL)
	must(b.WriteSelectColumns(t1.KeyColumns))
	must(b.WriteOrderBy(t1.KeyColumns, false))
	must(b.WriteLimit(128))
	mustBuild(b, "SELECT LOW_PRIORITY SQL_NO_CACHE `id` FROM `test`.`t1` ORDER BY `id` ASC LIMIT 128")

	b = sqlbuilder.NewSQLBuilder(t1, session.TTLJobTypeTTL)
	must(b.WriteSelectColumns(t1.KeyColumns))
	must(b.WriteCommonCondition(t1.KeyColumns, ">", d("';``~?%\"\n")))
	mustBuild(b, "SELECT LOW_PRIORITY SQL_NO_CACHE `id` FROM `test`.`t1` WHERE `id` > '\\';``~?%\\\"\\n'")

	b = sqlbuilder.NewSQLBuilder(t1, session.TTLJobTypeTTL)
	must(b.WriteSelectColumns(t1.KeyColumns))
	must(b.WriteCommonCondition(t1.KeyColumns, ">", d("a1';'")))
	must(b.WriteCommonCondition(t1.KeyColumns, "<=", d("a2\"")))
	must(b.WriteExpireCondition(time.UnixMilli(0).In(time.UTC)))
	must(b.WriteOrderBy(t1.KeyColumns, false))
	must(b.WriteLimit(128))
	mustBuild(b, "SELECT LOW_PRIORITY SQL_NO_CACHE `id` FROM `test`.`t1` WHERE `id` > 'a1\\';\\'' AND `id` <= 'a2\\\"' AND `time` < FROM_UNIXTIME(0) ORDER BY `id` ASC LIMIT 128")

	b = sqlbuilder.NewSQLBuilder(t2, session.TTLJobTypeTTL)
	must(b.WriteSelectColumns(t2.KeyColumns))
	must(b.WriteCommonCondition(t2.KeyColumns, ">", d("x1", 20)))
	mustBuild(b, "SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b` FROM `test2`.`t2` WHERE (`a`, `b`) > ('x1', 20)")

	b = sqlbuilder.NewSQLBuilder(t2, session.TTLJobTypeTTL)
	must(b.WriteSelectColumns(t2.KeyColumns))
	must(b.WriteCommonCondition(t2.KeyColumns, "<=", d("x2", 21)))
	must(b.WriteExpireCondition(time.UnixMilli(0).In(time.UTC)))
	must(b.WriteOrderBy(t2.KeyColumns, false))
	must(b.WriteLimit(100))
	mustBuild(b, "SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b` FROM `test2`.`t2` WHERE (`a`, `b`) <= ('x2', 21) AND `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b` ASC LIMIT 100")

	b = sqlbuilder.NewSQLBuilder(t2, session.TTLJobTypeTTL)
	must(b.WriteSelectColumns(t2.KeyColumns))
	must(b.WriteCommonCondition(t2.KeyColumns[0:1], "=", d("x3")))
	must(b.WriteCommonCondition(t2.KeyColumns[1:2], ">", d(31)))
	must(b.WriteExpireCondition(time.UnixMilli(0).In(time.UTC)))
	must(b.WriteOrderBy(t2.KeyColumns, false))
	must(b.WriteLimit(100))
	mustBuild(b, "SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b` FROM `test2`.`t2` WHERE `a` = 'x3' AND `b` > 31 AND `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b` ASC LIMIT 100")

	// test build delete queries
	b = sqlbuilder.NewSQLBuilder(t1, session.TTLJobTypeTTL)
	must(b.WriteDelete())
	_, err = b.Build()
	require.EqualError(t, err, "expire condition not write")

	b = sqlbuilder.NewSQLBuilder(t1, session.TTLJobTypeTTL)
	must(b.WriteDelete())
	must(b.WriteInCondition(t1.KeyColumns, d("a")))
	must(b.WriteExpireCondition(time.UnixMilli(0).In(time.UTC)))
	mustBuild(b, "DELETE LOW_PRIORITY FROM `test`.`t1` WHERE `id` IN ('a') AND `time` < FROM_UNIXTIME(0)")

	b = sqlbuilder.NewSQLBuilder(t1, session.TTLJobTypeTTL)
	must(b.WriteDelete())
	must(b.WriteInCondition(t1.KeyColumns, d("a"), d("b")))
	must(b.WriteExpireCondition(time.UnixMilli(0).In(time.UTC)))
	mustBuild(b, "DELETE LOW_PRIORITY FROM `test`.`t1` WHERE `id` IN ('a', 'b') AND `time` < FROM_UNIXTIME(0)")

	b = sqlbuilder.NewSQLBuilder(t1, session.TTLJobTypeTTL)
	must(b.WriteDelete())
	must(b.WriteInCondition(t2.KeyColumns, d("a", 1)))
	must(b.WriteExpireCondition(time.UnixMilli(0).In(time.UTC)))
	must(b.WriteLimit(100))
	mustBuild(b, "DELETE LOW_PRIORITY FROM `test`.`t1` WHERE (`a`, `b`) IN (('a', 1)) AND `time` < FROM_UNIXTIME(0) LIMIT 100")

	b = sqlbuilder.NewSQLBuilder(t1, session.TTLJobTypeTTL)
	must(b.WriteDelete())
	must(b.WriteInCondition(t2.KeyColumns, d("a", 1), d("b", 2)))
	must(b.WriteExpireCondition(time.UnixMilli(0).In(time.UTC)))
	must(b.WriteLimit(100))
	mustBuild(b, "DELETE LOW_PRIORITY FROM `test`.`t1` WHERE (`a`, `b`) IN (('a', 1), ('b', 2)) AND `time` < FROM_UNIXTIME(0) LIMIT 100")

	b = sqlbuilder.NewSQLBuilder(t1, session.TTLJobTypeTTL)
	must(b.WriteDelete())
	must(b.WriteInCondition(t2.KeyColumns, d("a", 1), d("b", 2)))
	must(b.WriteExpireCondition(time.UnixMilli(0).In(time.UTC)))
	mustBuild(b, "DELETE LOW_PRIORITY FROM `test`.`t1` WHERE (`a`, `b`) IN (('a', 1), ('b', 2)) AND `time` < FROM_UNIXTIME(0)")

	// test select partition table
	b = sqlbuilder.NewSQLBuilder(tp, session.TTLJobTypeTTL)
	must(b.WriteSelectColumns(tp.KeyColumns))
	must(b.WriteCommonCondition(tp.KeyColumns, ">", d("a1")))
	must(b.WriteExpireCondition(time.UnixMilli(0).In(time.UTC)))
	mustBuild(b, "SELECT LOW_PRIORITY SQL_NO_CACHE `id` FROM `testp`.`tp` PARTITION(`p1`) WHERE `id` > 'a1' AND `time` < FROM_UNIXTIME(0)")

	b = sqlbuilder.NewSQLBuilder(tp, session.TTLJobTypeTTL)
	must(b.WriteDelete())
	must(b.WriteInCondition(tp.KeyColumns, d("a"), d("b")))
	must(b.WriteExpireCondition(time.UnixMilli(0).In(time.UTC)))
	mustBuild(b, "DELETE LOW_PRIORITY FROM `testp`.`tp` PARTITION(`p1`) WHERE `id` IN ('a', 'b') AND `time` < FROM_UNIXTIME(0)")
}

func TestScanQueryGenerator(t *testing.T) {
	t1 := &cache.PhysicalTable{
		Schema: pmodel.NewCIStr("test"),
		TableInfo: &model.TableInfo{
			Name: pmodel.NewCIStr("t1"),
		},
		KeyColumns: []*model.ColumnInfo{
			{Name: pmodel.NewCIStr("id"), FieldType: *types.NewFieldType(mysql.TypeInt24)},
		},
		TTLTimeColumn: &model.ColumnInfo{
			Name:      pmodel.NewCIStr("time"),
			FieldType: *types.NewFieldType(mysql.TypeDatetime),
		},
	}

	t2 := &cache.PhysicalTable{
		Schema: pmodel.NewCIStr("test2"),
		TableInfo: &model.TableInfo{
			Name: pmodel.NewCIStr("t2"),
		},
		KeyColumns: []*model.ColumnInfo{
			{Name: pmodel.NewCIStr("a"), FieldType: *types.NewFieldType(mysql.TypeInt24)},
			{Name: pmodel.NewCIStr("b"), FieldType: *types.NewFieldType(mysql.TypeVarchar)},
			{Name: pmodel.NewCIStr("c"), FieldType: types.NewFieldTypeBuilder().SetType(mysql.TypeString).SetFlag(mysql.BinaryFlag).Build()},
		},
		TTLTimeColumn: &model.ColumnInfo{
			Name:      pmodel.NewCIStr("time"),
			FieldType: *types.NewFieldType(mysql.TypeDatetime),
		},
	}

	result := func(last []types.Datum, n int) [][]types.Datum {
		ds := make([][]types.Datum, n)
		ds[n-1] = last
		return ds
	}

	cases := []struct {
		tbl        *cache.PhysicalTable
		expire     time.Time
		rangeStart []types.Datum
		rangeEnd   []types.Datum
		path       [][]any
	}{
		{
			tbl:    t1,
			expire: time.UnixMilli(0).In(time.UTC),
			path: [][]any{
				{
					nil, 3,
					"SELECT LOW_PRIORITY SQL_NO_CACHE `id` FROM `test`.`t1` WHERE `time` < FROM_UNIXTIME(0) ORDER BY `id` ASC LIMIT 3",
				},
				{
					nil, 5, "",
				},
			},
		},
		{
			tbl:    t1,
			expire: time.UnixMilli(0).In(time.UTC),
			path: [][]any{
				{
					nil, 3,
					"SELECT LOW_PRIORITY SQL_NO_CACHE `id` FROM `test`.`t1` WHERE `time` < FROM_UNIXTIME(0) ORDER BY `id` ASC LIMIT 3",
				},
				{
					[][]types.Datum{}, 5, "",
				},
			},
		},
		{
			tbl:        t1,
			expire:     time.UnixMilli(0).In(time.UTC),
			rangeStart: d(1),
			rangeEnd:   d(100),
			path: [][]any{
				{
					nil, 3,
					"SELECT LOW_PRIORITY SQL_NO_CACHE `id` FROM `test`.`t1` WHERE `id` >= 1 AND `id` < 100 AND `time` < FROM_UNIXTIME(0) ORDER BY `id` ASC LIMIT 3",
				},
				{
					result(d(10), 3), 5,
					"SELECT LOW_PRIORITY SQL_NO_CACHE `id` FROM `test`.`t1` WHERE `id` > 10 AND `id` < 100 AND `time` < FROM_UNIXTIME(0) ORDER BY `id` ASC LIMIT 5",
				},
				{
					result(d(15), 4), 5,
					"",
				},
			},
		},
		{
			tbl:    t1,
			expire: time.UnixMilli(0).In(time.UTC),
			path: [][]any{
				{
					nil, 3,
					"SELECT LOW_PRIORITY SQL_NO_CACHE `id` FROM `test`.`t1` WHERE `time` < FROM_UNIXTIME(0) ORDER BY `id` ASC LIMIT 3",
				},
				{
					result(d(2), 3), 5,
					"SELECT LOW_PRIORITY SQL_NO_CACHE `id` FROM `test`.`t1` WHERE `id` > 2 AND `time` < FROM_UNIXTIME(0) ORDER BY `id` ASC LIMIT 5",
				},
				{
					result(d(4), 5), 6,
					"SELECT LOW_PRIORITY SQL_NO_CACHE `id` FROM `test`.`t1` WHERE `id` > 4 AND `time` < FROM_UNIXTIME(0) ORDER BY `id` ASC LIMIT 6",
				},
				{
					result(d(7), 5), 5, "",
				},
			},
		},
		{
			tbl:    t2,
			expire: time.UnixMilli(0).In(time.UTC),
			path: [][]any{
				{
					nil, 5,
					"SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b`, `c` FROM `test2`.`t2` WHERE `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b`, `c` ASC LIMIT 5",
				},
				{
					nil, 5, "",
				},
			},
		},
		{
			tbl:    t2,
			expire: time.UnixMilli(0).In(time.UTC),
			path: [][]any{
				{
					nil, 5,
					"SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b`, `c` FROM `test2`.`t2` WHERE `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b`, `c` ASC LIMIT 5",
				},
				{
					nil, 5, "",
				},
			},
		},
		{
			tbl:    t2,
			expire: time.UnixMilli(0).In(time.UTC),
			path: [][]any{
				{
					nil, 5,
					"SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b`, `c` FROM `test2`.`t2` WHERE `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b`, `c` ASC LIMIT 5",
				},
				{
					[][]types.Datum{}, 5, "",
				},
			},
		},
		{
			tbl:    t2,
			expire: time.UnixMilli(0).In(time.UTC),
			path: [][]any{
				{
					nil, 5,
					"SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b`, `c` FROM `test2`.`t2` WHERE `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b`, `c` ASC LIMIT 5",
				},
				{
					result(d(1, "x", []byte{0xf0}), 4), 5, "",
				},
			},
		},
		{
			tbl:        t2,
			expire:     time.UnixMilli(0).In(time.UTC),
			rangeStart: d(1, "x", []byte{0xe}),
			rangeEnd:   d(100, "z", []byte{0xff}),
			path: [][]any{
				{
					nil, 5,
					"SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b`, `c` FROM `test2`.`t2` WHERE `a` = 1 AND `b` = 'x' AND `c` >= x'0e' AND (`a`, `b`, `c`) < (100, 'z', x'ff') AND `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b`, `c` ASC LIMIT 5",
				},
				{
					result(d(1, "x", []byte{0x1a}), 5), 5,
					"SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b`, `c` FROM `test2`.`t2` WHERE `a` = 1 AND `b` = 'x' AND `c` > x'1a' AND (`a`, `b`, `c`) < (100, 'z', x'ff') AND `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b`, `c` ASC LIMIT 5",
				},
				{
					result(d(1, "x", []byte{0x20}), 4), 5,
					"SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b`, `c` FROM `test2`.`t2` WHERE `a` = 1 AND `b` > 'x' AND (`a`, `b`, `c`) < (100, 'z', x'ff') AND `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b`, `c` ASC LIMIT 5",
				},
				{
					result(d(1, "y", []byte{0x0a}), 5), 5,
					"SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b`, `c` FROM `test2`.`t2` WHERE `a` = 1 AND `b` = 'y' AND `c` > x'0a' AND (`a`, `b`, `c`) < (100, 'z', x'ff') AND `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b`, `c` ASC LIMIT 5",
				},
				{
					result(d(1, "y", []byte{0x11}), 4), 5,
					"SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b`, `c` FROM `test2`.`t2` WHERE `a` = 1 AND `b` > 'y' AND (`a`, `b`, `c`) < (100, 'z', x'ff') AND `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b`, `c` ASC LIMIT 5",
				},
				{
					result(d(1, "z", []byte{0x02}), 4), 5,
					"SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b`, `c` FROM `test2`.`t2` WHERE `a` > 1 AND (`a`, `b`, `c`) < (100, 'z', x'ff') AND `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b`, `c` ASC LIMIT 5",
				},
				{
					result(d(3, "a", []byte{0x01}), 5), 5,
					"SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b`, `c` FROM `test2`.`t2` WHERE `a` = 3 AND `b` = 'a' AND `c` > x'01' AND (`a`, `b`, `c`) < (100, 'z', x'ff') AND `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b`, `c` ASC LIMIT 5",
				},
				{
					result(d(3, "a", []byte{0x11}), 4), 5,
					"SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b`, `c` FROM `test2`.`t2` WHERE `a` = 3 AND `b` > 'a' AND (`a`, `b`, `c`) < (100, 'z', x'ff') AND `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b`, `c` ASC LIMIT 5",
				},
				{
					result(d(3, "c", []byte{0x12}), 4), 5,
					"SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b`, `c` FROM `test2`.`t2` WHERE `a` > 3 AND (`a`, `b`, `c`) < (100, 'z', x'ff') AND `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b`, `c` ASC LIMIT 5",
				},
				{
					result(d(5, "e", []byte{0xa1}), 4), 5, "",
				},
			},
		},
		{
			tbl:        t2,
			expire:     time.UnixMilli(0).In(time.UTC),
			rangeStart: d(1),
			rangeEnd:   d(100),
			path: [][]any{
				{
					nil, 5,
					"SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b`, `c` FROM `test2`.`t2` WHERE `a` >= 1 AND `a` < 100 AND `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b`, `c` ASC LIMIT 5",
				},
				{
					result(d(1, "x", []byte{0x1a}), 5), 5,
					"SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b`, `c` FROM `test2`.`t2` WHERE `a` = 1 AND `b` = 'x' AND `c` > x'1a' AND `a` < 100 AND `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b`, `c` ASC LIMIT 5",
				},
				{
					result(d(1, "x", []byte{0x20}), 4), 5,
					"SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b`, `c` FROM `test2`.`t2` WHERE `a` = 1 AND `b` > 'x' AND `a` < 100 AND `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b`, `c` ASC LIMIT 5",
				},
				{
					result(d(1, "y", []byte{0x0a}), 4), 5,
					"SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b`, `c` FROM `test2`.`t2` WHERE `a` > 1 AND `a` < 100 AND `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b`, `c` ASC LIMIT 5",
				},
			},
		},
		{
			tbl:        t2,
			expire:     time.UnixMilli(0).In(time.UTC),
			rangeStart: d(1, "x"),
			rangeEnd:   d(100, "z"),
			path: [][]any{
				{
					nil, 5,
					"SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b`, `c` FROM `test2`.`t2` WHERE `a` = 1 AND `b` >= 'x' AND (`a`, `b`) < (100, 'z') AND `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b`, `c` ASC LIMIT 5",
				},
				{
					result(d(1, "x", []byte{0x1a}), 5), 5,
					"SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b`, `c` FROM `test2`.`t2` WHERE `a` = 1 AND `b` = 'x' AND `c` > x'1a' AND (`a`, `b`) < (100, 'z') AND `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b`, `c` ASC LIMIT 5",
				},
				{
					result(d(1, "x", []byte{0x20}), 4), 5,
					"SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b`, `c` FROM `test2`.`t2` WHERE `a` = 1 AND `b` > 'x' AND (`a`, `b`) < (100, 'z') AND `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b`, `c` ASC LIMIT 5",
				},
				{
					result(d(1, "y", []byte{0x0a}), 4), 5,
					"SELECT LOW_PRIORITY SQL_NO_CACHE `a`, `b`, `c` FROM `test2`.`t2` WHERE `a` > 1 AND (`a`, `b`) < (100, 'z') AND `time` < FROM_UNIXTIME(0) ORDER BY `a`, `b`, `c` ASC LIMIT 5",
				},
			},
		},
	}

	for i, c := range cases {
		g, err := sqlbuilder.NewScanQueryGenerator(session.TTLJobTypeTTL, c.tbl, c.expire, c.rangeStart, c.rangeEnd)

		require.NoError(t, err, fmt.Sprintf("%d", i))
		for j, p := range c.path {
			msg := fmt.Sprintf("%d-%d", i, j)
			var result [][]types.Datum
			require.Equal(t, 3, len(p), msg)
			if arg := p[0]; arg != nil {
				r, ok := arg.([][]types.Datum)
				require.True(t, ok, msg)
				result = r
			}
			limit, ok := p[1].(int)
			require.True(t, ok, msg)
			sql, ok := p[2].(string)
			require.True(t, ok, msg)
			s, err := g.NextSQL(result, limit)
			require.NoError(t, err, msg)
			require.Equal(t, sql, s, msg)
			require.Equal(t, s == "", g.IsExhausted(), msg)
		}
	}
}

func TestBuildDeleteSQL(t *testing.T) {
	t1 := &cache.PhysicalTable{
		Schema: pmodel.NewCIStr("test"),
		TableInfo: &model.TableInfo{
			Name: pmodel.NewCIStr("t1"),
		},
		KeyColumns: []*model.ColumnInfo{
			{Name: pmodel.NewCIStr("id"), FieldType: *types.NewFieldType(mysql.TypeInt24)},
		},
		TTLTimeColumn: &model.ColumnInfo{
			Name:      pmodel.NewCIStr("time"),
			FieldType: *types.NewFieldType(mysql.TypeDatetime),
		},
	}

	t2 := &cache.PhysicalTable{
		Schema: pmodel.NewCIStr("test2"),
		TableInfo: &model.TableInfo{
			Name: pmodel.NewCIStr("t2"),
		},
		KeyColumns: []*model.ColumnInfo{
			{Name: pmodel.NewCIStr("a"), FieldType: *types.NewFieldType(mysql.TypeInt24)},
			{Name: pmodel.NewCIStr("b"), FieldType: *types.NewFieldType(mysql.TypeVarchar)},
		},
		TTLTimeColumn: &model.ColumnInfo{
			Name:      pmodel.NewCIStr("time"),
			FieldType: *types.NewFieldType(mysql.TypeDatetime),
		},
	}

	cases := []struct {
		tbl    *cache.PhysicalTable
		expire time.Time
		rows   [][]types.Datum
		sql    string
	}{
		{
			tbl:    t1,
			expire: time.UnixMilli(0).In(time.UTC),
			rows:   [][]types.Datum{d(1)},
			sql:    "DELETE LOW_PRIORITY FROM `test`.`t1` WHERE `id` IN (1) AND `time` < FROM_UNIXTIME(0) LIMIT 1",
		},
		{
			tbl:    t1,
			expire: time.UnixMilli(0).In(time.UTC),
			rows:   [][]types.Datum{d(2), d(3), d(4)},
			sql:    "DELETE LOW_PRIORITY FROM `test`.`t1` WHERE `id` IN (2, 3, 4) AND `time` < FROM_UNIXTIME(0) LIMIT 3",
		},
		{
			tbl:    t2,
			expire: time.UnixMilli(0).In(time.UTC),
			rows:   [][]types.Datum{d(1, "a")},
			sql:    "DELETE LOW_PRIORITY FROM `test2`.`t2` WHERE (`a`, `b`) IN ((1, 'a')) AND `time` < FROM_UNIXTIME(0) LIMIT 1",
		},
		{
			tbl:    t2,
			expire: time.UnixMilli(0).In(time.UTC),
			rows:   [][]types.Datum{d(1, "a"), d(2, "b")},
			sql:    "DELETE LOW_PRIORITY FROM `test2`.`t2` WHERE (`a`, `b`) IN ((1, 'a'), (2, 'b')) AND `time` < FROM_UNIXTIME(0) LIMIT 2",
		},
	}

	for _, c := range cases {
		sql, err := sqlbuilder.BuildDeleteSQL(c.tbl, session.TTLJobTypeTTL, c.rows, c.expire, 0)
		require.NoError(t, err)
		require.Equal(t, c.sql, sql)
	}
}

func d(vs ...any) []types.Datum {
	datums := make([]types.Datum, len(vs))
	for i, v := range vs {
		switch val := v.(type) {
		case string:
			datums[i] = types.NewStringDatum(val)
		case int:
			datums[i] = types.NewIntDatum(int64(val))
		case []byte:
			datums[i] = types.NewBytesDatum(val)
		default:
			panic(fmt.Sprintf("invalid value type: %T, value: %v", v, v))
		}
	}
	return datums
}
