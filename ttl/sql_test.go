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

package ttl_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/ttl"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/stretchr/testify/require"
)

func TestFormatSQLDatum(t *testing.T) {
	cases := []struct {
		ft         string
		values     []interface{}
		hex        bool
		notSupport bool
	}{
		{
			ft:     "int",
			values: []interface{}{1, 2, 3, -12},
		},
		{
			ft:     "decimal(5, 2)",
			values: []interface{}{"0.3", "128.71", "-245.32"},
		},
		{
			ft: "varchar(32) CHARACTER SET latin1",
			values: []interface{}{
				"aa';delete from t where 1;",
				string([]byte{0xf1, 0xf2}),
				string([]byte{0xf1, 0xf2, 0xf3, 0xf4}),
			},
		},
		{
			ft: "char(32) CHARACTER SET utf8mb4",
			values: []interface{}{
				"demo",
				"\n123",
				"aa';delete from t where 1;",
				"你好👋",
			},
		},
		{
			ft: "varchar(32) CHARACTER SET utf8mb4",
			values: []interface{}{
				"demo",
				"aa';delete from t where 1;",
				"你好👋",
			},
		},
		{
			ft: "varchar(32) CHARACTER SET binary",
			values: []interface{}{
				string([]byte{0xf1, 0xf2, 0xf3, 0xf4}),
				"你好👋",
				"abcdef",
			},
			hex: true,
		},
		{
			ft: "binary(8)",
			values: []interface{}{
				string([]byte{0xf1, 0xf2}),
				string([]byte{0xf1, 0xf2, 0xf3, 0xf4}),
			},
			hex: true,
		},
		{
			ft: "blob",
			values: []interface{}{
				string([]byte{0xf1, 0xf2}),
				string([]byte{0xf1, 0xf2, 0xf3, 0xf4}),
			},
			hex: true,
		},
		{
			ft:     "bit(1)",
			values: []interface{}{0, 1},
			hex:    true,
		},
		{
			ft:     "date",
			values: []interface{}{"2022-01-02", "1900-12-31"},
		},
		{
			ft:     "time",
			values: []interface{}{"00:00", "01:23", "13:51:22"},
		},
		{
			ft:     "datetime",
			values: []interface{}{"2022-01-02 12:11:11", "2022-01-02"},
		},
		{
			ft:     "timestamp",
			values: []interface{}{"2022-01-02 12:11:11", "2022-01-02"},
		},
		{
			ft:         "json",
			values:     []interface{}{"{}"},
			notSupport: true,
		},
	}

	store, do := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// create a table with n columns
	var sb strings.Builder
	sb.WriteString("CREATE TABLE t (id varchar(32) primary key")
	for i, c := range cases {
		_, err := fmt.Fprintf(&sb, ",\n  col%d %s DEFAULT NULL", i, c.ft)
		require.NoError(t, err)
	}
	sb.WriteString("\n);")
	tk.MustExec(sb.String())

	tbl, err := do.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
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
			exec, ok := tk.Session().(sqlexec.SQLExecutor)
			require.True(t, ok)
			selectSQL := fmt.Sprintf("select %s from t where id='%s'", colName, rowID)
			rs, err := exec.ExecuteInternal(ctx, selectSQL)
			require.NoError(t, err, selectSQL)
			rows, err := sqlexec.DrainRecordSet(ctx, rs, 1)
			require.NoError(t, err, selectSQL)
			require.Equal(t, 1, len(rows), selectSQL)
			col := tbl.Meta().FindPublicColumnByName(colName)
			d := rows[0].GetDatum(0, &col.FieldType)
			s, err := ttl.FormatSQLDatum(d, &col.FieldType)
			if c.notSupport {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				//fmt.Printf("%s: %s\n", c.ft, s)
				tk.MustQuery("select id from t where " + colName + "=" + s).Check(testkit.Rows(rowID))
			}
			if c.hex {
				require.True(t, strings.HasPrefix(s, "x'"), s)
			}
		}
	}
}

func TestSQLBuilder(t *testing.T) {
	must := func(err error) {
		require.NoError(t, err)
	}

	mustBuild := func(b *ttl.SQLBuilder, str string) {
		s, err := b.Build()
		require.NoError(t, err)
		require.Equal(t, str, s)
	}

	var b *ttl.SQLBuilder

	t1 := &ttl.PhysicalTable{
		Schema: model.NewCIStr("test"),
		TableInfo: &model.TableInfo{
			Name: model.NewCIStr("t1"),
		},
		KeyColumns: []*model.ColumnInfo{
			{Name: model.NewCIStr("id"), FieldType: *types.NewFieldType(mysql.TypeVarchar)},
		},
		TimeColumn: &model.ColumnInfo{
			Name:      model.NewCIStr("time"),
			FieldType: *types.NewFieldType(mysql.TypeDatetime),
		},
	}

	t2 := &ttl.PhysicalTable{
		Schema: model.NewCIStr("test2"),
		TableInfo: &model.TableInfo{
			Name: model.NewCIStr("t2"),
		},
		KeyColumns: []*model.ColumnInfo{
			{Name: model.NewCIStr("a"), FieldType: *types.NewFieldType(mysql.TypeVarchar)},
			{Name: model.NewCIStr("b"), FieldType: *types.NewFieldType(mysql.TypeInt24)},
		},
		TimeColumn: &model.ColumnInfo{
			Name:      model.NewCIStr("time"),
			FieldType: *types.NewFieldType(mysql.TypeDatetime),
		},
	}

	tp := &ttl.PhysicalTable{
		Schema: model.NewCIStr("testp"),
		TableInfo: &model.TableInfo{
			Name: model.NewCIStr("tp"),
		},
		KeyColumns: t1.KeyColumns,
		TimeColumn: t1.TimeColumn,
		PartitionDef: &model.PartitionDefinition{
			Name: model.NewCIStr("p1"),
		},
	}

	// test build select queries
	b = ttl.NewSQLBuilder(t1)
	must(b.WriteSelect())
	mustBuild(b, "SELECT LOW_PRIORITY `id` FROM `test`.`t1`")

	b = ttl.NewSQLBuilder(t1)
	must(b.WriteSelect())
	must(b.WriteCommonCondition(t1.KeyColumns, ">", d("a1")))
	mustBuild(b, "SELECT LOW_PRIORITY `id` FROM `test`.`t1` WHERE `id` > 'a1'")

	b = ttl.NewSQLBuilder(t1)
	must(b.WriteSelect())
	must(b.WriteCommonCondition(t1.KeyColumns, ">", d("a1")))
	must(b.WriteCommonCondition(t1.KeyColumns, "<=", d("c3")))
	mustBuild(b, "SELECT LOW_PRIORITY `id` FROM `test`.`t1` WHERE `id` > 'a1' AND `id` <= 'c3'")

	b = ttl.NewSQLBuilder(t1)
	must(b.WriteSelect())
	shLoc, err := time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err)
	must(b.WriteExpireCondition(time.UnixMilli(0).In(shLoc)))
	mustBuild(b, "SELECT LOW_PRIORITY `id` FROM `test`.`t1` WHERE `time` < '1970-01-01 08:00:00'")

	b = ttl.NewSQLBuilder(t1)
	must(b.WriteSelect())
	must(b.WriteCommonCondition(t1.KeyColumns, ">", d("a1")))
	must(b.WriteCommonCondition(t1.KeyColumns, "<=", d("c3")))
	must(b.WriteExpireCondition(time.UnixMilli(0).In(time.UTC)))
	mustBuild(b, "SELECT LOW_PRIORITY `id` FROM `test`.`t1` WHERE `id` > 'a1' AND `id` <= 'c3' AND `time` < '1970-01-01 00:00:00'")

	b = ttl.NewSQLBuilder(t1)
	must(b.WriteSelect())
	must(b.WriteOrderBy(t1.KeyColumns, false))
	mustBuild(b, "SELECT LOW_PRIORITY `id` FROM `test`.`t1` ORDER BY `id` ASC")

	b = ttl.NewSQLBuilder(t1)
	must(b.WriteSelect())
	must(b.WriteOrderBy(t1.KeyColumns, true))
	mustBuild(b, "SELECT LOW_PRIORITY `id` FROM `test`.`t1` ORDER BY `id` DESC")

	b = ttl.NewSQLBuilder(t1)
	must(b.WriteSelect())
	must(b.WriteOrderBy(t1.KeyColumns, false))
	must(b.WriteLimit(128))
	mustBuild(b, "SELECT LOW_PRIORITY `id` FROM `test`.`t1` ORDER BY `id` ASC LIMIT 128")

	b = ttl.NewSQLBuilder(t1)
	must(b.WriteSelect())
	must(b.WriteCommonCondition(t1.KeyColumns, ">", d("';``~?%\"\n")))
	mustBuild(b, "SELECT LOW_PRIORITY `id` FROM `test`.`t1` WHERE `id` > '\\';``~?%\\\"\\n'")

	b = ttl.NewSQLBuilder(t1)
	must(b.WriteSelect())
	must(b.WriteCommonCondition(t1.KeyColumns, ">", d("a1';'")))
	must(b.WriteCommonCondition(t1.KeyColumns, "<=", d("a2\"")))
	must(b.WriteExpireCondition(time.UnixMilli(0).In(time.UTC)))
	must(b.WriteOrderBy(t1.KeyColumns, false))
	must(b.WriteLimit(128))
	mustBuild(b, "SELECT LOW_PRIORITY `id` FROM `test`.`t1` WHERE `id` > 'a1\\';\\'' AND `id` <= 'a2\\\"' AND `time` < '1970-01-01 00:00:00' ORDER BY `id` ASC LIMIT 128")

	b = ttl.NewSQLBuilder(t2)
	must(b.WriteSelect())
	must(b.WriteCommonCondition(t2.KeyColumns, ">", d("x1", 20)))
	mustBuild(b, "SELECT LOW_PRIORITY `a`, `b` FROM `test2`.`t2` WHERE (`a`, `b`) > ('x1', 20)")

	b = ttl.NewSQLBuilder(t2)
	must(b.WriteSelect())
	must(b.WriteCommonCondition(t2.KeyColumns, "<=", d("x2", 21)))
	must(b.WriteExpireCondition(time.UnixMilli(0).In(time.UTC)))
	must(b.WriteOrderBy(t2.KeyColumns, false))
	must(b.WriteLimit(100))
	mustBuild(b, "SELECT LOW_PRIORITY `a`, `b` FROM `test2`.`t2` WHERE (`a`, `b`) <= ('x2', 21) AND `time` < '1970-01-01 00:00:00' ORDER BY `a`, `b` ASC LIMIT 100")

	b = ttl.NewSQLBuilder(t2)
	must(b.WriteSelect())
	must(b.WriteCommonCondition(t2.KeyColumns[0:1], "=", d("x3")))
	must(b.WriteCommonCondition(t2.KeyColumns[1:2], ">", d(31)))
	must(b.WriteExpireCondition(time.UnixMilli(0).In(time.UTC)))
	must(b.WriteOrderBy(t2.KeyColumns, false))
	must(b.WriteLimit(100))
	mustBuild(b, "SELECT LOW_PRIORITY `a`, `b` FROM `test2`.`t2` WHERE `a` = 'x3' AND `b` > 31 AND `time` < '1970-01-01 00:00:00' ORDER BY `a`, `b` ASC LIMIT 100")

	// test build delete queries
	b = ttl.NewSQLBuilder(t1)
	must(b.WriteDelete())
	_, err = b.Build()
	require.EqualError(t, err, "expire condition not write!")

	b = ttl.NewSQLBuilder(t1)
	must(b.WriteDelete())
	must(b.WriteInCondition(t1.KeyColumns, d("a")))
	must(b.WriteExpireCondition(time.UnixMilli(0).In(time.UTC)))
	mustBuild(b, "DELETE LOW_PRIORITY FROM `test`.`t1` WHERE `id` IN ('a') AND `time` < '1970-01-01 00:00:00'")

	b = ttl.NewSQLBuilder(t1)
	must(b.WriteDelete())
	must(b.WriteInCondition(t1.KeyColumns, d("a"), d("b")))
	must(b.WriteExpireCondition(time.UnixMilli(0).In(time.UTC)))
	mustBuild(b, "DELETE LOW_PRIORITY FROM `test`.`t1` WHERE `id` IN ('a', 'b') AND `time` < '1970-01-01 00:00:00'")

	b = ttl.NewSQLBuilder(t1)
	must(b.WriteDelete())
	must(b.WriteInCondition(t2.KeyColumns, d("a", 1)))
	must(b.WriteExpireCondition(time.UnixMilli(0).In(time.UTC)))
	must(b.WriteLimit(100))
	mustBuild(b, "DELETE LOW_PRIORITY FROM `test`.`t1` WHERE (`a`, `b`) IN (('a', 1)) AND `time` < '1970-01-01 00:00:00' LIMIT 100")

	b = ttl.NewSQLBuilder(t1)
	must(b.WriteDelete())
	must(b.WriteInCondition(t2.KeyColumns, d("a", 1), d("b", 2)))
	must(b.WriteExpireCondition(time.UnixMilli(0).In(time.UTC)))
	must(b.WriteLimit(100))
	mustBuild(b, "DELETE LOW_PRIORITY FROM `test`.`t1` WHERE (`a`, `b`) IN (('a', 1), ('b', 2)) AND `time` < '1970-01-01 00:00:00' LIMIT 100")

	b = ttl.NewSQLBuilder(t1)
	must(b.WriteDelete())
	must(b.WriteInCondition(t2.KeyColumns, d("a", 1), d("b", 2)))
	must(b.WriteExpireCondition(time.UnixMilli(0).In(time.UTC)))
	mustBuild(b, "DELETE LOW_PRIORITY FROM `test`.`t1` WHERE (`a`, `b`) IN (('a', 1), ('b', 2)) AND `time` < '1970-01-01 00:00:00'")

	// test select partition table
	b = ttl.NewSQLBuilder(tp)
	must(b.WriteSelect())
	must(b.WriteCommonCondition(tp.KeyColumns, ">", d("a1")))
	must(b.WriteExpireCondition(time.UnixMilli(0).In(time.UTC)))
	mustBuild(b, "SELECT LOW_PRIORITY `id` FROM `testp`.`tp` PARTITION(`p1`) WHERE `id` > 'a1' AND `time` < '1970-01-01 00:00:00'")

	b = ttl.NewSQLBuilder(tp)
	must(b.WriteDelete())
	must(b.WriteInCondition(tp.KeyColumns, d("a"), d("b")))
	must(b.WriteExpireCondition(time.UnixMilli(0).In(time.UTC)))
	mustBuild(b, "DELETE LOW_PRIORITY FROM `testp`.`tp` PARTITION(`p1`) WHERE `id` IN ('a', 'b') AND `time` < '1970-01-01 00:00:00'")
}

func TestScanQueryGenerator(t *testing.T) {
	t1 := &ttl.PhysicalTable{
		Schema: model.NewCIStr("test"),
		TableInfo: &model.TableInfo{
			Name: model.NewCIStr("t1"),
		},
		KeyColumns: []*model.ColumnInfo{
			{Name: model.NewCIStr("id"), FieldType: *types.NewFieldType(mysql.TypeVarchar)},
		},
		TimeColumn: &model.ColumnInfo{
			Name:      model.NewCIStr("time"),
			FieldType: *types.NewFieldType(mysql.TypeDatetime),
		},
	}

	t2 := &ttl.PhysicalTable{
		Schema: model.NewCIStr("test2"),
		TableInfo: &model.TableInfo{
			Name: model.NewCIStr("t2"),
		},
		KeyColumns: []*model.ColumnInfo{
			{Name: model.NewCIStr("a"), FieldType: *types.NewFieldType(mysql.TypeVarchar)},
			{Name: model.NewCIStr("b"), FieldType: *types.NewFieldType(mysql.TypeInt24)},
			{Name: model.NewCIStr("c"), FieldType: types.NewFieldTypeBuilder().SetType(mysql.TypeString).SetFlag(mysql.BinaryFlag).Build()},
		},
		TimeColumn: &model.ColumnInfo{
			Name:      model.NewCIStr("time"),
			FieldType: *types.NewFieldType(mysql.TypeDatetime),
		},
	}

	result := func(last []types.Datum, n int) [][]types.Datum {
		ds := make([][]types.Datum, n)
		ds[n-1] = last
		return ds
	}

	cases := []struct {
		tbl        *ttl.PhysicalTable
		expire     time.Time
		rangeStart []types.Datum
		rangeEnd   []types.Datum
		path       [][]interface{}
	}{
		{
			tbl:    t1,
			expire: time.UnixMilli(0).In(time.UTC),
			path: [][]interface{}{
				{
					nil, 3,
					"SELECT LOW_PRIORITY `id` FROM `test`.`t1` WHERE `time` < '1970-01-01 00:00:00' ORDER BY `id` ASC LIMIT 3",
				},
				{
					nil, 5, "",
				},
			},
		},
		{
			tbl:    t1,
			expire: time.UnixMilli(0).In(time.UTC),
			path: [][]interface{}{
				{
					nil, 3,
					"SELECT LOW_PRIORITY `id` FROM `test`.`t1` WHERE `time` < '1970-01-01 00:00:00' ORDER BY `id` ASC LIMIT 3",
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
			path: [][]interface{}{
				{
					nil, 3,
					"SELECT LOW_PRIORITY `id` FROM `test`.`t1` WHERE `id` > 1 AND `id` <= 100 AND `time` < '1970-01-01 00:00:00' ORDER BY `id` ASC LIMIT 3",
				},
				{
					result(d(10), 3), 5,
					"SELECT LOW_PRIORITY `id` FROM `test`.`t1` WHERE `id` > 10 AND `id` <= 100 AND `time` < '1970-01-01 00:00:00' ORDER BY `id` ASC LIMIT 5",
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
			path: [][]interface{}{
				{
					nil, 3,
					"SELECT LOW_PRIORITY `id` FROM `test`.`t1` WHERE `time` < '1970-01-01 00:00:00' ORDER BY `id` ASC LIMIT 3",
				},
				{
					result(d(2), 3), 5,
					"SELECT LOW_PRIORITY `id` FROM `test`.`t1` WHERE `id` > 2 AND `time` < '1970-01-01 00:00:00' ORDER BY `id` ASC LIMIT 5",
				},
				{
					result(d(4), 5), 6,
					"SELECT LOW_PRIORITY `id` FROM `test`.`t1` WHERE `id` > 4 AND `time` < '1970-01-01 00:00:00' ORDER BY `id` ASC LIMIT 6",
				},
				{
					result(d(7), 5), 5, "",
				},
			},
		},
		{
			tbl:    t2,
			expire: time.UnixMilli(0).In(time.UTC),
			path: [][]interface{}{
				{
					nil, 5,
					"SELECT LOW_PRIORITY `a`, `b`, `c` FROM `test2`.`t2` WHERE `time` < '1970-01-01 00:00:00' ORDER BY `a`, `b`, `c` ASC LIMIT 5",
				},
				{
					nil, 5, "",
				},
			},
		},
		{
			tbl:    t2,
			expire: time.UnixMilli(0).In(time.UTC),
			path: [][]interface{}{
				{
					nil, 5,
					"SELECT LOW_PRIORITY `a`, `b`, `c` FROM `test2`.`t2` WHERE `time` < '1970-01-01 00:00:00' ORDER BY `a`, `b`, `c` ASC LIMIT 5",
				},
				{
					nil, 5, "",
				},
			},
		},
		{
			tbl:    t2,
			expire: time.UnixMilli(0).In(time.UTC),
			path: [][]interface{}{
				{
					nil, 5,
					"SELECT LOW_PRIORITY `a`, `b`, `c` FROM `test2`.`t2` WHERE `time` < '1970-01-01 00:00:00' ORDER BY `a`, `b`, `c` ASC LIMIT 5",
				},
				{
					[][]types.Datum{}, 5, "",
				},
			},
		},
		{
			tbl:    t2,
			expire: time.UnixMilli(0).In(time.UTC),
			path: [][]interface{}{
				{
					nil, 5,
					"SELECT LOW_PRIORITY `a`, `b`, `c` FROM `test2`.`t2` WHERE `time` < '1970-01-01 00:00:00' ORDER BY `a`, `b`, `c` ASC LIMIT 5",
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
			path: [][]interface{}{
				{
					nil, 5,
					"SELECT LOW_PRIORITY `a`, `b`, `c` FROM `test2`.`t2` WHERE `a` = 1 AND `b` = 'x' AND `c` > x'0e' AND (`a`, `b`, `c`) <= (100, 'z', x'ff') AND `time` < '1970-01-01 00:00:00' ORDER BY `a`, `b`, `c` ASC LIMIT 5",
				},
				{
					result(d(1, "x", []byte{0x1a}), 5), 5,
					"SELECT LOW_PRIORITY `a`, `b`, `c` FROM `test2`.`t2` WHERE `a` = 1 AND `b` = 'x' AND `c` > x'1a' AND (`a`, `b`, `c`) <= (100, 'z', x'ff') AND `time` < '1970-01-01 00:00:00' ORDER BY `a`, `b`, `c` ASC LIMIT 5",
				},
				{
					result(d(1, "x", []byte{0x20}), 4), 5,
					"SELECT LOW_PRIORITY `a`, `b`, `c` FROM `test2`.`t2` WHERE `a` = 1 AND `b` > 'x' AND (`a`, `b`, `c`) <= (100, 'z', x'ff') AND `time` < '1970-01-01 00:00:00' ORDER BY `a`, `b`, `c` ASC LIMIT 5",
				},
				{
					result(d(1, "y", []byte{0x0a}), 5), 5,
					"SELECT LOW_PRIORITY `a`, `b`, `c` FROM `test2`.`t2` WHERE `a` = 1 AND `b` = 'y' AND `c` > x'0a' AND (`a`, `b`, `c`) <= (100, 'z', x'ff') AND `time` < '1970-01-01 00:00:00' ORDER BY `a`, `b`, `c` ASC LIMIT 5",
				},
				{
					result(d(1, "y", []byte{0x11}), 4), 5,
					"SELECT LOW_PRIORITY `a`, `b`, `c` FROM `test2`.`t2` WHERE `a` = 1 AND `b` > 'y' AND (`a`, `b`, `c`) <= (100, 'z', x'ff') AND `time` < '1970-01-01 00:00:00' ORDER BY `a`, `b`, `c` ASC LIMIT 5",
				},
				{
					result(d(1, "z", []byte{0x02}), 4), 5,
					"SELECT LOW_PRIORITY `a`, `b`, `c` FROM `test2`.`t2` WHERE `a` > 1 AND (`a`, `b`, `c`) <= (100, 'z', x'ff') AND `time` < '1970-01-01 00:00:00' ORDER BY `a`, `b`, `c` ASC LIMIT 5",
				},
				{
					result(d(3, "a", []byte{0x01}), 5), 5,
					"SELECT LOW_PRIORITY `a`, `b`, `c` FROM `test2`.`t2` WHERE `a` = 3 AND `b` = 'a' AND `c` > x'01' AND (`a`, `b`, `c`) <= (100, 'z', x'ff') AND `time` < '1970-01-01 00:00:00' ORDER BY `a`, `b`, `c` ASC LIMIT 5",
				},
				{
					result(d(3, "a", []byte{0x11}), 4), 5,
					"SELECT LOW_PRIORITY `a`, `b`, `c` FROM `test2`.`t2` WHERE `a` = 3 AND `b` > 'a' AND (`a`, `b`, `c`) <= (100, 'z', x'ff') AND `time` < '1970-01-01 00:00:00' ORDER BY `a`, `b`, `c` ASC LIMIT 5",
				},
				{
					result(d(3, "c", []byte{0x12}), 4), 5,
					"SELECT LOW_PRIORITY `a`, `b`, `c` FROM `test2`.`t2` WHERE `a` > 3 AND (`a`, `b`, `c`) <= (100, 'z', x'ff') AND `time` < '1970-01-01 00:00:00' ORDER BY `a`, `b`, `c` ASC LIMIT 5",
				},
				{
					result(d(5, "e", []byte{0xa1}), 4), 5, "",
				},
			},
		},
	}

	for i, c := range cases {
		g, err := ttl.NewScanQueryGenerator(c.tbl, c.expire, c.rangeStart, c.rangeEnd)
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

func d(vs ...interface{}) []types.Datum {
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
