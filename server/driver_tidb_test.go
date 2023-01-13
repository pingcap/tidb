// Copyright 2017 PingCAP, Inc.
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

package server

import (
	"testing"

	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

func createColumnByTypeAndLen(tp byte, cl uint32) *ColumnInfo {
	return &ColumnInfo{
		Schema:             "test",
		Table:              "dual",
		OrgTable:           "",
		Name:               "a",
		OrgName:            "a",
		ColumnLength:       cl,
		Charset:            uint16(mysql.CharsetNameToID(charset.CharsetUTF8)),
		Flag:               uint16(mysql.UnsignedFlag),
		Decimal:            uint8(0),
		Type:               tp,
		DefaultValueLength: uint64(0),
		DefaultValue:       nil,
	}
}
func TestConvertColumnInfo(t *testing.T) {
	// Test "mysql.TypeBit", for: https://github.com/pingcap/tidb/issues/5405.
	ftb := types.NewFieldTypeBuilder()
	ftb.SetType(mysql.TypeBit).SetFlag(mysql.UnsignedFlag).SetFlen(1).SetCharset(charset.CharsetUTF8).SetCollate(charset.CollationUTF8)
	resultField := ast.ResultField{
		Column: &model.ColumnInfo{
			Name:      model.NewCIStr("a"),
			ID:        0,
			Offset:    0,
			FieldType: ftb.Build(),
			Comment:   "column a is the first column in table dual",
		},
		ColumnAsName: model.NewCIStr("a"),
		TableAsName:  model.NewCIStr("dual"),
		DBName:       model.NewCIStr("test"),
	}
	colInfo := convertColumnInfo(&resultField)
	require.Equal(t, createColumnByTypeAndLen(mysql.TypeBit, 1), colInfo)

	// Test "mysql.TypeTiny", for: https://github.com/pingcap/tidb/issues/5405.
	ftpb := types.NewFieldTypeBuilder()
	ftpb.SetType(mysql.TypeTiny).SetFlag(mysql.UnsignedFlag).SetFlen(1).SetCharset(charset.CharsetUTF8).SetCollate(charset.CollationUTF8)
	resultField = ast.ResultField{
		Column: &model.ColumnInfo{
			Name:      model.NewCIStr("a"),
			ID:        0,
			Offset:    0,
			FieldType: ftpb.Build(),
			Comment:   "column a is the first column in table dual",
		},
		ColumnAsName: model.NewCIStr("a"),
		TableAsName:  model.NewCIStr("dual"),
		DBName:       model.NewCIStr("test"),
	}
	colInfo = convertColumnInfo(&resultField)
	require.Equal(t, createColumnByTypeAndLen(mysql.TypeTiny, 1), colInfo)

	ftpb1 := types.NewFieldTypeBuilder()
	ftpb1.SetType(mysql.TypeYear).SetFlag(mysql.ZerofillFlag).SetFlen(4).SetCharset(charset.CharsetBin).SetCollate(charset.CollationBin)
	resultField = ast.ResultField{
		Column: &model.ColumnInfo{
			Name:      model.NewCIStr("a"),
			ID:        0,
			Offset:    0,
			FieldType: ftpb1.Build(),
			Comment:   "column a is the first column in table dual",
		},
		ColumnAsName: model.NewCIStr("a"),
		TableAsName:  model.NewCIStr("dual"),
		DBName:       model.NewCIStr("test"),
	}
	colInfo = convertColumnInfo(&resultField)
	require.Equal(t, uint32(4), colInfo.ColumnLength)
}

func TestETLOperations(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1, t2")
	tk.MustExec("create table t(id int primary key, v1 int, v2 int)")
	tk.MustExec("create table t1(id int primary key, v int)")
	tk.MustExec("create table t2(id int primary key auto_increment, v int)")
	tidbCtx := &TiDBContext{Session: tk.Session()}
	p := parser.New()

	mustParseOne := func(sql string) ast.StmtNode {
		stmts, warns, err := p.ParseSQL(sql)
		require.Nil(t, err)
		require.Equal(t, len(warns), 0)
		require.Equal(t, len(stmts), 1)
		return stmts[0]
	}

	insert := mustParseOne("insert into t select tmp1.id, tmp1.v, tmp2.v from (select * from t1) tmp1 join (select * from t2) tmp2 on tmp1.id = tmp2.id")
	require.True(t, tidbCtx.tryETL(insert))

	selfInsert := mustParseOne("insert into t select * from t")
	require.False(t, tidbCtx.tryETL(selfInsert))

	nullInsert := mustParseOne("insert into t2(id) select null")
	require.False(t, tidbCtx.tryETL(nullInsert))

	asNameInsert := mustParseOne("insert into t(id, v1) select tmp2.id, tmp2.v from (select * from t1 tmp1) tmp2")
	require.True(t, tidbCtx.tryETL(asNameInsert))

	cascadeAsNameInsert := mustParseOne("insert into t(id, v1, v2) select tmp3.id, tmp3.v, tmp4.v from (select tmp2.id, tmp2.v from (select * from t1 tmp1) tmp2) tmp3 join (select * from t2) tmp4")
	require.True(t, tidbCtx.tryETL(cascadeAsNameInsert))
}
