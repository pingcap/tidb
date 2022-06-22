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

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
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
