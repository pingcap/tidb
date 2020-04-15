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
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/v4/types"
)

type tidbResultSetTestSuite struct{}

var _ = Suite(tidbResultSetTestSuite{})

func createColumnByTypeAndLen(tp byte, len uint32) *ColumnInfo {
	return &ColumnInfo{
		Schema:             "test",
		Table:              "dual",
		OrgTable:           "",
		Name:               "a",
		OrgName:            "a",
		ColumnLength:       len,
		Charset:            uint16(mysql.CharsetNameToID(charset.CharsetUTF8)),
		Flag:               uint16(mysql.UnsignedFlag),
		Decimal:            uint8(0),
		Type:               tp,
		DefaultValueLength: uint64(0),
		DefaultValue:       nil,
	}
}
func (ts tidbResultSetTestSuite) TestConvertColumnInfo(c *C) {
	// Test "mysql.TypeBit", for: https://github.com/pingcap/tidb/v4/issues/5405.
	resultField := ast.ResultField{
		Column: &model.ColumnInfo{
			Name:   model.NewCIStr("a"),
			ID:     0,
			Offset: 0,
			FieldType: types.FieldType{
				Tp:      mysql.TypeBit,
				Flag:    mysql.UnsignedFlag,
				Flen:    1,
				Decimal: 0,
				Charset: charset.CharsetUTF8,
				Collate: charset.CollationUTF8,
			},
			Comment: "column a is the first column in table dual",
		},
		ColumnAsName: model.NewCIStr("a"),
		TableAsName:  model.NewCIStr("dual"),
		DBName:       model.NewCIStr("test"),
	}
	colInfo := convertColumnInfo(&resultField)
	c.Assert(colInfo, DeepEquals, createColumnByTypeAndLen(mysql.TypeBit, 1))

	// Test "mysql.TypeTiny", for: https://github.com/pingcap/tidb/v4/issues/5405.
	resultField = ast.ResultField{
		Column: &model.ColumnInfo{
			Name:   model.NewCIStr("a"),
			ID:     0,
			Offset: 0,
			FieldType: types.FieldType{
				Tp:      mysql.TypeTiny,
				Flag:    mysql.UnsignedFlag,
				Flen:    1,
				Decimal: 0,
				Charset: charset.CharsetUTF8,
				Collate: charset.CollationUTF8,
			},
			Comment: "column a is the first column in table dual",
		},
		ColumnAsName: model.NewCIStr("a"),
		TableAsName:  model.NewCIStr("dual"),
		DBName:       model.NewCIStr("test"),
	}
	colInfo = convertColumnInfo(&resultField)
	c.Assert(colInfo, DeepEquals, createColumnByTypeAndLen(mysql.TypeTiny, 1))

	resultField = ast.ResultField{
		Column: &model.ColumnInfo{
			Name:   model.NewCIStr("a"),
			ID:     0,
			Offset: 0,
			FieldType: types.FieldType{
				Tp:      mysql.TypeYear,
				Flag:    mysql.ZerofillFlag,
				Flen:    4,
				Decimal: 0,
				Charset: charset.CharsetBin,
				Collate: charset.CollationBin,
			},
			Comment: "column a is the first column in table dual",
		},
		ColumnAsName: model.NewCIStr("a"),
		TableAsName:  model.NewCIStr("dual"),
		DBName:       model.NewCIStr("test"),
	}
	colInfo = convertColumnInfo(&resultField)
	c.Assert(colInfo.ColumnLength, Equals, uint32(4))
}
