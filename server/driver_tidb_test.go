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
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/charset"
)

type tidbResultSetTestSuite struct{}

var _ = Suite(tidbResultSetTestSuite{})

func (ts tidbResultSetTestSuite) TestConvertColumnInfo(c *C) {
	// Test "mysql.TypeBit", for: https://github.com/pingcap/tidb/issues/5405.
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
	c.Assert(colInfo.Schema, Equals, "test")
	c.Assert(colInfo.Table, Equals, "dual")
	c.Assert(colInfo.OrgTable, Equals, "")
	c.Assert(colInfo.Name, Equals, "a")
	c.Assert(colInfo.OrgName, Equals, "a")
	c.Assert(colInfo.ColumnLength, Equals, uint32(1))
	c.Assert(colInfo.Charset, Equals, uint16(mysql.CharsetIDs[charset.CharsetUTF8]))
	c.Assert(colInfo.Flag, Equals, uint16(mysql.UnsignedFlag))
	c.Assert(colInfo.Decimal, Equals, uint8(0))
	c.Assert(colInfo.Type, Equals, mysql.TypeBit)
	c.Assert(colInfo.DefaultValueLength, Equals, uint64(0))
	c.Assert(colInfo.DefaultValue, IsNil)
}
