// Copyright 2018 PingCAP, Inc.
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

package core

import (
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/v4/domain"
	"github.com/pingcap/tidb/v4/infoschema"
	"github.com/pingcap/tidb/v4/sessionctx"
	"github.com/pingcap/tidb/v4/types"
	"github.com/pingcap/tidb/v4/util/mock"
)

func newLongType() types.FieldType {
	return *(types.NewFieldType(mysql.TypeLong))
}

func newStringType() types.FieldType {
	ft := types.NewFieldType(mysql.TypeVarchar)
	ft.Charset, ft.Collate = types.DefaultCharsetForType(mysql.TypeVarchar)
	return *ft
}

func newDateType() types.FieldType {
	ft := types.NewFieldType(mysql.TypeDate)
	return *ft
}

// MockSignedTable is only used for plan related tests.
func MockSignedTable() *model.TableInfo {
	// column: a, b, c, d, e, c_str, d_str, e_str, f, g
	// PK: a
	// indices: c_d_e, e, f, g, f_g, c_d_e_str, c_d_e_str_prefix
	indices := []*model.IndexInfo{
		{
			Name: model.NewCIStr("c_d_e"),
			Columns: []*model.IndexColumn{
				{
					Name:   model.NewCIStr("c"),
					Length: types.UnspecifiedLength,
					Offset: 2,
				},
				{
					Name:   model.NewCIStr("d"),
					Length: types.UnspecifiedLength,
					Offset: 3,
				},
				{
					Name:   model.NewCIStr("e"),
					Length: types.UnspecifiedLength,
					Offset: 4,
				},
			},
			State:  model.StatePublic,
			Unique: true,
		},
		{
			Name: model.NewCIStr("e"),
			Columns: []*model.IndexColumn{
				{
					Name:   model.NewCIStr("e"),
					Length: types.UnspecifiedLength,
					Offset: 4,
				},
			},
			State:  model.StateWriteOnly,
			Unique: true,
		},
		{
			Name: model.NewCIStr("f"),
			Columns: []*model.IndexColumn{
				{
					Name:   model.NewCIStr("f"),
					Length: types.UnspecifiedLength,
					Offset: 8,
				},
			},
			State:  model.StatePublic,
			Unique: true,
		},
		{
			Name: model.NewCIStr("g"),
			Columns: []*model.IndexColumn{
				{
					Name:   model.NewCIStr("g"),
					Length: types.UnspecifiedLength,
					Offset: 9,
				},
			},
			State: model.StatePublic,
		},
		{
			Name: model.NewCIStr("f_g"),
			Columns: []*model.IndexColumn{
				{
					Name:   model.NewCIStr("f"),
					Length: types.UnspecifiedLength,
					Offset: 8,
				},
				{
					Name:   model.NewCIStr("g"),
					Length: types.UnspecifiedLength,
					Offset: 9,
				},
			},
			State:  model.StatePublic,
			Unique: true,
		},
		{
			Name: model.NewCIStr("c_d_e_str"),
			Columns: []*model.IndexColumn{
				{
					Name:   model.NewCIStr("c_str"),
					Length: types.UnspecifiedLength,
					Offset: 5,
				},
				{
					Name:   model.NewCIStr("d_str"),
					Length: types.UnspecifiedLength,
					Offset: 6,
				},
				{
					Name:   model.NewCIStr("e_str"),
					Length: types.UnspecifiedLength,
					Offset: 7,
				},
			},
			State: model.StatePublic,
		},
		{
			Name: model.NewCIStr("e_d_c_str_prefix"),
			Columns: []*model.IndexColumn{
				{
					Name:   model.NewCIStr("e_str"),
					Length: types.UnspecifiedLength,
					Offset: 7,
				},
				{
					Name:   model.NewCIStr("d_str"),
					Length: types.UnspecifiedLength,
					Offset: 6,
				},
				{
					Name:   model.NewCIStr("c_str"),
					Length: 10,
					Offset: 5,
				},
			},
			State: model.StatePublic,
		},
	}
	pkColumn := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    0,
		Name:      model.NewCIStr("a"),
		FieldType: newLongType(),
		ID:        1,
	}
	col0 := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    1,
		Name:      model.NewCIStr("b"),
		FieldType: newLongType(),
		ID:        2,
	}
	col1 := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    2,
		Name:      model.NewCIStr("c"),
		FieldType: newLongType(),
		ID:        3,
	}
	col2 := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    3,
		Name:      model.NewCIStr("d"),
		FieldType: newLongType(),
		ID:        4,
	}
	col3 := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    4,
		Name:      model.NewCIStr("e"),
		FieldType: newLongType(),
		ID:        5,
	}
	colStr1 := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    5,
		Name:      model.NewCIStr("c_str"),
		FieldType: newStringType(),
		ID:        6,
	}
	colStr2 := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    6,
		Name:      model.NewCIStr("d_str"),
		FieldType: newStringType(),
		ID:        7,
	}
	colStr3 := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    7,
		Name:      model.NewCIStr("e_str"),
		FieldType: newStringType(),
		ID:        8,
	}
	col4 := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    8,
		Name:      model.NewCIStr("f"),
		FieldType: newLongType(),
		ID:        9,
	}
	col5 := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    9,
		Name:      model.NewCIStr("g"),
		FieldType: newLongType(),
		ID:        10,
	}
	col6 := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    10,
		Name:      model.NewCIStr("h"),
		FieldType: newLongType(),
		ID:        11,
	}
	col7 := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    11,
		Name:      model.NewCIStr("i_date"),
		FieldType: newDateType(),
		ID:        12,
	}
	pkColumn.Flag = mysql.PriKeyFlag | mysql.NotNullFlag
	// Column 'b', 'c', 'd', 'f', 'g' is not null.
	col0.Flag = mysql.NotNullFlag
	col1.Flag = mysql.NotNullFlag
	col2.Flag = mysql.NotNullFlag
	col4.Flag = mysql.NotNullFlag
	col5.Flag = mysql.NotNullFlag
	col6.Flag = mysql.NoDefaultValueFlag
	table := &model.TableInfo{
		Columns:    []*model.ColumnInfo{pkColumn, col0, col1, col2, col3, colStr1, colStr2, colStr3, col4, col5, col6, col7},
		Indices:    indices,
		Name:       model.NewCIStr("t"),
		PKIsHandle: true,
	}
	return table
}

// MockUnsignedTable is only used for plan related tests.
func MockUnsignedTable() *model.TableInfo {
	// column: a, b
	// PK: a
	// indeices: b
	indices := []*model.IndexInfo{
		{
			Name: model.NewCIStr("b"),
			Columns: []*model.IndexColumn{
				{
					Name:   model.NewCIStr("b"),
					Length: types.UnspecifiedLength,
					Offset: 1,
				},
			},
			State:  model.StatePublic,
			Unique: true,
		},
		{
			Name: model.NewCIStr("b_c"),
			Columns: []*model.IndexColumn{
				{
					Name:   model.NewCIStr("b"),
					Length: types.UnspecifiedLength,
					Offset: 1,
				},
				{
					Name:   model.NewCIStr("c"),
					Length: types.UnspecifiedLength,
					Offset: 2,
				},
			},
			State: model.StatePublic,
		},
	}
	pkColumn := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    0,
		Name:      model.NewCIStr("a"),
		FieldType: newLongType(),
		ID:        1,
	}
	col0 := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    1,
		Name:      model.NewCIStr("b"),
		FieldType: newLongType(),
		ID:        2,
	}
	col1 := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    2,
		Name:      model.NewCIStr("c"),
		FieldType: newLongType(),
		ID:        3,
	}
	pkColumn.Flag = mysql.PriKeyFlag | mysql.NotNullFlag | mysql.UnsignedFlag
	// Column 'b', 'c', 'd', 'f', 'g' is not null.
	col0.Flag = mysql.NotNullFlag
	col1.Flag = mysql.UnsignedFlag
	table := &model.TableInfo{
		Columns:    []*model.ColumnInfo{pkColumn, col0, col1},
		Indices:    indices,
		Name:       model.NewCIStr("t2"),
		PKIsHandle: true,
	}
	return table
}

// MockView is only used for plan related tests.
func MockView() *model.TableInfo {
	selectStmt := "select b,c,d from t"
	col0 := &model.ColumnInfo{
		State:  model.StatePublic,
		Offset: 0,
		Name:   model.NewCIStr("b"),
		ID:     1,
	}
	col1 := &model.ColumnInfo{
		State:  model.StatePublic,
		Offset: 1,
		Name:   model.NewCIStr("c"),
		ID:     2,
	}
	col2 := &model.ColumnInfo{
		State:  model.StatePublic,
		Offset: 2,
		Name:   model.NewCIStr("d"),
		ID:     3,
	}
	view := &model.ViewInfo{SelectStmt: selectStmt, Security: model.SecurityDefiner, Definer: &auth.UserIdentity{Username: "root", Hostname: ""}, Cols: []model.CIStr{col0.Name, col1.Name, col2.Name}}
	table := &model.TableInfo{
		Name:    model.NewCIStr("v"),
		Columns: []*model.ColumnInfo{col0, col1, col2},
		View:    view,
	}
	return table
}

// MockContext is only used for plan related tests.
func MockContext() sessionctx.Context {
	ctx := mock.NewContext()
	ctx.Store = &mock.Store{
		Client: &mock.Client{},
	}
	ctx.GetSessionVars().CurrentDB = "test"
	do := &domain.Domain{}
	do.CreateStatsHandle(ctx)
	domain.BindDomain(ctx, do)
	return ctx
}

// MockPartitionInfoSchema mocks an info schema for partition table.
func MockPartitionInfoSchema(definitions []model.PartitionDefinition) infoschema.InfoSchema {
	tableInfo := MockSignedTable()
	cols := make([]*model.ColumnInfo, 0, len(tableInfo.Columns))
	cols = append(cols, tableInfo.Columns...)
	last := tableInfo.Columns[len(tableInfo.Columns)-1]
	cols = append(cols, &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    last.Offset + 1,
		Name:      model.NewCIStr("ptn"),
		FieldType: newLongType(),
		ID:        last.ID + 1,
	})
	partition := &model.PartitionInfo{
		Type:        model.PartitionTypeRange,
		Expr:        "ptn",
		Enable:      true,
		Definitions: definitions,
	}
	tableInfo.Columns = cols
	tableInfo.Partition = partition
	is := infoschema.MockInfoSchema([]*model.TableInfo{tableInfo})
	return is
}
