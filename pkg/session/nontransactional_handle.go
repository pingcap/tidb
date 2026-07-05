// Copyright 2026 PingCAP, Inc.
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

package session

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	sessiontypes "github.com/pingcap/tidb/pkg/session/types"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
)

type nonTransactionalDMLHandleKind string

const (
	nonTransactionalDMLHandleInt          nonTransactionalDMLHandleKind = "int"
	nonTransactionalDMLHandleExtra        nonTransactionalDMLHandleKind = "tidb_rowid"
	nonTransactionalDMLHandleCommonBinary nonTransactionalDMLHandleKind = "common_binary"
)

type nonTransactionalDMLHandleDescriptor struct {
	kind       nonTransactionalDMLHandleKind
	tableInfo  *model.TableInfo
	columnInfo *model.ColumnInfo
	columnName ast.ColumnName
	fieldType  types.FieldType
}

func buildNonTransactionalDMLHandleDescriptor(se sessiontypes.Session, stmt *ast.NonTransactionalDMLStmt,
	tableName *ast.TableName, shardColumnInfo *model.ColumnInfo, tableSources []*ast.TableSource) (*nonTransactionalDMLHandleDescriptor, error) {
	if se == nil || stmt == nil || tableName == nil {
		return nil, errors.New("Non-transactional DML parallel mode requires resolved table metadata")
	}
	if len(tableSources) != 1 {
		return nil, errors.New("Non-transactional DML parallel mode supports single-table DELETE and UPDATE only")
	}
	tbl, err := domain.GetDomain(se).InfoSchema().TableByName(context.Background(), tableName.Schema, tableName.Name)
	if err != nil {
		return nil, err
	}
	return buildNonTransactionalDMLHandleDescriptorFromTableInfo(stmt, tbl.Meta(), shardColumnInfo)
}

func buildNonTransactionalDMLHandleDescriptorFromTableInfo(stmt *ast.NonTransactionalDMLStmt,
	tableInfo *model.TableInfo, shardColumnInfo *model.ColumnInfo) (*nonTransactionalDMLHandleDescriptor, error) {
	if tableInfo == nil {
		return nil, errors.New("Non-transactional DML parallel mode requires table metadata")
	}
	if tableInfo.GetPartitionInfo() != nil {
		return nil, errors.New("Non-transactional DML parallel mode doesn't support partitioned tables")
	}
	if stmt == nil || stmt.ShardColumn == nil {
		return nil, errors.New("Non-transactional DML parallel mode requires a resolved shard column")
	}

	if stmt.ShardColumn.Name.L == model.ExtraHandleName.L && shardColumnInfo == nil {
		if tableInfo.HasClusteredIndex() {
			return nil, errors.New("Non-transactional DML parallel mode can't use _tidb_rowid on clustered tables")
		}
		return &nonTransactionalDMLHandleDescriptor{
			kind:      nonTransactionalDMLHandleExtra,
			tableInfo: tableInfo,
			columnName: ast.ColumnName{
				Name: pmodel.NewCIStr(model.ExtraHandleName.O),
			},
			fieldType: *types.NewFieldType(mysql.TypeLonglong),
		}, nil
	}

	if shardColumnInfo == nil {
		return nil, errors.New("Non-transactional DML parallel mode requires _tidb_rowid or a clustered primary key")
	}
	if tableInfo.PKIsHandle {
		return buildNonTransactionalDMLIntHandleDescriptor(stmt, tableInfo, shardColumnInfo)
	}
	if tableInfo.IsCommonHandle {
		return buildNonTransactionalDMLCommonHandleDescriptor(stmt, tableInfo, shardColumnInfo)
	}
	return nil, errors.New("Non-transactional DML parallel mode requires _tidb_rowid or a clustered primary key")
}

func buildNonTransactionalDMLIntHandleDescriptor(stmt *ast.NonTransactionalDMLStmt,
	tableInfo *model.TableInfo, shardColumnInfo *model.ColumnInfo) (*nonTransactionalDMLHandleDescriptor, error) {
	pkCol := tableInfo.GetPkColInfo()
	if pkCol == nil || pkCol.ID != shardColumnInfo.ID || !mysql.HasPriKeyFlag(shardColumnInfo.GetFlag()) {
		return nil, errors.New("Non-transactional DML parallel mode requires _tidb_rowid or a clustered primary key")
	}
	if mysql.HasUnsignedFlag(shardColumnInfo.GetFlag()) {
		return nil, errors.New("Non-transactional DML parallel mode doesn't support unsigned integer clustered primary keys")
	}
	if !isNonTransactionalDMLSignedIntegerType(shardColumnInfo.GetType()) {
		return nil, errors.New("Non-transactional DML parallel mode requires signed integer or binary common-handle clustered primary keys")
	}
	return &nonTransactionalDMLHandleDescriptor{
		kind:       nonTransactionalDMLHandleInt,
		tableInfo:  tableInfo,
		columnInfo: shardColumnInfo,
		columnName: *stmt.ShardColumn,
		fieldType:  shardColumnInfo.FieldType,
	}, nil
}

func buildNonTransactionalDMLCommonHandleDescriptor(stmt *ast.NonTransactionalDMLStmt,
	tableInfo *model.TableInfo, shardColumnInfo *model.ColumnInfo) (*nonTransactionalDMLHandleDescriptor, error) {
	primary := tableInfo.GetPrimaryKey()
	if primary == nil || !primary.Primary {
		return nil, errors.New("Non-transactional DML parallel mode requires a clustered primary key")
	}
	if len(primary.Columns) != 1 {
		return nil, errors.New("Non-transactional DML parallel mode doesn't support composite clustered primary keys")
	}
	primaryColumn := primary.Columns[0]
	if primaryColumn.Length != types.UnspecifiedLength {
		return nil, errors.New("Non-transactional DML parallel mode doesn't support prefix clustered primary keys")
	}
	if primaryColumn.Offset < 0 || primaryColumn.Offset >= len(tableInfo.Columns) {
		return nil, errors.New("Non-transactional DML parallel mode found invalid clustered primary key metadata")
	}
	pkCol := tableInfo.Columns[primaryColumn.Offset]
	if pkCol.ID != shardColumnInfo.ID {
		return nil, errors.New("Non-transactional DML parallel mode requires _tidb_rowid or a clustered primary key")
	}
	if !isNonTransactionalDMLBinaryCommonHandleColumn(shardColumnInfo) {
		return nil, errors.New("Non-transactional DML parallel mode common-handle string primary key requires binary collation")
	}
	return &nonTransactionalDMLHandleDescriptor{
		kind:       nonTransactionalDMLHandleCommonBinary,
		tableInfo:  tableInfo,
		columnInfo: shardColumnInfo,
		columnName: *stmt.ShardColumn,
		fieldType:  shardColumnInfo.FieldType,
	}, nil
}

func isNonTransactionalDMLSignedIntegerType(tp byte) bool {
	switch tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		return true
	default:
		return false
	}
}

func isNonTransactionalDMLBinaryCommonHandleColumn(col *model.ColumnInfo) bool {
	if col == nil {
		return false
	}
	tp := col.GetType()
	if !(types.IsTypeChar(tp) || types.IsTypeVarchar(tp)) {
		return false
	}
	return collate.IsBinCollation(col.GetCollate())
}
