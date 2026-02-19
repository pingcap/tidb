// Copyright 2015 PingCAP, Inc.
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

package ddl

import (
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/infoschema"
	infoschemactx "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/set"
)

func CheckTableInfoValidWithStmt(ctx *metabuild.Context, tbInfo *model.TableInfo, s *ast.CreateTableStmt) (err error) {
	return checkTableInfoValidWithStmt(ctx, tbInfo, s)
}

func checkTableInfoValidWithStmt(ctx *metabuild.Context, tbInfo *model.TableInfo, s *ast.CreateTableStmt) (err error) {
	// All of these rely on the AST structure of expressions, which were
	// lost in the model (got serialized into strings).
	if err := checkGeneratedColumn(ctx, s.Table.Schema, tbInfo.Name, s.Cols); err != nil {
		return errors.Trace(err)
	}

	// Check if table has a primary key if required.
	if ctx.PrimaryKeyRequired() && len(tbInfo.GetPkName().String()) == 0 {
		return infoschema.ErrTableWithoutPrimaryKey
	}
	if tbInfo.Partition != nil {
		if err := checkPartitionDefinitionConstraints(ctx.GetExprCtx(), tbInfo); err != nil {
			return errors.Trace(err)
		}
		if s.Partition != nil {
			if err := checkPartitionFuncType(ctx.GetExprCtx(), s.Partition.Expr, s.Table.Schema.O, tbInfo); err != nil {
				return errors.Trace(err)
			}
			if err := checkPartitioningKeysConstraints(ctx, s, tbInfo); err != nil {
				return errors.Trace(err)
			}
		}
	}
	if tbInfo.TTLInfo != nil {
		var foreignKeyCheckIs infoschemactx.MetaOnlyInfoSchema
		if is, ok := ctx.GetInfoSchema(); ok {
			foreignKeyCheckIs = is
		}
		if err = checkTTLInfoValid(s.Table.Schema, tbInfo, foreignKeyCheckIs); err != nil {
			return err
		}
	}

	return nil
}

func checkGeneratedColumn(ctx *metabuild.Context, schemaName ast.CIStr, tableName ast.CIStr, colDefs []*ast.ColumnDef) error {
	var colName2Generation = make(map[string]columnGenerationInDDL, len(colDefs))
	var exists bool
	var autoIncrementColumn string
	for i, colDef := range colDefs {
		for _, option := range colDef.Options {
			if option.Tp == ast.ColumnOptionGenerated {
				if err := checkIllegalFn4Generated(colDef.Name.Name.L, typeColumn, option.Expr); err != nil {
					return errors.Trace(err)
				}
			}
		}
		if containsColumnOption(colDef, ast.ColumnOptionAutoIncrement) {
			exists, autoIncrementColumn = true, colDef.Name.Name.L
		}
		generated, depCols, err := findDependedColumnNames(schemaName, tableName, colDef)
		if err != nil {
			return errors.Trace(err)
		}
		if !generated {
			colName2Generation[colDef.Name.Name.L] = columnGenerationInDDL{
				position:  i,
				generated: false,
			}
		} else {
			colName2Generation[colDef.Name.Name.L] = columnGenerationInDDL{
				position:    i,
				generated:   true,
				dependences: depCols,
			}
		}
	}

	// Check whether the generated column refers to any auto-increment columns
	if exists {
		if !ctx.EnableAutoIncrementInGenerated() {
			for colName, generated := range colName2Generation {
				if _, found := generated.dependences[autoIncrementColumn]; found {
					return dbterror.ErrGeneratedColumnRefAutoInc.GenWithStackByArgs(colName)
				}
			}
		}
	}

	for _, colDef := range colDefs {
		colName := colDef.Name.Name.L
		if err := verifyColumnGeneration(colName2Generation, colName); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func checkColumnarIndexIfNeedTiFlashReplica(store kv.Storage, dbName ast.CIStr, tblInfo *model.TableInfo) error {
	var hasColumnarIndex bool
	for _, idx := range tblInfo.Indices {
		if idx.IsColumnarIndex() {
			hasColumnarIndex = true
			break
		}
	}
	if !hasColumnarIndex {
		return nil
	}
	if store == nil {
		return errors.New("the store is nil")
	}
	if err := isTableTiFlashSupported(dbName, tblInfo); err != nil {
		return errors.Trace(err)
	}

	if tblInfo.TiFlashReplica == nil || tblInfo.TiFlashReplica.Count == 0 {
		replicas, err := infoschema.GetTiFlashStoreCount(store)
		if err != nil {
			return errors.Trace(err)
		}
		if replicas == 0 {
			return errors.Trace(dbterror.ErrUnsupportedAddColumnarIndex.FastGenByArgs("unsupported TiFlash store count is 0"))
		}

		// Always try to set to 1 as the default replica count.
		defaultReplicas := uint64(1)
		tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
			Count:          defaultReplicas,
			LocationLabels: make([]string, 0),
		}
	}

	return errors.Trace(checkTableTypeForColumnarIndex(tblInfo))
}

// checkTableInfoValidExtra is like checkTableInfoValid, but also assumes the
// table info comes from untrusted source and performs further checks such as
// name length and column count.
// (checkTableInfoValid is also used in repairing objects which don't perform
// these checks. Perhaps the two functions should be merged together regardless?)
func checkTableInfoValidExtra(ec errctx.Context, store kv.Storage, dbName ast.CIStr, tbInfo *model.TableInfo) error {
	if err := checkTooLongTable(tbInfo.Name); err != nil {
		return err
	}

	if err := checkDuplicateColumn(tbInfo.Columns); err != nil {
		return err
	}
	if err := checkTooLongColumns(tbInfo.Columns); err != nil {
		return err
	}
	if err := checkTooManyColumns(tbInfo.Columns); err != nil {
		return errors.Trace(err)
	}
	if err := checkTooManyIndexes(tbInfo.Indices); err != nil {
		return errors.Trace(err)
	}
	if err := checkColumnsAttributes(tbInfo.Columns); err != nil {
		return errors.Trace(err)
	}
	if err := checkGlobalIndexes(ec, tbInfo); err != nil {
		return errors.Trace(err)
	}
	if err := checkColumnarIndexIfNeedTiFlashReplica(store, dbName, tbInfo); err != nil {
		return errors.Trace(err)
	}

	// FIXME: perform checkConstraintNames
	if err := checkCharsetAndCollation(tbInfo.Charset, tbInfo.Collate); err != nil {
		return errors.Trace(err)
	}

	oldState := tbInfo.State
	tbInfo.State = model.StatePublic
	err := checkTableInfoValid(tbInfo)
	tbInfo.State = oldState
	return err
}

// checkTableInfoValid uses to check table info valid. This is used to validate table info.
func checkTableInfoValid(tblInfo *model.TableInfo) error {
	_, err := tables.TableFromMeta(autoid.NewAllocators(false), tblInfo)
	if err != nil {
		return err
	}
	return checkInvisibleIndexOnPK(tblInfo)
}

func checkDuplicateColumn(cols []*model.ColumnInfo) error {
	colNames := set.StringSet{}
	for _, col := range cols {
		colName := col.Name
		if colNames.Exist(colName.L) {
			return infoschema.ErrColumnExists.GenWithStackByArgs(colName.O)
		}
		colNames.Insert(colName.L)
	}
	return nil
}

func checkTooLongColumns(cols []*model.ColumnInfo) error {
	for _, col := range cols {
		if err := checkTooLongColumn(col.Name); err != nil {
			return err
		}
	}
	return nil
}

func checkTooManyColumns(colDefs []*model.ColumnInfo) error {
	if uint32(len(colDefs)) > atomic.LoadUint32(&config.GetGlobalConfig().TableColumnCountLimit) {
		return dbterror.ErrTooManyFields
	}
	return nil
}

func checkTooManyIndexes(idxDefs []*model.IndexInfo) error {
	if len(idxDefs) > config.GetGlobalConfig().IndexLimit {
		return dbterror.ErrTooManyKeys.GenWithStackByArgs(config.GetGlobalConfig().IndexLimit)
	}
	return nil
}

// checkColumnsAttributes checks attributes for multiple columns.
func checkColumnsAttributes(colDefs []*model.ColumnInfo) error {
	for _, colDef := range colDefs {
		if err := checkColumnAttributes(colDef.Name.O, &colDef.FieldType); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// checkColumnAttributes check attributes for single column.
func checkColumnAttributes(colName string, tp *types.FieldType) error {
	switch tp.GetType() {
	case mysql.TypeNewDecimal, mysql.TypeDouble, mysql.TypeFloat:
		if tp.GetFlen() < tp.GetDecimal() {
			return types.ErrMBiggerThanD.GenWithStackByArgs(colName)
		}
	case mysql.TypeDatetime, mysql.TypeDuration, mysql.TypeTimestamp:
		if tp.GetDecimal() != types.UnspecifiedFsp && (tp.GetDecimal() < types.MinFsp || tp.GetDecimal() > types.MaxFsp) {
			return types.ErrTooBigPrecision.GenWithStackByArgs(tp.GetDecimal(), colName, types.MaxFsp)
		}
	}
	return nil
}
