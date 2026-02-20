// Copyright 2016 PingCAP, Inc.
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

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package ddl

import (
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

func checkFieldTypeCompatible(ft *types.FieldType, other *types.FieldType) bool {
	// int(1) could match the type with int(8)
	partialEqual := ft.GetType() == other.GetType() &&
		ft.GetDecimal() == other.GetDecimal() &&
		ft.GetCharset() == other.GetCharset() &&
		ft.GetCollate() == other.GetCollate() &&
		(ft.GetFlen() == other.GetFlen() || ft.StorageLength() != types.VarStorageLen) &&
		mysql.HasUnsignedFlag(ft.GetFlag()) == mysql.HasUnsignedFlag(other.GetFlag()) &&
		mysql.HasAutoIncrementFlag(ft.GetFlag()) == mysql.HasAutoIncrementFlag(other.GetFlag()) &&
		mysql.HasNotNullFlag(ft.GetFlag()) == mysql.HasNotNullFlag(other.GetFlag()) &&
		mysql.HasZerofillFlag(ft.GetFlag()) == mysql.HasZerofillFlag(other.GetFlag()) &&
		mysql.HasBinaryFlag(ft.GetFlag()) == mysql.HasBinaryFlag(other.GetFlag()) &&
		mysql.HasPriKeyFlag(ft.GetFlag()) == mysql.HasPriKeyFlag(other.GetFlag())
	if !partialEqual || len(ft.GetElems()) != len(other.GetElems()) {
		return false
	}
	for i := range ft.GetElems() {
		if ft.GetElems()[i] != other.GetElems()[i] {
			return false
		}
	}
	return true
}

func checkTiFlashReplicaCompatible(source *model.TiFlashReplicaInfo, target *model.TiFlashReplicaInfo) bool {
	if source == target {
		return true
	}
	if source == nil || target == nil {
		return false
	}
	if source.Count != target.Count ||
		source.Available != target.Available || len(source.LocationLabels) != len(target.LocationLabels) {
		return false
	}
	for i, lable := range source.LocationLabels {
		if target.LocationLabels[i] != lable {
			return false
		}
	}
	return true
}

func checkTableDefCompatible(source *model.TableInfo, target *model.TableInfo) error {
	// check temp table
	if target.TempTableType != model.TempTableNone {
		return errors.Trace(dbterror.ErrPartitionExchangeTempTable.FastGenByArgs(target.Name))
	}

	// check auto_random
	if source.AutoRandomBits != target.AutoRandomBits ||
		source.AutoRandomRangeBits != target.AutoRandomRangeBits ||
		source.Charset != target.Charset ||
		source.Collate != target.Collate ||
		source.ShardRowIDBits != target.ShardRowIDBits ||
		source.MaxShardRowIDBits != target.MaxShardRowIDBits ||
		source.PKIsHandle != target.PKIsHandle ||
		source.IsCommonHandle != target.IsCommonHandle ||
		!checkTiFlashReplicaCompatible(source.TiFlashReplica, target.TiFlashReplica) {
		return errors.Trace(dbterror.ErrTablesDifferentMetadata)
	}
	if len(source.Cols()) != len(target.Cols()) {
		return errors.Trace(dbterror.ErrTablesDifferentMetadata)
	}
	// Col compatible check
	for i, sourceCol := range source.Cols() {
		targetCol := target.Cols()[i]
		if sourceCol.IsVirtualGenerated() != targetCol.IsVirtualGenerated() {
			return dbterror.ErrUnsupportedOnGeneratedColumn.GenWithStackByArgs("Exchanging partitions for non-generated columns")
		}
		// It should strictyle compare expressions for generated columns
		if sourceCol.Name.L != targetCol.Name.L ||
			sourceCol.Hidden != targetCol.Hidden ||
			!checkFieldTypeCompatible(&sourceCol.FieldType, &targetCol.FieldType) ||
			sourceCol.GeneratedExprString != targetCol.GeneratedExprString {
			return errors.Trace(dbterror.ErrTablesDifferentMetadata)
		}
		if sourceCol.State != model.StatePublic ||
			targetCol.State != model.StatePublic {
			return errors.Trace(dbterror.ErrTablesDifferentMetadata)
		}
		if sourceCol.ID != targetCol.ID {
			return dbterror.ErrPartitionExchangeDifferentOption.GenWithStackByArgs(fmt.Sprintf("column: %s", sourceCol.Name))
		}
	}
	if len(source.Indices) != len(target.Indices) {
		return errors.Trace(dbterror.ErrTablesDifferentMetadata)
	}
	for _, sourceIdx := range source.Indices {
		if sourceIdx.Global {
			return dbterror.ErrPartitionExchangeDifferentOption.GenWithStackByArgs(fmt.Sprintf("global index: %s", sourceIdx.Name))
		}
		var compatIdx *model.IndexInfo
		for _, targetIdx := range target.Indices {
			if strings.EqualFold(sourceIdx.Name.L, targetIdx.Name.L) {
				compatIdx = targetIdx
			}
		}
		// No match index
		if compatIdx == nil {
			return errors.Trace(dbterror.ErrTablesDifferentMetadata)
		}
		// Index type is not compatible
		if sourceIdx.Tp != compatIdx.Tp ||
			sourceIdx.Unique != compatIdx.Unique ||
			sourceIdx.Primary != compatIdx.Primary {
			return errors.Trace(dbterror.ErrTablesDifferentMetadata)
		}
		// The index column
		if len(sourceIdx.Columns) != len(compatIdx.Columns) {
			return errors.Trace(dbterror.ErrTablesDifferentMetadata)
		}
		for i, sourceIdxCol := range sourceIdx.Columns {
			compatIdxCol := compatIdx.Columns[i]
			if sourceIdxCol.Length != compatIdxCol.Length ||
				sourceIdxCol.Name.L != compatIdxCol.Name.L {
				return errors.Trace(dbterror.ErrTablesDifferentMetadata)
			}
		}
		if sourceIdx.ID != compatIdx.ID {
			return dbterror.ErrPartitionExchangeDifferentOption.GenWithStackByArgs(fmt.Sprintf("index: %s", sourceIdx.Name))
		}
	}

	return nil
}

func checkExchangePartition(pt *model.TableInfo, nt *model.TableInfo) error {
	if nt.IsView() || nt.IsSequence() {
		return errors.Trace(dbterror.ErrCheckNoSuchTable)
	}
	if pt.GetPartitionInfo() == nil {
		return errors.Trace(dbterror.ErrPartitionMgmtOnNonpartitioned)
	}
	if nt.GetPartitionInfo() != nil {
		return errors.Trace(dbterror.ErrPartitionExchangePartTable.GenWithStackByArgs(nt.Name))
	}

	if nt.Affinity != nil || pt.Affinity != nil {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("EXCHANGE PARTITION of a table with AFFINITY option")
	}

	if len(nt.ForeignKeys) > 0 {
		return errors.Trace(dbterror.ErrPartitionExchangeForeignKey.GenWithStackByArgs(nt.Name))
	}

	return nil
}

func (e *executor) ExchangeTablePartition(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	ptSchema, pt, err := e.getSchemaAndTableByIdent(ident)
	if err != nil {
		return errors.Trace(err)
	}

	ptMeta := pt.Meta()

	ntIdent := ast.Ident{Schema: spec.NewTable.Schema, Name: spec.NewTable.Name}

	// We should check local temporary here using session's info schema because the local temporary tables are only stored in session.
	ntLocalTempTable, err := sessiontxn.GetTxnManager(ctx).GetTxnInfoSchema().TableByName(context.Background(), ntIdent.Schema, ntIdent.Name)
	if err == nil && ntLocalTempTable.Meta().TempTableType == model.TempTableLocal {
		return errors.Trace(dbterror.ErrPartitionExchangeTempTable.FastGenByArgs(ntLocalTempTable.Meta().Name))
	}

	ntSchema, nt, err := e.getSchemaAndTableByIdent(ntIdent)
	if err != nil {
		return errors.Trace(err)
	}

	ntMeta := nt.Meta()
	if isReservedSchemaObjInNextGen(ntMeta.ID) {
		return dbterror.ErrForbiddenDDL.FastGenByArgs(fmt.Sprintf("Exchange partition on system table '%s.%s'", ntSchema.Name.L, ntMeta.Name.L))
	}
	err = checkExchangePartition(ptMeta, ntMeta)
	if err != nil {
		return errors.Trace(err)
	}

	partName := spec.PartitionNames[0].L

	// NOTE: if pt is subPartitioned, it should be checked

	defID, err := tables.FindPartitionByName(ptMeta, partName)
	if err != nil {
		return errors.Trace(err)
	}

	err = checkTableDefCompatible(ptMeta, ntMeta)
	if err != nil {
		return errors.Trace(err)
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       ntSchema.ID,
		TableID:        ntMeta.ID,
		SchemaName:     ntSchema.Name.L,
		TableName:      ntMeta.Name.L,
		Type:           model.ActionExchangeTablePartition,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{
			{Database: ptSchema.Name.L, Table: ptMeta.Name.L},
			{Database: ntSchema.Name.L, Table: ntMeta.Name.L},
		},
		SQLMode: ctx.GetSessionVars().SQLMode,
	}
	args := &model.ExchangeTablePartitionArgs{
		PartitionID:    defID,
		PTSchemaID:     ptSchema.ID,
		PTTableID:      ptMeta.ID,
		PartitionName:  partName,
		WithValidation: spec.WithValidation,
	}

	err = e.doDDLJob2(ctx, job, args)
	if err != nil {
		return errors.Trace(err)
	}
	ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackError("after the exchange, please analyze related table of the exchange to update statistics"))
	return nil
}
