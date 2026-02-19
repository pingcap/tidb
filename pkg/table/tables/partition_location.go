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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tables

import (
	"fmt"
	"sort"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

func (t *partitionedTable) CheckForExchangePartition(ctx expression.EvalContext, pi *model.PartitionInfo, r []types.Datum, partID, ntID int64) error {
	defID, err := t.locatePartition(ctx, r)
	if err != nil {
		return err
	}
	if defID != partID && defID != ntID {
		return errors.WithStack(table.ErrRowDoesNotMatchGivenPartitionSet)
	}
	return nil
}

// locatePartitionCommon returns the partition idx of the input record.
func (t *partitionedTable) locatePartitionCommon(ctx expression.EvalContext, tp ast.PartitionType, partitionExpr *PartitionExpr, num uint64, columnsPartitioned bool, r []types.Datum) (int, error) {
	var err error
	var idx int
	switch tp {
	case ast.PartitionTypeRange:
		if columnsPartitioned {
			idx, err = t.locateRangeColumnPartition(ctx, partitionExpr, r)
		} else {
			idx, err = t.locateRangePartition(ctx, partitionExpr, r)
		}
		if err != nil {
			return -1, err
		}
		pi := t.Meta().Partition
		if pi.CanHaveOverlappingDroppingPartition() {
			if pi.IsDropping(idx) {
				// Give an error, since it should not be written to!
				// For read it can check the Overlapping partition and ignore the error.
				// One should use the next non-dropping partition for range, or the default
				// partition for list partitioned table with default partition, for read.
				return idx, table.ErrNoPartitionForGivenValue.GenWithStackByArgs(fmt.Sprintf("matching a partition being dropped, '%s'", pi.Definitions[idx].Name.String()))
			}
		}
	case ast.PartitionTypeHash:
		// Note that only LIST and RANGE supports REORGANIZE PARTITION
		idx, err = t.locateHashPartition(ctx, partitionExpr, num, r)
	case ast.PartitionTypeKey:
		idx, err = partitionExpr.LocateKeyPartition(num, r)
	case ast.PartitionTypeList:
		idx, err = partitionExpr.locateListPartition(ctx, r)
		pi := t.Meta().Partition
		if idx != pi.GetOverlappingDroppingPartitionIdx(idx) {
			return idx, table.ErrNoPartitionForGivenValue.GenWithStackByArgs(fmt.Sprintf("matching a partition being dropped, '%s'", pi.Definitions[idx].Name.String()))
		}
	case ast.PartitionTypeNone:
		idx = 0
	}
	if err != nil {
		return -1, errors.Trace(err)
	}
	return idx, nil
}

func (t *partitionedTable) locatePartitionIdx(ctx expression.EvalContext, r []types.Datum) (int, error) {
	pi := t.Meta().GetPartitionInfo()
	columnsSet := len(t.meta.Partition.Columns) > 0
	return t.locatePartitionCommon(ctx, pi.Type, t.partitionExpr, pi.Num, columnsSet, r)
}

func (t *partitionedTable) locatePartition(ctx expression.EvalContext, r []types.Datum) (int64, error) {
	idx, err := t.locatePartitionIdx(ctx, r)
	if err != nil {
		return 0, errors.Trace(err)
	}
	pi := t.Meta().GetPartitionInfo()
	return pi.Definitions[idx].ID, nil
}

func (t *partitionedTable) locateReorgPartition(ctx expression.EvalContext, r []types.Datum) (int64, error) {
	pi := t.Meta().GetPartitionInfo()
	columnsSet := len(pi.DDLColumns) > 0
	// Note that for KEY/HASH partitioning, since we do not support LINEAR,
	// all partitions will be reorganized,
	// so we can use the number in Dropping or AddingDefinitions,
	// depending on current state.
	reorgDefs := pi.AddingDefinitions
	switch pi.DDLAction {
	case model.ActionReorganizePartition, model.ActionRemovePartitioning, model.ActionAlterTablePartitioning:
		if pi.DDLState == model.StatePublic {
			reorgDefs = pi.DroppingDefinitions
		}
		fallthrough
	default:
		if pi.DDLState == model.StateDeleteReorganization {
			reorgDefs = pi.DroppingDefinitions
		}
	}
	idx, err := t.locatePartitionCommon(ctx, pi.DDLType, t.reorgPartitionExpr, uint64(len(reorgDefs)), columnsSet, r)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return reorgDefs[idx].ID, nil
}

func (t *partitionedTable) locateRangeColumnPartition(ctx expression.EvalContext, partitionExpr *PartitionExpr, r []types.Datum) (int, error) {
	upperBounds := partitionExpr.UpperBounds
	var lastError error
	evalBuffer := t.evalBufferPool.Get().(*chunk.MutRow)
	defer t.evalBufferPool.Put(evalBuffer)
	idx := sort.Search(len(upperBounds), func(i int) bool {
		evalBuffer.SetDatums(r...)
		ret, isNull, err := upperBounds[i].EvalInt(ctx, evalBuffer.ToRow())
		if err != nil {
			lastError = err
			return true // Does not matter, will propagate the last error anyway.
		}
		if isNull {
			// If the column value used to determine the partition is NULL, the row is inserted into the lowest partition.
			// See https://dev.mysql.com/doc/mysql-partitioning-excerpt/5.7/en/partitioning-handling-nulls.html
			return true // Always less than any other value (NULL cannot be in the partition definition VALUE LESS THAN).
		}
		return ret > 0
	})
	if lastError != nil {
		return 0, errors.Trace(lastError)
	}
	if idx >= len(upperBounds) {
		return 0, table.ErrNoPartitionForGivenValue.GenWithStackByArgs("from column_list")
	}
	return idx, nil
}

func (pe *PartitionExpr) locateListPartition(ctx expression.EvalContext, r []types.Datum) (int, error) {
	lp := pe.ForListPruning
	if len(lp.ColPrunes) == 0 {
		return lp.locateListPartitionByRow(ctx, r)
	}
	tc, ec := ctx.TypeCtx(), ctx.ErrCtx()
	return lp.locateListColumnsPartitionByRow(tc, ec, r)
}

func (t *partitionedTable) locateRangePartition(ctx expression.EvalContext, partitionExpr *PartitionExpr, r []types.Datum) (int, error) {
	var (
		ret    int64
		val    int64
		isNull bool
		err    error
	)
	if col, ok := partitionExpr.Expr.(*expression.Column); ok {
		if r[col.Index].IsNull() {
			isNull = true
		}
		ret = r[col.Index].GetInt64()
	} else {
		evalBuffer := t.evalBufferPool.Get().(*chunk.MutRow)
		defer t.evalBufferPool.Put(evalBuffer)
		evalBuffer.SetDatums(r...)
		val, isNull, err = partitionExpr.Expr.EvalInt(ctx, evalBuffer.ToRow())
		if err != nil {
			return 0, err
		}
		ret = val
	}
	unsigned := mysql.HasUnsignedFlag(partitionExpr.Expr.GetType(ctx).GetFlag())
	ranges := partitionExpr.ForRangePruning
	length := len(ranges.LessThan)
	pos := sort.Search(length, func(i int) bool {
		if isNull {
			return true
		}
		return ranges.Compare(i, ret, unsigned) > 0
	})
	if isNull {
		pos = 0
	}
	if pos < 0 || pos >= length {
		// The data does not belong to any of the partition returns `table has no partition for value %s`.
		var valueMsg string
		if unsigned {
			valueMsg = fmt.Sprintf("%d", uint64(ret))
		} else {
			valueMsg = fmt.Sprintf("%d", ret)
		}
		return 0, table.ErrNoPartitionForGivenValue.GenWithStackByArgs(valueMsg)
	}
	return pos, nil
}

// TODO: supports linear hashing
func (t *partitionedTable) locateHashPartition(ctx expression.EvalContext, partExpr *PartitionExpr, numParts uint64, r []types.Datum) (int, error) {
	if col, ok := partExpr.Expr.(*expression.Column); ok {
		var data types.Datum
		switch r[col.Index].Kind() {
		case types.KindInt64, types.KindUint64:
			data = r[col.Index]
		default:
			var err error
			data, err = r[col.Index].ConvertTo(ctx.TypeCtx(), types.NewFieldType(mysql.TypeLonglong))
			if err != nil {
				return 0, err
			}
		}
		ret := data.GetInt64()
		ret = ret % int64(numParts)
		if ret < 0 {
			ret = -ret
		}
		return int(ret), nil
	}
	evalBuffer := t.evalBufferPool.Get().(*chunk.MutRow)
	defer t.evalBufferPool.Put(evalBuffer)
	evalBuffer.SetDatums(r...)
	ret, isNull, err := partExpr.Expr.EvalInt(ctx, evalBuffer.ToRow())
	if err != nil {
		return 0, err
	}
	if isNull {
		return 0, nil
	}
	ret = ret % int64(numParts)
	if ret < 0 {
		ret = -ret
	}
	return int(ret), nil
}

// GetPartition returns a Table, which is actually a partition.
func (t *partitionedTable) GetPartition(pid int64) table.PhysicalTable {
	part := t.getPartition(pid)

	// Explicitly check if the partition is nil, and return a nil interface if it is
	if part == nil {
		return nil // Return a truly nil interface instead of an interface holding a nil pointer
	}

	return part
}

// getPartition returns a Table, which is actually a partition.
func (t *partitionedTable) getPartition(pid int64) *partition {
	// Attention, can't simply use `return t.partitions[pid]` here.
	// Because A nil of type *partition is a kind of `table.PhysicalTable`
	part, ok := t.partitions[pid]
	if !ok {
		// Should never happen!
		return nil
	}
	return part
}

// GetReorganizedPartitionedTable returns the same table
// but only with the AddingDefinitions used.
func GetReorganizedPartitionedTable(t table.Table) (table.PartitionedTable, error) {
	// This is used during Reorganize partitions; All data from DroppingDefinitions
	// will be copied to AddingDefinitions, so only setup with AddingDefinitions!

	// Do not change any Definitions of t, but create a new struct.
	if t.GetPartitionedTable() == nil {
		return nil, dbterror.ErrUnsupportedReorganizePartition.GenWithStackByArgs()
	}
	tblInfo := t.Meta().Clone()
	pi := tblInfo.Partition
	pi.Definitions = pi.AddingDefinitions
	pi.Num = uint64(len(pi.Definitions))
	pi.AddingDefinitions = nil
	pi.DroppingDefinitions = nil

	// Reorganized status, use the new values
	pi.Type = pi.DDLType
	pi.Expr = pi.DDLExpr
	pi.Columns = pi.DDLColumns
	if pi.NewTableID != 0 {
		tblInfo.ID = pi.NewTableID
	}

	constraints, err := table.LoadCheckConstraint(tblInfo)
	if err != nil {
		return nil, err
	}
	var tc TableCommon
	initTableCommon(&tc, tblInfo, tblInfo.ID, t.Cols(), t.Allocators(nil), constraints)

	// and rebuild the partitioning structure
	return newPartitionedTable(&tc, tblInfo)
}

// GetPartitionByRow returns a Table, which is actually a Partition.
func (t *partitionedTable) GetPartitionByRow(ctx expression.EvalContext, r []types.Datum) (table.PhysicalTable, error) {
	pid, err := t.locatePartition(ctx, r)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return t.partitions[pid], nil
}

// GetPartitionIdxByRow returns the index in PartitionDef for the matching partition
func (t *partitionedTable) GetPartitionIdxByRow(ctx expression.EvalContext, r []types.Datum) (int, error) {
	return t.locatePartitionIdx(ctx, r)
}

// GetPartitionByRow returns a Table, which is actually a Partition.
func (t *partitionTableWithGivenSets) GetPartitionByRow(ctx expression.EvalContext, r []types.Datum) (table.PhysicalTable, error) {
	pid, err := t.locatePartition(ctx, r)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if _, ok := t.givenSetPartitions[pid]; !ok {
		return nil, errors.WithStack(table.ErrRowDoesNotMatchGivenPartitionSet)
	}
	return t.partitions[pid], nil
}
