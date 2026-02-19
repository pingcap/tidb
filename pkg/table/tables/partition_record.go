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
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
)

func checkConstraintForExchangePartition(ctx table.MutateContext, row []types.Datum, partID, ntID int64) error {
	support, ok := ctx.GetExchangePartitionDMLSupport()
	if !ok {
		return errors.New("ctx does not support operations when exchanging a partition")
	}

	type InfoSchema interface {
		TableByID(ctx context.Context, id int64) (val table.Table, ok bool)
	}

	is, ok := support.GetInfoSchemaToCheckExchangeConstraint().(InfoSchema)
	if !ok {
		return errors.Errorf("exchange partition process assert inforSchema failed")
	}
	gCtx := context.Background()
	nt, tableFound := is.TableByID(gCtx, ntID)
	if !tableFound {
		// Now partID is nt tableID.
		nt, tableFound = is.TableByID(gCtx, partID)
		if !tableFound {
			return errors.Errorf("exchange partition process table by id failed")
		}
	}

	if err := table.CheckRowConstraintWithDatum(ctx.GetExprCtx(), nt.WritableConstraint(), row, nt.Meta()); err != nil {
		// TODO: make error include ExchangePartition info.
		return err
	}
	return nil
}

// AddRecord implements the AddRecord method for the table.Table interface.
func (t *partitionedTable) AddRecord(ctx table.MutateContext, txn kv.Transaction, r []types.Datum, opts ...table.AddRecordOption) (recordID kv.Handle, err error) {
	return partitionedTableAddRecord(ctx, txn, t, r, nil, opts)
}

func partitionedTableAddRecord(ctx table.MutateContext, txn kv.Transaction, t *partitionedTable, r []types.Datum, partitionSelection map[int64]struct{}, opts []table.AddRecordOption) (recordID kv.Handle, err error) {
	opt := table.NewAddRecordOpt(opts...)
	pid, err := t.locatePartition(ctx.GetExprCtx().GetEvalCtx(), r)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if partitionSelection != nil {
		if _, ok := partitionSelection[pid]; !ok {
			return nil, errors.WithStack(table.ErrRowDoesNotMatchGivenPartitionSet)
		}
	}
	exchangePartitionInfo := t.Meta().ExchangePartitionInfo
	if exchangePartitionInfo != nil && exchangePartitionInfo.ExchangePartitionDefID == pid &&
		vardef.EnableCheckConstraint.Load() {
		err = checkConstraintForExchangePartition(ctx, r, pid, exchangePartitionInfo.ExchangePartitionTableID)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}
	tbl := t.getPartition(pid)
	recordID, err = tbl.addRecord(ctx, txn, r, opt)
	if err != nil {
		return
	}
	if t.Meta().Partition.DDLState == model.StateDeleteOnly || t.Meta().Partition.DDLState == model.StatePublic {
		return
	}
	if _, ok := t.reorganizePartitions[pid]; ok {
		// Double write to the ongoing reorganized partition
		pid, err = t.locateReorgPartition(ctx.GetExprCtx().GetEvalCtx(), r)
		if err != nil {
			return nil, errors.Trace(err)
		}
		tbl = t.getPartition(pid)
		if !tbl.Meta().HasClusteredIndex() {
			// Preserve the _tidb_rowid also in the new partition!
			r = append(r, types.NewIntDatum(recordID.IntValue()))
		}
		recordID, err = tbl.addRecord(ctx, txn, r, opt)
		if err != nil {
			return
		}
	}
	return
}

// partitionTableWithGivenSets is used for this kind of grammar: partition (p0,p1)
// Basically it is the same as partitionedTable except that partitionTableWithGivenSets
// checks the given partition set for AddRecord/UpdateRecord operations.
type partitionTableWithGivenSets struct {
	*partitionedTable
	givenSetPartitions map[int64]struct{}
}

// NewPartitionTableWithGivenSets creates a new partition table from a partition table.
func NewPartitionTableWithGivenSets(tbl table.PartitionedTable, partitions map[int64]struct{}) table.PartitionedTable {
	if raw, ok := tbl.(*partitionedTable); ok {
		return &partitionTableWithGivenSets{
			partitionedTable:   raw,
			givenSetPartitions: partitions,
		}
	}
	return tbl
}

// AddRecord implements the AddRecord method for the table.Table interface.
func (t *partitionTableWithGivenSets) AddRecord(ctx table.MutateContext, txn kv.Transaction, r []types.Datum, opts ...table.AddRecordOption) (recordID kv.Handle, err error) {
	return partitionedTableAddRecord(ctx, txn, t.partitionedTable, r, t.givenSetPartitions, opts)
}

func (t *partitionTableWithGivenSets) GetAllPartitionIDs() []int64 {
	ptIDs := make([]int64, 0, len(t.partitions))
	for id := range t.givenSetPartitions {
		ptIDs = append(ptIDs, id)
	}
	return ptIDs
}

func dataEqRec(loc *time.Location, tblInfo *model.TableInfo, row []types.Datum, rec []byte) (bool, error) {
	columnFt := make(map[int64]*types.FieldType)
	for idx := range tblInfo.Columns {
		col := tblInfo.Columns[idx]
		columnFt[col.ID] = &col.FieldType
	}
	foundData, err := tablecodec.DecodeRowToDatumMap(rec, columnFt, loc)
	if err != nil {
		return false, errors.Trace(err)
	}
	for idx, col := range tblInfo.Cols() {
		if d, ok := foundData[col.ID]; ok {
			if !d.Equals(row[idx]) {
				return false, nil
			}
		}
	}
	return true, nil
}

// RemoveRecord implements table.Table RemoveRecord interface.
func (t *partitionedTable) RemoveRecord(ctx table.MutateContext, txn kv.Transaction, h kv.Handle, r []types.Datum, opts ...table.RemoveRecordOption) error {
	opt := table.NewRemoveRecordOpt(opts...)
	ectx := ctx.GetExprCtx()
	from, err := t.locatePartition(ectx.GetEvalCtx(), r)
	if err != nil {
		return errors.Trace(err)
	}

	tbl := t.getPartition(from)
	err = tbl.removeRecord(ctx, txn, h, r, opt)
	if err != nil {
		return errors.Trace(err)
	}

	if _, ok := t.reorganizePartitions[from]; ok {
		newFrom, err := t.locateReorgPartition(ectx.GetEvalCtx(), r)
		if err != nil || newFrom == 0 {
			return errors.Trace(err)
		}

		if t.Meta().HasClusteredIndex() {
			return t.getPartition(newFrom).removeRecord(ctx, txn, h, r, opt)
		}
		encodedRecordID := codec.EncodeInt(nil, h.IntValue())
		newFromKey := tablecodec.EncodeRowKey(newFrom, encodedRecordID)

		val, err := getKeyInTxn(context.Background(), txn, newFromKey)
		if err != nil {
			return errors.Trace(err)
		}
		if len(val) > 0 {
			same, err := dataEqRec(ctx.GetExprCtx().GetEvalCtx().Location(), t.Meta(), r, val)
			if err != nil || !same {
				return errors.Trace(err)
			}
			return t.getPartition(newFrom).removeRecord(ctx, txn, h, r, opt)
		}
	}
	return nil
}

func (t *partitionedTable) GetAllPartitionIDs() []int64 {
	ptIDs := make([]int64, 0, len(t.partitions))
	for id := range t.partitions {
		if _, ok := t.doubleWritePartitions[id]; ok {
			continue
		}
		ptIDs = append(ptIDs, id)
	}
	return ptIDs
}

// UpdateRecord implements table.Table UpdateRecord interface.
// `touched` means which columns are really modified, used for secondary indices.
// Length of `oldData` and `newData` equals to length of `t.WritableCols()`.
func (t *partitionedTable) UpdateRecord(ctx table.MutateContext, txn kv.Transaction, h kv.Handle, currData, newData []types.Datum, touched []bool, opts ...table.UpdateRecordOption) error {
	return partitionedTableUpdateRecord(ctx, txn, t, h, currData, newData, touched, nil, opts...)
}

func (t *partitionTableWithGivenSets) UpdateRecord(ctx table.MutateContext, txn kv.Transaction, h kv.Handle, currData, newData []types.Datum, touched []bool, opts ...table.UpdateRecordOption) error {
	return partitionedTableUpdateRecord(ctx, txn, t.partitionedTable, h, currData, newData, touched, t.givenSetPartitions, opts...)
}

func partitionedTableUpdateRecord(ctx table.MutateContext, txn kv.Transaction, t *partitionedTable, h kv.Handle, currData, newData []types.Datum, touched []bool, partitionSelection map[int64]struct{}, opts ...table.UpdateRecordOption) error {
	opt := table.NewUpdateRecordOpt(opts...)
	ectx := ctx.GetExprCtx()
	from, err := t.locatePartition(ectx.GetEvalCtx(), currData)
	if err != nil {
		return errors.Trace(err)
	}
	to, err := t.locatePartition(ectx.GetEvalCtx(), newData)
	if err != nil {
		return errors.Trace(err)
	}
	if partitionSelection != nil {
		if _, ok := partitionSelection[to]; !ok {
			return errors.WithStack(table.ErrRowDoesNotMatchGivenPartitionSet)
		}
		// Should not have been read from this partition! Checked already in GetPartitionByRow()
		if _, ok := partitionSelection[from]; !ok {
			return errors.WithStack(table.ErrRowDoesNotMatchGivenPartitionSet)
		}
	}
	// TODO: Remove this and require EXCHANGE PARTITION to have same CONSTRAINTs on the tables!
	exchangePartitionInfo := t.Meta().ExchangePartitionInfo
	if exchangePartitionInfo != nil && exchangePartitionInfo.ExchangePartitionDefID == to &&
		vardef.EnableCheckConstraint.Load() {
		err = checkConstraintForExchangePartition(ctx, newData, to, exchangePartitionInfo.ExchangePartitionTableID)
		if err != nil {
			return errors.WithStack(err)
		}
	}

	memBuffer := txn.GetMemBuffer()
	sh := memBuffer.Staging()
	defer memBuffer.Cleanup(sh)

	deleteOnly := t.Meta().Partition.DDLState == model.StateDeleteOnly || t.Meta().Partition.DDLState == model.StatePublic
	newRecordHandle := h
	finishFunc := func(err error, _ kv.Handle) error {
		if err != nil {
			return err
		}
		memBuffer.Release(sh)
		return nil
	}
	if from == to && t.Meta().HasClusteredIndex() {
		err = t.getPartition(to).updateRecord(ctx, txn, h, currData, newData, touched, opt)
		if err != nil {
			return errors.Trace(err)
		}
	} else if from != to {
		// The old and new data locate in different partitions.
		// Remove record from old partition
		err = t.getPartition(from).RemoveRecord(ctx, txn, h, currData)
		if err != nil {
			return errors.Trace(err)
		}
		// and add record to new partition, which will also give it a new Record ID/_tidb_rowid!
		newRecordHandle, err = t.getPartition(to).addRecord(ctx, txn, newData, opt.GetAddRecordOpt())
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		// to == from && !t.Meta().HasClusteredIndex()
		// We don't yet know if there will be a new record id generate or not,
		// better defer handling current record until checked reorganized partitions so we know!
		finishFunc = func(err error, newRecordHandle kv.Handle) error {
			if err != nil {
				return err
			}
			if newRecordHandle == nil {
				err = t.getPartition(to).updateRecord(ctx, txn, h, currData, newData, touched, opt)
				if err != nil {
					return err
				}
				memBuffer.Release(sh)
				return nil
			}
			err = t.getPartition(from).RemoveRecord(ctx, txn, h, currData)
			if err != nil {
				return err
			}
			if !deleteOnly {
				// newData now contains the new record ID
				_, err = t.getPartition(to).addRecord(ctx, txn, newData, opt.GetAddRecordOptKeepRecordID())
				if err != nil {
					return err
				}
			}
			memBuffer.Release(sh)
			return nil
		}
	}

	var newTo, newFrom int64
	if _, ok := t.reorganizePartitions[to]; ok {
		newTo, err = t.locateReorgPartition(ectx.GetEvalCtx(), newData)
		if err != nil {
			return errors.Trace(err)
		}
	}
	if _, ok := t.reorganizePartitions[from]; ok {
		newFrom, err = t.locateReorgPartition(ectx.GetEvalCtx(), currData)
		if err != nil {
			return errors.Trace(err)
		}
	}
	if newFrom == 0 && newTo == 0 {
		return finishFunc(err, nil)
	}
	if t.Meta().HasClusteredIndex() {
		// Always do Remove+Add, to always have the indexes in-sync,
		// since the indexes might not been created yet, i.e. not backfilled yet.
		if newFrom != 0 {
			err = t.getPartition(newFrom).RemoveRecord(ctx, txn, h, currData)
			if err != nil {
				return errors.Trace(err)
			}
		}
		if newTo != 0 && !deleteOnly {
			_, err = t.getPartition(newTo).addRecord(ctx, txn, newData, opt.GetAddRecordOpt())
			if err != nil {
				return errors.Trace(err)
			}
		}
		return finishFunc(err, nil)
	}

	var found map[string]kv.ValueEntry
	var newFromKey, newToKey kv.Key

	keys := make([]kv.Key, 0, 2)
	encodedRecordID := codec.EncodeInt(nil, h.IntValue())
	if newFrom != 0 {
		newFromKey = tablecodec.EncodeRowKey(newFrom, encodedRecordID)
		keys = append(keys, newFromKey)
	}
	if !deleteOnly && newTo != 0 {
		// Only need to check if writing.
		if newTo == newFrom {
			newToKey = newFromKey
		} else if newRecordHandle.Equal(h) {
			// And no new record id generated (else new unique id, cannot be found)
			newToKey = tablecodec.EncodeRowKey(newTo, encodedRecordID)
			keys = append(keys, newToKey)
		}
	}
	var newFromVal, newToVal []byte
	switch len(keys) {
	case 0:
	// No lookup
	case 1:
		val, err := getKeyInTxn(context.Background(), txn, keys[0])
		if err != nil {
			return errors.Trace(err)
		}
		if newFrom != 0 {
			newFromVal = val
		}
		if !deleteOnly && newTo != 0 {
			newToVal = val
		}
	default:
		found, err = txn.BatchGet(context.Background(), keys)
		if err != nil {
			return errors.Trace(err)
		}
		if len(newFromKey) > 0 {
			if val, ok := found[string(newFromKey)]; ok {
				newFromVal = val.Value
			}
		}
		if len(newToKey) > 0 {
			if val, ok := found[string(newToKey)]; ok {
				newToVal = val.Value
			}
		}
	}
	var newToKeyAndValIsSame *bool
	if len(newFromVal) > 0 {
		var same bool
		same, err = dataEqRec(ctx.GetExprCtx().GetEvalCtx().Location(), t.Meta(), currData, newFromVal)
		if err != nil {
			return errors.Trace(err)
		}
		if same {
			// Always do Remove+Add, to always have the indexes in-sync,
			// since the indexes might not been created yet, i.e. not backfilled yet.
			err = t.getPartition(newFrom).RemoveRecord(ctx, txn, h, currData)
			if err != nil {
				return errors.Trace(err)
			}
		}
		if newTo == newFrom {
			newToKeyAndValIsSame = &same
		}
	}
	if deleteOnly || newTo == 0 {
		return finishFunc(err, nil)
	}
	if len(newToVal) > 0 {
		if newToKeyAndValIsSame == nil {
			same, err := dataEqRec(ctx.GetExprCtx().GetEvalCtx().Location(), t.Meta(), currData, newToVal)
			if err != nil {
				return errors.Trace(err)
			}
			newToKeyAndValIsSame = &same
		}
		if !*newToKeyAndValIsSame {
			// Generate a new ID
			newRecordHandle, err = AllocHandle(context.Background(), ctx, t)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	// Set/Add the recordID/_tidb_rowid to newData so it will be used also in the
	// newTo partition, and all its indexes.
	if len(newData) > len(t.Cols()) {
		newData[len(t.Cols())] = types.NewIntDatum(newRecordHandle.IntValue())
	} else {
		newData = append(newData, types.NewIntDatum(newRecordHandle.IntValue()))
	}
	addRecordOpt := opt.GetAddRecordOptKeepRecordID()
	_, err = t.getPartition(newTo).addRecord(ctx, txn, newData, addRecordOpt)
	if err != nil {
		return errors.Trace(err)
	}
	var newHandle kv.Handle
	if !h.Equal(newRecordHandle) {
		newHandle = newRecordHandle
	}
	return finishFunc(err, newHandle)
}
