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

package tables

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"go.uber.org/zap"
)

var indexConditionECtx exprctx.BuildContext

// indexPartialCondition is a data structure to help implement the partial index.
type indexPartialCondition struct {
	conditionExpr expression.Expression
	// conditionEvalBufferPool stores many eval buffer to avoid allocating chunk for evaluating partial index condition for each time.
	// It's only initialized if the `partialConditionExpr` is not nil.
	conditionEvalBufferPool sync.Pool
}

// index is the data structure for index data in the KV store.
type index struct {
	idxInfo  *model.IndexInfo
	tblInfo  *model.TableInfo
	phyTblID int64
	// initNeedRestoreData is used to initialize `needRestoredData` in `index.Create()`.
	// This routine cannot be done in `NewIndex()` because `needRestoreData` relies on `NewCollationEnabled()` and
	// the collation global variable is initialized *after* `NewIndex()`.
	initNeedRestoreData sync.Once
	needRestoredData    bool

	indexPartialCondition
}

// NeedRestoredData checks whether the index columns needs restored data.
func NeedRestoredData(idxCols []*model.IndexColumn, colInfos []*model.ColumnInfo) bool {
	for _, idxCol := range idxCols {
		if model.ColumnNeedRestoredData(idxCol, colInfos) {
			return true
		}
	}
	return false
}

// NewIndex builds a new Index object.
func NewIndex(physicalID int64, tblInfo *model.TableInfo, indexInfo *model.IndexInfo) (table.Index, error) {
	index := &index{
		idxInfo:  indexInfo,
		tblInfo:  tblInfo,
		phyTblID: physicalID,
	}

	conditionString := indexInfo.ConditionExprString
	if len(conditionString) > 0 {
		var err error
		index.conditionExpr, err = expression.ParseSimpleExpr(indexConditionECtx, conditionString, expression.WithTableInfo("", tblInfo))
		if err != nil {
			return nil, errors.Trace(err)
		}
		index.conditionEvalBufferPool = sync.Pool{
			New: func() any {
				// For INSERT path, it'll only pass all writable columns.
				// For UPDATE/DELETE path, it'll contain all columns.
				// As the writable columns are always at the beginning of the `tblInfo.Columns`, it'll not affect
				// the offsets of related columns in the expression. Therefore, it's fine to always record all
				// columns here.
				evalBufferTypes := make([]*types.FieldType, 0, len(tblInfo.Columns)+1)
				for _, col := range tblInfo.Columns {
					evalBufferTypes = append(evalBufferTypes, &col.FieldType)
				}

				if !tblInfo.HasClusteredIndex() {
					// If the table doesn't have clustered index, we need to append an extra handle column.
					evalBufferTypes = append(evalBufferTypes, types.NewFieldType(mysql.TypeLonglong))
				}

				evalBuffer := chunk.MutRowFromTypes(evalBufferTypes)
				return &evalBuffer
			},
		}
	}
	return index, nil
}

// Meta returns index info.
func (c *index) Meta() *model.IndexInfo {
	return c.idxInfo
}

// TableMeta returns table info.
func (c *index) TableMeta() *model.TableInfo {
	return c.tblInfo
}

func (c *index) castIndexValuesToChangingTypes(indexedValues []types.Datum) error {
	var err error
	for i, idxCol := range c.idxInfo.Columns {
		tblCol := c.tblInfo.Columns[idxCol.Offset]
		if !idxCol.UseChangingType || tblCol.ChangingFieldType == nil {
			continue
		}
		indexedValues[i], err = table.CastColumnValueWithStrictMode(indexedValues[i], tblCol.ChangingFieldType)
		if err != nil {
			return err
		}
	}
	return nil
}

// GenIndexKey generates storage key for index values. Returned distinct indicates whether the
// indexed values should be distinct in storage (i.e. whether handle is encoded in the key).
func (c *index) GenIndexKey(ec errctx.Context, loc *time.Location, indexedValues []types.Datum, h kv.Handle, buf []byte) (key []byte, distinct bool, err error) {
	idxTblID := c.phyTblID
	fullHandle := h
	if c.idxInfo.Global {
		idxTblID = c.tblInfo.ID
		pi := c.tblInfo.GetPartitionInfo()
		if pi != nil && pi.NewTableID != 0 {
			isNew, ok := pi.DDLChangedIndex[c.idxInfo.ID]
			if ok && isNew {
				idxTblID = pi.NewTableID
			}
		}

		if _, ok := fullHandle.(kv.PartitionHandle); !ok &&
			c.idxInfo.GlobalIndexVersion >= model.GlobalIndexVersionV1 {
			fullHandle = kv.NewPartitionHandle(c.phyTblID, h)
		}
	}

	if err = c.castIndexValuesToChangingTypes(indexedValues); err != nil {
		return
	}

	key, distinct, err = tablecodec.GenIndexKey(loc, c.tblInfo, c.idxInfo, idxTblID, indexedValues, fullHandle, buf)
	err = ec.HandleError(err)
	return
}

// GenIndexValue generates the index value.
func (c *index) GenIndexValue(ec errctx.Context, loc *time.Location, distinct, untouched bool, indexedValues []types.Datum,
	h kv.Handle, restoredData []types.Datum, buf []byte) ([]byte, error) {
	c.initNeedRestoreData.Do(func() {
		c.needRestoredData = NeedRestoredData(c.idxInfo.Columns, c.tblInfo.Columns)
	})

	if err := c.castIndexValuesToChangingTypes(indexedValues); err != nil {
		return nil, errors.Trace(err)
	}

	idx, err := tablecodec.GenIndexValuePortal(loc, c.tblInfo, c.idxInfo, c.needRestoredData, distinct, untouched, indexedValues, h, c.phyTblID, restoredData, buf)
	err = ec.HandleError(err)
	return idx, err
}

// getIndexedValue will produce the result like:
// 1. If not multi-valued index, return directly.
// 2. (i1, [m1,m2], i2, ...) ==> [(i1, m1, i2, ...), (i1, m2, i2, ...)]
// 3. (i1, null, i2, ...) ==> [(i1, null, i2, ...)]
// 4. (i1, [], i2, ...) ==> nothing.
func (c *index) getIndexedValue(indexedValues []types.Datum) [][]types.Datum {
	if !c.idxInfo.MVIndex {
		return [][]types.Datum{indexedValues}
	}

	vals := make([][]types.Datum, 0, 16)
	jsonIdx := 0
	jsonIsNull := false
	existsVals := make(map[string]struct{})
	var buf []byte
	for !jsonIsNull {
		val := make([]types.Datum, 0, len(indexedValues))
		for i, v := range indexedValues {
			if !c.tblInfo.Columns[c.idxInfo.Columns[i].Offset].FieldType.IsArray() {
				val = append(val, v)
			} else {
				// if the datum type is not JSON, it must come from cleanup index.
				if v.IsNull() || v.Kind() != types.KindMysqlJSON {
					val = append(val, v)
					jsonIsNull = true
					continue
				}
				elemCount := v.GetMysqlJSON().GetElemCount()
				for {
					// JSON cannot be indexed, if the value is JSON type, it must be multi-valued index.
					if jsonIdx >= elemCount {
						goto out
					}
					binaryJSON := v.GetMysqlJSON().ArrayGetElem(jsonIdx)
					jsonIdx++
					buf = buf[:0]
					key := string(binaryJSON.HashValue(buf))
					if _, exists := existsVals[key]; exists {
						continue
					}
					existsVals[key] = struct{}{}
					val = append(val, types.NewDatum(binaryJSON.GetValue()))
					break
				}
			}
		}
		vals = append(vals, val)
	}
out:
	return vals
}

// MeetPartialCondition checks whether the row meets the partial index condition of the index.
func (c *index) MeetPartialCondition(row []types.Datum) (meet bool, err error) {
	if c.conditionExpr == nil {
		return true, nil
	}

	evalBuffer := c.conditionEvalBufferPool.Get().(*chunk.MutRow)
	defer c.conditionEvalBufferPool.Put(evalBuffer)
	evalBuffer.SetDatums(row...)

	return c.MeetPartialConditionWithChunk(evalBuffer.ToRow())
}

func (c *index) MeetPartialConditionWithChunk(row chunk.Row) (meet bool, err error) {
	defer func() {
		r := recover()
		if r != nil {
			err = errors.Errorf("panic in MeetPartialConditionWithChunk: %v", r)
			intest.Assert(false, "should never panic in MeetPartialConditionWithChunk")
			logutil.BgLogger().Warn("panic in MeetPartialConditionWithChunk", zap.Error(err), zap.Any("recover message", r))
		}
	}()

	datum, isNull, err := c.conditionExpr.EvalInt(indexConditionECtx.GetEvalCtx(), row)
	if err != nil {
		return false, err
	}
	// If the result is NULL, it usually means the original column itself is NULL.
	// In this case, we should refuse to consider the index for partial index condition.
	return datum > 0 && !isNull, nil
}

// Create creates a new entry in the kvIndex data.
// If the index is unique and there is an existing entry with the same key,
// Create will return the existing entry's handle as the first return value, ErrKeyExists as the second return value.
func (c *index) Create(sctx table.MutateContext, txn kv.Transaction, indexedValue []types.Datum, h kv.Handle, handleRestoreData []types.Datum, opts ...table.CreateIdxOption) (kv.Handle, error) {
	opt := table.NewCreateIdxOpt(opts...)
	return c.create(sctx, txn, indexedValue, h, handleRestoreData, false, opt)
}

func (c *index) create(sctx table.MutateContext, txn kv.Transaction, indexedValue []types.Datum, h kv.Handle, handleRestoreData []types.Datum, untouched bool, opt *table.CreateIdxOpt) (kv.Handle, error) {
	if c.Meta().Unique {
		txn.CacheTableInfo(c.phyTblID, c.tblInfo)
	}
	indexedValues := c.getIndexedValue(indexedValue)
	ctx := opt.Ctx()
	if ctx != nil {
		var r tracing.Region
		r, ctx = tracing.StartRegionEx(ctx, "index.Create")
		defer r.End()
	} else {
		ctx = context.TODO()
	}
	writeBufs := sctx.GetMutateBuffers().GetWriteStmtBufs()
	skipCheck := opt.DupKeyCheck() == table.DupKeyCheckSkip
	allowOverwriteOfOldGlobalIndex := false
	if c.idxInfo.Global && c.tblInfo.Partition.DDLState == model.StateDeleteReorganization &&
		// TODO: Also do the same for DROP PARTITION
		c.tblInfo.Partition.DDLAction == model.ActionTruncateTablePartition {
		allowOverwriteOfOldGlobalIndex = true
		if len(c.tblInfo.Partition.DroppingDefinitions) > 0 {
			skipCheck = false
		}
	}
	evalCtx := sctx.GetExprCtx().GetEvalCtx()
	loc, ec := evalCtx.Location(), evalCtx.ErrCtx()
	for _, value := range indexedValues {
		key, distinct, err := c.GenIndexKey(ec, loc, value, h, writeBufs.IndexKeyBuf)
		if err != nil {
			return nil, err
		}

		var (
			tempKey         []byte
			keyVer          byte
			keyIsTempIdxKey bool
			hasTempKey      bool
		)
		if !opt.FromBackFill() {
			key, tempKey, keyVer = GenTempIdxKeyByState(c.idxInfo, key)
			if keyVer == tablecodec.TempIndexKeyTypeBackfill || keyVer == tablecodec.TempIndexKeyTypeDelete {
				key, tempKey = tempKey, nil
				keyIsTempIdxKey = true
			}
			hasTempKey = keyIsTempIdxKey || len(tempKey) > 0
		}

		if txn.IsPipelined() {
			// For pipelined DML, disable the untouched optimization to avoid extra RPCs for MemBuffer.Get().
			// TODO: optimize this.
			untouched = false
		}

		if untouched {
			// If the index kv was untouched(unchanged), and the key/value already exists in mem-buffer,
			// should not overwrite the key with un-commit flag.
			// So if the key exists, just do nothing and return.
			v, err := kv.GetValue(ctx, txn.GetMemBuffer(), key)
			if err == nil {
				if len(v) != 0 {
					continue
				}
				// The key is marked as deleted in the memory buffer, as the existence check is done lazily
				// for optimistic transactions by default. The "untouched" key could still exist in the store,
				// it's needed to commit this key to do the existence check so unset the untouched flag.
				if !txn.IsPessimistic() {
					keyFlags, err := txn.GetMemBuffer().GetFlags(key)
					if err != nil {
						return nil, err
					}
					if keyFlags.HasPresumeKeyNotExists() {
						untouched = false
					}
				}
			}
		}

		// save the key buffer to reuse.
		writeBufs.IndexKeyBuf = key
		idxVal, err := c.GenIndexValue(ec, loc, distinct, untouched, value, h, handleRestoreData, nil)
		if err != nil {
			return nil, err
		}

		ignoreAssertion := opt.IgnoreAssertion() || c.idxInfo.State != model.StatePublic

		if !distinct || skipCheck || untouched {
			val := idxVal
			if untouched && hasTempKey {
				// Untouched key-values never occur in the storage and the temp index is not public.
				// It is unnecessary to write the untouched temp index key-values.
				continue
			}
			if keyIsTempIdxKey {
				tempVal := tablecodec.TempIndexValueElem{Value: idxVal, KeyVer: keyVer, Distinct: distinct}
				val = tempVal.Encode(nil)
				// during some step of add-index, such as in write-reorg state, this
				// key is THE temp index key.
				err = txn.GetMemBuffer().Set(key, val)
			} else if c.mayDDLMergingTempIndex() {
				// Here may have the situation:
				// DML: Writing the normal index key.
				// DDL: Writing the same normal index key, but it does not lock primary record.
				err = txn.GetMemBuffer().SetWithFlags(key, val, kv.SetNeedLocked)
			} else {
				err = txn.GetMemBuffer().Set(key, val)
			}
			if err != nil {
				return nil, err
			}
			if keyIsTempIdxKey {
				metrics.DDLAddOneTempIndexWrite(sctx.ConnectionID(), c.tblInfo.ID, false)
			}
			if len(tempKey) > 0 {
				tempVal := tablecodec.TempIndexValueElem{Value: idxVal, KeyVer: keyVer, Distinct: distinct}
				val = tempVal.Encode(nil)
				err = txn.GetMemBuffer().Set(tempKey, val)
				if err != nil {
					return nil, err
				}
				metrics.DDLAddOneTempIndexWrite(sctx.ConnectionID(), c.tblInfo.ID, true)
			}
			if !ignoreAssertion && !untouched {
				if opt.DupKeyCheck() == table.DupKeyCheckLazy && !txn.IsPessimistic() {
					err = setAssertion(txn, key, kv.AssertUnknown)
				} else {
					err = setAssertion(txn, key, kv.AssertNotExist)
				}
			}
			if err != nil {
				return nil, err
			}
			continue
		}

		var value []byte
		var tempIdxVal tablecodec.TempIndexValue
		if allowOverwriteOfOldGlobalIndex {
			// In DeleteReorganization, overwrite Global Index keys pointing to
			// old dropped/truncated partitions.
			// Note that a partitioned table cannot be temporary table

			// Check mem buffer first. Note that it may be a tombstone
			// resulting in err == nil and len(value) == 0.
			// err == nil will also turn off lazyCheck later,
			// and skip kv.SetPresumeKeyNotExists flag, to allow
			// taking locks on already existing row.
			value, err = txn.GetMemBuffer().GetLocal(ctx, key)
			if kv.IsErrNotFound(err) {
				// Not in mem buffer, must do non-lazy read, since we must check
				// if exists now, to be able to overwrite.
				value, err = kv.GetValue(ctx, txn.GetSnapshot(), key)
				if err == nil && len(value) == 0 {
					err = kv.ErrNotExist
				}
			}
			if err == nil && len(value) != 0 {
				handle, errPart := tablecodec.DecodeHandleInIndexValue(value)
				if errPart != nil {
					return nil, errPart
				}
				if partHandle, ok := handle.(kv.PartitionHandle); ok {
					for _, id := range c.tblInfo.Partition.IDsInDDLToIgnore() {
						if id == partHandle.PartitionID {
							// Simply overwrite it
							err = setAssertion(txn, key, kv.AssertUnknown)
							if err != nil {
								return nil, err
							}
							value = nil
							break
						}
					}
				}
			}
		} else if c.tblInfo.TempTableType != model.TempTableNone {
			// Always check key for temporary table because it does not write to TiKV
			value, err = kv.GetValue(ctx, txn, key)
		} else if hasTempKey {
			// For temp index keys, we can't get the temp value from memory buffer, even if the lazy check is enabled.
			// Otherwise, it may cause the temp index value to be overwritten, leading to data inconsistency.
			var dupHandle kv.Handle
			if keyIsTempIdxKey {
				dupHandle, value, err = FetchDuplicatedHandleForTempIndexKey(ctx, key, txn)
			} else if len(tempKey) > 0 {
				dupHandle, value, err = FetchDuplicatedHandleForTempIndexKey(ctx, tempKey, txn)
			}
			if err != nil {
				return nil, err
			}
			if dupHandle != nil {
				return dupHandle, kv.ErrKeyExists
			}
			if len(value) > 0 {
				tempIdxVal, err = tablecodec.DecodeTempIndexValue(value)
				if err != nil {
					return nil, err
				}
			}
		} else if opt.DupKeyCheck() == table.DupKeyCheckLazy {
			value, err = txn.GetMemBuffer().GetLocal(ctx, key)
		} else {
			value, err = kv.GetValue(ctx, txn, key)
		}
		if err != nil && !kv.IsErrNotFound(err) {
			return nil, err
		}

		// The index key value is not found or deleted.
		if err != nil || len(value) == 0 || (!tempIdxVal.IsEmpty() && tempIdxVal.Current().Delete) {
			val := idxVal
			lazyCheck := opt.DupKeyCheck() == table.DupKeyCheckLazy && err != nil
			if hasTempKey {
				if keyIsTempIdxKey {
					tempVal := tablecodec.TempIndexValueElem{Value: idxVal, KeyVer: keyVer, Distinct: true}
					val = tempVal.Encode(value)
				}
				err = txn.GetMemBuffer().Set(key, val)
				if err != nil {
					return nil, err
				}
				if keyIsTempIdxKey {
					metrics.DDLAddOneTempIndexWrite(sctx.ConnectionID(), c.tblInfo.ID, false)
				}
				if len(tempKey) > 0 {
					tempVal := tablecodec.TempIndexValueElem{Value: idxVal, KeyVer: keyVer, Distinct: true}
					val = tempVal.Encode(value)
					err = txn.GetMemBuffer().Set(tempKey, val)
					if err != nil {
						return nil, err
					}
					metrics.DDLAddOneTempIndexWrite(sctx.ConnectionID(), c.tblInfo.ID, true)
				}
			} else if lazyCheck {
				flags := []kv.FlagsOp{kv.SetPresumeKeyNotExists}
				if opt.PessimisticLazyDupKeyCheck() == table.DupKeyCheckInPrewrite && txn.IsPessimistic() {
					flags = append(flags, kv.SetNeedConstraintCheckInPrewrite)
				}
				err = txn.GetMemBuffer().SetWithFlags(key, val, flags...)
			} else {
				err = txn.GetMemBuffer().Set(key, val)
			}
			if err != nil {
				return nil, err
			}

			if ignoreAssertion {
				continue
			}
			if lazyCheck && !txn.IsPessimistic() {
				err = setAssertion(txn, key, kv.AssertUnknown)
			} else {
				err = setAssertion(txn, key, kv.AssertNotExist)
			}
			if err != nil {
				return nil, err
			}
			continue
		}
		// temp index key should have been handled by FetchDuplicatedHandleForTempIndexKey.
		intest.Assert(!hasTempKey)
		handle, err := tablecodec.DecodeHandleInIndexValue(value)
		if err != nil {
			return nil, err
		}
		return handle, kv.ErrKeyExists
	}
	return nil, nil
}

// Delete removes the entry for handle h and indexedValues from KV index.
