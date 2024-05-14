// Copyright 2021 PingCAP, Inc.
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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"go.uber.org/zap"
)

var (
	// ErrInconsistentRowValue is the error when values in a row insertion does not match the expected ones.
	ErrInconsistentRowValue = dbterror.ClassTable.NewStd(errno.ErrInconsistentRowValue)
	// ErrInconsistentHandle is the error when the handle in the row/index insertions does not match.
	ErrInconsistentHandle = dbterror.ClassTable.NewStd(errno.ErrInconsistentHandle)
	// ErrInconsistentIndexedValue is the error when decoded values from the index mutation cannot match row value
	ErrInconsistentIndexedValue = dbterror.ClassTable.NewStd(errno.ErrInconsistentIndexedValue)
)

type mutation struct {
	key     kv.Key
	flags   kv.KeyFlags
	value   []byte
	indexID int64 // only for index mutations
}

type columnMaps struct {
	ColumnIDToInfo       map[int64]*model.ColumnInfo
	ColumnIDToFieldType  map[int64]*types.FieldType
	IndexIDToInfo        map[int64]*model.IndexInfo
	IndexIDToRowColInfos map[int64][]rowcodec.ColInfo
}

// CheckDataConsistency checks whether the given set of mutations corresponding to a single row is consistent.
// Namely, assume the database is consistent before, applying the mutations shouldn't break the consistency.
// It aims at reducing bugs that will corrupt data, and preventing mistakes from spreading if possible.
//
// 3 conditions are checked:
// (1) row.value is consistent with input data
// (2) the handle is consistent in row and index insertions
// (3) the keys of the indices are consistent with the values of rows
//
// The check doesn't work and just returns nil when:
// (1) the table is partitioned
// (2) new collation is enabled and restored data is needed
//
// The check is performed on almost every write. Its performance matters.
// Let M = the number of mutations, C = the number of columns in the table,
// I = the sum of the number of columns in all indices,
// The time complexity is O(M * C + I)
// The space complexity is O(M + C + I)
func CheckDataConsistency(
	txn kv.Transaction, sessVars *variable.SessionVars, t *TableCommon,
	rowToInsert, rowToRemove []types.Datum, memBuffer kv.MemBuffer, sh kv.StagingHandle,
) error {
	if t.Meta().GetPartitionInfo() != nil {
		return nil
	}
	if txn.IsPipelined() {
		return nil
	}
	if sh == 0 {
		// some implementations of MemBuffer doesn't support staging, e.g. that in pkg/lightning/backend/kv
		return nil
	}
	indexMutations, rowInsertion, err := collectTableMutationsFromBufferStage(t, memBuffer, sh)
	if err != nil {
		return errors.Trace(err)
	}

	columnMaps := getColumnMaps(txn, t)

	// Row insertion consistency check contributes the least to defending data-index consistency, but costs most CPU resources.
	// So we disable it for now.
	//
	// if rowToInsert != nil {
	// 	if err := checkRowInsertionConsistency(
	// 		sessVars, rowToInsert, rowInsertion, columnMaps.ColumnIDToInfo, columnMaps.ColumnIDToFieldType, t.Meta().Name.O,
	// 	); err != nil {
	// 		return errors.Trace(err)
	// 	}
	// }

	if rowInsertion.key != nil {
		if err = checkHandleConsistency(rowInsertion, indexMutations, columnMaps.IndexIDToInfo, t.Meta()); err != nil {
			return errors.Trace(err)
		}
	}

	if err := checkIndexKeys(
		sessVars, t, rowToInsert, rowToRemove, indexMutations, columnMaps.IndexIDToInfo, columnMaps.IndexIDToRowColInfos,
	); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// checkHandleConsistency checks whether the handles, with regard to a single-row change,
// in row insertions and index insertions are consistent.
// A PUT_index implies a PUT_row with the same handle.
// Deletions are not checked since the values of deletions are unknown
func checkHandleConsistency(rowInsertion mutation, indexMutations []mutation, indexIDToInfo map[int64]*model.IndexInfo, tblInfo *model.TableInfo) error {
	var insertionHandle kv.Handle
	var err error

	if rowInsertion.key == nil {
		return nil
	}
	insertionHandle, err = tablecodec.DecodeRowKey(rowInsertion.key)
	if err != nil {
		return errors.Trace(err)
	}

	for _, m := range indexMutations {
		if len(m.value) == 0 {
			continue
		}

		// Generate correct index id for check.
		idxID := m.indexID & tablecodec.IndexIDMask
		indexInfo, ok := indexIDToInfo[idxID]
		if !ok {
			return errors.New("index not found")
		}

		// If this is the temporary index data, need to remove the last byte of index data(version about when it is written).
		var (
			value       []byte
			orgKey      []byte
			indexHandle kv.Handle
		)
		if idxID != m.indexID {
			if tablecodec.TempIndexValueIsUntouched(m.value) {
				// We never commit the untouched key values to the storage. Skip this check.
				continue
			}
			var tempIdxVal tablecodec.TempIndexValue
			tempIdxVal, err = tablecodec.DecodeTempIndexValue(m.value)
			if err != nil {
				return err
			}
			if !tempIdxVal.IsEmpty() {
				value = tempIdxVal.Current().Value
			}
			if len(value) == 0 {
				// Skip the deleted operation values.
				continue
			}
			orgKey = append(orgKey, m.key...)
			tablecodec.TempIndexKey2IndexKey(orgKey)
			indexHandle, err = tablecodec.DecodeIndexHandle(orgKey, value, len(indexInfo.Columns))
		} else {
			indexHandle, err = tablecodec.DecodeIndexHandle(m.key, m.value, len(indexInfo.Columns))
		}
		if err != nil {
			return errors.Trace(err)
		}
		// NOTE: handle type can be different, see issue 29520
		if indexHandle.IsInt() == insertionHandle.IsInt() && indexHandle.Compare(insertionHandle) != 0 {
			err = ErrInconsistentHandle.GenWithStackByArgs(tblInfo.Name, indexInfo.Name.O, indexHandle, insertionHandle, m, rowInsertion)
			logutil.BgLogger().Error("inconsistent handle in index and record insertions", zap.Error(err))
			return err
		}
	}

	return err
}

// checkIndexKeys checks whether the decoded data from keys of index mutations are consistent with the expected ones.
//
// How it works:
//
// Assume the set of row values changes from V1 to V2, we check
// (1) V2 - V1 = {added indices}
// (2) V1 - V2 = {deleted indices}
//
// To check (1), we need
// (a) {added indices} is a subset of {needed indices} => each index mutation is consistent with the input/row key/value
// (b) {needed indices} is a subset of {added indices}. The check process would be exactly the same with how we generate the mutations, thus ignored.
func checkIndexKeys(
	sessVars *variable.SessionVars, t *TableCommon, rowToInsert, rowToRemove []types.Datum,
	indexMutations []mutation, indexIDToInfo map[int64]*model.IndexInfo,
	indexIDToRowColInfos map[int64][]rowcodec.ColInfo,
) error {
	var indexData []types.Datum
	for _, m := range indexMutations {
		var value []byte
		// Generate correct index id for check.
		idxID := m.indexID & tablecodec.IndexIDMask
		indexInfo, ok := indexIDToInfo[idxID]
		if !ok {
			return errors.New("index not found")
		}
		rowColInfos, ok := indexIDToRowColInfos[idxID]
		if !ok {
			return errors.New("index not found")
		}

		var isTmpIdxValAndDeleted bool
		// If this is temp index data, need remove last byte of index data.
		if idxID != m.indexID {
			if tablecodec.TempIndexValueIsUntouched(m.value) {
				// We never commit the untouched key values to the storage. Skip this check.
				continue
			}
			tmpVal, err := tablecodec.DecodeTempIndexValue(m.value)
			if err != nil {
				return err
			}
			curElem := tmpVal.Current()
			isTmpIdxValAndDeleted = curElem.Delete
			value = append(value, curElem.Value...)
		} else {
			value = append(value, m.value...)
		}

		// when we cannot decode the key to get the original value
		if len(value) == 0 && NeedRestoredData(indexInfo.Columns, t.Meta().Columns) {
			continue
		}

		decodedIndexValues, err := tablecodec.DecodeIndexKV(
			m.key, value, len(indexInfo.Columns), tablecodec.HandleNotNeeded, rowColInfos,
		)
		if err != nil {
			return errors.Trace(err)
		}

		// reuse the underlying memory, save an allocation
		if indexData == nil {
			indexData = make([]types.Datum, 0, len(decodedIndexValues))
		} else {
			indexData = indexData[:0]
		}

		for i, v := range decodedIndexValues {
			fieldType := t.Columns[indexInfo.Columns[i].Offset].FieldType.ArrayType()
			datum, err := tablecodec.DecodeColumnValue(v, fieldType, sessVars.Location())
			if err != nil {
				return errors.Trace(err)
			}
			indexData = append(indexData, datum)
		}

		// When it is in add index new backfill state.
		if len(value) == 0 || isTmpIdxValAndDeleted {
			err = compareIndexData(sessVars.StmtCtx, t.Columns, indexData, rowToRemove, indexInfo, t.Meta())
		} else {
			err = compareIndexData(sessVars.StmtCtx, t.Columns, indexData, rowToInsert, indexInfo, t.Meta())
		}
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// checkRowInsertionConsistency checks whether the values of row mutations are consistent with the expected ones
// We only check data added since a deletion of a row doesn't care about its value (and we cannot know it)
func checkRowInsertionConsistency(
	sessVars *variable.SessionVars, rowToInsert []types.Datum, rowInsertion mutation,
	columnIDToInfo map[int64]*model.ColumnInfo, columnIDToFieldType map[int64]*types.FieldType, tableName string,
) error {
	if rowToInsert == nil {
		// it's a deletion
		return nil
	}

	decodedData, err := tablecodec.DecodeRowToDatumMap(rowInsertion.value, columnIDToFieldType, sessVars.Location())
	if err != nil {
		return errors.Trace(err)
	}

	// NOTE: we cannot check if the decoded values contain all columns since some columns may be skipped. It can even be empty
	// Instead, we check that decoded index values are consistent with the input row.

	for columnID, decodedDatum := range decodedData {
		inputDatum := rowToInsert[columnIDToInfo[columnID].Offset]
		cmp, err := decodedDatum.Compare(sessVars.StmtCtx.TypeCtx(), &inputDatum, collate.GetCollator(decodedDatum.Collation()))
		if err != nil {
			return errors.Trace(err)
		}
		if cmp != 0 {
			err = ErrInconsistentRowValue.GenWithStackByArgs(tableName, inputDatum.String(), decodedDatum.String())
			logutil.BgLogger().Error("inconsistent row value in row insertion", zap.Error(err))
			return err
		}
	}
	return nil
}

// collectTableMutationsFromBufferStage collects mutations of the current table from the mem buffer stage
// It returns: (1) all index mutations (2) the only row insertion
// If there are no row insertions, the 2nd returned value is nil
// If there are multiple row insertions, an error is returned
func collectTableMutationsFromBufferStage(t *TableCommon, memBuffer kv.MemBuffer, sh kv.StagingHandle) (
	[]mutation, mutation, error,
) {
	indexMutations := make([]mutation, 0)
	var rowInsertion mutation
	var err error
	inspector := func(key kv.Key, flags kv.KeyFlags, data []byte) {
		// only check the current table
		if tablecodec.DecodeTableID(key) == t.physicalTableID {
			m := mutation{key, flags, data, 0}
			if rowcodec.IsRowKey(key) {
				if len(data) > 0 {
					if rowInsertion.key == nil {
						rowInsertion = m
					} else {
						err = errors.Errorf(
							"multiple row mutations added/mutated, one = %+v, another = %+v", rowInsertion, m,
						)
					}
				}
			} else {
				_, m.indexID, _, err = tablecodec.DecodeIndexKey(m.key)
				if err != nil {
					err = errors.Trace(err)
				}
				indexMutations = append(indexMutations, m)
			}
		}
	}
	memBuffer.InspectStage(sh, inspector)
	return indexMutations, rowInsertion, err
}

// compareIndexData compares the decoded index data with the input data.
// Returns error if the index data is not a subset of the input data.
func compareIndexData(
	sc *stmtctx.StatementContext, cols []*table.Column, indexData, input []types.Datum, indexInfo *model.IndexInfo,
	tableInfo *model.TableInfo,
) error {
	for i := range indexData {
		decodedMutationDatum := indexData[i]
		expectedDatum := input[indexInfo.Columns[i].Offset]

		tablecodec.TruncateIndexValue(
			&expectedDatum, indexInfo.Columns[i],
			cols[indexInfo.Columns[i].Offset].ColumnInfo,
		)
		tablecodec.TruncateIndexValue(
			&decodedMutationDatum, indexInfo.Columns[i],
			cols[indexInfo.Columns[i].Offset].ColumnInfo,
		)

		comparison, err := CompareIndexAndVal(sc, expectedDatum, decodedMutationDatum,
			collate.GetCollator(decodedMutationDatum.Collation()),
			cols[indexInfo.Columns[i].Offset].ColumnInfo.FieldType.IsArray() && expectedDatum.Kind() == types.KindMysqlJSON)
		if err != nil {
			return errors.Trace(err)
		}

		if comparison != 0 {
			err = ErrInconsistentIndexedValue.GenWithStackByArgs(
				tableInfo.Name.O, indexInfo.Name.O, cols[indexInfo.Columns[i].Offset].ColumnInfo.Name.O,
				decodedMutationDatum.String(), expectedDatum.String(),
			)
			logutil.BgLogger().Error("inconsistent indexed value in index insertion", zap.Error(err))
			return err
		}
	}
	return nil
}

// CompareIndexAndVal compare index valued and row value.
func CompareIndexAndVal(sctx *stmtctx.StatementContext, rowVal types.Datum, idxVal types.Datum, collator collate.Collator, cmpMVIndex bool) (int, error) {
	var cmpRes int
	var err error
	if cmpMVIndex {
		// If it is multi-valued index, we should check the JSON contains the indexed value.
		bj := rowVal.GetMysqlJSON()
		count := bj.GetElemCount()
		for elemIdx := 0; elemIdx < count; elemIdx++ {
			jsonDatum := types.NewJSONDatum(bj.ArrayGetElem(elemIdx))
			cmpRes, err = jsonDatum.Compare(sctx.TypeCtx(), &idxVal, collate.GetBinaryCollator())
			if err != nil {
				return 0, errors.Trace(err)
			}
			if cmpRes == 0 {
				break
			}
		}
	} else {
		cmpRes, err = idxVal.Compare(sctx.TypeCtx(), &rowVal, collator)
	}
	return cmpRes, err
}

// getColumnMaps tries to get the columnMaps from transaction options. If there isn't one, it builds one and stores it.
// It saves redundant computations of the map.
func getColumnMaps(txn kv.Transaction, t *TableCommon) columnMaps {
	getter := func() (map[int64]columnMaps, bool) {
		m, ok := txn.GetOption(kv.TableToColumnMaps).(map[int64]columnMaps)
		return m, ok
	}
	setter := func(maps map[int64]columnMaps) {
		txn.SetOption(kv.TableToColumnMaps, maps)
	}
	columnMaps := getOrBuildColumnMaps(getter, setter, t)
	return columnMaps
}

// getOrBuildColumnMaps tries to get the columnMaps from some place. If there isn't one, it builds one and stores it.
// It saves redundant computations of the map.
func getOrBuildColumnMaps(
	getter func() (map[int64]columnMaps, bool), setter func(map[int64]columnMaps), t *TableCommon,
) columnMaps {
	tableMaps, ok := getter()
	if !ok || tableMaps == nil {
		tableMaps = make(map[int64]columnMaps)
	}
	maps, ok := tableMaps[t.tableID]
	if !ok {
		maps = columnMaps{
			make(map[int64]*model.ColumnInfo, len(t.Meta().Columns)),
			make(map[int64]*types.FieldType, len(t.Meta().Columns)),
			make(map[int64]*model.IndexInfo, len(t.Indices())),
			make(map[int64][]rowcodec.ColInfo, len(t.Indices())),
		}

		for _, col := range t.Meta().Columns {
			maps.ColumnIDToInfo[col.ID] = col
			maps.ColumnIDToFieldType[col.ID] = &(col.FieldType)
		}
		for _, index := range t.Indices() {
			if index.Meta().Primary && t.meta.IsCommonHandle {
				continue
			}
			maps.IndexIDToInfo[index.Meta().ID] = index.Meta()
			maps.IndexIDToRowColInfos[index.Meta().ID] = BuildRowcodecColInfoForIndexColumns(index.Meta(), t.Meta())
		}

		tableMaps[t.tableID] = maps
		setter(tableMaps)
	}
	return maps
}

// only used in tests
// commands is a comma separated string, each representing a type of corruptions to the mutations
// The injection depends on actual encoding rules.
func corruptMutations(t *TableCommon, txn kv.Transaction, sh kv.StagingHandle, cmds string) error {
	commands := strings.Split(cmds, ",")
	memBuffer := txn.GetMemBuffer()

	indexMutations, _, err := collectTableMutationsFromBufferStage(t, memBuffer, sh)
	if err != nil {
		return errors.Trace(err)
	}

	for _, cmd := range commands {
		switch cmd {
		case "extraIndex":
			// an extra index mutation
			{
				if len(indexMutations) == 0 {
					continue
				}
				indexMutation := indexMutations[0]
				key := make([]byte, len(indexMutation.key))
				copy(key, indexMutation.key)
				key[len(key)-1]++
				if len(indexMutation.value) == 0 {
					if err := memBuffer.Delete(key); err != nil {
						return errors.Trace(err)
					}
				} else {
					if err := memBuffer.Set(key, indexMutation.value); err != nil {
						return errors.Trace(err)
					}
				}
			}
		case "missingIndex":
			// an index mutation is missing
			// "missIndex" should be placed in front of "extraIndex"es,
			// in case it removes the mutation that was just added
			{
				indexMutation := indexMutations[0]
				memBuffer.RemoveFromBuffer(indexMutation.key)
			}
		case "corruptIndexKey":
			// a corrupted index mutation.
			// TODO: distinguish which part is corrupted, value or handle
			{
				indexMutation := indexMutations[0]
				key := indexMutation.key
				memBuffer.RemoveFromBuffer(key)
				key[len(key)-1]++
				if len(indexMutation.value) == 0 {
					if err := memBuffer.Delete(key); err != nil {
						return errors.Trace(err)
					}
				} else {
					if err := memBuffer.Set(key, indexMutation.value); err != nil {
						return errors.Trace(err)
					}
				}
			}
		case "corruptIndexValue":
			// TODO: distinguish which part to corrupt, int handle, common handle, or restored data?
			// It doesn't make much sense to always corrupt the last byte
			{
				if len(indexMutations) == 0 {
					continue
				}
				indexMutation := indexMutations[0]
				value := indexMutation.value
				if len(value) > 0 {
					value[len(value)-1]++
					if err := memBuffer.Set(indexMutation.key, value); err != nil {
						return errors.Trace(err)
					}
				}
			}
		default:
			return fmt.Errorf("unknown command to corrupt mutation: %s", cmd)
		}
	}
	return nil
}

func injectMutationError(t *TableCommon, txn kv.Transaction, sh kv.StagingHandle) error {
	failpoint.Inject("corruptMutations", func(commands failpoint.Value) {
		failpoint.Return(corruptMutations(t, txn, sh, commands.(string)))
	})
	return nil
}
