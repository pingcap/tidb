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
	"slices"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
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
// The time complexity is O(M * C + I), with one exception: a multi-valued index on an array of N
// elements produces N mutations for a single row. Checking them costs O(N * log N) on the common path,
// because the array elements are sorted and cached once per row and column, and every mutation is then
// checked with a binary search. A binary-search miss falls back to the per-element scan, so the worst
// case for such a row remains O(N^2).
// The space complexity is O(M + C + I), plus O(N) for the cached elements of a multi-valued index.
func CheckDataConsistency(
	txn kv.Transaction, tc types.Context, t *TableCommon,
	rowToInsert, rowToRemove []types.Datum, memBuffer kv.MemBuffer, sh kv.StagingHandle,
) error {
	return checkDataConsistency(txn, tc, t, rowToInsert, rowToRemove, memBuffer, sh, nil)
}

func checkDataConsistency(
	txn kv.Transaction, tc types.Context, t *TableCommon,
	rowToInsert, rowToRemove []types.Datum, memBuffer kv.MemBuffer, sh kv.StagingHandle,
	extraIndexesLayout table.IndexesLayout,
) error {
	if t.Meta().GetPartitionInfo() != nil {
		// TODO: Support check for partitions as well
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
		tc, t, rowToInsert, rowToRemove, indexMutations, columnMaps.IndexIDToInfo, columnMaps.IndexIDToRowColInfos, extraIndexesLayout,
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
	tc types.Context, t *TableCommon, rowToInsert, rowToRemove []types.Datum,
	indexMutations []mutation, indexIDToInfo map[int64]*model.IndexInfo,
	indexIDToRowColInfos map[int64][]rowcodec.ColInfo,
	extraIndexesLayout table.IndexesLayout,
) error {
	useNewCollate := t.encoder.UseNewCollate()
	var indexData []types.Datum
	// The caches are only used by multi-valued indexes, which are uncommon, so they are created lazily
	// in the loop below. They are per-row and are keyed by the offset of the column in that row.
	var insertMVElems, removeMVElems mvIndexSortedElems
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
		if len(value) == 0 && NeedRestoredData(useNewCollate, indexInfo.Columns, t.Meta().Columns) {
			continue
		}

		decodedIndexValues, err := tablecodec.DecodeIndexKVWithCollate(
			useNewCollate,
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

		loc := tc.Location()
		for i, v := range decodedIndexValues {
			fieldType := t.Columns[indexInfo.Columns[i].Offset].FieldType.ArrayType()
			datum, err := tablecodec.DecodeColumnValue(v, fieldType, loc)
			if err != nil {
				return errors.Trace(err)
			}
			indexData = append(indexData, datum)
		}

		// When it is in add index new backfill state.
		isRemove := len(value) == 0 || isTmpIdxValAndDeleted
		input, mvElems := rowToInsert, &insertMVElems
		if isRemove {
			input, mvElems = rowToRemove, &removeMVElems
		}
		err = compareIndexDataWithMVCache(
			useNewCollate, tc, t.Columns, indexData, input, indexInfo, t.Meta(),
			extraIndexesLayout.GetIndexLayout(idxID), mvElems,
		)
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

// compareIndexDataWithMVCache compares the decoded index data with the input data.
// Returns error if the index data is not a subset of the input data.
// mvElems, when non-nil, carries the per-column caches of sorted array elements for a multi-valued
// index. The cache is per-row and keyed by the offset of the column in that row, so it must never be
// reused across rows. The map itself is allocated lazily, only when the check actually populates it.
func compareIndexDataWithMVCache(
	useNewCollate bool,
	tc types.Context, cols []*table.Column, indexData, input []types.Datum, indexInfo *model.IndexInfo,
	tableInfo *model.TableInfo,
	extraIndexLayout table.IndexRowLayoutOption,
	mvElems *mvIndexSortedElems,
) error {
	for i := range indexData {
		offsetInRow := indexInfo.Columns[i].Offset
		if len(extraIndexLayout) > 0 {
			offsetInRow = extraIndexLayout[i]
		}
		offsetInTable := indexInfo.Columns[i].Offset
		decodedMutationDatum := indexData[i]
		expectedDatum := input[offsetInRow]

		tablecodec.TruncateIndexValue(
			&expectedDatum, indexInfo.Columns[i],
			cols[offsetInTable].ColumnInfo,
		)
		tablecodec.TruncateIndexValue(
			&decodedMutationDatum, indexInfo.Columns[i],
			cols[offsetInTable].ColumnInfo,
		)

		var comparison int
		var err error
		// Only real multi-valued index columns need the containment semantics. The condition keeps
		// the previous behavior, and the cache is used on top of it.
		cmpMVIndex := cols[offsetInTable].ColumnInfo.FieldType.IsArray() &&
			expectedDatum.Kind() == types.KindMysqlJSON
		if cmpMVIndex && indexInfo.MVIndex {
			comparison, err = compareMVIndexVal(
				tc, &expectedDatum, &decodedMutationDatum, offsetInRow, mvElems)
		} else {
			comparison, err = CompareIndexAndVal(
				tc, expectedDatum, decodedMutationDatum,
				collate.GetCollatorWithCollate(useNewCollate, decodedMutationDatum.Collation()),
				cmpMVIndex)
		}
		if err != nil {
			return errors.Trace(err)
		}

		if comparison != 0 {
			err = ErrInconsistentIndexedValue.GenWithStackByArgs(
				tableInfo.Name.O, indexInfo.Name.O, cols[offsetInTable].ColumnInfo.Name.O,
				decodedMutationDatum.String(), expectedDatum.String(),
			)
			logutil.BgLogger().Error("inconsistent indexed value in index insertion", zap.Error(err))
			return err
		}
	}
	return nil
}

// mvIndexSortedElems caches the sorted array elements of a row, keyed by the offset of the column
// in that row. It is only used for multi-valued indexes.
type mvIndexSortedElems map[int][]types.BinaryJSON

// compareMVIndexVal checks that the indexed value of one element of a multi-valued index exists in
// the row's array. When mvElems is nil it falls back to the per-element scan. Otherwise the sorted
// elements of the row are cached on first use, and every later mutation of the same column is checked
// with a binary search. Both paths use the same comparison, so a binary-search hit can never be a
// false positive; a miss is re-checked by the fallback to stay conservative.
func compareMVIndexVal(
	tc types.Context, rowVal, idxVal *types.Datum, offsetInRow int, mvElems *mvIndexSortedElems,
) (int, error) {
	if mvElems == nil {
		return CompareIndexAndVal(tc, *rowVal, *idxVal, collate.GetBinaryCollator(), true)
	}
	bj := rowVal.GetMysqlJSON()
	count := bj.GetElemCount()
	// An empty array contains nothing, and the per-element scan returns 0 without iterating.
	if count == 0 {
		return 0, nil
	}
	// A NULL index value never matches an element, for the reasons explained in CompareIndexAndVal.
	if idxVal.IsNull() {
		return -1, nil
	}
	idxJSON, err := idxVal.ToMysqlJSON()
	if err != nil {
		return 0, errors.Trace(err)
	}
	sorted, ok := (*mvElems)[offsetInRow]
	if !ok {
		sorted = sortedJSONArrayElems(bj)
		// Allocate the cache lazily, so that a multi-valued index whose column is not reached
		// (for example a NULL row value) does not pay for the allocation.
		if *mvElems == nil {
			*mvElems = make(mvIndexSortedElems)
		}
		(*mvElems)[offsetInRow] = sorted
	}
	if _, found := slices.BinarySearchFunc(sorted, idxJSON, types.CompareBinaryJSON); found {
		return 0, nil
	}
	// A miss usually means the value is really absent, but re-check with the per-element scan before
	// reporting an inconsistency: CompareBinaryJSON is not a strict weak ordering (its mixed float
	// and integer comparisons use an absolute epsilon, so equality is not transitive), which puts
	// the sortedness of the slice and the binary-search result on unspecified footing. The scan
	// compares every element pairwise and is the authoritative verdict.
	return CompareIndexAndVal(tc, *rowVal, *idxVal, collate.GetBinaryCollator(), true)
}

// sortedJSONArrayElems copies the elements of a JSON array and sorts them by CompareBinaryJSON.
func sortedJSONArrayElems(bj types.BinaryJSON) []types.BinaryJSON {
	count := bj.GetElemCount()
	elems := make([]types.BinaryJSON, 0, count)
	for elemIdx := range count {
		elems = append(elems, bj.ArrayGetElem(elemIdx))
	}
	slices.SortFunc(elems, types.CompareBinaryJSON)
	return elems
}

// CompareIndexAndVal compare index valued and row value.
func CompareIndexAndVal(tc types.Context, rowVal types.Datum, idxVal types.Datum, collator collate.Collator, cmpMVIndex bool) (int, error) {
	var cmpRes int
	var err error
	if cmpMVIndex {
		// If it is multi-valued index, we should check the JSON contains the indexed value.
		bj := rowVal.GetMysqlJSON()
		count := bj.GetElemCount()
		if count == 0 {
			// The loop below would not run, so the result stays 0.
			return 0, nil
		}
		if idxVal.IsNull() {
			// The write path only produces a NULL index value when the column itself is NULL, and a
			// NULL column never takes this path (the callers require a JSON row value). JSON null
			// array elements are impossible too, because CAST(... AS <type> ARRAY) rejects them.
			// So a NULL index value is a mismatch.
			return -1, nil
		}
		// Convert the indexed value to JSON once instead of once per array element.
		var idxJSON types.BinaryJSON
		idxJSON, err = idxVal.ToMysqlJSON()
		if err != nil {
			return 0, errors.Trace(err)
		}
		for elemIdx := range count {
			cmpRes = types.CompareBinaryJSON(bj.ArrayGetElem(elemIdx), idxJSON)
			if cmpRes == 0 {
				break
			}
		}
	} else {
		cmpRes, err = idxVal.Compare(tc, &rowVal, collator)
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
