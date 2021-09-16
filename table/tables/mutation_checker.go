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

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/rowcodec"
	"go.uber.org/zap"
)

type mutation struct {
	key   kv.Key
	flags kv.KeyFlags
	value []byte
}

type indexHelperInfo struct {
	indexInfo   *model.IndexInfo
	rowColInfos []rowcodec.ColInfo
}

// CheckIndexConsistency checks whether the given set of mutations corresponding to a single row is consistent.
// Namely, assume the database is consistent before, applying the mutations shouldn't break the consistency.
// It aims at reducing bugs that will corrupt data, and preventing mistakes from spreading if possible.
//
// The check doesn't work and just returns nil when:
// (1) the table is partitioned
// (2) new collation is enabled and restored data is needed
//
// How it works:
//
// Assume the set of row values changes from V1 to V2, we check
// (1) V2 - V1 = {added indices}
// (2) V1 - V2 = {deleted indices}
//
// To check (1), we need
// (a) {added indices} is a subset of {needed indices} => each index mutation is consistent with the input/row key/value
// (b) {needed indices} is a subset of {added indices}. The check process would be exactly the same with how we generate
// 		the mutations, thus ignored.
func CheckIndexConsistency(sessVars *variable.SessionVars, t *TableCommon,
	rowToInsert, rowToRemove []types.Datum, memBuffer kv.MemBuffer, sh kv.StagingHandle) error {
	if t.Meta().GetPartitionInfo() != nil {
		return nil
	}
	sc := sessVars.StmtCtx
	if sh == 0 {
		// some implementations of MemBuffer doesn't support staging, e.g. that in br/pkg/lightning/backend/kv
		return nil
	}
	indexMutations, rowInsertion, err := collectTableMutationsFromBufferStage(t, memBuffer, sh)
	if err != nil {
		return errors.Trace(err)
	}

	if rowToInsert != nil {
		if err := checkRowInsertionConsistency(sessVars, getOrBuildColumnMap(sessVars.StmtCtx, t), rowToInsert, rowInsertion); err != nil {
			return errors.Trace(err)
		}
	}
	if err := checkIndexKeys(sc, sessVars, t, rowToInsert, rowToRemove, indexMutations); err != nil {
		return errors.Trace(err)
	}
	// TODO: check whether handles match in index and row mutations
	return nil
}

// getOrBuildColumnMap tries to get the column map from stmt ctx. If there isn't one, it builds one and stores it.
// It saves redundant computations of the map.
func getOrBuildColumnMap(sc *stmtctx.StatementContext, t *TableCommon) map[int64]*model.ColumnInfo {
	if sc.ColumnMap == nil {
		sc.ColumnMap = make(map[int64]map[int64]*model.ColumnInfo)
	}
	columnMap, ok := sc.ColumnMap[t.tableID]
	if !ok {
		columnMap = make(map[int64]*model.ColumnInfo)
		for _, col := range t.Meta().Columns {
			columnMap[col.ID] = col
		}
		sc.ColumnMap[t.tableID] = columnMap
	}
	return columnMap
}

// checkIndexKeys checks whether the decoded data from keys of index mutations are consistent with the expected ones.
func checkIndexKeys(sc *stmtctx.StatementContext, sessVars *variable.SessionVars, t *TableCommon,
	rowToInsert, rowToRemove []types.Datum, indexMutations []mutation) error {
	indexIDMap := make(map[int64]indexHelperInfo)
	for _, index := range t.indices {
		if index.Meta().Primary && t.meta.IsCommonHandle {
			continue
		}
		indexIDMap[index.Meta().ID] = indexHelperInfo{
			index.Meta(),
			BuildRowcodecColInfoForIndexColumns(index.Meta(), t.Meta()),
		}
	}

	var indexData []types.Datum
	for _, m := range indexMutations {
		_, indexID, _, err := tablecodec.DecodeIndexKey(m.key)
		if err != nil {
			//FIXME: continue or return error?
			return errors.Trace(err)
		}

		indexHelperInfo, ok := indexIDMap[indexID]
		if !ok {
			return errors.New("index not found")
		}

		// when we cannot decode the key to get the original value
		if len(m.value) == 0 && NeedRestoredData(indexHelperInfo.indexInfo.Columns, t.Meta().Columns) {
			continue
		}

		decodedIndexValues, err := tablecodec.DecodeIndexKV(m.key, m.value, len(indexHelperInfo.indexInfo.Columns),
			tablecodec.HandleNotNeeded, indexHelperInfo.rowColInfos)
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
			fieldType := &t.Columns[indexHelperInfo.indexInfo.Columns[i].Offset].FieldType
			datum, err := tablecodec.DecodeColumnValue(v, fieldType, sessVars.Location())
			if err != nil {
				return errors.Trace(err)
			}
			indexData = append(indexData, datum)
		}

		if len(m.value) == 0 {
			err = compareIndexData(sc, t.Columns, indexData, rowToRemove, indexHelperInfo.indexInfo)
		} else {
			err = compareIndexData(sc, t.Columns, indexData, rowToInsert, indexHelperInfo.indexInfo)
		}
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// checkRowInsertionConsistency checks whether the values of row mutations are consistent with the expected ones
// We only check data added since a deletion of a row doesn't care about its value (and we cannot know it)
func checkRowInsertionConsistency(sessVars *variable.SessionVars, columnMap map[int64]*model.ColumnInfo, rowToInsert []types.Datum, rowInsertion mutation) error {
	if rowToInsert == nil {
		// it's a deletion
		return nil
	}

	if err := checkRowMutationWithData(sessVars, rowInsertion.value, rowToInsert, columnMap); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// checkRowMutationWithData checks if the given row mutation is consistent with the expected one
// precondition: expectedData contains all fields in order
func checkRowMutationWithData(sessVars *variable.SessionVars, mutationValue []byte, expectedData []types.Datum, columnMap map[int64]*model.ColumnInfo) error {
	columnFieldMap := make(map[int64]*types.FieldType)
	for id, col := range columnMap {
		columnFieldMap[id] = &col.FieldType
	}
	decodedData, err := tablecodec.DecodeRowToDatumMap(mutationValue, columnFieldMap, sessVars.Location())
	if err != nil {
		return errors.Trace(err)
	}

	// NOTE: we cannot check if the decoded values contain all columns since some columns may be skipped. It can even be empty
	// Instead, we check that decoded index values are consistent with the input row.

	for columnID, decodedDatum := range decodedData {
		inputDatum := expectedData[columnMap[columnID].Offset]
		cmp, err := decodedDatum.CompareDatum(sessVars.StmtCtx, &inputDatum)
		if err != nil {
			return errors.Trace(err)
		}
		if cmp != 0 {
			logutil.BgLogger().Error("inconsistent row mutation", zap.String("decoded datum", decodedDatum.String()),
				zap.String("input datum", inputDatum.String()))
			return errors.Errorf("inconsistent row mutation, row datum = {%v}, input datum = {%v}", decodedDatum.String(), inputDatum.String())
		}
	}

	return nil
}

// collectTableMutationsFromBufferStage collects mutations of the current table from the mem buffer stage
// It returns: (1) all index mutations (2) the only row insertion
// If there are no row insertions, the 2nd returned value is nil
// If there are multiple row insertions, an error is returned
func collectTableMutationsFromBufferStage(t *TableCommon, memBuffer kv.MemBuffer, sh kv.StagingHandle) ([]mutation, mutation, error) {
	indexMutations := make([]mutation, 0)
	var rowInsertion mutation
	var err error
	inspector := func(key kv.Key, flags kv.KeyFlags, data []byte) {
		// only check the current table
		if tablecodec.DecodeTableID(key) == t.physicalTableID {
			m := mutation{key, flags, data}
			if rowcodec.IsRowKey(key) {
				if len(data) > 0 {
					if rowInsertion.key == nil {
						rowInsertion = m
					} else {
						err = errors.Errorf("multiple row mutations added/mutated, one = %+v, another = %+v", rowInsertion, m)
					}
				}
			} else {
				indexMutations = append(indexMutations, m)
			}
		}
	}
	memBuffer.InspectStage(sh, inspector)
	return indexMutations, rowInsertion, err
}

// compareIndexData compares the decoded index data with the input data.
// Returns error if the index data is not a subset of the input data.
func compareIndexData(sc *stmtctx.StatementContext, cols []*table.Column, indexData, input []types.Datum, indexInfo *model.IndexInfo) error {
	for i, decodedMutationDatum := range indexData {
		expectedDatum := input[indexInfo.Columns[i].Offset]

		tablecodec.TruncateIndexValue(&expectedDatum, indexInfo.Columns[i],
			cols[indexInfo.Columns[i].Offset].ColumnInfo)
		tablecodec.TruncateIndexValue(&decodedMutationDatum, indexInfo.Columns[i],
			cols[indexInfo.Columns[i].Offset].ColumnInfo)

		comparison, err := decodedMutationDatum.CompareDatum(sc, &expectedDatum)
		if err != nil {
			return errors.Trace(err)
		}

		if comparison != 0 {
			logutil.BgLogger().Error("inconsistent index values",
				zap.String("truncated mutation datum", fmt.Sprintf("%v", decodedMutationDatum)),
				zap.String("truncated expected datum", fmt.Sprintf("%v", expectedDatum)))
			return errors.New("inconsistent index values")
		}
	}
	return nil
}
