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

	"github.com/pingcap/tidb/table"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/rowcodec"
	"go.uber.org/zap"
)

type mutation = struct {
	key   kv.Key
	flags kv.KeyFlags
	value []byte
}

type indexHelperInfo = struct {
	indexInfo   *model.IndexInfo
	rowColInfos []rowcodec.ColInfo
}

// CheckIndexConsistency checks whether the given set of mutations corresponding to a single row is consistent.
// Namely, assume the database is consistent before, applying the mutations shouldn't break the consistency.
// It aims at reducing bugs that will corrupt data, and preventing mistakes from spreading if possible.
//
// Assume the set of row values changes from V1 to V2. We check
// (1) V2 - V1 = {added indices}
// (2) V1 - V2 = {deleted indices}
//
// To check (1), we need
// (a) {added indices} is a subset of {needed indices} => each index mutation is consistent with the input/row key/value
// (b) {needed indices} is a subset of {added indices}. The check process would be exactly the same with how we generate
// 		the mutations, thus ignored.
func CheckIndexConsistency(sessVars *variable.SessionVars, t *TableCommon,
	dataAdded, dataRemoved []types.Datum, memBuffer kv.MemBuffer, sh kv.StagingHandle) error {
	sc := sessVars.StmtCtx
	if sh == 0 {
		return nil
	}
	mutations := collectTableMutationsFromBufferStage(t, memBuffer, sh)
	if err := checkRowAdditionConsistency(sessVars, t.Meta().Columns, dataAdded, mutations); err != nil {
		return errors.Trace(err)
	}
	if err := checkIndexKeys(sc, sessVars, t, dataAdded, dataRemoved, mutations); err != nil {
		return errors.Trace(err)
	}
	// TODO: check whether handles match in index and row mutations
	return nil
}

// checkIndexKeys checks whether the decoded data from keys of index mutations are consistent with the expected ones.
func checkIndexKeys(sc *stmtctx.StatementContext, sessVars *variable.SessionVars, t *TableCommon,
	dataAdded []types.Datum, dataRemoved []types.Datum, mutations []mutation) error {
	indexIDMap := make(map[int64]indexHelperInfo)
	for _, index := range t.indices {
		indexIDMap[index.Meta().ID] = indexHelperInfo{
			index.Meta(),
			BuildRowcodecColInfoForIndexColumns(index.Meta(), t.Meta()),
		}
	}

	for _, m := range mutations {
		if !tablecodec.IsIndexKey(m.key) {
			continue
		}

		_, indexID, _, err := tablecodec.DecodeIndexKey(m.key)
		if err != nil {
			continue
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
		indexData := make([]types.Datum, 0)
		for i, v := range decodedIndexValues {
			fieldType := &t.Columns[indexHelperInfo.indexInfo.Columns[i].Offset].FieldType
			datum, err := tablecodec.DecodeColumnValue(v, fieldType, sessVars.Location())
			if err != nil {
				return errors.Trace(err)
			}
			indexData = append(indexData, datum)
		}

		if len(m.value) == 0 {
			err = compareIndexData(sc, t.Columns, indexData, dataRemoved, indexHelperInfo.indexInfo)
		} else {
			err = compareIndexData(sc, t.Columns, indexData, dataAdded, indexHelperInfo.indexInfo)
		}
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// checkRowAdditionConsistency checks whether the values of row mutations are consistent with the expected ones
// We only check data added since a deletion of a row doesn't care about its value (and we cannot know it)
func checkRowAdditionConsistency(sessVars *variable.SessionVars, tableColumns []*model.ColumnInfo, dataAdded []types.Datum, mutations []mutation) error {
	rowsAdded, _ := extractRowMutations(mutations)
	if dataAdded == nil {
		// should be unreachable
		return nil
	}
	if len(rowsAdded) == 0 {
		return errors.New("record mutations not found for a put of a row")
	}
	if len(rowsAdded) > 1 {
		// impossible to have multiple insertions of records when modifying a single row of in a table
		logutil.BgLogger().Error("multiple row mutations added/mutated", zap.Any("rowsAdded", rowsAdded))
		return errors.New("multiple row mutations added/mutated")
	}

	columnMap := make(map[int64]*model.ColumnInfo)
	for _, col := range tableColumns {
		columnMap[col.ID] = col
	}

	if err := checkRowMutationWithData(sessVars, rowsAdded[0].value, dataAdded, columnMap); err != nil {
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
			return errors.New(fmt.Sprintf("inconsistent row mutation, row datum = {%v}, input datum = {%v}",
				decodedDatum.String(), inputDatum.String()))
		}
	}

	return nil
}

func collectTableMutationsFromBufferStage(t *TableCommon, memBuffer kv.MemBuffer, sh kv.StagingHandle) []mutation {
	mutations := make([]mutation, 0)
	inspector := func(key kv.Key, flags kv.KeyFlags, data []byte) {
		// only check the current table
		if tablecodec.DecodeTableID(key) == t.physicalTableID {
			mutations = append(mutations, mutation{key, flags, data})
		}
	}
	memBuffer.InspectStage(sh, inspector)
	return mutations
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

// extractRowMutations extracts row mutations and classify them into 2 categories: put and delete
func extractRowMutations(mutations []mutation) ([]mutation, []mutation) {
	insertions := make([]mutation, 0)
	deletions := make([]mutation, 0)
	for _, m := range mutations {
		if rowcodec.IsRowKey(m.key) {
			if len(m.value) > 0 {
				insertions = append(insertions, m)
			} else {
				deletions = append(deletions, m)
			}
		}
	}
	return insertions, deletions
}
