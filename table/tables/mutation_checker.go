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
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/rowcodec"
	"go.uber.org/zap"
)

type Mutation = struct {
	key   kv.Key
	flags kv.KeyFlags
	value []byte
}

type IndexHelperInfo = struct {
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
func CheckIndexConsistency(sc *stmtctx.StatementContext, sessVars *variable.SessionVars, t *TableCommon,
	dataAdded, dataRemoved []types.Datum, memBuffer kv.MemBuffer, sh kv.StagingHandle) error {
	mutations := collectTableMutationsFromBufferStage(t, memBuffer, sh)
	if err := checkRowValues(sc, sessVars, t, dataAdded, dataRemoved, mutations); err != nil {
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
	dataAdded []types.Datum, dataRemoved []types.Datum, mutations []Mutation) error {
	indexIdMap := make(map[int64]IndexHelperInfo)
	for _, index := range t.indices {
		indexIdMap[index.Meta().ID] = IndexHelperInfo{
			index.Meta(),
			BuildRowcodecColInfoForIndexColumns(index.Meta(), t.Meta()),
		}
	}

	for _, m := range mutations {
		if !tablecodec.IsIndexKey(m.key) {
			continue
		}

		_, indexId, _, err := tablecodec.DecodeIndexKey(m.key)
		if err != nil {
			continue
		}

		indexHelperInfo, ok := indexIdMap[indexId]
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
			err = compareIndexData(sc, t, indexData, dataRemoved, indexHelperInfo)
		} else {
			err = compareIndexData(sc, t, indexData, dataAdded, indexHelperInfo)
		}
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// checkRowValues checks whether  the values of row mutations are consistent with the expected ones
func checkRowValues(sc *stmtctx.StatementContext, sessVars *variable.SessionVars, t *TableCommon,
	dataAdded, dataRemoved []types.Datum, mutations []Mutation) error {
	rowsAdded, rowsRemoved := ExtractRowMutations(mutations)
	if len(rowsAdded) > 1 || len(rowsRemoved) > 1 {
		// TODO: is it possible?
		logutil.BgLogger().Error("multiple row mutations added/mutated", zap.Any("rowsAdded", rowsAdded),
			zap.Any("rowsRemoved", rowsRemoved))
		return errors.New("multiple row mutations added/mutated")
	}

	columnMap := make(map[int64]*model.ColumnInfo)
	columnFieldMap := make(map[int64]*types.FieldType)
	for _, col := range t.Meta().Columns {
		columnMap[col.ID] = col
		columnFieldMap[col.ID] = &col.FieldType
	}

	if err := checkRowMutationsWithData(sc, sessVars, rowsAdded, columnFieldMap, dataAdded, columnMap); err != nil {
		return errors.Trace(err)
	}
	if err := checkRowMutationsWithData(sc, sessVars, rowsRemoved, columnFieldMap, dataRemoved, columnMap); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func checkRowMutationsWithData(sc *stmtctx.StatementContext, sessVars *variable.SessionVars, rowMutations []Mutation,
	columnFieldMap map[int64]*types.FieldType, expectedData []types.Datum, columnMap map[int64]*model.ColumnInfo) error {
	if len(rowMutations) > 0 {
		decodedData, err := tablecodec.DecodeRowToDatumMap(rowMutations[0].value, columnFieldMap, sessVars.Location())
		if err != nil {
			return errors.Trace(err)
		}

		// TODO: we cannot check if the decoded values contain all columns since some columns may be skipped.
		// Instead we check data in the value are consistent with input.

		for columnId, decodedDatum := range decodedData {
			inputDatum := expectedData[columnMap[columnId].Offset]
			cmp, err := decodedDatum.CompareDatum(sc, &inputDatum)
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
	}
	return nil
}

func collectTableMutationsFromBufferStage(t *TableCommon, memBuffer kv.MemBuffer, sh kv.StagingHandle) []Mutation {
	mutations := make([]Mutation, 0)
	inspector := func(key kv.Key, flags kv.KeyFlags, data []byte) {
		// TODO: shall we check only the current table, or all tables involved?
		if tablecodec.DecodeTableID(key) == t.physicalTableID {
			mutations = append(mutations, Mutation{key, flags, data})
		}
	}
	memBuffer.InspectStage(sh, inspector)
	return mutations
}

func compareIndexData(sc *stmtctx.StatementContext, t *TableCommon, indexData, input []types.Datum, indexHelperInfo IndexHelperInfo) error {
	for i, decodedMutationDatum := range indexData {
		expectedDatum := input[indexHelperInfo.indexInfo.Columns[i].Offset]

		tablecodec.TruncateIndexValue(&expectedDatum, indexHelperInfo.indexInfo.Columns[i],
			t.Columns[indexHelperInfo.indexInfo.Columns[i].Offset].ColumnInfo)

		tablecodec.TruncateIndexValue(&decodedMutationDatum, indexHelperInfo.indexInfo.Columns[i],
			t.Columns[indexHelperInfo.indexInfo.Columns[i].Offset].ColumnInfo)

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

// ExtractRowMutations extracts row mutations and classify them into 2 categories: put and delete
func ExtractRowMutations(mutations []Mutation) ([]Mutation, []Mutation) {
	handlesAdded := make([]Mutation, 0)
	handlesRemoved := make([]Mutation, 0)
	// TODO: assumption: value in mem buffer
	for _, m := range mutations {
		if rowcodec.IsRowKey(m.key) {
			if len(m.value) > 0 {
				handlesAdded = append(handlesAdded, m)
			} else {
				handlesRemoved = append(handlesRemoved, m)
			}
		}
	}
	return handlesAdded, handlesRemoved
}
