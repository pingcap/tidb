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
	indexInfo *model.IndexInfo
	colInfos  []rowcodec.ColInfo
}

// CheckIndexConsistency checks whether the given set of mutations corresponding to a single row is consistent.
// Namely, assume the database is consistent before, applying the mutations shouldn't break the consistency.
// It aims at reducing bugs that will corrupt data, and prevent mistakes from spreading if possible.
//
// Assume the set of row values changes from V1 to V2. We check
// (1) V2 - V1 = {added indices}
// (2) V1 - V2 = {deleted indices}
//
// To check (1), we need
// (a) {added indices} is a subset of {needed indices} <=> each index mutation is consistent with the input/row key/value
// (b) {needed indices} is a subset of {added indices}. The check process would be exactly the same with how we calculate
// 		the mutations, thus ignored.
func CheckIndexConsistency(sc *stmtctx.StatementContext, sessVars *variable.SessionVars, t *TableCommon,
	dataAdded, dataRemoved []types.Datum, memBuffer kv.MemBuffer, sh kv.StagingHandle) error {
	// collect mutations
	mutations := make([]Mutation, 0)
	inspector := func(key kv.Key, flags kv.KeyFlags, data []byte) {
		// TODO: shall we check only the current table, or all tables involved?
		if tablecodec.DecodeTableID(key) == t.physicalTableID {
			mutations = append(mutations, Mutation{key, flags, data})
		}
	}
	memBuffer.InspectStage(sh, inspector)

	// get the handle
	handlesAdded, handlesRemoved := ExtractHandles(mutations, t)
	if len(handlesAdded) > 1 || len(handlesRemoved) > 1 {
		// TODO: is it possible?
		logutil.BgLogger().Error("multiple handles added/mutated", zap.Any("handlesAdded", handlesAdded),
			zap.Any("handlesRemoved", handlesRemoved))
		return errors.New("multiple handles added/mutated")
	}

	// 1. TODO: compare handlesAdded vs. dataAdded, handlesRemoved vs. dataRemoved
	indexIdMap := make(map[int64]IndexHelperInfo)
	for _, index := range t.indices {
		indexIdMap[index.Meta().ID] = IndexHelperInfo{
			index.Meta(),
			BuildRowcodecColInfoForIndexColumns(index.Meta(), t.Meta()),
		}
	}

	// 2. check index keys: consistent with input data
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

		colInfos := BuildRowcodecColInfoForIndexColumns(indexHelperInfo.indexInfo, t.Meta())

		if len(m.value) == 0 {
			// FIXME: for a delete index mutation, we cannot know the value of it.
			// When new collation is enabled, we cannot decode value from the key.
			// => ignore it for now
			continue
		}
		decodedIndexValues, err := tablecodec.DecodeIndexKV(m.key, m.value, len(indexHelperInfo.indexInfo.Columns),
			tablecodec.HandleNotNeeded, colInfos)
		if err != nil {
			return errors.Trace(err)
		}
		indexData := make([]types.Datum, 0)
		for i, v := range decodedIndexValues {
			d, err := tablecodec.DecodeColumnValue(v, &t.Columns[indexHelperInfo.indexInfo.Columns[i].Offset].FieldType, sessVars.TimeZone)
			if err != nil {
				return errors.Trace(err)
			}
			indexData = append(indexData, d)
			logutil.BgLogger().Warn("decoded index value", zap.String("datum", d.String()))
		}

		// TODO: when is it nil?
		if m.value == nil {
			continue
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

// ExtractHandles extract handles of row mutations and classify them into 2 categories: put and delete
func ExtractHandles(mutations []Mutation, t *TableCommon) ([]kv.Handle, []kv.Handle) {
	handlesAdded := make([]kv.Handle, 0)
	handlesRemoved := make([]kv.Handle, 0)
	for _, m := range mutations {
		handle, err := tablecodec.DecodeRowKey(m.key)
		if err != nil {
			// TODO: remove it later
			logutil.BgLogger().Warn("decode row key failed", zap.Error(err))
			continue
		}

		// TODO: distinguish between nil and empty value
		if m.value == nil {
			logutil.BgLogger().Warn("row.value = nil", zap.String("handle", handle.String()))
			continue
		}
		if len(m.value) > 0 {
			handlesAdded = append(handlesAdded, handle)
		} else {
			handlesRemoved = append(handlesRemoved, handle)
		}
	}
	return handlesAdded, handlesRemoved
}
