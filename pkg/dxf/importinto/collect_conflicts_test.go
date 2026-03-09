// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package importinto_test

import (
	"encoding/json"
	"testing"

	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/importinto"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/pkg/lightning/verification"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func expectedCollectConflictsChecksum(t *testing.T, hdlCtx *conflictedKVHandleContext) *importinto.Checksum {
	t.Helper()

	encodeCfg := &encode.EncodingConfig{
		Table:                hdlCtx.tbl,
		UseIdentityAutoRowID: true,
	}
	controller := &importer.LoadDataController{
		ASTArgs: &importer.ASTArgs{},
		Plan:    &importer.Plan{},
		Table:   hdlCtx.tbl,
	}
	encoder, err := importer.NewTableKVEncoderForDupResolve(encodeCfg, controller)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, encoder.Close())
	}()

	checksum := verification.NewKVChecksumWithKeyspace(hdlCtx.store.GetCodec().GetKeyspace())
	for i := range 3 {
		rowID := i + 1
		row := []types.Datum{types.NewDatum(rowID), types.NewDatum(rowID), types.NewDatum(rowID)}
		pairs, err := encoder.Encode(row, int64(rowID))
		require.NoError(t, err)
		for range 3 {
			checksum.Update(pairs.Pairs)
		}
		pairs.Clear()
	}
	return &importinto.Checksum{
		Sum:  checksum.Sum(),
		KVs:  checksum.SumKVS(),
		Size: checksum.SumSize(),
	}
}

func TestCollectConflictsStepExecutor(t *testing.T) {
	hdlCtx := prepareConflictedKVHandleContext(t)
	stMeta := importinto.CollectConflictsStepMeta{Infos: hdlCtx.conflictedKVInfo}
	bytes, err := json.Marshal(stMeta)
	require.NoError(t, err)
	st := &proto.Subtask{Meta: bytes}
	stepExe := importinto.NewCollectConflictsStepExecutor(&proto.TaskBase{RequiredSlots: 1}, hdlCtx.store, hdlCtx.taskMeta, hdlCtx.logger)
	runConflictedKVHandleStep(t, st, stepExe)
	outSTMeta := &importinto.CollectConflictsStepMeta{}
	require.NoError(t, json.Unmarshal(st.Meta, outSTMeta))
	expectedSum := expectedCollectConflictsChecksum(t, hdlCtx)
	require.EqualValues(t, expectedSum, outSTMeta.Checksum)
	require.EqualValues(t, 9, outSTMeta.ConflictedRowCount)
	// we are running them concurrently, so the number of filenames may vary.
	require.GreaterOrEqual(t, len(outSTMeta.ConflictedRowFilenames), 2)
	require.LessOrEqual(t, len(outSTMeta.ConflictedRowFilenames), 9)
	require.False(t, outSTMeta.TooManyConflictsFromIndex)
}
