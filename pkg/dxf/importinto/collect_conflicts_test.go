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

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/importinto"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

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
	expectedSums := []uint64{
		2944242980394429146, // classic
		6636364898488969870, // next-gen
	}
	if kerneltype.IsClassic() {
		expectedSums = expectedSums[:1]
	} else {
		expectedSums = expectedSums[1:]
	}
	expectedSize := uint64(909) + uint64(27)*uint64(len(hdlCtx.store.GetCodec().GetKeyspace()))
	require.Contains(t, expectedSums, outSTMeta.Checksum.Sum)
	require.EqualValues(t, 27, outSTMeta.Checksum.KVs)
	require.EqualValues(t, expectedSize, outSTMeta.Checksum.Size)
	require.EqualValues(t, 9, outSTMeta.ConflictedRowCount)
	// we are running them concurrently, so the number of filenames may vary.
	require.GreaterOrEqual(t, len(outSTMeta.ConflictedRowFilenames), 2)
	require.LessOrEqual(t, len(outSTMeta.ConflictedRowFilenames), 9)
	require.False(t, outSTMeta.ConflictedRowRecordingCapped)
	require.False(t, outSTMeta.TooManyConflictsFromIndex)
}

func TestCollectConflictsStepExecutorFilesTruncated(t *testing.T) {
	hdlCtx := prepareConflictedKVHandleContext(t)
	stMeta := importinto.CollectConflictsStepMeta{Infos: hdlCtx.conflictedKVInfo}
	bytes, err := json.Marshal(stMeta)
	require.NoError(t, err)
	st := &proto.Subtask{Meta: bytes}

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/dxf/importinto/forceHandleConflictsBySingleThread", "return(true)")
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/dxf/importinto/conflictedkv/mockTotalConflictRowFileSizeLimit", func(limitP *int64) {
		*limitP = 32
	})

	stepExe := importinto.NewCollectConflictsStepExecutor(&proto.TaskBase{RequiredSlots: 1}, hdlCtx.store, hdlCtx.taskMeta, hdlCtx.logger)
	runConflictedKVHandleStep(t, st, stepExe)
	outSTMeta := &importinto.CollectConflictsStepMeta{}
	require.NoError(t, json.Unmarshal(st.Meta, outSTMeta))
	require.True(t, outSTMeta.ConflictedRowRecordingCapped)
	require.Greater(t, outSTMeta.ConflictedRowCount, int64(0))
	require.NotEmpty(t, outSTMeta.ConflictedRowFilenames)
}
