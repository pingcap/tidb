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
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/importinto"
	"github.com/stretchr/testify/require"
)

func TestCollectConflictsStepExecutor(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("skip test for next-gen kernel temporarily, we need to adapt the test later")
	}
	hdlCtx := prepareConflictedKVHandleContext(t)
	stMeta := importinto.CollectConflictsStepMeta{Infos: hdlCtx.conflictedKVInfo}
	bytes, err := json.Marshal(stMeta)
	require.NoError(t, err)
	st := &proto.Subtask{Meta: bytes}
	stepExe := importinto.NewCollectConflictsStepExecutor(&proto.TaskBase{RequiredSlots: 1}, hdlCtx.store, hdlCtx.taskMeta, hdlCtx.logger)
	runConflictedKVHandleStep(t, st, stepExe)
	outSTMeta := &importinto.CollectConflictsStepMeta{}
	require.NoError(t, json.Unmarshal(st.Meta, outSTMeta))
	require.EqualValues(t, &importinto.Checksum{Sum: 6734985763851266693, KVs: 27, Size: 909}, outSTMeta.Checksum)
	require.EqualValues(t, 9, outSTMeta.ConflictedRowCount)
	// one for each kv group
	require.Len(t, outSTMeta.ConflictedRowFilenames, 2)
	require.False(t, outSTMeta.TooManyConflictsFromIndex)
}
