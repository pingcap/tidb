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
	require.EqualValues(t, &importinto.Checksum{
		Sum:  6636364898488969870,
		KVs:  27,
		Size: 1017,
	}, outSTMeta.Checksum)
	require.EqualValues(t, 9, outSTMeta.ConflictedRowCount)
	// we are running them concurrently, so the number of filenames may vary.
	require.GreaterOrEqual(t, len(outSTMeta.ConflictedRowFilenames), 2)
	require.LessOrEqual(t, len(outSTMeta.ConflictedRowFilenames), 9)
	require.False(t, outSTMeta.TooManyConflictsFromIndex)
}
