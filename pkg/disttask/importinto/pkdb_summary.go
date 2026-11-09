// Copyright 2026 PingCAP, Inc.
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

package importinto

import (
	"encoding/json"

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/lightning/pkdbsummary"
)

func collectImportStatistics(handle storage.TaskHandle, task *proto.Task, meta *TaskMeta) *pkdbsummary.Result {
	// Only committed, successful subtask metadata contributes. Failed/retried
	// attempts and live progress counters cannot be summed as source records.
	metas, err := handle.GetPreviousSubtaskMetas(task.ID, getStepOfEncode(meta.Plan.IsGlobalSort()))
	var total uint64
	known := err == nil
	for _, data := range metas {
		var subtask ImportStepMeta
		if json.Unmarshal(data, &subtask) != nil || subtask.SourceRecords == nil {
			known = false
			continue
		}
		total += *subtask.SourceRecords
	}
	conflictsKnown := true
	if meta.Plan.IsGlobalSort() {
		conflicts, err := handle.GetPreviousSubtaskMetas(task.ID, proto.ImportStepCollectConflicts)
		conflictsKnown = err == nil
		for _, data := range conflicts {
			var conflict CollectConflictsStepMeta
			if json.Unmarshal(data, &conflict) != nil || conflict.TooManyConflictsFromIndex {
				conflictsKnown = false
			}
		}
	}
	outcomes := pkdbsummary.Outcomes{}
	// capture removes conflicting source rows; it does not implement REPLACE.
	// Capped index-conflict enumeration cannot prove source-row outcomes.
	if known && conflictsKnown && meta.Result.ConflictedRowCnt <= total {
		outcomes.Skipped = meta.Result.ConflictedRowCnt
		outcomes.Imported = total - outcomes.Skipped
	}
	result := pkdbsummary.NewResult(total, known, outcomes)
	return &result
}
