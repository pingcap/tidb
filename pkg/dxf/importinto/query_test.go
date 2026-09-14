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
	"context"
	"encoding/json"
	"testing"

	"github.com/pingcap/tidb/pkg/dxf/framework/planner"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/ingestor/engineapi"
	"github.com/pingcap/tidb/pkg/ingestor/globalsort"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func TestImportQueryPlanning(t *testing.T) {
	plan := &LogicalPlan{Plan: importer.Plan{
		DataSourceType: importer.DataSourceTypeQuery, CloudStorageURI: "noop://sort/query", MaxNodeCnt: 1,
		Query: &importer.QueryPlan{Keyspace: "tenant", SQL: "select count(*) from src"},
	}}
	require.False(t, ShouldUseAsyncPrepare(&plan.Plan))
	// There is no file path, file list, or chunk map to initialize or partition.
	physical, err := plan.ToPhysicalPlan(planner.PlanCtx{
		Ctx: context.Background(), GlobalSort: true, NextTaskStep: proto.ImportStepQuery,
	})
	require.NoError(t, err)
	require.Len(t, physical.Processors, 1)
	require.Equal(t, proto.ImportStepQuery, physical.Processors[0].Step)
	spec := physical.Processors[0].Pipeline.(*ImportSpec)
	require.EqualValues(t, 1, spec.ImportStepMeta.ID)
	require.Empty(t, spec.ImportStepMeta.Chunks)
	data, err := plan.ToTaskMeta()
	require.NoError(t, err)
	var decoded TaskMeta
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Equal(t, plan.Plan.Query, decoded.Plan.Query)
	require.Equal(t, 1, decoded.Plan.MaxNodeCnt)
	sch := &importScheduler{GlobalSort: true, fromQuery: true}
	task := &proto.TaskBase{Step: proto.StepInit}
	for _, step := range []proto.Step{
		proto.ImportStepQuery, proto.ImportStepMergeSort, proto.ImportStepWriteAndIngest,
		proto.ImportStepCollectConflicts, proto.ImportStepConflictResolution, proto.ImportStepPostProcess, proto.StepDone,
	} {
		require.Equal(t, step, sch.GetNextStep(task))
		task.Step = step
	}
}

func TestImportQueryDownstreamMetadata(t *testing.T) {
	ctx := context.Background()
	queryMetas := genEncodeStepMetas(t, 1)
	var sourceMeta ImportStepMeta
	require.NoError(t, json.Unmarshal(queryMetas[0], &sourceMeta))
	sourceMeta.Checksum = map[int64]Checksum{-1: {Sum: 42, KVs: 3, Size: 90}}
	sourceMeta.MaxIDs = map[autoid.AllocatorType]int64{autoid.RowIDAllocType: 123}
	sourceMeta.RecordedConflictKVCount = 2
	sourceMeta.SortedDataMeta.ConflictInfo = engineapi.ConflictInfo{Count: 2, Files: []string{"query-conflicts"}}
	bs, err := json.Marshal(sourceMeta)
	require.NoError(t, err)
	queryMetas[0] = bs
	planCtx := planner.PlanCtx{
		Ctx: ctx, GlobalSort: true, ThreadCnt: 16,
		PreviousSubtaskMetas: map[proto.Step][][]byte{
			proto.ImportStepQuery: queryMetas,
			// A wrong source-step lookup must fail instead of silently producing empty results.
			proto.ImportStepEncodeAndSort: {[]byte("unexpected encode metadata")},
		},
	}
	p := &LogicalPlan{Plan: importer.Plan{Query: &importer.QueryPlan{}, CloudStorageURI: "noop://sort"}}

	// With no overlap, both data and index groups come directly from Query.
	specs, err := generateMergeSortSpecs(planCtx, p)
	require.NoError(t, err)
	require.Empty(t, specs)
	kvs, err := getSortedKVMetasForIngest(planCtx, p, nil)
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, []byte("d_0_a"), kvs[globalsort.DataKVGroup].StartKey)
	require.Equal(t, []byte("i1_0_a"), kvs["1"].StartKey)

	// Merge data only; ingest must still obtain the unmerged index from Query.
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/dxf/importinto/forceMergeSort", `return("data")`)
	specs, err = generateMergeSortSpecs(planCtx, p)
	require.NoError(t, err)
	require.Len(t, specs, 1)
	require.Equal(t, []string{"d_0_/1"}, specs[0].(*MergeSortSpec).DataFiles)
	planCtx.PreviousSubtaskMetas[proto.ImportStepMergeSort] = genMergeStepMetas(t, 1)
	kvs, err = getSortedKVMetasForIngest(planCtx, p, nil)
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, []byte("x_0_a"), kvs[globalsort.DataKVGroup].StartKey)
	require.Equal(t, []byte("i1_0_a"), kvs["1"].StartKey)

	conflicts, err := collectConflictInfos(ctx, nil, planCtx, proto.ImportStepQuery)
	require.NoError(t, err)
	require.Equal(t, &sourceMeta.SortedDataMeta.ConflictInfo, conflicts.ConflictInfos[globalsort.DataKVGroup])
	post := &PostProcessSpec{fromQuery: true}
	bs, err = post.ToSubtaskMeta(planCtx)
	require.NoError(t, err)
	var result PostProcessStepMeta
	require.NoError(t, json.Unmarshal(bs, &result))
	require.Equal(t, sourceMeta.Checksum, result.Checksum)
	require.Equal(t, sourceMeta.MaxIDs, result.MaxIDs)
}
