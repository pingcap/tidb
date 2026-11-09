// Copyright 2023 PingCAP, Inc.
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
	"errors"
	"testing"

	"github.com/pingcap/failpoint"
	brstorage "github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	drivererr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type importIntoSuite struct {
	suite.Suite
}

func TestImportInto(t *testing.T) {
	suite.Run(t, &importIntoSuite{})
}

func (s *importIntoSuite) enableFailPoint(path, term string) {
	require.NoError(s.T(), failpoint.Enable(path, term))
	s.T().Cleanup(func() {
		_ = failpoint.Disable(path)
	})
}

func (s *importIntoSuite) TestSchedulerGetEligibleInstances() {
	sch := importScheduler{}
	task := &proto.Task{Meta: []byte("{}")}
	ctx := context.WithValue(context.Background(), "etcd", true)
	eligibleInstances, err := sch.GetEligibleInstances(ctx, task)
	s.NoError(err)
	// order of slice is not stable, change to map
	s.Empty(eligibleInstances)

	task.Meta = []byte(`{"EligibleInstances":[{"ip": "1.1.1.1", "listening_port": 4000}]}`)
	eligibleInstances, err = sch.GetEligibleInstances(ctx, task)
	s.NoError(err)
	s.Equal([]string{"1.1.1.1:4000"}, eligibleInstances)
}

func (s *importIntoSuite) TestUpdateCurrentTask() {
	taskMeta := TaskMeta{
		Plan: importer.Plan{
			DisableTiKVImportMode: true,
		},
	}
	bs, err := json.Marshal(taskMeta)
	require.NoError(s.T(), err)

	sch := importScheduler{}
	require.Equal(s.T(), int64(0), sch.currTaskID.Load())
	require.False(s.T(), sch.disableTiKVImportMode.Load())

	sch.updateCurrentTask(&proto.Task{
		TaskBase: proto.TaskBase{ID: 1},
		Meta:     bs,
	})
	require.Equal(s.T(), int64(1), sch.currTaskID.Load())
	require.True(s.T(), sch.disableTiKVImportMode.Load())

	sch.updateCurrentTask(&proto.Task{
		TaskBase: proto.TaskBase{ID: 1},
		Meta:     bs,
	})
	require.Equal(s.T(), int64(1), sch.currTaskID.Load())
	require.True(s.T(), sch.disableTiKVImportMode.Load())
}

func (s *importIntoSuite) TestSchedulerInit() {
	meta := TaskMeta{
		Plan: importer.Plan{
			CloudStorageURI: "",
		},
	}
	bytes, err := json.Marshal(meta)
	s.NoError(err)
	sch := importScheduler{
		BaseScheduler: scheduler.NewBaseScheduler(context.Background(), &proto.Task{
			Meta: bytes,
		}, scheduler.Param{}),
	}
	s.NoError(sch.Init())
	s.False(sch.Extension.(*importScheduler).GlobalSort)

	meta.Plan.CloudStorageURI = "s3://test"
	bytes, err = json.Marshal(meta)
	s.NoError(err)
	sch = importScheduler{
		BaseScheduler: scheduler.NewBaseScheduler(context.Background(), &proto.Task{
			Meta: bytes,
		}, scheduler.Param{}),
	}
	s.NoError(sch.Init())
	s.True(sch.Extension.(*importScheduler).GlobalSort)
}

func (s *importIntoSuite) TestGetNextStep() {
	task := &proto.TaskBase{Step: proto.StepInit}
	ext := &importScheduler{}
	for _, nextStep := range []proto.Step{proto.ImportStepImport, proto.ImportStepPostProcess, proto.StepDone} {
		s.Equal(nextStep, ext.GetNextStep(task))
		task.Step = nextStep
	}

	task.Step = proto.StepInit
	ext = &importScheduler{GlobalSort: true}
	for _, nextStep := range []proto.Step{proto.ImportStepEncodeAndSort, proto.ImportStepMergeSort,
		proto.ImportStepWriteAndIngest, proto.ImportStepPostProcess, proto.StepDone} {
		s.Equal(nextStep, ext.GetNextStep(task))
		task.Step = nextStep
	}
}

func (s *importIntoSuite) TestGetStepOfEncode() {
	s.Equal(proto.ImportStepImport, getStepOfEncode(false))
	s.Equal(proto.ImportStepEncodeAndSort, getStepOfEncode(true))
}

func (s *importIntoSuite) TestIsRetryable() {
	ext := &importScheduler{}
	require.True(s.T(), ext.IsRetryableErr(drivererr.ErrRegionUnavailable))
}

func TestIsImporting2TiKV(t *testing.T) {
	ext := &importScheduler{}
	require.False(t, ext.isImporting2TiKV(&proto.Task{TaskBase: proto.TaskBase{Step: proto.ImportStepEncodeAndSort}}))
	require.False(t, ext.isImporting2TiKV(&proto.Task{TaskBase: proto.TaskBase{Step: proto.ImportStepMergeSort}}))
	require.False(t, ext.isImporting2TiKV(&proto.Task{TaskBase: proto.TaskBase{Step: proto.ImportStepPostProcess}}))
	require.True(t, ext.isImporting2TiKV(&proto.Task{TaskBase: proto.TaskBase{Step: proto.ImportStepImport}}))
	require.True(t, ext.isImporting2TiKV(&proto.Task{TaskBase: proto.TaskBase{Step: proto.ImportStepWriteAndIngest}}))
}

type sourceStatisticsHandle struct {
	storage.TaskHandle
	metas  [][]byte
	err    error
	capped bool
}

func (h sourceStatisticsHandle) GetPreviousSubtaskMetas(_ int64, step proto.Step) ([][]byte, error) {
	if step == proto.ImportStepCollectConflicts {
		data, _ := json.Marshal(CollectConflictsStepMeta{TooManyConflictsFromIndex: h.capped})
		return [][]byte{data}, h.err
	}
	return h.metas, h.err
}

// TestCollectImportSourceStatistics aggregates actual successful subtask inputs,
// checks source conflict deduplication, and preserves uncertainty for old/capped
// metadata instead of substituting estimated row IDs or live progress counters.
func TestCollectImportSourceStatistics(t *testing.T) {
	for _, scenario := range []string{"ordinary", "conflicts", "old metadata", "capped conflicts", "metadata unavailable"} {
		t.Run(scenario, func(t *testing.T) {
			total := uint64(5)
			data, err := json.Marshal(ImportStepMeta{SourceRecords: &total})
			require.NoError(t, err)
			handle := sourceStatisticsHandle{metas: [][]byte{data, data}}
			meta := &TaskMeta{}
			meta.Result.LoadedRowCnt = 999999
			if scenario == "conflicts" {
				meta.Result.ConflictedRowCnt = 3
			}
			if scenario == "old metadata" {
				handle.metas = append(handle.metas, []byte(`{}`))
			}
			if scenario == "capped conflicts" {
				meta.Plan.CloudStorageURI = "s3://sort"
				handle.capped = true
			}
			if scenario == "metadata unavailable" {
				handle.err = errors.New("unavailable")
			}
			result := collectImportStatistics(handle, &proto.Task{TaskBase: proto.TaskBase{ID: 1}}, meta)
			if scenario == "old metadata" || scenario == "metadata unavailable" {
				require.Nil(t, result.Total)
				require.False(t, result.Complete)
				return
			}
			require.Equal(t, uint64(10), *result.Total)
			require.Equal(t, uint64(10), result.Outcomes.Count()+*result.Unresolved)
			if scenario == "capped conflicts" {
				require.Equal(t, uint64(10), *result.Unresolved)
				return
			}
			require.Equal(t, meta.Result.ConflictedRowCnt, result.Skipped)
			require.Equal(t, uint64(10)-result.Skipped, result.Imported)
		})
	}
}

// Source counts belong in internal metadata, while source chunks are offloaded.
// Both serialization paths must support legacy, zero and nonzero source counts.
func TestImportStepMetaSourceRecordsExternalStorage(t *testing.T) {
	ctx := context.Background()
	store, err := brstorage.NewLocalStorage(t.TempDir())
	require.NoError(t, err)
	zero, count := uint64(0), uint64(42)
	cases := []struct {
		name    string
		records *uint64
	}{
		{name: "legacy"},
		{name: "zero", records: &zero},
		{name: "count", records: &count},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			meta := ImportStepMeta{
				BaseExternalMeta: external.BaseExternalMeta{ExternalPath: tc.name + ".json"},
				SourceRecords:    tc.records,
				ID:               7,
				Chunks:           []Chunk{{Path: "source.csv"}},
			}
			internal, err := meta.Marshal()
			require.NoError(t, err)
			var internalFields map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(internal, &internalFields))
			require.NotContains(t, internalFields, "Chunks")
			if tc.records == nil {
				require.NotContains(t, internalFields, "source-records")
			} else {
				var got uint64
				require.NoError(t, json.Unmarshal(internalFields["source-records"], &got))
				require.Equal(t, *tc.records, got)
			}
			require.NoError(t, meta.WriteJSONToExternalStorage(ctx, store, &meta))
			external, err := store.ReadFile(ctx, meta.ExternalPath)
			require.NoError(t, err)
			var externalFields map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(external, &externalFields))
			require.Contains(t, externalFields, "Chunks")
			require.NotContains(t, externalFields, "source-records")
			var restored ImportStepMeta
			require.NoError(t, json.Unmarshal(internal, &restored))
			require.NoError(t, restored.ReadJSONFromExternalStorage(ctx, store, &restored))
			require.Equal(t, meta, restored)
		})
	}
}

// TestUpdateResultIsIdempotent starts with stale aggregate values, then repeats
// result collection to prove scheduler retries replace rather than add counts.
func TestUpdateResultIsIdempotent(t *testing.T) {
	source := uint64(5)
	data, err := json.Marshal(ImportStepMeta{SourceRecords: &source, Result: Result{LoadedRowCnt: 3}})
	require.NoError(t, err)
	handle := sourceStatisticsHandle{metas: [][]byte{data, data}}
	meta := &TaskMeta{Result: Result{LoadedRowCnt: 999}}
	task := &proto.Task{TaskBase: proto.TaskBase{ID: 1}}
	for range 2 {
		require.NoError(t, updateResult(handle, task, meta, false))
		require.Equal(t, uint64(6), meta.Result.LoadedRowCnt)
		result := collectImportStatistics(handle, task, meta)
		require.Equal(t, uint64(10), *result.Total)
		require.Equal(t, uint64(10), result.Imported)
	}
}
