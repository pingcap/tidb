// Copyright 2025 PingCAP, Inc.
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

package local

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/pkg/ingestor/engineapi"
	"github.com/pingcap/tidb/pkg/ingestor/errdef"
	"github.com/pingcap/tidb/pkg/ingestor/ingestcli"
	ingestclimock "github.com/pingcap/tidb/pkg/ingestor/ingestcli/mock"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/resourcemanager/pool/workerpool"
	rcmgrutil "github.com/pingcap/tidb/pkg/resourcemanager/util"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util"
	tidblogutil "github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

type mockDiagnosticIngestData struct {
	mockIngestData
	dataID          int64
	loadedKVCount   int64
	importedKVCount int64
	importedKVSize  int64
}

func (m mockDiagnosticIngestData) DataID() int64 {
	return m.dataID
}

func (m mockDiagnosticIngestData) LoadedKVCount() int64 {
	return m.loadedKVCount
}

func (m mockDiagnosticIngestData) ImportedKVCount() int64 {
	return m.importedKVCount
}

func (m mockDiagnosticIngestData) ImportedKVSize() int64 {
	return m.importedKVSize
}

func newRegionJobWorkerPoolForTest(
	workerCtx context.Context,
	preRunJobFn func(ctx context.Context, job *regionJob) error,
	writeFn func(ctx context.Context, job *regionJob) (*tikvWriteResult, error),
	ingestFn func(ctx context.Context, job *regionJob) error,
) (
	op *workerpool.WorkerPool[*regionJob, *regionJob],
	jobWg *sync.WaitGroup,
	inChan chan<- *regionJob,
	outChan <-chan *regionJob,
) {
	jobWg = &sync.WaitGroup{}
	wctx := workerpool.NewContext(workerCtx)
	jobToWorkerCh := make(chan *regionJob, 256)
	jobFromWorkerCh := make(chan *regionJob, 256) // make a larger channel for test

	if preRunJobFn == nil {
		preRunJobFn = func(ctx context.Context, job *regionJob) error {
			return nil
		}
	}
	if writeFn == nil {
		writeFn = func(ctx context.Context, job *regionJob) (*tikvWriteResult, error) {
			return &tikvWriteResult{}, nil
		}
	}
	if ingestFn == nil {
		ingestFn = func(ctx context.Context, job *regionJob) error {
			return nil
		}
	}

	pool := workerpool.NewWorkerPool(
		"RegionJobOperator",
		rcmgrutil.DistTask,
		4,
		func() workerpool.Worker[*regionJob, *regionJob] {
			return &regionJobBaseWorker{
				ctx:         wctx,
				jobInCh:     jobToWorkerCh,
				jobOutCh:    jobFromWorkerCh,
				jobWg:       jobWg,
				preRunJobFn: preRunJobFn,
				writeFn:     writeFn,
				ingestFn:    ingestFn,
				regenerateJobsFn: func(
					ctx context.Context, data engineapi.IngestData, sortedJobRanges []engineapi.Range,
					regionSplitSize, regionSplitKeys int64,
				) ([]*regionJob, error) {
					return []*regionJob{
						{}, {}, {},
					}, nil
				},
			}
		},
	)
	pool.SetResultSender(jobFromWorkerCh)
	pool.SetTaskReceiver(jobToWorkerCh)
	return pool, jobWg, jobToWorkerCh, jobFromWorkerCh
}

func TestRegionJobBaseWorker(t *testing.T) {
	// All below tests are for basic functionality of the region job operator, there
	// are other tests inside local_test.go.
	// To fully run a region job, we also need the job retryer and the routine to
	// receive executed job, so we need to manually handle jobWg and jobOutCh here.

	dummyRegion := &split.RegionInfo{Region: &metapb.Region{
		Id: 1, Peers: []*metapb.Peer{{StoreId: 1}}}, Leader: &metapb.Peer{StoreId: 1},
	}

	prepareAndExecuteWithCtx := func(
		t *testing.T,
		workerCtx context.Context,
		generateCount int,
		job *regionJob,
		preRunFn func(ctx context.Context, job *regionJob) error,
		writeFn func(ctx context.Context, job *regionJob) (*tikvWriteResult, error),
		ingestFn func(ctx context.Context, job *regionJob) error) (<-chan *regionJob, error,
	) {
		// mock jobWg.Done() called in the worker
		testfailpoint.Enable(t,
			"github.com/pingcap/tidb/pkg/lightning/backend/local/mockJobWgDone",
			fmt.Sprintf("return(%d)", generateCount))
		workGroup, workerCtx := util.NewErrorGroupWithRecoverWithCtx(workerCtx)
		pool, jobWg, jobInCh, jobOutCh := newRegionJobWorkerPoolForTest(workerCtx, preRunFn, writeFn, ingestFn)

		wctx := workerpool.NewContext(workerCtx)
		workGroup.Go(func() error {
			pool.Start(wctx)
			<-wctx.Done()
			pool.Release()
			return wctx.OperatorErr()
		})

		workGroup.Go(func() error {
			jobWg.Add(1)
			jobInCh <- job
			close(jobInCh)
			jobWg.Wait()

			wctx.Cancel()
			return nil
		})

		err := workGroup.Wait()
		require.Equal(t, 0, len(jobInCh))
		return jobOutCh, err
	}

	prepareAndExecute := func(
		t *testing.T,
		generateCount int,
		job *regionJob,
		preRunFn func(ctx context.Context, job *regionJob) error,
		writeFn func(ctx context.Context, job *regionJob) (*tikvWriteResult, error),
		ingestFn func(ctx context.Context, job *regionJob) error) (<-chan *regionJob, error,
	) {
		return prepareAndExecuteWithCtx(t, context.Background(), generateCount, job, preRunFn, writeFn, ingestFn)
	}

	t.Run("diagnostic helper fields", func(t *testing.T) {
		fields := ingestDataDiagFields(mockDiagnosticIngestData{
			dataID:          7,
			loadedKVCount:   4,
			importedKVCount: 2,
			importedKVSize:  11,
		})
		fieldByKey := make(map[string]zap.Field, len(fields))
		for _, field := range fields {
			fieldByKey[field.Key] = field
		}
		require.Equal(t, int64(7), fieldByKey["dataID"].Integer)
		require.Equal(t, int64(4), fieldByKey["loadedKVs"].Integer)
		require.Equal(t, int64(2), fieldByKey["dataImportedKVs"].Integer)
		require.Equal(t, int64(11), fieldByKey["dataImportedBytes"].Integer)

		require.Empty(t, ingestDataDiagFields(mockIngestData{}))
	})

	t.Run("generate jobs log includes diagnostic fields", func(t *testing.T) {
		core, observedLogs := observer.New(zapcore.InfoLevel)
		ctx := tidblogutil.WithLogger(context.Background(), zap.New(core))
		local := &Backend{splitCli: initTestSplitClient([][]byte{{}, []byte("z")}, nil)}
		data := mockDiagnosticIngestData{
			mockIngestData: mockIngestData{{[]byte("b"), []byte("b")}},
			dataID:         9,
			loadedKVCount:  1,
		}

		jobs, err := local.generateJobForRange(ctx, data, []engineapi.Range{{Start: []byte("a"), End: []byte("z")}}, 1024, 1024)
		require.NoError(t, err)
		require.Len(t, jobs, 1)

		entries := observedLogs.FilterMessage("generate region jobs").All()
		require.Len(t, entries, 1)
		fields := entries[0].ContextMap()
		require.Equal(t, int64(9), fields["dataID"])
		require.Equal(t, int64(1), fields["loadedKVs"])
		require.Equal(t, int64(0), fields["dataImportedKVs"])
		require.Equal(t, int64(0), fields["dataImportedBytes"])
		require.Equal(t, int64(1), fields["regionJobCount"])
		require.NotContains(t, fields, "len(jobs)")
	})

	t.Run("verify external import statistics logs comparison", func(t *testing.T) {
		core, observedLogs := observer.New(zapcore.InfoLevel)
		ctx := tidblogutil.WithLogger(context.Background(), zap.New(core))
		extEngine := external.NewExternalEngine(
			ctx,
			objstore.NewMemStorage(),
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			1,
			123,
			0,
			0,
			false,
			1<<30,
			engineapi.OnDuplicateKeyError,
			"",
		)

		require.NoError(t, verifyImportedStatistics(ctx, extEngine, 0))
		err := verifyImportedStatistics(ctx, extEngine, 1)
		require.ErrorContains(t, err, "imported length mismatch, expected 0, got 1")

		entries := observedLogs.FilterMessage("verify external imported statistics").All()
		require.Len(t, entries, 2)
		fields := entries[0].ContextMap()
		require.Equal(t, int64(0), fields["loadedKVs"])
		require.Equal(t, int64(0), fields["importedCount"])
		require.Equal(t, true, fields["importedLoadedMatch"])
		fields = entries[1].ContextMap()
		require.Equal(t, int64(0), fields["loadedKVs"])
		require.Equal(t, int64(1), fields["importedCount"])
		require.Equal(t, false, fields["importedLoadedMatch"])
	})

	t.Run("send job to out channel after run job", func(t *testing.T) {
		jobOutCh, err := prepareAndExecute(
			t, 1,
			&regionJob{stage: regionScanned, ingestData: mockIngestData{}, region: dummyRegion},
			nil, nil, nil)
		require.NoError(t, err)
		require.Equal(t, 1, len(jobOutCh))
		outJob := <-jobOutCh
		require.Equal(t, ingested, outJob.stage)
	})

	t.Run("if the region has no leader, rescan the region", func(t *testing.T) {
		job := &regionJob{stage: regionScanned, ingestData: mockIngestData{}, region: &split.RegionInfo{
			Region: &metapb.Region{Id: 1, Peers: []*metapb.Peer{{StoreId: 1}}},
		}}

		jobOutCh, err := prepareAndExecute(
			t, 3, job,
			nil, nil, nil)
		require.NoError(t, err)
		require.Equal(t, 3, len(jobOutCh))
		for range 3 {
			outJob := <-jobOutCh
			require.ErrorIs(t, outJob.lastRetryableErr, errdef.ErrNoLeader)
		}
	})

	t.Run("empty job", func(t *testing.T) {
		writeFn := func(ctx context.Context, job *regionJob) (*tikvWriteResult, error) {
			return &tikvWriteResult{emptyJob: true}, nil
		}
		jobOutCh, err := prepareAndExecute(
			t, 1,
			&regionJob{stage: regionScanned, ingestData: mockIngestData{}, region: dummyRegion},
			nil, writeFn, nil)
		require.NoError(t, err)
		require.Equal(t, 1, len(jobOutCh))
		outJob := <-jobOutCh
		require.Equal(t, ingested, outJob.stage)
	})

	t.Run("empty job write summary log includes diagnostic fields", func(t *testing.T) {
		core, observedLogs := observer.New(zapcore.InfoLevel)
		ctx := tidblogutil.WithLogger(context.Background(), zap.New(core))
		writeFn := func(ctx context.Context, job *regionJob) (*tikvWriteResult, error) {
			return &tikvWriteResult{emptyJob: true}, nil
		}
		jobOutCh, err := prepareAndExecuteWithCtx(
			t, ctx, 1,
			&regionJob{
				keyRange: engineapi.Range{Start: []byte("a"), End: []byte("b")},
				stage:    regionScanned,
				ingestData: mockDiagnosticIngestData{
					dataID:        11,
					loadedKVCount: 3,
				},
				region: dummyRegion,
			},
			nil, writeFn, nil)
		require.NoError(t, err)
		require.Equal(t, 1, len(jobOutCh))

		entries := observedLogs.FilterMessage("region job write summary").All()
		require.Len(t, entries, 1)
		fields := entries[0].ContextMap()
		require.Equal(t, int64(11), fields["dataID"])
		require.Equal(t, int64(3), fields["loadedKVs"])
		require.Equal(t, uint64(1), fields["regionID"])
		require.Equal(t, true, fields["emptyJob"])
		require.Equal(t, int64(0), fields["writtenKVs"])
		require.Equal(t, int64(0), fields["writtenBytes"])
	})

	t.Run("non-empty job write summary log includes write result", func(t *testing.T) {
		core, observedLogs := observer.New(zapcore.InfoLevel)
		ctx := tidblogutil.WithLogger(context.Background(), zap.New(core))
		writeFn := func(ctx context.Context, job *regionJob) (*tikvWriteResult, error) {
			return &tikvWriteResult{count: 3, totalBytes: 12}, nil
		}
		jobOutCh, err := prepareAndExecuteWithCtx(
			t, ctx, 1,
			&regionJob{
				keyRange: engineapi.Range{Start: []byte("a"), End: []byte("b")},
				stage:    regionScanned,
				ingestData: mockDiagnosticIngestData{
					dataID:          12,
					loadedKVCount:   5,
					importedKVCount: 1,
					importedKVSize:  6,
				},
				region: dummyRegion,
			},
			nil, writeFn, nil)
		require.NoError(t, err)
		require.Equal(t, 1, len(jobOutCh))

		entries := observedLogs.FilterMessage("region job write summary").All()
		require.Len(t, entries, 1)
		fields := entries[0].ContextMap()
		require.Equal(t, int64(12), fields["dataID"])
		require.Equal(t, int64(5), fields["loadedKVs"])
		require.Equal(t, int64(1), fields["dataImportedKVs"])
		require.Equal(t, int64(6), fields["dataImportedBytes"])
		require.Equal(t, false, fields["emptyJob"])
		require.Equal(t, int64(3), fields["writtenKVs"])
		require.Equal(t, int64(12), fields["writtenBytes"])
	})

	t.Run("meet non-retryable error during ingest", func(t *testing.T) {
		ingestFn := func(ctx context.Context, job *regionJob) error {
			return &ingestcli.IngestAPIError{Err: errdef.ErrKVDiskFull}
		}
		jobOutCh, err := prepareAndExecute(
			t, 1,
			&regionJob{stage: regionScanned, ingestData: mockIngestData{}, region: dummyRegion},
			nil, nil, ingestFn)
		require.ErrorIs(t, err, errdef.ErrKVDiskFull)
		require.Equal(t, 1, len(jobOutCh))
		outJob := <-jobOutCh
		// the job is left in wrote stage
		require.Equal(t, wrote, outJob.stage)
		require.Nil(t, outJob.lastRetryableErr)
	})

	t.Run("retry job from regionScanned", func(t *testing.T) {
		ingestFn := func(ctx context.Context, job *regionJob) error {
			return &ingestcli.IngestAPIError{Err: errdef.ErrKVIngestFailed}
		}
		jobOutCh, err := prepareAndExecute(
			t, 1,
			&regionJob{stage: regionScanned, ingestData: mockIngestData{}, region: dummyRegion},
			nil, nil, ingestFn)
		require.NoError(t, err)
		require.Equal(t, 1, len(jobOutCh))
		outJob := <-jobOutCh
		require.Equal(t, regionScanned, outJob.stage)
		require.ErrorIs(t, outJob.lastRetryableErr, errdef.ErrKVIngestFailed)
	})

	t.Run("retry job from regionScanned, and region got from the ingest error", func(t *testing.T) {
		ingestFn := func(ctx context.Context, job *regionJob) error {
			return &ingestcli.IngestAPIError{Err: errdef.ErrKVEpochNotMatch, NewRegion: &split.RegionInfo{Region: &metapb.Region{Id: 123}}}
		}
		jobOutCh, err := prepareAndExecute(
			t, 1,
			&regionJob{stage: regionScanned, ingestData: mockIngestData{}, region: dummyRegion},
			nil, nil, ingestFn)
		require.NoError(t, err)
		require.Equal(t, 1, len(jobOutCh))
		outJob := <-jobOutCh
		require.Equal(t, regionScanned, outJob.stage)
		require.ErrorIs(t, outJob.lastRetryableErr, errdef.ErrKVEpochNotMatch)
		require.Equal(t, &split.RegionInfo{Region: &metapb.Region{Id: 123}}, outJob.region)
	})

	t.Run("regenerate jobs", func(t *testing.T) {
		ingestFn := func(ctx context.Context, job *regionJob) error {
			return &ingestcli.IngestAPIError{Err: errdef.ErrKVNotLeader}
		}
		// put int 1 job, and generate 2 more jobs from regenerateFunc.
		jobOutCh, err := prepareAndExecute(
			t, 3,
			&regionJob{stage: regionScanned, ingestData: mockIngestData{}, region: dummyRegion},
			nil, nil, ingestFn)
		require.NoError(t, err)
		require.Equal(t, 3, len(jobOutCh))
	})

	t.Run("local write prewrite fail", func(t *testing.T) {
		preRunFn := func(ctx context.Context, job *regionJob) error {
			return errors.New("the remaining storage capacity of TiKV is less than 10%%; please increase the storage capacity of TiKV and try again")
		}
		_, err := prepareAndExecute(
			t, 1,
			&regionJob{stage: regionScanned, ingestData: mockIngestData{}, region: dummyRegion},
			preRunFn, nil, nil)
		require.Error(t, err)
		require.Regexp(t, "the remaining storage capacity of TiKV.*", err.Error())
	})
}

func TestIsRetryableTiKVWriteError(t *testing.T) {
	w := &regionJobBaseWorker{}
	require.True(t, w.isRetryableImportTiKVError(io.EOF))
	require.True(t, w.isRetryableImportTiKVError(errors.Trace(io.EOF)))
}

func TestCloudRegionJobWorker(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockIngestCli := ingestclimock.NewMockClient(ctrl)

	cloudW := &objStoreRegionJobWorker{
		regionJobBaseWorker: &regionJobBaseWorker{},
		ingestCli:           mockIngestCli,
		writeBatchSize:      8,
		bufPool:             nil,
	}
	cloudW.regionJobBaseWorker.writeFn = cloudW.write
	cloudW.regionJobBaseWorker.ingestFn = cloudW.ingest
	cloudW.regionJobBaseWorker.preRunJobFn = cloudW.preRunJob

	t.Run("empty job", func(t *testing.T) {
		job := &regionJob{
			keyRange:   engineapi.Range{Start: []byte("a"), End: []byte("z")},
			stage:      regionScanned,
			ingestData: mockIngestData{},
		}
		writeRes, err := cloudW.write(context.Background(), job)
		require.NoError(t, err)
		require.True(t, writeRes.emptyJob)
		require.True(t, ctrl.Satisfied())
	})

	t.Run("failed to create ingest client", func(t *testing.T) {
		job := &regionJob{
			keyRange:   engineapi.Range{Start: []byte("a"), End: []byte("z")},
			stage:      regionScanned,
			ingestData: mockIngestData{{[]byte("a"), []byte("a")}},
		}
		mockIngestCli.EXPECT().WriteClient(gomock.Any(), gomock.Any()).Return(nil, errors.New("mock error"))
		writeRes, err := cloudW.write(context.Background(), job)
		require.ErrorContains(t, err, "mock error")
		require.Nil(t, writeRes)
		require.True(t, ctrl.Satisfied())
	})

	t.Run("failed to write data", func(t *testing.T) {
		job := &regionJob{
			keyRange:   engineapi.Range{Start: []byte("a"), End: []byte("z")},
			stage:      regionScanned,
			ingestData: mockIngestData{{[]byte("a"), []byte("a")}},
		}
		writeCli := ingestclimock.NewMockWriteClient(ctrl)
		mockIngestCli.EXPECT().WriteClient(gomock.Any(), gomock.Any()).Return(writeCli, nil)
		writeCli.EXPECT().Write(gomock.Any()).Return(errors.New("mock error"))
		writeCli.EXPECT().Close()
		writeRes, err := cloudW.write(context.Background(), job)
		require.ErrorContains(t, err, "mock error")
		require.Nil(t, writeRes)
		require.True(t, ctrl.Satisfied())
	})

	t.Run("failed to closeAndRecv", func(t *testing.T) {
		job := &regionJob{
			keyRange:   engineapi.Range{Start: []byte("a"), End: []byte("z")},
			stage:      regionScanned,
			ingestData: mockIngestData{{[]byte("a"), []byte("a")}},
		}
		writeCli := ingestclimock.NewMockWriteClient(ctrl)
		mockIngestCli.EXPECT().WriteClient(gomock.Any(), gomock.Any()).Return(writeCli, nil)
		writeCli.EXPECT().Write(gomock.Any()).Return(nil)
		writeCli.EXPECT().Recv().Return(nil, errors.New("mock error"))
		writeCli.EXPECT().Close()
		resp, err := cloudW.write(context.Background(), job)
		require.ErrorContains(t, err, "mock error")
		require.Nil(t, resp)
		require.True(t, ctrl.Satisfied())
	})

	t.Run("write data success, and we have trailing pairs after iteration loop", func(t *testing.T) {
		job := &regionJob{
			keyRange: engineapi.Range{Start: []byte("a"), End: []byte("z")},
			stage:    regionScanned,
			ingestData: mockIngestData{
				{[]byte("aa"), []byte("aaaa")},
				{[]byte("ab"), []byte("abab")},
				{[]byte("ac"), []byte("acac")},
			},
		}
		writeCli := ingestclimock.NewMockWriteClient(ctrl)
		mockIngestCli.EXPECT().WriteClient(gomock.Any(), gomock.Any()).Return(writeCli, nil)
		writeCli.EXPECT().Write(gomock.Any()).Return(nil)
		writeCli.EXPECT().Write(gomock.Any()).Return(nil)
		writeCli.EXPECT().Recv().Return(&ingestcli.WriteResponse{}, nil)
		writeCli.EXPECT().Close()
		res, err := cloudW.write(context.Background(), job)
		require.NoError(t, err)
		require.EqualValues(t, 3, res.count)
		require.EqualValues(t, 18, res.totalBytes)
		require.True(t, ctrl.Satisfied())
	})

	t.Run("ingest failed", func(t *testing.T) {
		job := &regionJob{
			keyRange:    engineapi.Range{Start: []byte("a"), End: []byte("z")},
			stage:       wrote,
			ingestData:  mockIngestData{},
			writeResult: &tikvWriteResult{},
		}
		mockIngestCli.EXPECT().Ingest(gomock.Any(), gomock.Any()).Return(errors.New("mock error"))
		err := cloudW.ingest(context.Background(), job)
		require.ErrorContains(t, err, "mock error")
		require.True(t, ctrl.Satisfied())
	})

	t.Run("ingest success", func(t *testing.T) {
		job := &regionJob{
			keyRange:    engineapi.Range{Start: []byte("a"), End: []byte("z")},
			stage:       wrote,
			ingestData:  mockIngestData{},
			writeResult: &tikvWriteResult{},
		}
		mockIngestCli.EXPECT().Ingest(gomock.Any(), gomock.Any()).Return(nil)
		err := cloudW.ingest(context.Background(), job)
		require.NoError(t, err)
		require.True(t, ctrl.Satisfied())
	})
}
