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
	"github.com/pingcap/tidb/pkg/resourcemanager/pool/workerpool"
	rcmgrutil "github.com/pingcap/tidb/pkg/resourcemanager/util"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

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

	prepareAndExecute := func(
		t *testing.T,
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
		workGroup, workerCtx := util.NewErrorGroupWithRecoverWithCtx(context.Background())
		pool, jobWg, jobInCh, jobOutCh := newRegionJobWorkerPoolForTest(workerCtx, preRunFn, writeFn, ingestFn)

		wctx := workerpool.NewContext(workerCtx)
		workGroup.Go(func() error {
			pool.Start(wctx)
			<-wctx.Done()
			pool.Release()
			return wctx.OperatorErr()
		})

		workGroup.Go(func() error {
			jobInCh <- job
			jobWg.Add(1)
			close(jobInCh)
			jobWg.Wait()

			wctx.Cancel()
			return nil
		})

		err := workGroup.Wait()
		require.Equal(t, 0, len(jobInCh))
		return jobOutCh, err
	}

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
