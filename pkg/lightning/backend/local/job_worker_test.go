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
	"io"
	"sync"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/pkg/ingestor/engineapi"
	"github.com/pingcap/tidb/pkg/ingestor/ingestcli"
	ingestclimock "github.com/pingcap/tidb/pkg/ingestor/ingestcli/mock"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestRegionJobBaseWorker(t *testing.T) {
	newWorker := func() *regionJobBaseWorker {
		return &regionJobBaseWorker{
			jobInCh:  make(chan *regionJob, 10),
			jobOutCh: make(chan *regionJob, 10),
			jobWg:    &sync.WaitGroup{},
			preRunJobFn: func(ctx context.Context, job *regionJob) error {
				return nil
			},
			writeFn: func(ctx context.Context, job *regionJob) (*tikvWriteResult, error) {
				return &tikvWriteResult{}, nil
			},
			ingestFn: func(ctx context.Context, job *regionJob) error {
				return nil
			},
			regenerateJobsFn: func(
				ctx context.Context, data engineapi.IngestData, sortedJobRanges []engineapi.Range,
				regionSplitSize, regionSplitKeys int64,
			) ([]*regionJob, error) {
				return []*regionJob{
					{}, {}, {},
				}, nil
			},
		}
	}

	// all below tests are for basic functionality of the worker, there are other
	// tests inside local_test.go.
	// to fully run a region job, we also need the job retryer and the routine to
	// receive executed job, so we will not check some invariants, like jobWg must
	// be 0 after run.

	t.Run("run on closed channel", func(t *testing.T) {
		w := newWorker()
		close(w.jobInCh)
		require.NoError(t, w.run(context.Background()))
	})

	t.Run("send job to out channel after run job", func(t *testing.T) {
		w := newWorker()
		job := &regionJob{stage: regionScanned, ingestData: mockIngestData{}}
		w.jobInCh <- job
		close(w.jobInCh)
		require.NoError(t, w.run(context.Background()))
		require.Equal(t, 1, len(w.jobOutCh))
		outJob := <-w.jobOutCh
		require.Equal(t, ingested, outJob.stage)
	})

	t.Run("empty job", func(t *testing.T) {
		w := newWorker()
		w.writeFn = func(ctx context.Context, job *regionJob) (*tikvWriteResult, error) {
			return &tikvWriteResult{emptyJob: true}, nil
		}
		job := &regionJob{stage: regionScanned, ingestData: mockIngestData{}}
		w.jobInCh <- job
		close(w.jobInCh)
		require.NoError(t, w.run(context.Background()))
		require.Equal(t, 1, len(w.jobOutCh))
		outJob := <-w.jobOutCh
		require.Equal(t, ingested, outJob.stage)
	})

	t.Run("meet non-retryable error during ingest", func(t *testing.T) {
		w := newWorker()
		w.ingestFn = func(ctx context.Context, job *regionJob) error {
			return &ingestAPIError{err: common.ErrKVDiskFull}
		}
		job := &regionJob{stage: regionScanned, ingestData: mockIngestData{}}
		w.jobInCh <- job
		close(w.jobInCh)
		require.ErrorIs(t, w.run(context.Background()), common.ErrKVDiskFull)
		require.Equal(t, 1, len(w.jobOutCh))
		outJob := <-w.jobOutCh
		// the job is left in wrote stage
		require.Equal(t, wrote, outJob.stage)
		require.Nil(t, outJob.lastRetryableErr)
	})

	t.Run("retry job from regionScanned", func(t *testing.T) {
		w := newWorker()
		w.ingestFn = func(ctx context.Context, job *regionJob) error {
			return &ingestAPIError{err: common.ErrKVIngestFailed}
		}
		job := &regionJob{stage: regionScanned, ingestData: mockIngestData{}}
		w.jobInCh <- job
		close(w.jobInCh)
		require.NoError(t, w.run(context.Background()))
		require.Equal(t, 1, len(w.jobOutCh))
		outJob := <-w.jobOutCh
		require.Equal(t, regionScanned, outJob.stage)
		require.ErrorIs(t, outJob.lastRetryableErr, common.ErrKVIngestFailed)
	})

	t.Run("retry job from regionScanned, and region got from the ingest error", func(t *testing.T) {
		w := newWorker()
		w.ingestFn = func(ctx context.Context, job *regionJob) error {
			return &ingestAPIError{err: common.ErrKVEpochNotMatch, newRegion: &split.RegionInfo{Region: &metapb.Region{Id: 123}}}
		}
		job := &regionJob{stage: regionScanned, ingestData: mockIngestData{}}
		w.jobInCh <- job
		close(w.jobInCh)
		require.NoError(t, w.run(context.Background()))
		require.Equal(t, 1, len(w.jobOutCh))
		outJob := <-w.jobOutCh
		require.Equal(t, regionScanned, outJob.stage)
		require.ErrorIs(t, outJob.lastRetryableErr, common.ErrKVEpochNotMatch)
		require.Equal(t, &split.RegionInfo{Region: &metapb.Region{Id: 123}}, outJob.region)
	})

	t.Run("regenerate jobs", func(t *testing.T) {
		w := newWorker()
		w.ingestFn = func(ctx context.Context, job *regionJob) error {
			return &ingestAPIError{err: common.ErrKVNotLeader}
		}
		job := &regionJob{stage: regionScanned, ingestData: mockIngestData{}}
		w.jobInCh <- job
		close(w.jobInCh)
		require.NoError(t, w.run(context.Background()))
		require.Equal(t, 3, len(w.jobOutCh))
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
