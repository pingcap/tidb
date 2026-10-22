// Copyright 2024 PingCAP, Inc.
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

package backup

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
)

type MockBackupClient struct {
	backuppb.BackupClient

	recvFunc func(context.Context) (*backuppb.BackupResponse, error)
}

func (mbc *MockBackupClient) Backup(ctx context.Context, _ *backuppb.BackupRequest, _ ...grpc.CallOption) (backuppb.Backup_BackupClient, error) {
	return &MockBackupBackupClient{ctx: ctx, recvFunc: mbc.recvFunc}, nil
}

type MockBackupBackupClient struct {
	backuppb.Backup_BackupClient

	ctx      context.Context
	recvFunc func(context.Context) (*backuppb.BackupResponse, error)
}

func (mbbc *MockBackupBackupClient) CloseSend() error {
	return nil
}

func (mbbc *MockBackupBackupClient) Recv() (*backuppb.BackupResponse, error) {
	if mbbc.recvFunc != nil {
		return mbbc.recvFunc(mbbc.ctx)
	}
	return &backuppb.BackupResponse{}, nil
}

func TestTimeoutRecv(t *testing.T) {
	ctx := context.Background()
	originTimeout := TimeoutOneResponse
	TimeoutOneResponse = time.Millisecond * 800
	t.Cleanup(func() {
		TimeoutOneResponse = originTimeout
	})
	recordTimeoutErr := func(ctx context.Context, timeoutObserved chan<- bool) error {
		err := ctx.Err()
		timeoutObserved <- err != nil
		if err != nil {
			return err
		}
		return context.Canceled
	}
	// Just Timeout Once
	{
		timeoutObserved := make(chan bool, 1)
		err := startBackup(ctx, 0, NewResourceMemoryLimiter(100), backuppb.BackupRequest{}, &MockBackupClient{
			recvFunc: func(ctx context.Context) (*backuppb.BackupResponse, error) {
				time.Sleep(time.Second)
				return nil, recordTimeoutErr(ctx, timeoutObserved)
			},
		}, 1, nil)
		require.Error(t, err)
		require.True(t, <-timeoutObserved)
	}

	// Timeout Not At First
	{
		count := 0
		timeoutObserved := make(chan bool, 1)
		err := startBackup(ctx, 0, NewResourceMemoryLimiter(100), backuppb.BackupRequest{}, &MockBackupClient{
			recvFunc: func(ctx context.Context) (*backuppb.BackupResponse, error) {
				if err := ctx.Err(); err != nil {
					timeoutObserved <- true
					return nil, err
				}
				if count == 15 {
					time.Sleep(time.Second)
					return nil, recordTimeoutErr(ctx, timeoutObserved)
				}
				count += 1
				time.Sleep(time.Millisecond * 80)
				return &backuppb.BackupResponse{}, nil
			},
		}, 1, make(chan *ResponseAndStore, 15))
		require.Error(t, err)
		require.Equal(t, count, 15)
		require.True(t, <-timeoutObserved)
	}
}

func TestTimeoutRecvCancel(t *testing.T) {
	ctx := context.Background()
	cctx, cancel := context.WithCancel(ctx)

	_, trecv := StartTimeoutRecv(cctx, time.Hour, 0)
	cancel()
	trecv.wg.Wait()
}

func TestTimeoutRecvCanceled(t *testing.T) {
	ctx := context.Background()
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	tctx, trecv := StartTimeoutRecv(cctx, time.Hour, 0)
	trecv.Stop()
	require.Equal(t, "context canceled", tctx.Err().Error())
}

func TestBackupResponseObservedBeforeCancellation(t *testing.T) {
	for _, test := range []struct {
		name     string
		errPb    *backuppb.Error
		category string
		code     string
	}{
		{
			name:     "memory pressure",
			errPb:    &backuppb.Error{Msg: "resource is not enough Memory usage too high, usage: 512 bytes, mem limit 256 bytes"},
			category: "tikv_memory_pressure",
			code:     "BR:KV:ErrKVStorage",
		},
		{
			name:     "disk full",
			errPb:    &backuppb.Error{Detail: &backuppb.Error_RegionError{RegionError: &errorpb.Error{DiskFull: &errorpb.DiskFull{}}}},
			category: "disk_full",
			code:     "BR:KV:ErrKVDiskFull",
		},
		{
			name:  "lock resolution",
			errPb: &backuppb.Error{Detail: &backuppb.Error_KvError{KvError: &kvrpcpb.KeyError{Locked: &kvrpcpb.LockInfo{}}}},
		},
		{
			name:     "cluster mismatch",
			errPb:    &backuppb.Error{Detail: &backuppb.Error_ClusterIdError{ClusterIdError: &backuppb.ClusterIDError{}}},
			category: "configuration",
			code:     "BR:KV:ErrKVClusterIDMismatch",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			summary.InitCollector(false)
			t.Cleanup(func() { summary.InitCollector(false) })
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			err := startBackup(ctx, 7, NewResourceMemoryLimiter(100), backuppb.BackupRequest{}, &MockBackupClient{
				recvFunc: func(context.Context) (*backuppb.BackupResponse, error) {
					cancel()
					return &backuppb.BackupResponse{Error: test.errPb}, nil
				},
			}, 1, nil)
			require.ErrorIs(t, err, context.Canceled)
			encoder := zapcore.NewMapObjectEncoder()
			for _, field := range summary.FailureFields(err, func(text string) string { return text }) {
				field.AddTo(encoder)
			}
			require.Equal(t, "canceled", encoder.Fields["error-category"])
			if test.category == "" {
				require.NotContains(t, encoder.Fields, "observed-errors")
				return
			}
			data, err := json.Marshal(encoder.Fields["observed-errors"])
			require.NoError(t, err)
			var observations []map[string]any
			require.NoError(t, json.Unmarshal(data, &observations))
			require.Len(t, observations, 1)
			require.Equal(t, test.category, observations[0]["category"])
			require.Equal(t, test.code, observations[0]["error-code"])
			require.Equal(t, float64(7), observations[0]["store-id"])
			require.Equal(t, float64(1), observations[0]["count"])
		})
	}
}
