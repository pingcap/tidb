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

package metering

import (
	"context"
	goerrors "errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/pingcap/metering_sdk/common"
	mconfig "github.com/pingcap/metering_sdk/config"
	meteringreader "github.com/pingcap/metering_sdk/reader/metering"
	"github.com/pingcap/metering_sdk/storage"
	meteringwriter "github.com/pingcap/metering_sdk/writer/metering"
	writermock "github.com/pingcap/metering_sdk/writer/mock"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

func setupMeterForTest(t *testing.T, meter *Meter) {
	t.Helper()
	SetMetering(meter)
	t.Cleanup(func() {
		meteringInstance.Store(nil)
	})
}

func TestNewMeterEmptyBucket(t *testing.T) {
	cfg := &mconfig.MeteringConfig{}
	m, err := NewMeter(cfg)
	require.Nil(t, m)
	require.NoError(t, err)
}

func TestNewMeterValidConfig(t *testing.T) {
	cfg := &mconfig.MeteringConfig{
		Bucket: "test-bucket",
		Type:   "s3",
		AWS: &mconfig.MeteringAWSConfig{
			AssumeRoleARN: "test-role-arn",
		},
	}
	m, err := NewMeter(cfg)
	require.NotNil(t, m)
	require.NoError(t, err)
	require.NotEmpty(t, m.uuid)
}

func TestMeterRegisterUnregisterRecorderInClassic(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("only test it on classic")
	}
	require.Nil(t, meteringInstance.Load())
	RegisterRecorder(&proto.TaskBase{ID: 1})
	require.Nil(t, meteringInstance.Load())

	meter := newMeterWithWriter(nil, nil)
	setupMeterForTest(t, meter)
	RegisterRecorder(&proto.TaskBase{ID: 1})
	require.Empty(t, meter.recorders)
}

func TestMeterRegisterUnregisterRecorder(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("metering is a feature of nextgen")
	}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	mockWriter := writermock.NewMockMeteringWriter(ctrl)
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	t.Run("recorder still there after flush", func(t *testing.T) {
		meter := newMeterWithWriter(logger, mockWriter)
		setupMeterForTest(t, meter)
		r := RegisterRecorder(&proto.TaskBase{ID: 1})
		require.Contains(t, meter.recorders, int64(1))
		r.objStoreAccess.Requests.Get.Add(1)
		mockWriter.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil)
		meter.flush(ctx, 1000000)
		require.Contains(t, meter.recorders, int64(1))
		require.True(t, ctrl.Satisfied())
	})

	t.Run("if recorder unregistered before flush, it should be removed after flush", func(t *testing.T) {
		meter := newMeterWithWriter(logger, mockWriter)
		setupMeterForTest(t, meter)
		r := RegisterRecorder(&proto.TaskBase{ID: 1})
		require.Contains(t, meter.recorders, int64(1))
		r.objStoreAccess.Requests.Get.Add(1)
		UnregisterRecorder(1)
		require.Contains(t, meter.recorders, int64(1))
		require.True(t, meter.recorders[1].unregistered)
		mockWriter.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil)
		meter.flush(ctx, 1000000)
		require.NotContains(t, meter.recorders, int64(1))
		require.True(t, ctrl.Satisfied())

		// when no meter data to flush, unregistered recorder should also be removed
		RegisterRecorder(&proto.TaskBase{ID: 2})
		require.Contains(t, meter.recorders, int64(2))
		UnregisterRecorder(2)
		meter.flush(ctx, 2000000)
		require.NotContains(t, meter.recorders, int64(2))
		require.True(t, ctrl.Satisfied())
	})

	t.Run("unregistered recorder should only be removed after all data are scraped and flushed", func(t *testing.T) {
		meter := newMeterWithWriter(logger, mockWriter)
		setupMeterForTest(t, meter)
		r := RegisterRecorder(&proto.TaskBase{ID: 1})
		require.Contains(t, meter.recorders, int64(1))
		r.objStoreAccess.Requests.Get.Add(1)
		mockWriter.EXPECT().Write(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, data any) error {
			md := data.(*common.MeteringData)
			require.EqualValues(t, 1, md.Data[0][getRequestsField])
			require.NotContains(t, md.Data[0], putRequestsField)
			r.objStoreAccess.Requests.Put.Add(2)
			// unregister after scrape, but before write, the afterFlush
			// shouldn't remove it
			UnregisterRecorder(1)
			return nil
		})
		meter.flush(ctx, 1000000)
		require.Contains(t, meter.recorders, int64(1))
		require.True(t, ctrl.Satisfied())

		// next flush should remove it
		mockWriter.EXPECT().Write(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, data any) error {
			md := data.(*common.MeteringData)
			require.EqualValues(t, 2, md.Data[0][putRequestsField])
			return nil
		})
		meter.flush(ctx, 2000000)
		require.NotContains(t, meter.recorders, int64(1))
		require.True(t, ctrl.Satisfied())
	})

	t.Run("if recorder re-registered before we do cleanup, it should not be removed", func(t *testing.T) {
		meter := newMeterWithWriter(logger, mockWriter)
		setupMeterForTest(t, meter)
		r := RegisterRecorder(&proto.TaskBase{ID: 1})
		require.Contains(t, meter.recorders, int64(1))
		r.objStoreAccess.Requests.Get.Add(1)
		UnregisterRecorder(1)
		require.Contains(t, meter.recorders, int64(1))
		require.True(t, meter.recorders[1].unregistered)
		mockWriter.EXPECT().Write(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, data any) error {
			md := data.(*common.MeteringData)
			require.EqualValues(t, 1, md.Data[0][getRequestsField])
			require.NotContains(t, md.Data[0], putRequestsField)
			// re-register before afterFlush do cleanup
			RegisterRecorder(&proto.TaskBase{ID: 1})
			require.False(t, meter.recorders[1].unregistered)
			return nil
		})
		meter.flush(ctx, 1000000)
		require.Contains(t, meter.recorders, int64(1))
		require.True(t, ctrl.Satisfied())
	})

	t.Run("if we register again after unregister, data should be start from zero", func(t *testing.T) {
		meter := newMeterWithWriter(logger, mockWriter)
		setupMeterForTest(t, meter)
		r := RegisterRecorder(&proto.TaskBase{ID: 1})
		r.clusterTraffic.Read.Add(123456789)
		mockWriter.EXPECT().Write(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, data any) error {
			md := data.(*common.MeteringData)
			require.EqualValues(t, 123456789, md.Data[0][clusterReadBytesField])
			require.NotContains(t, md.Data[0], putRequestsField)
			UnregisterRecorder(1)
			require.True(t, meter.recorders[1].unregistered)
			return nil
		})
		meter.flush(ctx, 1000000)
		require.NotContains(t, meter.lastFlushedData, int64(1))
		require.NotContains(t, meter.recorders, int64(1))

		// register again, data should start from zero
		r = RegisterRecorder(&proto.TaskBase{ID: 1})
		r.clusterTraffic.Read.Add(123)
		mockWriter.EXPECT().Write(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, data any) error {
			md := data.(*common.MeteringData)
			require.EqualValues(t, 123, md.Data[0][clusterReadBytesField])
			require.False(t, meter.recorders[1].unregistered)
			return nil
		})
		meter.flush(ctx, 2000000)
		require.Contains(t, meter.lastFlushedData, int64(1))
		require.Contains(t, meter.recorders, int64(1))

		UnregisterRecorder(1)
		meter.flush(ctx, 3000000)
		require.NotContains(t, meter.lastFlushedData, int64(1))
		require.NotContains(t, meter.recorders, int64(1))
	})
}

func checkMeterData(t *testing.T, expected, got map[string]any) {
	t.Helper()
	for k, v := range expected {
		require.EqualValues(t, v, got[k], "field %s not equal", k)
	}
	for _, f := range []string{getRequestsField, putRequestsField,
		objStoreReadBytesField, objStoreWriteBytesField,
		clusterReadBytesField, clusterWriteBytesField} {
		if _, ok := expected[f]; !ok {
			require.NotContains(t, got, f, "field %s should not exist", f)
		}
	}
}

func TestMeterFlush(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("metering is a feature of nextgen")
	}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	mockWriter := writermock.NewMockMeteringWriter(ctrl)
	logger := zap.Must(zap.NewDevelopment())

	t.Run("normal flush", func(t *testing.T) {
		meter := newMeterWithWriter(logger, mockWriter)
		setupMeterForTest(t, meter)
		r := RegisterRecorder(&proto.TaskBase{ID: 1})
		require.Contains(t, meter.recorders, int64(1))
		r.objStoreAccess.Requests.Get.Add(1)
		r.objStoreAccess.Requests.Put.Add(2)
		r.objStoreAccess.RecRead(11)
		r.objStoreAccess.RecWrite(22)
		r.IncClusterReadBytes(3)
		r.IncClusterWriteBytes(4)
		mockWriter.EXPECT().Write(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, data any) error {
			md := data.(*common.MeteringData)
			require.Len(t, md.Data, 1)
			checkMeterData(t, map[string]any{
				getRequestsField:        1,
				putRequestsField:        2,
				objStoreReadBytesField:  11,
				objStoreWriteBytesField: 22,
				clusterReadBytesField:   3,
				clusterWriteBytesField:  4,
			}, md.Data[0])
			return nil
		})
		meter.flush(ctx, 1000000)
		require.True(t, ctrl.Satisfied())

		// flush again with no new data
		meter.flush(ctx, 2000000)
		require.True(t, ctrl.Satisfied())

		// flush with new data, only new data are written.
		r.objStoreAccess.Requests.Put.Add(100)
		mockWriter.EXPECT().Write(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, data any) error {
			md := data.(*common.MeteringData)
			require.Len(t, md.Data, 1)
			checkMeterData(t, map[string]any{
				putRequestsField: 100,
			}, md.Data[0])
			return nil
		})
		meter.flush(ctx, 3000000)
		require.True(t, ctrl.Satisfied())
	})

	t.Run("if we failed to write, next write should not accumulate data, those should retry later", func(t *testing.T) {
		meter := newMeterWithWriter(logger, mockWriter)
		setupMeterForTest(t, meter)
		r := RegisterRecorder(&proto.TaskBase{ID: 1})
		require.Contains(t, meter.recorders, int64(1))
		r.objStoreAccess.Requests.Get.Add(1)
		require.Empty(t, meter.lastFlushedData)
		mockWriter.EXPECT().Write(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, data any) error {
			return goerrors.New("some err")
		})
		meter.flush(ctx, 1000000)
		// update lastFlushedData even flush failed
		require.Contains(t, meter.lastFlushedData, int64(1))
		require.EqualValues(t, 1, meter.lastFlushedData[1].getRequests)
		// add to pending retry data
		require.Contains(t, meter.pendingRetryData.data, int64(1000000))
		require.True(t, ctrl.Satisfied())

		// flush again, we should write the incremental data.
		r.objStoreAccess.Requests.Get.Add(3)
		mockWriter.EXPECT().Write(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, data any) error {
			md := data.(*common.MeteringData)
			require.Len(t, md.Data, 1)
			checkMeterData(t, map[string]any{
				getRequestsField: 3,
			}, md.Data[0])
			return nil
		})
		meter.flush(ctx, 2000000)
		require.Contains(t, meter.lastFlushedData, int64(1))
		require.EqualValues(t, 4, meter.lastFlushedData[1].getRequests)
		require.True(t, ctrl.Satisfied())
	})
}

func TestMeterRetryWrite(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("metering is a feature of nextgen")
	}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	mockWriter := writermock.NewMockMeteringWriter(ctrl)
	logger := zap.Must(zap.NewDevelopment())

	t.Run("no pending retry data", func(t *testing.T) {
		meter := newMeterWithWriter(logger, mockWriter)
		require.Empty(t, meter.pendingRetryData.data)
		meter.retryWrite(ctx)
		require.Empty(t, meter.pendingRetryData.data)
		require.True(t, ctrl.Satisfied())
	})

	t.Run("3 pending items, 1st keeps failing and finally dropped, 2nd succeeds and removed, 3rd success after retry 5 times", func(t *testing.T) {
		meter := newMeterWithWriter(logger, mockWriter)
		meter.pendingRetryData.addFailedData(1000000, []map[string]any{{"get_requests": 12}})
		meter.pendingRetryData.addFailedData(2000000, []map[string]any{{"get_requests": 23}})
		meter.pendingRetryData.addFailedData(3000000, []map[string]any{{"get_requests": 34}})
		require.Len(t, meter.pendingRetryData.data, 3)
		thirdRetryCnt := 0
		mockWriter.EXPECT().Write(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, data any) error {
			md := data.(*common.MeteringData)
			require.Len(t, md.Data, 1)
			switch md.Timestamp {
			case 1000000:
				return goerrors.New("some err")
			case 2000000:
			case 3000000:
				thirdRetryCnt++
				if thirdRetryCnt < 5 {
					return goerrors.New("some err")
				}
			}
			return nil
		}).Times(16)
		for r := range 10 {
			meter.retryWrite(ctx)
			retryCnt := r + 1
			if retryCnt < 5 {
				require.Len(t, meter.pendingRetryData.data, 2)
				require.EqualValues(t, retryCnt, meter.pendingRetryData.data[1000000].retryCnt)
				require.EqualValues(t, retryCnt, meter.pendingRetryData.data[3000000].retryCnt)
			} else if retryCnt == 10 {
				// after 10 retries, the first item should be dropped
				require.Empty(t, meter.pendingRetryData.data)
			} else {
				require.Len(t, meter.pendingRetryData.data, 1)
				require.EqualValues(t, retryCnt, meter.pendingRetryData.data[1000000].retryCnt)
			}
		}
		require.True(t, ctrl.Satisfied())
	})
}

func TestMeterStartFlushLoop(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("metering is a feature of nextgen")
	}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	logger := zap.Must(zap.NewDevelopment())
	runOneTest := func(t *testing.T, runRecordersFn func() map[int64]uint64, writeFn func(ctx context.Context, data any) error) map[int64]uint64 {
		ctx, cancel := context.WithCancel(context.Background())
		mockWriter := writermock.NewMockMeteringWriter(ctrl)
		mockWriter.EXPECT().Write(gomock.Any(), gomock.Any()).DoAndReturn(writeFn).AnyTimes()
		mockWriter.EXPECT().Close().Return(nil)
		meter := newMeterWithWriter(logger, mockWriter)
		setupMeterForTest(t, meter)
		var (
			wg  util.WaitGroupWrapper
			res map[int64]uint64
		)
		synctest.Test(t, func(t *testing.T) {
			wg.RunWithLog(func() {
				res = runRecordersFn()
			})
			wg.RunWithLog(func() {
				meter.StartFlushLoop(ctx)
			})
			wg.RunWithLog(func() {
				time.Sleep(time.Hour)
				cancel()
			})
			wg.Wait()
		})
		// all pending data should be handled
		require.Empty(t, meter.pendingRetryData.data)
		require.True(t, ctrl.Satisfied())
		return res
	}

	var (
		writeMu           sync.Mutex
		shouldFailTS      = make(map[int64]int)
		written           = make(map[int64]uint64)
		firstTimeForTask3 int64
		lostDataForTask3  uint64
	)
	mockWriteFn := func(ctx context.Context, data any) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		md := data.(*common.MeteringData)
		if _, ok := shouldFailTS[md.Timestamp]; !ok {
			shouldFailTS[md.Timestamp] = 0
		}
		onlyTask3 := len(md.Data) == 1 && md.Data[0]["task_id"].(int64) == 3
		if onlyTask3 && firstTimeForTask3 == 0 {
			firstTimeForTask3 = md.Timestamp
		}
		cnt := shouldFailTS[md.Timestamp]
		// for task 1/2, we succeed after 5 tries, for task 3 and its first write,
		// we always fail.
		if cnt < 5 {
			shouldFailTS[md.Timestamp]++
			return goerrors.New("some err")
		} else if onlyTask3 && md.Timestamp == firstTimeForTask3 {
			lostDataForTask3 = md.Data[0][getRequestsField].(uint64)
			return goerrors.New("some err")
		}
		for _, d := range md.Data {
			written[d["task_id"].(int64)] += d[getRequestsField].(uint64)
		}
		return nil
	}
	runRecordersFn := func() map[int64]uint64 {
		r1 := RegisterRecorder(&proto.TaskBase{ID: 1})
		r2 := RegisterRecorder(&proto.TaskBase{ID: 2})
		// keeps adding data for the first 40 minutes
		for range 80 {
			cnt := uint64(11)
			r1.objStoreAccess.Requests.Get.Add(cnt)
			r2.objStoreAccess.Requests.Get.Add(cnt * 3)
			time.Sleep(30 * time.Second)
		}
		UnregisterRecorder(1)
		UnregisterRecorder(2)
		time.Sleep(10 * time.Minute)
		// after 40+10 minutes, add data for r3 for 100 seconds
		r3 := RegisterRecorder(&proto.TaskBase{ID: 3})
		for range 10 {
			cnt := uint64(23)
			r3.objStoreAccess.Requests.Get.Add(cnt)
			time.Sleep(10 * time.Second)
		}
		UnregisterRecorder(3)
		return map[int64]uint64{
			1: r1.objStoreAccess.Requests.Get.Load(),
			2: r2.objStoreAccess.Requests.Get.Load(),
			3: r3.objStoreAccess.Requests.Get.Load(),
		}
	}
	expected := runOneTest(t, runRecordersFn, mockWriteFn)
	require.Equal(t, map[int64]uint64{
		1: 880,
		2: 2640,
		3: 230,
	}, expected)
	require.Equal(t, len(expected), len(written))
	require.Equal(t, expected[1], written[1])
	require.Equal(t, expected[2], written[2])
	require.Greater(t, lostDataForTask3, uint64(0))
	require.Equal(t, expected[3]-lostDataForTask3, written[3])
}

func TestMeterClose(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("metering is a feature of nextgen")
	}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockWriter := writermock.NewMockMeteringWriter(ctrl)
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	t.Run("normal close", func(t *testing.T) {
		meter := newMeterWithWriter(logger, mockWriter)
		setupMeterForTest(t, meter)
		mockWriter.EXPECT().Close().Return(nil)
		require.NoError(t, meter.Close())
	})

	t.Run("close with error", func(t *testing.T) {
		meter := newMeterWithWriter(logger, mockWriter)
		setupMeterForTest(t, meter)
		mockWriter.EXPECT().Close().Return(goerrors.New("some err"))
		require.ErrorContains(t, meter.Close(), "some err")
	})
}

func TestMeterSimpleFlushAndReadBack(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("metering is a feature of nextgen")
	}
	meter, reader := createLocalMeter(t, t.TempDir())
	setupMeterForTest(t, meter)
	curTime := time.Now()

	writeTime := curTime.Truncate(time.Minute).Add(time.Minute)
	data := readMeteringData(t, reader, writeTime.Add(-time.Minute).Unix())
	require.Len(t, data, 0)
	recorder := RegisterRecorder(&proto.TaskBase{ID: 1, Keyspace: "ks1"})
	recorder.objStoreAccess.Requests.Get.Add(10)
	recorder.objStoreAccess.Requests.Put.Add(20)
	recorder.objStoreAccess.RecRead(11)
	recorder.objStoreAccess.RecWrite(22)
	recorder.IncClusterReadBytes(300)
	recorder.IncClusterWriteBytes(400)
	meter.flush(context.Background(), writeTime.Unix())
	data = readMeteringData(t, reader, writeTime.Add(-time.Minute).Unix())
	require.Len(t, data, 0)
	data = readMeteringData(t, reader, writeTime.Unix())
	require.Len(t, data, 1)
	require.Equal(t, "1", data[0]["version"])
	require.Equal(t, "ks1", data[0]["cluster_id"])
	require.Equal(t, "dxf", data[0]["source_name"])
	require.Equal(t, float64(10), data[0][getRequestsField].(float64))
	require.Equal(t, float64(20), data[0][putRequestsField].(float64))
	require.Equal(t, float64(11), data[0][objStoreReadBytesField].(float64))
	require.Equal(t, float64(22), data[0][objStoreWriteBytesField].(float64))
	require.Equal(t, float64(300), data[0][clusterReadBytesField].(float64))
	require.Equal(t, float64(400), data[0][clusterWriteBytesField].(float64))
}

func createLocalMeter(t *testing.T, dir string) (*Meter, *meteringreader.MeteringReader) {
	t.Helper()
	meterConfig := &mconfig.MeteringConfig{
		Type:   "s3",
		Bucket: "bucket",
		AWS: &mconfig.MeteringAWSConfig{
			AssumeRoleARN: "test-role-arn",
		},
	}
	m, err := NewMeter(meterConfig)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, m.Close())
	})

	// Replace the S3 writer with the local writer.
	localConfig := &storage.ProviderConfig{
		Type: storage.ProviderTypeLocalFS,
		LocalFS: &storage.LocalFSConfig{
			BasePath:   dir,
			CreateDirs: true,
		},
	}
	provider, err := storage.NewObjectStorageProvider(localConfig)
	require.NoError(t, err)
	cfg := mconfig.DefaultConfig().WithLogger(m.logger)
	m.writer = meteringwriter.NewMeteringWriterFromConfig(provider, cfg, meterConfig)
	reader := meteringreader.NewMeteringReader(provider, cfg)
	t.Cleanup(func() {
		require.NoError(t, reader.Close())
	})
	return m, reader
}

func readMeteringData(t *testing.T, reader *meteringreader.MeteringReader, ts int64) []map[string]any {
	_, err := reader.ListFilesByTimestamp(context.Background(), ts)
	require.NoError(t, err)

	categories, err := reader.GetCategories(context.Background(), ts)
	require.NoError(t, err)
	if len(categories) == 0 {
		return nil
	}

	category := categories[0]
	categoryFiles, err := reader.GetFilesByCategory(context.Background(), ts, category)
	require.NoError(t, err)
	if len(categoryFiles) == 0 {
		return nil
	}

	filePath := categoryFiles[0]
	meteringData, err := reader.ReadFile(context.Background(), filePath)
	require.NoError(t, err)
	return meteringData.Data
}
