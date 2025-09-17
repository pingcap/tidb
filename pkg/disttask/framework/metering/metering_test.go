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
	"sync"
	"testing"
	"time"

	mconfig "github.com/pingcap/metering_sdk/config"
	meteringreader "github.com/pingcap/metering_sdk/reader/metering"
	"github.com/pingcap/metering_sdk/storage"
	meteringwriter "github.com/pingcap/metering_sdk/writer/metering"
	"github.com/stretchr/testify/require"
)

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

func TestRecord(t *testing.T) {
	m := &Meter{
		data: make(map[int64]Data),
	}
	md := &Data{
		putRequests: 5,
	}
	m.Record("keyspace1", TaskTypeAddIndex, 1, md)
	require.Equal(t, uint64(5), m.data[1].putRequests)
	m.Record("keyspace1", TaskTypeAddIndex, 1, &Data{getRequests: 3})
	require.Equal(t, uint64(5), m.data[1].putRequests)
	require.Equal(t, uint64(3), m.data[1].getRequests)
}

func TestConcurrentRecord(t *testing.T) {
	m := &Meter{data: make(map[int64]Data)}
	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			m.Record("ks1", TaskTypeAddIndex, 1, &Data{putRequests: 1})
			wg.Done()
		}()
	}
	wg.Wait()
	require.Equal(t, uint64(100), m.data[1].putRequests)
}

func TestFlush(t *testing.T) {
	meter, reader := createLocalMeter(t, t.TempDir())
	ts := time.Now().Unix()/writeInterval*writeInterval + writeInterval
	data := readMeteringData(t, reader, ts-writeInterval)
	require.Len(t, data, 0)
	meter.Record("ks1", TaskTypeAddIndex, 1, &Data{putRequests: 10, getRequests: 20, readDataBytes: 300, writeDataBytes: 400})
	meter.flush(ts)
	data = readMeteringData(t, reader, ts-writeInterval)
	require.Len(t, data, 0)
	data = readMeteringData(t, reader, ts)
	require.Len(t, data, 1)
	require.Equal(t, "1", data[0]["version"])
	require.Equal(t, "ks1", data[0]["cluster_id"])
	require.Equal(t, "dxf", data[0]["source_name"])
	require.Equal(t, float64(10), data[0]["put_external_requests"].(map[string]any)["value"])
	require.Equal(t, float64(20), data[0]["get_external_requests"].(map[string]any)["value"])
	require.Equal(t, float64(300), data[0]["read_data_bytes"].(map[string]any)["value"])
	require.Equal(t, float64(400), data[0]["write_data_bytes"].(map[string]any)["value"])
}

func createLocalMeter(t *testing.T, dir string) (*Meter, *meteringreader.MeteringReader) {
	m, err := NewMeter(&mconfig.MeteringConfig{
		Bucket: "bucket",
		AWS: &mconfig.MeteringAWSConfig{
			AssumeRoleARN: "test-role-arn",
		},
	})
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
	meteringConfig := mconfig.DefaultConfig().WithLogger(m.logger)
	m.writer = meteringwriter.NewMeteringWriter(provider, meteringConfig)
	reader := meteringreader.NewMeteringReader(provider, meteringConfig)
	t.Cleanup(func() {
		require.NoError(t, reader.Close())
	})
	m.ctx = context.Background()
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
