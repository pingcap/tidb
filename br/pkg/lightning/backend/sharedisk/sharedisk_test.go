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

package sharedisk

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"runtime"
	"strconv"
	"testing"
	"time"
	"unsafe"

	kv2 "github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/membuf"
	storage2 "github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestWriter(t *testing.T) {
	//t.Skip("")
	bucket := "globalsorttest"
	prefix := "tools_test_data/sharedisk"
	uri := fmt.Sprintf("s3://%s/%s?access-key=%s&secret-access-key=%s&endpoint=http://%s:%s&force-path-style=true",
		bucket, prefix, "minioadmin", "minioadmin", "127.0.0.1", "9000")
	backend, err := storage2.ParseBackend(uri, nil)
	require.NoError(t, err)
	storage, err := storage2.New(context.Background(), backend, &storage2.ExternalStorageOptions{})
	require.NoError(t, err)

	ctx := context.Background()
	err = cleanupFiles(ctx, storage, "jobID/engineUUID")
	require.NoError(t, err)

	const (
		memLimit       uint64 = 64 * 1024 * 1024
		sizeDist       uint64 = 1024 * 1024
		keyDist               = 8 * 1024
		writeBatchSize        = 8 * 1024
	)
	writer := NewWriter(context.Background(), storage, "jobID/engineUUID", 0,
		membuf.NewPool(), memLimit, keyDist, sizeDist, writeBatchSize, DummyOnCloseFunc)

	var kvs []common.KvPair
	value := make([]byte, 128)
	for i := 0; i < 16; i++ {
		binary.BigEndian.PutUint64(value[i*8:], uint64(i))
	}
	for i := 1; i <= 200000; i++ {
		var kv common.KvPair
		kv.Key = make([]byte, 16)
		kv.Val = make([]byte, 128)
		copy(kv.Val, value)
		//key := rand.Intn(10000000)
		key := i
		binary.BigEndian.PutUint64(kv.Key, uint64(key))
		binary.BigEndian.PutUint64(kv.Key[8:], uint64(i))
		kvs = append(kvs, kv)
	}
	err = writer.AppendRows(ctx, nil, kv2.MakeRowsFromKvPairs(kvs))
	require.NoError(t, err)

	logutil.BgLogger().Info("writer info", zap.Any("seq", writer.currentSeq))

	_, err = writer.Close(ctx)
	require.NoError(t, err)

	i := 0
	data, stats, err := GetAllFileNames(ctx, storage, "jobID")
	require.NoError(t, err)

	data.ForEach(func(_, _ int, fileName string) {
		dataReader, err := newKVReader(ctx, fileName, storage, 0, 4096)
		require.NoError(t, err)
		for {
			k, v, err := dataReader.nextKV()
			require.NoError(t, err)
			if k == nil && v == nil {
				break
			}
			i++
			key := binary.BigEndian.Uint64(k)
			logutil.BgLogger().Info("print kv", zap.Any("key", key))
		}
	})
	logutil.BgLogger().Info("flush cnt", zap.Any("cnt", writer.currentSeq+1))

	require.Equal(t, 200000, i)

	for _, fileName := range stats {
		statReader, err := newStatsReader(ctx, storage, fileName, 4096)
		require.NoError(t, err)
		for {
			prop, err := statReader.nextProp()
			require.NoError(t, err)
			if prop == nil {
				break
			}
			logutil.BgLogger().Info("print prop", zap.Any("offset", prop.offset))
			require.Less(t, prop.DataSeq, 5)
		}
	}

	dataFileName := make([]string, 0)
	fileStartOffsets := make([]uint64, 0)
	data.ForEach(func(_, _ int, fileName string) {
		dataFileName = append(dataFileName, fileName)
		fileStartOffsets = append(fileStartOffsets, 0)
	})
	mIter, err := NewMergeIter(ctx, dataFileName, fileStartOffsets, storage, 4096)
	require.NoError(t, err)
	mCnt := 0
	var prevKey []byte
	for mIter.Next() {
		mCnt++
		if len(prevKey) > 0 {
			currKey := mIter.Key()
			require.Equal(t, 1, bytes.Compare(currKey, prevKey))
		}
		prevKey = mIter.Key()

	}
	require.Equal(t, 200000, mCnt)
}

func cleanupFiles(ctx context.Context, store storage2.ExternalStorage, subDir string) error {
	return store.WalkDir(ctx, &storage2.WalkOption{SubDir: subDir},
		func(path string, size int64) error {
			return store.DeleteFile(ctx, path)
		})
}

func randomString(n int) string {
	const alphanum = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	var bytes = make([]byte, n)
	for i := range bytes {
		bytes[i] = alphanum[rand.Intn(len(alphanum))]
	}
	return string(bytes)
}

func TestWriterPerfOnly(t *testing.T) {
	var keySize = 1000
	var valueSize = 10
	var rowCnt = 10000000
	const (
		memLimit       uint64 = 1024 * 1024 * 1024
		keyDist               = 8 * 1024
		sizeDist       uint64 = 1024 * 1024
		writeBatchSize        = 8 * 1024
	)

	bucket := "globalsorttest"
	prefix := "tools_test_data/globalsorttestwriter"
	uri := fmt.Sprintf("s3://%s/%s&force-path-style=true",
		bucket, prefix)
	//uri := fmt.Sprintf("s3://%s/%s?access-key=%s&secret-access-key=%s&endpoint=http://%s:%s&force-path-style=true",
	//	bucket, prefix, "minioadmin", "minioadmin", "127.0.0.1", "9000")

	backend, err := storage2.ParseBackend(uri, nil)
	require.NoError(t, err)
	storage, err := storage2.New(context.Background(), backend, &storage2.ExternalStorageOptions{})
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, err)

	writer := NewWriter(context.Background(), storage, "test", 0,
		membuf.NewPool(), memLimit, keyDist, sizeDist, writeBatchSize, DummyOnCloseFunc)
	pool := membuf.NewPool()
	defer pool.Destroy()
	defer writer.Close(ctx)

	k := randomString(keySize)
	v := randomString(valueSize)

	for i := 0; i < rowCnt; i += 10000 {
		var kvs []common.KvPair
		for j := 0; j < 10000; j++ {
			var kv common.KvPair
			kv.Key = []byte(k)
			kv.Val = []byte(v)
			kvs = append(kvs, kv)
		}
		err = writer.AppendRows(ctx, nil, kv2.MakeRowsFromKvPairs(kvs))
	}
	err = writer.flushKVs(context.Background())
	require.NoError(t, err)
}

func TestWriterPerf(t *testing.T) {
	t.Skip("")
	var keySize = 1000
	var valueSize = 10
	var rowCnt = 100000
	var readBufferSize = 64 * 1024
	const (
		memLimit       uint64 = 64 * 1024 * 1024
		keyDist               = 8 * 1024
		sizeDist       uint64 = 1024 * 1024
		writeBatchSize        = 8 * 1024
	)

	bucket := "globalsorttest"
	prefix := "tools_test_data/sharedisk"
	//uri := fmt.Sprintf("s3://%s/%s&force-path-style=true",
	//	bucket, prefix)
	uri := fmt.Sprintf("s3://%s/%s?access-key=%s&secret-access-key=%s&endpoint=http://%s:%s&force-path-style=true",
		bucket, prefix, "minioadmin", "minioadmin", "127.0.0.1", "9000")

	backend, err := storage2.ParseBackend(uri, nil)
	require.NoError(t, err)
	storage, err := storage2.New(context.Background(), backend, &storage2.ExternalStorageOptions{})
	require.NoError(t, err)

	ctx := context.Background()
	err = cleanupFiles(ctx, storage, "test")
	require.NoError(t, err)

	writer := NewWriter(context.Background(), storage, "test", 0,
		membuf.NewPool(), memLimit, keyDist, sizeDist, writeBatchSize, DummyOnCloseFunc)
	writer.Close(ctx)

	var startMemory runtime.MemStats

	k := randomString(keySize)
	v := randomString(valueSize)

	for i := 0; i < rowCnt; i += 10000 {
		var kvs []common.KvPair
		for j := 0; j < 10000; j++ {
			var kv common.KvPair
			kv.Key = []byte(k)
			kv.Val = []byte(v)
			kvs = append(kvs, kv)
		}
		err = writer.AppendRows(ctx, nil, kv2.MakeRowsFromKvPairs(kvs))
	}
	err = writer.flushKVs(context.Background())
	require.NoError(t, err)
	//writer.currentSeq = 500

	logutil.BgLogger().Info("writer info", zap.Any("seq", writer.currentSeq))

	runtime.ReadMemStats(&startMemory)
	logutil.BgLogger().Info("meminfo before read", zap.Any("alloc", startMemory.Alloc), zap.Any("heapInUse", startMemory.HeapInuse), zap.Any("total", startMemory.TotalAlloc))

	//defer func() {
	//	for i := 0; i < writer.currentSeq; i++ {
	//		storage.DeleteFile(ctx, "test/"+strconv.Itoa(i))
	//		storage.DeleteFile(ctx, "test_stat/"+strconv.Itoa(i))
	//	}
	//}()

	dataFileName := make([]string, 0)
	fileStartOffsets := make([]uint64, 0)
	for i := 0; i < writer.currentSeq; i++ {
		dataFileName = append(dataFileName, "test/0/"+strconv.Itoa(i))
		fileStartOffsets = append(fileStartOffsets, 0)
	}

	startTs := time.Now()
	mIter, err := NewMergeIter(ctx, dataFileName, fileStartOffsets, storage, readBufferSize)
	require.NoError(t, err)
	defer mIter.Close()
	mCnt := 0
	prevKey := make([]byte, 0, keySize)

	runtime.ReadMemStats(&startMemory)
	logutil.BgLogger().Info("meminfo new merge iter", zap.Any("alloc", startMemory.Alloc), zap.Any("heapInUse", startMemory.HeapInuse), zap.Any("total", startMemory.TotalAlloc))

	for mIter.Next() {
		mCnt++
		if mCnt%1000000 == 0 {
			runtime.ReadMemStats(&startMemory)
			logutil.BgLogger().Info("meminfo % 1000000", zap.Any("alloc", startMemory.Alloc), zap.Any("heapInUse", startMemory.HeapInuse), zap.Any("total", startMemory.TotalAlloc))
		}
		if len(prevKey) > 0 {
			currKey := mIter.Key()
			require.Equal(t, 1, bytes.Compare(currKey, prevKey))
			copy(prevKey, currKey)
		}
	}

	require.Equal(t, rowCnt, mCnt)
	logutil.BgLogger().Info("read data rate", zap.Any("sort total/ ms", time.Since(startTs).Milliseconds()), zap.Any("io cnt", ReadIOCnt.Load()), zap.Any("bytes", ReadByteForTest.Load()), zap.Any("time", ReadTimeForTest.Load()), zap.Any("rate: m/s", ReadByteForTest.Load()*1000000.0/ReadTimeForTest.Load()/1024.0/1024.0))
}

func TestRangePropertySize(t *testing.T) {
	r := RangeProperty{}
	// Please make sure propertyLengthExceptKey is updated as expected.
	require.Equal(t, 64, int(unsafe.Sizeof(r)))
}
