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

package external

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/docker/go-units"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/ingestor/engineapi"
	"github.com/pingcap/tidb/pkg/lightning/membuf"
	"github.com/stretchr/testify/require"
)

func testGetFirstAndLastKey(
	t *testing.T,
	data engineapi.IngestData,
	lowerBound, upperBound []byte,
	expectedFirstKey, expectedLastKey []byte,
) {
	firstKey, lastKey, err := data.GetFirstAndLastKey(lowerBound, upperBound)
	require.NoError(t, err)
	require.Equal(t, expectedFirstKey, firstKey)
	require.Equal(t, expectedLastKey, lastKey)
}

func testNewIter(
	t *testing.T,
	data engineapi.IngestData,
	lowerBound, upperBound []byte,
	expectedKVs []KVPair,
) {
	ctx := context.Background()
	iter := data.NewIter(ctx, lowerBound, upperBound, nil)
	var kvs []KVPair
	for iter.First(); iter.Valid(); iter.Next() {
		require.NoError(t, iter.Error())
		kvs = append(kvs, KVPair{Key: iter.Key(), Value: iter.Value()})
	}
	require.NoError(t, iter.Error())
	require.NoError(t, iter.Close())
	require.Equal(t, expectedKVs, kvs)
}

func TestMemoryIngestData(t *testing.T) {
	kvs := []KVPair{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
		{Key: []byte("key3"), Value: []byte("value3")},
		{Key: []byte("key4"), Value: []byte("value4")},
		{Key: []byte("key5"), Value: []byte("value5")},
	}
	data := &MemoryIngestData{
		kvs: kvs,
		ts:  123,
	}

	require.EqualValues(t, 123, data.GetTS())
	testGetFirstAndLastKey(t, data, nil, nil, []byte("key1"), []byte("key5"))
	testGetFirstAndLastKey(t, data, []byte("key1"), []byte("key6"), []byte("key1"), []byte("key5"))
	testGetFirstAndLastKey(t, data, []byte("key2"), []byte("key5"), []byte("key2"), []byte("key4"))
	testGetFirstAndLastKey(t, data, []byte("key25"), []byte("key35"), []byte("key3"), []byte("key3"))
	testGetFirstAndLastKey(t, data, []byte("key25"), []byte("key26"), nil, nil)
	testGetFirstAndLastKey(t, data, []byte("key0"), []byte("key1"), nil, nil)
	testGetFirstAndLastKey(t, data, []byte("key6"), []byte("key9"), nil, nil)

	testNewIter(t, data, nil, nil, kvs)
	testNewIter(t, data, []byte("key1"), []byte("key6"), kvs)
	testNewIter(t, data, []byte("key2"), []byte("key5"), kvs[1:4])
	testNewIter(t, data, []byte("key25"), []byte("key35"), kvs[2:3])
	testNewIter(t, data, []byte("key25"), []byte("key26"), nil)
	testNewIter(t, data, []byte("key0"), []byte("key1"), nil)
	testNewIter(t, data, []byte("key6"), []byte("key9"), nil)

	data = &MemoryIngestData{
		ts: 234,
	}
	encodedKVs := make([]KVPair, 0, len(kvs)*2)
	duplicatedKVs := make([]KVPair, 0, len(kvs)*2)

	for i := range kvs {
		encodedKey := slices.Clone(kvs[i].Key)
		encodedKVs = append(encodedKVs, KVPair{Key: encodedKey, Value: kvs[i].Value})
		if i%2 == 0 {
			continue
		}

		// duplicatedKeys will be like key2_0, key2_1, key4_0, key4_1
		duplicatedKVs = append(duplicatedKVs, KVPair{Key: encodedKey, Value: kvs[i].Value})

		encodedKey = slices.Clone(kvs[i].Key)
		newValues := make([]byte, len(kvs[i].Value)+1)
		copy(newValues, kvs[i].Value)
		newValues[len(kvs[i].Value)] = 1
		encodedKVs = append(encodedKVs, KVPair{Key: encodedKey, Value: newValues})
		duplicatedKVs = append(duplicatedKVs, KVPair{Key: encodedKey, Value: newValues})
	}
	data.kvs = encodedKVs

	require.EqualValues(t, 234, data.GetTS())
	testGetFirstAndLastKey(t, data, nil, nil, []byte("key1"), []byte("key5"))
	testGetFirstAndLastKey(t, data, []byte("key1"), []byte("key6"), []byte("key1"), []byte("key5"))
	testGetFirstAndLastKey(t, data, []byte("key2"), []byte("key5"), []byte("key2"), []byte("key4"))
	testGetFirstAndLastKey(t, data, []byte("key25"), []byte("key35"), []byte("key3"), []byte("key3"))
	testGetFirstAndLastKey(t, data, []byte("key25"), []byte("key26"), nil, nil)
	testGetFirstAndLastKey(t, data, []byte("key0"), []byte("key1"), nil, nil)
	testGetFirstAndLastKey(t, data, []byte("key6"), []byte("key9"), nil, nil)
}

func TestSplit(t *testing.T) {
	cases := []struct {
		input    []int
		conc     int
		expected [][]int
	}{
		{
			input:    []int{1, 2, 3, 4, 5},
			conc:     1,
			expected: [][]int{{1, 2, 3, 4, 5}},
		},
		{
			input:    []int{1, 2, 3, 4, 5},
			conc:     2,
			expected: [][]int{{1, 2, 3}, {4, 5}},
		},
		{
			input:    []int{1, 2, 3, 4, 5},
			conc:     0,
			expected: [][]int{{1, 2, 3, 4, 5}},
		},
		{
			input:    []int{1, 2, 3, 4, 5},
			conc:     5,
			expected: [][]int{{1}, {2}, {3}, {4}, {5}},
		},
		{
			input:    []int{},
			conc:     5,
			expected: nil,
		},
		{
			input:    []int{1, 2, 3, 4, 5},
			conc:     100,
			expected: [][]int{{1}, {2}, {3}, {4}, {5}},
		},
	}

	for _, c := range cases {
		got := split(c.input, c.conc)
		require.Equal(t, c.expected, got)
	}
}

func prepareKVFiles(t *testing.T, store storage.ExternalStorage, contents [][]KVPair) (dataFiles, statFiles []string) {
	ctx := context.Background()
	for i, c := range contents {
		var summary *WriterSummary
		// we want to create a file for each content, so make the below size larger.
		writer := NewWriterBuilder().SetPropKeysDistance(4).
			SetMemorySizeLimit(8*units.MiB).SetBlockSize(8*units.MiB).
			SetOnCloseFunc(func(s *WriterSummary) { summary = s }).
			Build(store, "/test", fmt.Sprintf("%d", i))
		for _, p := range c {
			require.NoError(t, writer.WriteRow(ctx, p.Key, p.Value, nil))
		}
		require.NoError(t, writer.Close(ctx))
		require.Len(t, summary.MultipleFilesStats, 1)
		require.Len(t, summary.MultipleFilesStats[0].Filenames, 1)
		require.Zero(t, summary.ConflictInfo.Count)
		require.Empty(t, summary.ConflictInfo.Files)
		dataFiles = append(dataFiles, summary.MultipleFilesStats[0].Filenames[0][0])
		statFiles = append(statFiles, summary.MultipleFilesStats[0].Filenames[0][1])
	}
	return
}

func getAllDataFromDataAndRanges(t *testing.T, dataAndRanges *engineapi.DataAndRanges) []KVPair {
	ctx := context.Background()
	iter := dataAndRanges.Data.NewIter(ctx, nil, nil, membuf.NewPool())
	var allKVs []KVPair
	for iter.First(); iter.Valid(); iter.Next() {
		allKVs = append(allKVs, KVPair{Key: iter.Key(), Value: iter.Value()})
	}
	require.NoError(t, iter.Close())
	return allKVs
}

func TestEngineOnDup(t *testing.T) {
	ctx := context.Background()
	contents := [][]KVPair{{
		{Key: []byte{4}, Value: []byte("bbb")},
		{Key: []byte{4}, Value: []byte("bbb")},
		{Key: []byte{1}, Value: []byte("aa")},
		{Key: []byte{1}, Value: []byte("aa")},
		{Key: []byte{1}, Value: []byte("aa")},
		{Key: []byte{2}, Value: []byte("vv")},
		{Key: []byte{3}, Value: []byte("sds")},
	}}

	getEngineFn := func(store storage.ExternalStorage, onDup engineapi.OnDuplicateKey, inDataFiles, inStatFiles []string) *Engine {
		return NewExternalEngine(
			ctx,
			store, inDataFiles, inStatFiles,
			[]byte{1}, []byte{5},
			[][]byte{{1}, {2}, {3}, {4}, {5}},
			[][]byte{{1}, {3}, {5}},
			10,
			123,
			456,
			789,
			true,
			16*units.GiB,
			onDup,
			"/",
		)
	}

	t.Run("on duplicate ignore", func(t *testing.T) {
		onDup := engineapi.OnDuplicateKeyIgnore
		store := storage.NewMemStorage()
		dataFiles, statFiles := prepareKVFiles(t, store, contents)
		extEngine := getEngineFn(store, onDup, dataFiles, statFiles)
		loadDataCh := make(chan engineapi.DataAndRanges, 4)
		require.ErrorContains(t, extEngine.LoadIngestData(ctx, loadDataCh), "duplicate key found")
		t.Cleanup(func() {
			require.NoError(t, extEngine.Close())
		})
	})

	t.Run("on duplicate error", func(t *testing.T) {
		onDup := engineapi.OnDuplicateKeyError
		store := storage.NewMemStorage()
		dataFiles, statFiles := prepareKVFiles(t, store, contents)
		extEngine := getEngineFn(store, onDup, dataFiles, statFiles)
		loadDataCh := make(chan engineapi.DataAndRanges, 4)
		require.ErrorContains(t, extEngine.LoadIngestData(ctx, loadDataCh), "[Lightning:Restore:ErrFoundDuplicateKey]found duplicate key '\x01', value 'aa'")
		t.Cleanup(func() {
			require.NoError(t, extEngine.Close())
		})
	})

	t.Run("on duplicate record or remove, no duplicates", func(t *testing.T) {
		for _, od := range []engineapi.OnDuplicateKey{engineapi.OnDuplicateKeyRecord, engineapi.OnDuplicateKeyRemove} {
			store := storage.NewMemStorage()
			dfiles, sfiles := prepareKVFiles(t, store, [][]KVPair{{
				{Key: []byte{4}, Value: []byte("bbb")},
				{Key: []byte{1}, Value: []byte("aa")},
				{Key: []byte{2}, Value: []byte("vv")},
				{Key: []byte{3}, Value: []byte("sds")},
			}})
			extEngine := getEngineFn(store, od, dfiles, sfiles)
			loadDataCh := make(chan engineapi.DataAndRanges, 4)
			require.NoError(t, extEngine.LoadIngestData(ctx, loadDataCh))
			t.Cleanup(func() {
				require.NoError(t, extEngine.Close())
			})
			require.Len(t, loadDataCh, 1)
			dataAndRanges := <-loadDataCh
			allKVs := getAllDataFromDataAndRanges(t, &dataAndRanges)
			require.EqualValues(t, []KVPair{
				{Key: []byte{1}, Value: []byte("aa")},
				{Key: []byte{2}, Value: []byte("vv")},
				{Key: []byte{3}, Value: []byte("sds")},
				{Key: []byte{4}, Value: []byte("bbb")},
			}, allKVs)
			info := extEngine.ConflictInfo()
			require.Zero(t, info.Count)
			require.Empty(t, info.Files)
		}
	})

	t.Run("on duplicate record or remove, partial duplicated", func(t *testing.T) {
		contents2 := [][]KVPair{
			{{Key: []byte{1}, Value: []byte("aa")}, {Key: []byte{1}, Value: []byte("aa")}},
			{{Key: []byte{1}, Value: []byte("aa")}, {Key: []byte{2}, Value: []byte("vv")}, {Key: []byte{3}, Value: []byte("sds")}},
			{{Key: []byte{4}, Value: []byte("bbb")}, {Key: []byte{4}, Value: []byte("bbb")}},
		}
		for _, cont := range [][][]KVPair{contents, contents2} {
			for _, od := range []engineapi.OnDuplicateKey{engineapi.OnDuplicateKeyRecord, engineapi.OnDuplicateKeyRemove} {
				store := storage.NewMemStorage()
				dataFiles, statFiles := prepareKVFiles(t, store, cont)
				extEngine := getEngineFn(store, od, dataFiles, statFiles)
				loadDataCh := make(chan engineapi.DataAndRanges, 4)
				require.NoError(t, extEngine.LoadIngestData(ctx, loadDataCh))
				t.Cleanup(func() {
					require.NoError(t, extEngine.Close())
				})
				require.Len(t, loadDataCh, 1)
				dataAndRanges := <-loadDataCh
				allKVs := getAllDataFromDataAndRanges(t, &dataAndRanges)
				require.EqualValues(t, []KVPair{
					{Key: []byte{2}, Value: []byte("vv")},
					{Key: []byte{3}, Value: []byte("sds")},
				}, allKVs)
				info := extEngine.ConflictInfo()
				if od == engineapi.OnDuplicateKeyRemove {
					require.Zero(t, info.Count)
					require.Empty(t, info.Files)
				} else {
					require.EqualValues(t, 5, info.Count)
					require.Len(t, info.Files, 1)
					dupPairs := readKVFile(t, store, info.Files[0])
					require.EqualValues(t, []KVPair{
						{Key: []byte{1}, Value: []byte("aa")},
						{Key: []byte{1}, Value: []byte("aa")},
						{Key: []byte{1}, Value: []byte("aa")},
						{Key: []byte{4}, Value: []byte("bbb")},
						{Key: []byte{4}, Value: []byte("bbb")},
					}, dupPairs)
				}
			}
		}
	})

	t.Run("on duplicate record or remove, all duplicated", func(t *testing.T) {
		for _, od := range []engineapi.OnDuplicateKey{engineapi.OnDuplicateKeyRecord, engineapi.OnDuplicateKeyRemove} {
			store := storage.NewMemStorage()
			dfiles, sfiles := prepareKVFiles(t, store, [][]KVPair{{
				{Key: []byte{1}, Value: []byte("aaa")},
				{Key: []byte{1}, Value: []byte("aaa")},
				{Key: []byte{1}, Value: []byte("aaa")},
				{Key: []byte{1}, Value: []byte("aaa")},
			}})
			extEngine := getEngineFn(store, od, dfiles, sfiles)
			loadDataCh := make(chan engineapi.DataAndRanges, 4)
			require.NoError(t, extEngine.LoadIngestData(ctx, loadDataCh))
			t.Cleanup(func() {
				require.NoError(t, extEngine.Close())
			})
			require.Len(t, loadDataCh, 1)
			dataAndRanges := <-loadDataCh
			allKVs := getAllDataFromDataAndRanges(t, &dataAndRanges)
			require.Empty(t, allKVs)
			info := extEngine.ConflictInfo()
			if od == engineapi.OnDuplicateKeyRemove {
				require.Zero(t, info.Count)
				require.Empty(t, info.Files)
			} else {
				require.EqualValues(t, 4, info.Count)
				require.Len(t, info.Files, 1)
				dupPairs := readKVFile(t, store, info.Files[0])
				require.EqualValues(t, []KVPair{
					{Key: []byte{1}, Value: []byte("aaa")},
					{Key: []byte{1}, Value: []byte("aaa")},
					{Key: []byte{1}, Value: []byte("aaa")},
					{Key: []byte{1}, Value: []byte("aaa")},
				}, dupPairs)
			}
		}
	})
}

// TestMemKVsAndBuffersMultiFileDataLoss 测试验证当多个文件被同一个 goroutine 处理时，
// 是否会出现数据丢失的问题。这个测试专门用于验证 memKVsAndBuffers 在 build 时
// 有两个文件，但在 MemoryIngestData 输出时只有一个文件内容的问题。
func TestMemKVsAndBuffersMultiFileDataLoss(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemStorage()

	// 创建两个文件，每个文件包含不同的、可识别的数据
	// 文件1: file1_key1, file1_key2, file1_key3
	// 文件2: file2_key1, file2_key2, file2_key3
	file1KVs := []KVPair{
		{Key: []byte("file1_key1"), Value: []byte("file1_value1")},
		{Key: []byte("file1_key2"), Value: []byte("file1_value2")},
		{Key: []byte("file1_key3"), Value: []byte("file1_value3")},
	}
	file2KVs := []KVPair{
		{Key: []byte("file2_key1"), Value: []byte("file2_value1")},
		{Key: []byte("file2_key2"), Value: []byte("file2_value2")},
		{Key: []byte("file2_key3"), Value: []byte("file2_value3")},
	}

	// 准备两个文件
	dataFiles, statFiles := prepareKVFiles(t, store, [][]KVPair{file1KVs, file2KVs})
	require.Len(t, dataFiles, 2)
	require.Len(t, statFiles, 2)

	// 创建 buffer pools
	smallBlockBufPool := membuf.NewPool(
		membuf.WithBlockNum(0),
		membuf.WithBlockSize(smallBlockSize),
	)
	largeBlockBufPool := membuf.NewPool(
		membuf.WithBlockNum(0),
		membuf.WithBlockSize(ConcurrentReaderBufferSizePerConc),
	)

	// 读取所有数据
	output := &memKVsAndBuffers{}
	startKey := []byte("file1_key1")
	endKey := []byte("file2_key3\x00") // 包含两个文件的所有 key

	err := readAllData(
		ctx,
		store,
		dataFiles,
		statFiles,
		startKey,
		endKey,
		smallBlockBufPool,
		largeBlockBufPool,
		output,
	)
	require.NoError(t, err)

	// 验证 build 前 kvsPerFile 有两个文件
	require.Len(t, output.kvsPerFile, 2, "kvsPerFile should have 2 files before build")
	require.Len(t, output.kvsPerFile[0], 3, "first file should have 3 KVs")
	require.Len(t, output.kvsPerFile[1], 3, "second file should have 3 KVs")

	// 验证第一个文件的内容
	file1Keys := make(map[string]bool)
	for _, kv := range output.kvsPerFile[0] {
		file1Keys[string(kv.Key)] = true
	}

	//for _, kv := range output.kvsPerFile[0] {
	//	logutil.BgLogger().Info("print kv from first file", zap.String("key", string(kv.Key)), zap.String("value", string(kv.Value)))
	//}
	//for _, kv := range output.kvsPerFile[1] {
	//	logutil.BgLogger().Info("print kv from second file", zap.String("key", string(kv.Key)), zap.String("value", string(kv.Value)))
	//}

	//require.True(t, file1Keys["file1_key1"], "first file should contain file1_key1")
	//require.True(t, file1Keys["file1_key2"], "first file should contain file1_key2")
	//require.True(t, file1Keys["file1_key3"], "first file should contain file1_key3")

	// 验证第二个文件的内容
	file2Keys := make(map[string]bool)
	for _, kv := range output.kvsPerFile[1] {
		file2Keys[string(kv.Key)] = true
	}
	//for _, kv := range output.kvsPerFile[1] {
	//	logutil.BgLogger().Info("print kv from second file", zap.String("key", string(kv.Key)), zap.String("value", string(kv.Value)))
	//}
	//require.True(t, file2Keys["file2_key1"], "second file should contain file2_key1")
	//require.True(t, file2Keys["file2_key2"], "second file should contain file2_key2")
	//require.True(t, file2Keys["file2_key3"], "second file should contain file2_key3")

	// 执行 build
	output.build(ctx)

	// 验证 build 后 kvs 包含所有数据
	require.Len(t, output.kvs, 6, "kvs should have 6 KVs after build (3 from each file)")

	// 验证所有 key 都存在
	allKeys := make(map[string]bool)
	for _, kv := range output.kvs {
		allKeys[string(kv.Key)] = true
	}
	// 打印实际读取到的 key，用于调试
	if len(allKeys) != 6 {
		t.Logf("Expected 6 keys, but got %d keys", len(allKeys))
		for k := range allKeys {
			t.Logf("  Found key: %s", k)
		}
	}
	require.True(t, allKeys["file1_key1"], "kvs should contain file1_key1")
	require.True(t, allKeys["file1_key2"], "kvs should contain file1_key2")
	require.True(t, allKeys["file1_key3"], "kvs should contain file1_key3")
	require.True(t, allKeys["file2_key1"], "kvs should contain file2_key1")
	require.True(t, allKeys["file2_key2"], "kvs should contain file2_key2")
	require.True(t, allKeys["file2_key3"], "kvs should contain file2_key3")

	// 创建 Engine 并测试 MemoryIngestData
	engine := NewExternalEngine(
		ctx,
		store, dataFiles, statFiles,
		startKey, endKey,
		[][]byte{startKey, endKey},
		[][]byte{startKey, endKey},
		1, // workerConcurrency = 1 确保同一个 goroutine 处理多个文件
		123,
		456,
		789,
		true,
		16*units.GiB,
		engineapi.OnDuplicateKeyRemove,
		"/",
	)
	defer engine.Close()

	// 加载数据
	loadDataCh := make(chan engineapi.DataAndRanges, 4)
	err = engine.LoadIngestData(ctx, loadDataCh)
	require.NoError(t, err)

	// 验证只有一个 DataAndRanges
	require.Len(t, loadDataCh, 1, "should have 1 DataAndRanges")

	// 获取数据并验证
	dataAndRanges := <-loadDataCh
	allKVs := getAllDataFromDataAndRanges(t, &dataAndRanges)

	// 验证 MemoryIngestData 包含所有数据
	require.Len(t, allKVs, 6, "MemoryIngestData should have 6 KVs (3 from each file)")

	// 验证所有 key 都存在
	memoryKeys := make(map[string]bool)
	for _, kv := range allKVs {
		memoryKeys[string(kv.Key)] = true
	}
	require.True(t, memoryKeys["file1_key1"], "MemoryIngestData should contain file1_key1")
	require.True(t, memoryKeys["file1_key2"], "MemoryIngestData should contain file1_key2")
	require.True(t, memoryKeys["file1_key3"], "MemoryIngestData should contain file1_key3")
	require.True(t, memoryKeys["file2_key1"], "MemoryIngestData should contain file2_key1")
	require.True(t, memoryKeys["file2_key2"], "MemoryIngestData should contain file2_key2")
	require.True(t, memoryKeys["file2_key3"], "MemoryIngestData should contain file2_key3")

	// 验证值也正确
	kvMap := make(map[string][]byte)
	for _, kv := range allKVs {
		kvMap[string(kv.Key)] = kv.Value
	}
	require.Equal(t, []byte("file1_value1"), kvMap["file1_key1"])
	require.Equal(t, []byte("file1_value2"), kvMap["file1_key2"])
	require.Equal(t, []byte("file1_value3"), kvMap["file1_key3"])
	require.Equal(t, []byte("file2_value1"), kvMap["file2_key1"])
	require.Equal(t, []byte("file2_value2"), kvMap["file2_key2"])
	require.Equal(t, []byte("file2_value3"), kvMap["file2_key3"])
}
