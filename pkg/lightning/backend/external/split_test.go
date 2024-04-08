// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package external

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"net/http"
	_ "net/http/pprof"
	"slices"
	"testing"
	"time"

	"github.com/docker/go-units"
	"github.com/google/uuid"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
	"golang.org/x/sync/errgroup"
)

func TestGeneralProperties(t *testing.T) {
	seed := time.Now().Unix()
	rand.Seed(uint64(seed))
	t.Logf("seed: %d", seed)

	ctx := context.Background()
	memStore := storage.NewMemStorage()

	kvNum := rand.Intn(1000) + 100
	keys := make([][]byte, kvNum)
	values := make([][]byte, kvNum)
	for i := range keys {
		keyLen := rand.Intn(100) + 1
		valueLen := rand.Intn(100) + 1
		keys[i] = make([]byte, keyLen+2)
		values[i] = make([]byte, valueLen+2)
		rand.Read(keys[i][:keyLen])
		rand.Read(values[i][:valueLen])
		keys[i][keyLen] = byte(i / 255)
		keys[i][keyLen+1] = byte(i % 255)
		values[i][valueLen] = byte(i / 255)
		values[i][valueLen+1] = byte(i % 255)
	}

	dataFiles, statFiles, err := MockExternalEngine(memStore, keys, values)
	require.NoError(t, err)
	multiFileStat := mockOneMultiFileStat(dataFiles, statFiles)
	splitter, err := NewRangeSplitter(
		ctx, multiFileStat, memStore, 1000, 30, 1000, 1,
	)
	var lastEndKey []byte
notExhausted:
	endKey, dataFiles, statFiles, splitKeys, err := splitter.SplitOneRangesGroup()
	require.NoError(t, err)

	// endKey should be strictly greater than lastEndKey
	if lastEndKey != nil && endKey != nil {
		cmp := bytes.Compare(endKey, lastEndKey)
		require.Equal(t, 1, cmp, "endKey: %v, lastEndKey: %v", endKey, lastEndKey)
	}

	// check dataFiles and statFiles
	lenDataFiles := len(dataFiles)
	lenStatFiles := len(statFiles)
	require.Equal(t, lenDataFiles, lenStatFiles)
	require.Greater(t, lenDataFiles, 0)
	if len(splitKeys) > 0 {
		// splitKeys should be strictly increasing
		for i := 1; i < len(splitKeys); i++ {
			cmp := bytes.Compare(splitKeys[i], splitKeys[i-1])
			require.Equal(t, 1, cmp, "splitKeys: %v", splitKeys)
		}
		// first splitKeys should be strictly greater than lastEndKey
		cmp := bytes.Compare(splitKeys[0], lastEndKey)
		require.Equal(t, 1, cmp, "splitKeys: %v, lastEndKey: %v", splitKeys, lastEndKey)
		// last splitKeys should be strictly less than endKey
		if endKey != nil {
			cmp = bytes.Compare(splitKeys[len(splitKeys)-1], endKey)
			require.Equal(t, -1, cmp, "splitKeys: %v, endKey: %v", splitKeys, endKey)
		}
	}

	lastEndKey = endKey
	if endKey != nil {
		goto notExhausted
	}
	require.NoError(t, splitter.Close())
}

func TestOnlyOneGroup(t *testing.T) {
	ctx := context.Background()
	memStore := storage.NewMemStorage()
	subDir := "/mock-test"

	writer := NewWriterBuilder().
		SetMemorySizeLimit(20).
		SetPropSizeDistance(1).
		SetPropKeysDistance(1).
		Build(memStore, subDir, "5")

	dataFiles, statFiles, err := MockExternalEngineWithWriter(memStore, writer, subDir, [][]byte{{1}, {2}}, [][]byte{{1}, {2}})
	require.NoError(t, err)
	require.Len(t, dataFiles, 1)
	require.Len(t, statFiles, 1)

	multiFileStat := mockOneMultiFileStat(dataFiles, statFiles)

	splitter, err := NewRangeSplitter(
		ctx, multiFileStat, memStore, 1000, 30, 1000, 10,
	)
	require.NoError(t, err)
	endKey, dataFiles, statFiles, splitKeys, err := splitter.SplitOneRangesGroup()
	require.NoError(t, err)
	require.Nil(t, endKey)
	require.Len(t, dataFiles, 1)
	require.Len(t, statFiles, 1)
	require.Len(t, splitKeys, 0)
	require.NoError(t, splitter.Close())

	splitter, err = NewRangeSplitter(
		ctx, multiFileStat, memStore, 1000, 30, 1000, 1,
	)
	require.NoError(t, err)
	endKey, dataFiles, statFiles, splitKeys, err = splitter.SplitOneRangesGroup()
	require.NoError(t, err)
	require.Nil(t, endKey)
	require.Len(t, dataFiles, 1)
	require.Len(t, statFiles, 1)
	require.Equal(t, [][]byte{{2}}, splitKeys)
	require.NoError(t, splitter.Close())
}

func TestSortedData(t *testing.T) {
	ctx := context.Background()
	memStore := storage.NewMemStorage()
	kvNum := 100

	keys := make([][]byte, kvNum)
	values := make([][]byte, kvNum)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key%03d", i))
		values[i] = []byte(fmt.Sprintf("val%03d", i))
	}

	dataFiles, statFiles, err := MockExternalEngine(memStore, keys, values)
	require.NoError(t, err)
	// we just need to make sure there are multiple files.
	require.Greater(t, len(dataFiles), 1)
	avgKVPerFile := math.Ceil(float64(kvNum) / float64(len(dataFiles)))
	rangesGroupKV := 30
	groupFileNumUpperBound := int(math.Ceil(float64(rangesGroupKV-1)/avgKVPerFile)) + 1

	multiFileStat := mockOneMultiFileStat(dataFiles, statFiles)
	splitter, err := NewRangeSplitter(
		ctx, multiFileStat, memStore, 1000, int64(rangesGroupKV), 1000, 10,
	)
	require.NoError(t, err)

notExhausted:
	endKey, dataFiles, statFiles, _, err := splitter.SplitOneRangesGroup()
	require.NoError(t, err)
	require.LessOrEqual(t, len(dataFiles), groupFileNumUpperBound)
	require.LessOrEqual(t, len(statFiles), groupFileNumUpperBound)
	if endKey != nil {
		goto notExhausted
	}
	require.NoError(t, splitter.Close())
}

func TestRangeSplitterStrictCase(t *testing.T) {
	ctx := context.Background()
	memStore := storage.NewMemStorage()
	subDir := "/mock-test"

	writer1 := NewWriterBuilder().
		SetMemorySizeLimit(2*(lengthBytes*2+10)).
		SetBlockSize(2*(lengthBytes*2+10)).
		SetPropSizeDistance(1).
		SetPropKeysDistance(1).
		Build(memStore, subDir, "1")
	keys1 := [][]byte{
		[]byte("key01"), []byte("key11"), []byte("key21"),
	}
	values1 := [][]byte{
		[]byte("val01"), []byte("val11"), []byte("val21"),
	}
	dataFiles1, statFiles1, err := MockExternalEngineWithWriter(memStore, writer1, subDir, keys1, values1)
	require.NoError(t, err)
	require.Len(t, dataFiles1, 2)
	require.Len(t, statFiles1, 2)

	writer2 := NewWriterBuilder().
		SetMemorySizeLimit(2*(lengthBytes*2+10)).
		SetBlockSize(2*(lengthBytes*2+10)).
		SetPropSizeDistance(1).
		SetPropKeysDistance(1).
		Build(memStore, subDir, "2")
	keys2 := [][]byte{
		[]byte("key02"), []byte("key12"), []byte("key22"),
	}
	values2 := [][]byte{
		[]byte("val02"), []byte("val12"), []byte("val22"),
	}
	dataFiles12, statFiles12, err := MockExternalEngineWithWriter(memStore, writer2, subDir, keys2, values2)
	require.NoError(t, err)
	require.Len(t, dataFiles12, 4)
	require.Len(t, statFiles12, 4)

	writer3 := NewWriterBuilder().
		SetMemorySizeLimit(2*(lengthBytes*2+10)).
		SetBlockSize(2*(lengthBytes*2+10)).
		SetPropSizeDistance(1).
		SetPropKeysDistance(1).
		Build(memStore, subDir, "3")
	keys3 := [][]byte{
		[]byte("key03"), []byte("key13"), []byte("key23"),
	}
	values3 := [][]byte{
		[]byte("val03"), []byte("val13"), []byte("val23"),
	}
	dataFiles123, statFiles123, err := MockExternalEngineWithWriter(memStore, writer3, subDir, keys3, values3)
	require.NoError(t, err)
	require.Len(t, dataFiles123, 6)
	require.Len(t, statFiles123, 6)

	// "/mock-test/X/0" contains "key0X" and "key1X"
	// "/mock-test/X/1" contains "key2X"
	require.Equal(t, []string{
		"/mock-test/1/0", "/mock-test/1/1",
		"/mock-test/2/0", "/mock-test/2/1",
		"/mock-test/3/0", "/mock-test/3/1",
	}, dataFiles123)

	multi := mockOneMultiFileStat(dataFiles123[:4], statFiles123[:4])
	multi2 := mockOneMultiFileStat(dataFiles123[4:], statFiles123[4:])
	multiFileStat := []MultipleFilesStat{multi[0], multi2[0]}
	// group keys = 2, region keys = 1
	splitter, err := NewRangeSplitter(
		ctx, multiFileStat, memStore, 1000, 2, 1000, 1,
	)
	require.NoError(t, err)

	// [key01, key03), split at key02
	endKey, dataFiles, statFiles, splitKeys, err := splitter.SplitOneRangesGroup()
	require.NoError(t, err)
	require.EqualValues(t, kv.Key("key03"), endKey)
	require.Equal(t, []string{"/mock-test/1/0", "/mock-test/2/0"}, dataFiles)
	require.Equal(t, []string{"/mock-test/1_stat/0", "/mock-test/2_stat/0"}, statFiles)
	require.Equal(t, [][]byte{[]byte("key02")}, splitKeys)

	// [key03, key12), split at key11
	endKey, dataFiles, statFiles, splitKeys, err = splitter.SplitOneRangesGroup()
	require.NoError(t, err)
	require.EqualValues(t, kv.Key("key12"), endKey)
	require.Equal(t, []string{"/mock-test/1/0", "/mock-test/2/0", "/mock-test/3/0"}, dataFiles)
	require.Equal(t, []string{"/mock-test/1_stat/0", "/mock-test/2_stat/0", "/mock-test/3_stat/0"}, statFiles)
	require.Equal(t, [][]byte{[]byte("key11")}, splitKeys)

	// [key12, key21), split at key13. the last key of "/mock-test/1/0" is "key11",
	// so it's not used
	endKey, dataFiles, statFiles, splitKeys, err = splitter.SplitOneRangesGroup()
	require.NoError(t, err)
	require.EqualValues(t, kv.Key("key21"), endKey)
	require.Equal(t, []string{"/mock-test/2/0", "/mock-test/3/0"}, dataFiles)
	require.Equal(t, []string{"/mock-test/2_stat/0", "/mock-test/3_stat/0"}, statFiles)
	require.Equal(t, [][]byte{[]byte("key13")}, splitKeys)

	// [key21, key23), split at key22.
	// the last key of "/mock-test/2/0" is "key12", and the last key of "/mock-test/3/0" is "key13",
	// so they are not used
	endKey, dataFiles, statFiles, splitKeys, err = splitter.SplitOneRangesGroup()
	require.NoError(t, err)
	require.EqualValues(t, kv.Key("key23"), endKey)
	require.Equal(t, []string{"/mock-test/1/1", "/mock-test/2/1"}, dataFiles)
	require.Equal(t, []string{"/mock-test/1_stat/1", "/mock-test/2_stat/1"}, statFiles)
	require.Equal(t, [][]byte{[]byte("key22")}, splitKeys)

	// [key23, nil), no split key
	endKey, dataFiles, statFiles, splitKeys, err = splitter.SplitOneRangesGroup()
	require.NoError(t, err)
	require.Nil(t, endKey)
	require.Equal(t, []string{"/mock-test/3/1"}, dataFiles)
	require.Equal(t, []string{"/mock-test/3_stat/1"}, statFiles)
	require.Len(t, splitKeys, 0)

	// read after drain all data
	endKey, dataFiles, statFiles, splitKeys, err = splitter.SplitOneRangesGroup()
	require.NoError(t, err)
	require.Nil(t, endKey)
	require.Len(t, dataFiles, 0)
	require.Len(t, statFiles, 0)
	require.Len(t, splitKeys, 0)
	require.NoError(t, splitter.Close())
}

func TestExactlyKeyNum(t *testing.T) {
	ctx := context.Background()
	memStore := storage.NewMemStorage()
	kvNum := 3

	keys := make([][]byte, kvNum)
	values := make([][]byte, kvNum)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key%03d", i))
		values[i] = []byte(fmt.Sprintf("value%03d", i))
	}

	subDir := "/mock-test"

	writer := NewWriterBuilder().
		SetMemorySizeLimit(15).
		SetPropSizeDistance(1).
		SetPropKeysDistance(1).
		Build(memStore, subDir, "5")

	dataFiles, statFiles, err := MockExternalEngineWithWriter(memStore, writer, subDir, keys, values)
	require.NoError(t, err)
	multiFileStat := mockOneMultiFileStat(dataFiles, statFiles)

	// maxRangeKeys = 3
	splitter, err := NewRangeSplitter(
		ctx, multiFileStat, memStore, 1000, 100, 1000, 3,
	)
	require.NoError(t, err)
	endKey, splitDataFiles, splitStatFiles, splitKeys, err := splitter.SplitOneRangesGroup()
	require.NoError(t, err)
	require.Nil(t, endKey)
	require.Equal(t, dataFiles, splitDataFiles)
	require.Equal(t, statFiles, splitStatFiles)
	require.Len(t, splitKeys, 0)

	// rangesGroupKeys = 3
	splitter, err = NewRangeSplitter(
		ctx, multiFileStat, memStore, 1000, 3, 1000, 1,
	)
	require.NoError(t, err)
	endKey, splitDataFiles, splitStatFiles, splitKeys, err = splitter.SplitOneRangesGroup()
	require.NoError(t, err)
	require.Nil(t, endKey)
	require.Equal(t, dataFiles, splitDataFiles)
	require.Equal(t, statFiles, splitStatFiles)
	require.Equal(t, [][]byte{[]byte("key001"), []byte("key002")}, splitKeys)
}

func Test3KFilesRangeSplitter(t *testing.T) {
	store := openTestingStorage(t)
	ctx := context.Background()

	// use HTTP pprof to debug
	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()

	// test the case that after one round merge step, we have 3000 stat files. In
	// current merge step parameters, we will merge 4000 files of 256MB into 16
	// files, so we directly write 4000*256MB/16 = 64GB data to onefile writer.
	fileNum := 3000
	statCh := make(chan []MultipleFilesStat, fileNum)
	onClose := func(s *WriterSummary) {
		statCh <- s.MultipleFilesStats
	}

	eg := errgroup.Group{}
	eg.SetLimit(30)
	for i := 0; i < fileNum; i++ {
		i := i
		eg.Go(func() error {
			w := NewWriterBuilder().
				SetMemorySizeLimit(DefaultMemSizeLimit).
				SetBlockSize(32*units.MiB). // dataKVGroupBlockSize
				SetPropKeysDistance(8*1024).
				SetPropSizeDistance(size.MB).
				SetOnCloseFunc(onClose).
				BuildOneFile(store, "/mock-test", uuid.New().String())
			err := w.Init(ctx, int64(5*size.MB))
			require.NoError(t, err)
			// we don't need data files
			err = w.dataWriter.Close(ctx)
			require.NoError(t, err)
			w.dataWriter = storage.NoopWriter{}

			kvSize := 20 * size.KB
			keySize := size.KB
			key := make([]byte, keySize)
			key[keySize-1] = byte(i % 256)
			key[keySize-2] = byte(i / 256)
			minKey := slices.Clone(key)
			var maxKey []byte

			memSize := uint64(0)
			for j := 0; j < int(64*size.GB/kvSize); j++ {

				// copied from OneFileWriter.WriteRow

				if memSize >= DefaultMemSizeLimit {
					memSize = 0
					w.kvStore.Close()
					encodedStat := w.rc.encode()
					_, err := w.statWriter.Write(ctx, encodedStat)
					if err != nil {
						return err
					}
					w.rc.reset()
					// the new prop should have the same offset with kvStore.
					w.rc.currProp.offset = w.kvStore.offset
				}
				if len(w.rc.currProp.firstKey) == 0 {
					w.rc.currProp.firstKey = key
				}
				w.rc.currProp.lastKey = key

				memSize += kvSize
				w.totalSize += kvSize
				w.rc.currProp.size += kvSize - 2*lengthBytes
				w.rc.currProp.keys++

				if w.rc.currProp.size >= w.rc.propSizeDist ||
					w.rc.currProp.keys >= w.rc.propKeysDist {
					newProp := *w.rc.currProp
					w.rc.props = append(w.rc.props, &newProp)
					// reset currProp, and start to update this prop.
					w.rc.currProp.firstKey = nil
					w.rc.currProp.offset = memSize
					w.rc.currProp.keys = 0
					w.rc.currProp.size = 0
				}

				if j == int(64*size.GB/kvSize)-1 {
					maxKey = slices.Clone(key)
				}

				// increase the key

				for k := keySize - 3; k >= 0; k-- {
					key[k]++
					if key[k] != 0 {
						break
					}
				}
			}

			// copied from mergeOverlappingFilesInternal
			var stat MultipleFilesStat
			stat.Filenames = append(stat.Filenames,
				[2]string{w.dataFile, w.statFile})
			stat.build([]kv.Key{minKey}, []kv.Key{maxKey})
			statCh <- []MultipleFilesStat{stat}
			return w.Close(ctx)
		})
	}

	require.NoError(t, eg.Wait())

	multiStat := make([]MultipleFilesStat, 0, fileNum)
	for i := 0; i < fileNum; i++ {
		multiStat = append(multiStat, <-statCh...)
	}
	splitter, err := NewRangeSplitter(
		ctx,
		multiStat,
		store,
		int64(config.DefaultBatchSize),
		int64(math.MaxInt64),
		int64(config.SplitRegionSize),
		int64(config.SplitRegionKeys),
	)
	require.NoError(t, err)
	var lastEndKey []byte
	for {
		endKey, _, statFiles, _, err := splitter.SplitOneRangesGroup()
		require.NoError(t, err)
		require.Greater(t, len(statFiles), 0)
		if endKey == nil {
			break
		}
		if lastEndKey != nil {
			cmp := bytes.Compare(endKey, lastEndKey)
			require.Equal(t, 1, cmp, "endKey: %v, lastEndKey: %v", endKey, lastEndKey)
		}
		lastEndKey = slices.Clone(endKey)
	}
	err = splitter.Close()
	require.NoError(t, err)
}
