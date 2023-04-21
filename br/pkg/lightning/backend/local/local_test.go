// Copyright 2019 PingCAP, Inc.
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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/rand"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/docker/go-units"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/errorpb"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/keyspace"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/engine"
	"github.com/pingcap/tidb/util/hack"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNextKey(t *testing.T) {
	require.Equal(t, []byte{}, nextKey([]byte{}))

	cases := [][]byte{
		{0},
		{255},
		{1, 255},
	}
	for _, b := range cases {
		next := nextKey(b)
		require.Equal(t, append(b, 0), next)
	}

	// in the old logic, this should return []byte{} which is not the actually smallest eky
	next := nextKey([]byte{1, 255})
	require.Equal(t, -1, bytes.Compare(next, []byte{2}))

	// another test case, nextkey()'s return should be smaller than key with a prefix of the origin key
	next = nextKey([]byte{1, 255})
	require.Equal(t, -1, bytes.Compare(next, []byte{1, 255, 0, 1, 2}))

	// test recode key
	// key with int handle
	for _, handleID := range []int64{math.MinInt64, 1, 255, math.MaxInt32 - 1} {
		key := tablecodec.EncodeRowKeyWithHandle(1, tidbkv.IntHandle(handleID))
		require.Equal(t, []byte(tablecodec.EncodeRowKeyWithHandle(1, tidbkv.IntHandle(handleID+1))), nextKey(key))
	}

	// overflowed
	key := tablecodec.EncodeRowKeyWithHandle(1, tidbkv.IntHandle(math.MaxInt64))
	next = tablecodec.EncodeTablePrefix(2)
	require.Less(t, string(key), string(next))
	require.Equal(t, next, nextKey(key))

	testDatums := [][]types.Datum{
		{types.NewIntDatum(1), types.NewIntDatum(2)},
		{types.NewIntDatum(255), types.NewIntDatum(256)},
		{types.NewIntDatum(math.MaxInt32), types.NewIntDatum(math.MaxInt32 + 1)},
		{types.NewStringDatum("test"), types.NewStringDatum("test\000")},
		{types.NewStringDatum("test\255"), types.NewStringDatum("test\255\000")},
	}

	stmtCtx := new(stmtctx.StatementContext)
	for _, datums := range testDatums {
		keyBytes, err := codec.EncodeKey(stmtCtx, nil, types.NewIntDatum(123), datums[0])
		require.NoError(t, err)
		h, err := tidbkv.NewCommonHandle(keyBytes)
		require.NoError(t, err)
		key := tablecodec.EncodeRowKeyWithHandle(1, h)
		nextKeyBytes, err := codec.EncodeKey(stmtCtx, nil, types.NewIntDatum(123), datums[1])
		require.NoError(t, err)
		nextHdl, err := tidbkv.NewCommonHandle(nextKeyBytes)
		require.NoError(t, err)
		nextValidKey := []byte(tablecodec.EncodeRowKeyWithHandle(1, nextHdl))
		// nextKey may return a key that can't be decoded, but it must not be larger than the valid next key.
		require.True(t, bytes.Compare(nextKey(key), nextValidKey) <= 0, "datums: %v", datums)
	}

	// a special case that when len(string datum) % 8 == 7, nextKey twice should not panic.
	keyBytes, err := codec.EncodeKey(stmtCtx, nil, types.NewStringDatum("1234567"))
	require.NoError(t, err)
	h, err := tidbkv.NewCommonHandle(keyBytes)
	require.NoError(t, err)
	key = tablecodec.EncodeRowKeyWithHandle(1, h)
	nextOnce := nextKey(key)
	// should not panic
	_ = nextKey(nextOnce)

	// dIAAAAAAAAD/PV9pgAAAAAD/AAABA4AAAAD/AAAAAQOAAAD/AAAAAAEAAAD8
	// a index key with: table: 61, index: 1, int64: 1, int64: 1
	a := []byte{116, 128, 0, 0, 0, 0, 0, 0, 255, 61, 95, 105, 128, 0, 0, 0, 0, 255, 0, 0, 1, 3, 128, 0, 0, 0, 255, 0, 0, 0, 1, 3, 128, 0, 0, 255, 0, 0, 0, 0, 1, 0, 0, 0, 252}
	require.Equal(t, append(a, 0), nextKey(a))
}

// The first half of this test is same as the test in tikv:
// https://github.com/tikv/tikv/blob/dbfe7730dd0fddb34cb8c3a7f8a079a1349d2d41/components/engine_rocks/src/properties.rs#L572
func TestRangeProperties(t *testing.T) {
	type testCase struct {
		key   []byte
		vLen  int
		count int
	}
	cases := []testCase{
		// handle "a": size(size = 1, offset = 1),keys(1,1)
		{[]byte("a"), 0, 1},
		{[]byte("b"), defaultPropSizeIndexDistance / 8, 1},
		{[]byte("c"), defaultPropSizeIndexDistance / 4, 1},
		{[]byte("d"), defaultPropSizeIndexDistance / 2, 1},
		{[]byte("e"), defaultPropSizeIndexDistance / 8, 1},
		// handle "e": size(size = DISTANCE + 4, offset = DISTANCE + 5),keys(4,5)
		{[]byte("f"), defaultPropSizeIndexDistance / 4, 1},
		{[]byte("g"), defaultPropSizeIndexDistance / 2, 1},
		{[]byte("h"), defaultPropSizeIndexDistance / 8, 1},
		{[]byte("i"), defaultPropSizeIndexDistance / 4, 1},
		// handle "i": size(size = DISTANCE / 8 * 9 + 4, offset = DISTANCE / 8 * 17 + 9),keys(4,5)
		{[]byte("j"), defaultPropSizeIndexDistance / 2, 1},
		{[]byte("k"), defaultPropSizeIndexDistance / 2, 1},
		// handle "k": size(size = DISTANCE + 2, offset = DISTANCE / 8 * 25 + 11),keys(2,11)
		{[]byte("l"), 0, defaultPropKeysIndexDistance / 2},
		{[]byte("m"), 0, defaultPropKeysIndexDistance / 2},
		// handle "m": keys = DEFAULT_PROP_KEYS_INDEX_DISTANCE,offset = 11+DEFAULT_PROP_KEYS_INDEX_DISTANCE
		{[]byte("n"), 1, defaultPropKeysIndexDistance},
		// handle "n": keys = DEFAULT_PROP_KEYS_INDEX_DISTANCE, offset = 11+2*DEFAULT_PROP_KEYS_INDEX_DISTANCE
		{[]byte("o"), 1, 1},
		// handle　"o": keys = 1, offset = 12 + 2*DEFAULT_PROP_KEYS_INDEX_DISTANCE
	}

	collector := newRangePropertiesCollector()
	for _, p := range cases {
		v := make([]byte, p.vLen)
		for i := 0; i < p.count; i++ {
			_ = collector.Add(pebble.InternalKey{UserKey: p.key, Trailer: pebble.InternalKeyKindSet}, v)
		}
	}

	userProperties := make(map[string]string, 1)
	_ = collector.Finish(userProperties)

	props, err := decodeRangeProperties(hack.Slice(userProperties[propRangeIndex]), noopKeyAdapter{})
	require.NoError(t, err)

	// Smallest key in props.
	require.Equal(t, cases[0].key, props[0].Key)
	// Largest key in props.
	require.Equal(t, cases[len(cases)-1].key, props[len(props)-1].Key)
	require.Len(t, props, 7)

	props2 := rangeProperties([]rangeProperty{
		{[]byte("b"), rangeOffsets{defaultPropSizeIndexDistance + 10, defaultPropKeysIndexDistance / 2}},
		{[]byte("h"), rangeOffsets{defaultPropSizeIndexDistance * 3 / 2, defaultPropKeysIndexDistance * 3 / 2}},
		{[]byte("k"), rangeOffsets{defaultPropSizeIndexDistance * 3, defaultPropKeysIndexDistance * 7 / 4}},
		{[]byte("mm"), rangeOffsets{defaultPropSizeIndexDistance * 5, defaultPropKeysIndexDistance * 2}},
		{[]byte("q"), rangeOffsets{defaultPropSizeIndexDistance * 7, defaultPropKeysIndexDistance*9/4 + 10}},
		{[]byte("y"), rangeOffsets{defaultPropSizeIndexDistance*7 + 100, defaultPropKeysIndexDistance*9/4 + 1010}},
	})

	sizeProps := newSizeProperties()
	sizeProps.addAll(props)
	sizeProps.addAll(props2)

	res := []*rangeProperty{
		{[]byte("a"), rangeOffsets{1, 1}},
		{[]byte("b"), rangeOffsets{defaultPropSizeIndexDistance + 10, defaultPropKeysIndexDistance / 2}},
		{[]byte("e"), rangeOffsets{defaultPropSizeIndexDistance + 4, 4}},
		{[]byte("h"), rangeOffsets{defaultPropSizeIndexDistance/2 - 10, defaultPropKeysIndexDistance}},
		{[]byte("i"), rangeOffsets{defaultPropSizeIndexDistance*9/8 + 4, 4}},
		{[]byte("k"), rangeOffsets{defaultPropSizeIndexDistance*5/2 + 2, defaultPropKeysIndexDistance/4 + 2}},
		{[]byte("m"), rangeOffsets{defaultPropKeysIndexDistance, defaultPropKeysIndexDistance}},
		{[]byte("mm"), rangeOffsets{defaultPropSizeIndexDistance * 2, defaultPropKeysIndexDistance / 4}},
		{[]byte("n"), rangeOffsets{defaultPropKeysIndexDistance * 2, defaultPropKeysIndexDistance}},
		{[]byte("o"), rangeOffsets{2, 1}},
		{[]byte("q"), rangeOffsets{defaultPropSizeIndexDistance * 2, defaultPropKeysIndexDistance/4 + 10}},
		{[]byte("y"), rangeOffsets{100, 1000}},
	}

	require.Equal(t, 12, sizeProps.indexHandles.Len())
	idx := 0
	sizeProps.iter(func(p *rangeProperty) bool {
		require.Equal(t, res[idx], p)
		idx++
		return true
	})

	fullRange := Range{start: []byte("a"), end: []byte("z")}
	ranges := splitRangeBySizeProps(fullRange, sizeProps, 2*defaultPropSizeIndexDistance, defaultPropKeysIndexDistance*5/2)

	require.Equal(t, []Range{
		{start: []byte("a"), end: []byte("e")},
		{start: []byte("e"), end: []byte("k")},
		{start: []byte("k"), end: []byte("mm")},
		{start: []byte("mm"), end: []byte("q")},
		{start: []byte("q"), end: []byte("z")},
	}, ranges)

	ranges = splitRangeBySizeProps(fullRange, sizeProps, 2*defaultPropSizeIndexDistance, defaultPropKeysIndexDistance)
	require.Equal(t, []Range{
		{start: []byte("a"), end: []byte("e")},
		{start: []byte("e"), end: []byte("h")},
		{start: []byte("h"), end: []byte("k")},
		{start: []byte("k"), end: []byte("m")},
		{start: []byte("m"), end: []byte("mm")},
		{start: []byte("mm"), end: []byte("n")},
		{start: []byte("n"), end: []byte("q")},
		{start: []byte("q"), end: []byte("z")},
	}, ranges)
}

func TestRangePropertiesWithPebble(t *testing.T) {
	sizeDistance := uint64(500)
	keysDistance := uint64(20)
	opt := &pebble.Options{
		MemTableSize:             512 * units.MiB,
		MaxConcurrentCompactions: 16,
		L0CompactionThreshold:    math.MaxInt32, // set to max try to disable compaction
		L0StopWritesThreshold:    math.MaxInt32, // set to max try to disable compaction
		MaxOpenFiles:             10000,
		DisableWAL:               true,
		ReadOnly:                 false,
		TablePropertyCollectors: []func() pebble.TablePropertyCollector{
			func() pebble.TablePropertyCollector {
				return &RangePropertiesCollector{
					props:               make([]rangeProperty, 0, 1024),
					propSizeIdxDistance: sizeDistance,
					propKeysIdxDistance: keysDistance,
				}
			},
		},
	}
	db, _ := makePebbleDB(t, opt)
	defer db.Close()

	// local collector
	collector := &RangePropertiesCollector{
		props:               make([]rangeProperty, 0, 1024),
		propSizeIdxDistance: sizeDistance,
		propKeysIdxDistance: keysDistance,
	}
	writeOpt := &pebble.WriteOptions{Sync: false}
	value := make([]byte, 100)
	for i := 0; i < 10; i++ {
		wb := db.NewBatch()
		for j := 0; j < 100; j++ {
			key := make([]byte, 8)
			valueLen := rand.Intn(50)
			binary.BigEndian.PutUint64(key, uint64(i*100+j))
			err := wb.Set(key, value[:valueLen], writeOpt)
			require.NoError(t, err)
			err = collector.Add(pebble.InternalKey{UserKey: key, Trailer: pebble.InternalKeyKindSet}, value[:valueLen])
			require.NoError(t, err)
		}
		require.NoError(t, wb.Commit(writeOpt))
	}
	// flush one sst
	require.NoError(t, db.Flush())

	props := make(map[string]string, 1)
	require.NoError(t, collector.Finish(props))

	sstMetas, err := db.SSTables(pebble.WithProperties())
	require.NoError(t, err)
	for i, level := range sstMetas {
		if i == 0 {
			require.Equal(t, 1, len(level))
		} else {
			require.Empty(t, level)
		}
	}

	require.Equal(t, props, sstMetas[0][0].Properties.UserProperties)
}

func testLocalWriter(t *testing.T, needSort bool, partitialSort bool) {
	opt := &pebble.Options{
		MemTableSize:             1024 * 1024,
		MaxConcurrentCompactions: 16,
		L0CompactionThreshold:    math.MaxInt32, // set to max try to disable compaction
		L0StopWritesThreshold:    math.MaxInt32, // set to max try to disable compaction
		DisableWAL:               true,
		ReadOnly:                 false,
	}
	db, tmpPath := makePebbleDB(t, opt)
	defer db.Close()

	_, engineUUID := backend.MakeUUID("ww", 0)
	engineCtx, cancel := context.WithCancel(context.Background())
	f := &Engine{
		db:           db,
		UUID:         engineUUID,
		sstDir:       tmpPath,
		ctx:          engineCtx,
		cancel:       cancel,
		sstMetasChan: make(chan metaOrFlush, 64),
		keyAdapter:   noopKeyAdapter{},
		logger:       log.L(),
	}
	f.sstIngester = dbSSTIngester{e: f}
	f.wg.Add(1)
	go f.ingestSSTLoop()
	sorted := needSort && !partitialSort
	pool := membuf.NewPool()
	defer pool.Destroy()
	kvBuffer := pool.NewBuffer()
	w, err := openLocalWriter(&backend.LocalWriterConfig{IsKVSorted: sorted}, f, keyspace.CodecV1, 1024, kvBuffer)
	require.NoError(t, err)

	ctx := context.Background()
	var kvs []common.KvPair
	value := make([]byte, 128)
	for i := 0; i < 16; i++ {
		binary.BigEndian.PutUint64(value[i*8:], uint64(i))
	}
	var keys [][]byte
	for i := 1; i <= 20000; i++ {
		var kv common.KvPair
		kv.Key = make([]byte, 16)
		kv.Val = make([]byte, 128)
		copy(kv.Val, value)
		key := rand.Intn(1000)
		binary.BigEndian.PutUint64(kv.Key, uint64(key))
		binary.BigEndian.PutUint64(kv.Key[8:], uint64(i))
		kvs = append(kvs, kv)
		keys = append(keys, kv.Key)
	}
	var rows1 []common.KvPair
	var rows2 []common.KvPair
	var rows3 []common.KvPair
	rows4 := kvs[:12000]
	if partitialSort {
		sort.Slice(rows4, func(i, j int) bool {
			return bytes.Compare(rows4[i].Key, rows4[j].Key) < 0
		})
		rows1 = rows4[:6000]
		rows3 = rows4[6000:]
		rows2 = kvs[12000:]
	} else {
		if needSort {
			sort.Slice(kvs, func(i, j int) bool {
				return bytes.Compare(kvs[i].Key, kvs[j].Key) < 0
			})
		}
		rows1 = kvs[:6000]
		rows2 = kvs[6000:12000]
		rows3 = kvs[12000:]
	}
	err = w.AppendRows(ctx, "", []string{}, kv.MakeRowsFromKvPairs(rows1))
	require.NoError(t, err)
	err = w.AppendRows(ctx, "", []string{}, kv.MakeRowsFromKvPairs(rows2))
	require.NoError(t, err)
	err = w.AppendRows(ctx, "", []string{}, kv.MakeRowsFromKvPairs(rows3))
	require.NoError(t, err)
	flushStatus, err := w.Close(context.Background())
	require.NoError(t, err)
	require.NoError(t, f.flushEngineWithoutLock(ctx))
	require.True(t, flushStatus.Flushed())
	o := &pebble.IterOptions{}
	it := db.NewIter(o)

	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})
	require.Equal(t, 20000, int(f.Length.Load()))
	require.Equal(t, 144*20000, int(f.TotalSize.Load()))
	valid := it.SeekGE(keys[0])
	require.True(t, valid)
	for _, k := range keys {
		require.Equal(t, k, it.Key())
		it.Next()
	}
	close(f.sstMetasChan)
	f.wg.Wait()
}

func TestLocalWriterWithSort(t *testing.T) {
	testLocalWriter(t, false, false)
}

func TestLocalWriterWithIngest(t *testing.T) {
	testLocalWriter(t, true, false)
}

func TestLocalWriterWithIngestUnsort(t *testing.T) {
	testLocalWriter(t, true, true)
}

type mockSplitClient struct {
	split.SplitClient
}

func (c *mockSplitClient) GetRegion(ctx context.Context, key []byte) (*split.RegionInfo, error) {
	return &split.RegionInfo{
		Leader: &metapb.Peer{Id: 1},
		Region: &metapb.Region{
			Id:       1,
			StartKey: key,
		},
	}, nil
}

type testIngester struct{}

func (i testIngester) mergeSSTs(metas []*sstMeta, dir string) (*sstMeta, error) {
	if len(metas) == 0 {
		return nil, errors.New("sst metas is empty")
	} else if len(metas) == 1 {
		return metas[0], nil
	}
	if metas[len(metas)-1].seq-metas[0].seq != int32(len(metas)-1) {
		panic("metas is not add in order")
	}

	newMeta := &sstMeta{
		seq: metas[len(metas)-1].seq,
	}
	for _, m := range metas {
		newMeta.totalSize += m.totalSize
		newMeta.totalCount += m.totalCount
	}
	return newMeta, nil
}

func (i testIngester) ingest([]*sstMeta) error {
	return nil
}

func TestLocalIngestLoop(t *testing.T) {
	opt := &pebble.Options{
		MemTableSize:             1024 * 1024,
		MaxConcurrentCompactions: 16,
		L0CompactionThreshold:    math.MaxInt32, // set to max try to disable compaction
		L0StopWritesThreshold:    math.MaxInt32, // set to max try to disable compaction
		DisableWAL:               true,
		ReadOnly:                 false,
	}
	db, tmpPath := makePebbleDB(t, opt)
	defer db.Close()
	_, engineUUID := backend.MakeUUID("ww", 0)
	engineCtx, cancel := context.WithCancel(context.Background())
	f := Engine{
		db:           db,
		UUID:         engineUUID,
		sstDir:       tmpPath,
		ctx:          engineCtx,
		cancel:       cancel,
		sstMetasChan: make(chan metaOrFlush, 64),
		config: backend.LocalEngineConfig{
			Compact:            true,
			CompactThreshold:   100,
			CompactConcurrency: 4,
		},
		logger: log.L(),
	}
	f.sstIngester = testIngester{}
	f.wg.Add(1)
	go f.ingestSSTLoop()

	// add some routines to add ssts
	var wg sync.WaitGroup
	wg.Add(4)
	totalSize := int64(0)
	concurrency := 4
	count := 500
	var metaSeqLock sync.Mutex
	maxMetaSeq := int32(0)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			flushCnt := rand.Int31n(10) + 1
			seq := int32(0)
			for i := 0; i < count; i++ {
				size := int64(rand.Int31n(50) + 1)
				m := &sstMeta{totalSize: size, totalCount: 1}
				atomic.AddInt64(&totalSize, size)
				metaSeq, err := f.addSST(engineCtx, m)
				require.NoError(t, err)
				if int32(i) >= flushCnt {
					f.mutex.RLock()
					err = f.flushEngineWithoutLock(engineCtx)
					require.NoError(t, err)
					f.mutex.RUnlock()
					flushCnt += rand.Int31n(10) + 1
				}
				seq = metaSeq
			}
			metaSeqLock.Lock()
			if atomic.LoadInt32(&maxMetaSeq) < seq {
				atomic.StoreInt32(&maxMetaSeq, seq)
			}
			metaSeqLock.Unlock()
		}()
	}
	wg.Wait()

	f.mutex.RLock()
	err := f.flushEngineWithoutLock(engineCtx)
	require.NoError(t, err)
	f.mutex.RUnlock()

	close(f.sstMetasChan)
	f.wg.Wait()
	require.NoError(t, f.ingestErr.Get())
	require.Equal(t, f.TotalSize.Load(), totalSize)
	require.Equal(t, int64(concurrency*count), f.Length.Load())
	require.Equal(t, atomic.LoadInt32(&maxMetaSeq), f.finishedMetaSeq.Load())
}

func makeRanges(input []string) []Range {
	ranges := make([]Range, 0, len(input)/2)
	for i := 0; i < len(input)-1; i += 2 {
		ranges = append(ranges, Range{start: []byte(input[i]), end: []byte(input[i+1])})
	}
	return ranges
}

func TestDedupAndMergeRanges(t *testing.T) {
	cases := [][]string{
		// empty
		{},
		{},
		// without overlap
		{"1", "2", "3", "4", "5", "6", "7", "8"},
		{"1", "2", "3", "4", "5", "6", "7", "8"},
		// merge all as one
		{"1", "12", "12", "13", "13", "14", "14", "15", "15", "999"},
		{"1", "999"},
		// overlap
		{"1", "12", "12", "13", "121", "129", "122", "133", "14", "15", "15", "999"},
		{"1", "133", "14", "999"},

		// out of order, same as test 3
		{"15", "999", "1", "12", "121", "129", "12", "13", "122", "133", "14", "15"},
		{"1", "133", "14", "999"},

		// not continuous
		{"000", "001", "002", "004", "100", "108", "107", "200", "255", "300"},
		{"000", "001", "002", "004", "100", "200", "255", "300"},
	}

	for i := 0; i < len(cases)-1; i += 2 {
		input := makeRanges(cases[i])
		output := makeRanges(cases[i+1])

		require.Equal(t, output, sortAndMergeRanges(input))
	}
}

func TestFilterOverlapRange(t *testing.T) {
	cases := [][]string{
		// both empty input
		{},
		{},
		{},

		// ranges are empty
		{},
		{"0", "1"},
		{},

		// finished ranges are empty
		{"0", "1", "2", "3"},
		{},
		{"0", "1", "2", "3"},

		// single big finished range
		{"00", "10", "20", "30", "40", "50", "60", "70"},
		{"25", "65"},
		{"00", "10", "20", "25", "65", "70"},

		// single big input
		{"10", "99"},
		{"00", "10", "15", "30", "45", "60"},
		{"10", "15", "30", "45", "60", "99"},

		// multi input and finished
		{"00", "05", "05", "10", "10", "20", "30", "45", "50", "70", "70", "90"},
		{"07", "12", "14", "16", "17", "30", "45", "70"},
		{"00", "05", "05", "07", "12", "14", "16", "17", "30", "45", "70", "90"},
	}

	for i := 0; i < len(cases)-2; i += 3 {
		input := makeRanges(cases[i])
		finished := makeRanges(cases[i+1])
		output := makeRanges(cases[i+2])

		require.Equal(t, output, filterOverlapRange(input, finished))
	}
}

func testMergeSSTs(t *testing.T, kvs [][]common.KvPair, meta *sstMeta) {
	opt := &pebble.Options{
		MemTableSize:             1024 * 1024,
		MaxConcurrentCompactions: 16,
		L0CompactionThreshold:    math.MaxInt32, // set to max try to disable compaction
		L0StopWritesThreshold:    math.MaxInt32, // set to max try to disable compaction
		DisableWAL:               true,
		ReadOnly:                 false,
	}
	db, tmpPath := makePebbleDB(t, opt)
	defer db.Close()
	_, engineUUID := backend.MakeUUID("ww", 0)
	engineCtx, cancel := context.WithCancel(context.Background())

	f := &Engine{
		db:           db,
		UUID:         engineUUID,
		sstDir:       tmpPath,
		ctx:          engineCtx,
		cancel:       cancel,
		sstMetasChan: make(chan metaOrFlush, 64),
		config: backend.LocalEngineConfig{
			Compact:            true,
			CompactThreshold:   100,
			CompactConcurrency: 4,
		},
		logger: log.L(),
	}

	createSSTWriter := func() (*sstWriter, error) {
		path := filepath.Join(f.sstDir, uuid.New().String()+".sst")
		writer, err := newSSTWriter(path)
		if err != nil {
			return nil, err
		}
		sw := &sstWriter{sstMeta: &sstMeta{path: path}, writer: writer}
		return sw, nil
	}

	metas := make([]*sstMeta, 0, len(kvs))

	for _, kv := range kvs {
		w, err := createSSTWriter()
		require.NoError(t, err)

		err = w.writeKVs(kv)
		require.NoError(t, err)

		require.NoError(t, w.writer.Close())
		metas = append(metas, w.sstMeta)
	}

	i := dbSSTIngester{e: f}
	newMeta, err := i.mergeSSTs(metas, tmpPath)
	require.NoError(t, err)

	require.Equal(t, meta.totalCount, newMeta.totalCount)
	require.Equal(t, meta.totalSize, newMeta.totalSize)
}

func TestMergeSSTs(t *testing.T) {
	kvs := make([][]common.KvPair, 0, 5)
	for i := 0; i < 5; i++ {
		var pairs []common.KvPair
		for j := 0; j < 10; j++ {
			var kv common.KvPair
			kv.Key = make([]byte, 16)
			key := i*100 + j
			binary.BigEndian.PutUint64(kv.Key, uint64(key))
			pairs = append(pairs, kv)
		}

		kvs = append(kvs, pairs)
	}

	testMergeSSTs(t, kvs, &sstMeta{totalCount: 50, totalSize: 800})
}

func TestMergeSSTsDuplicated(t *testing.T) {
	kvs := make([][]common.KvPair, 0, 5)
	for i := 0; i < 4; i++ {
		var pairs []common.KvPair
		for j := 0; j < 10; j++ {
			var kv common.KvPair
			kv.Key = make([]byte, 16)
			key := i*100 + j
			binary.BigEndian.PutUint64(kv.Key, uint64(key))
			pairs = append(pairs, kv)
		}

		kvs = append(kvs, pairs)
	}

	// make a duplication
	kvs = append(kvs, kvs[0])

	testMergeSSTs(t, kvs, &sstMeta{totalCount: 40, totalSize: 640})
}

type mockPdClient struct {
	pd.Client
	stores  []*metapb.Store
	regions []*pd.Region
}

func (c *mockPdClient) GetAllStores(ctx context.Context, opts ...pd.GetStoreOption) ([]*metapb.Store, error) {
	return c.stores, nil
}

func (c *mockPdClient) ScanRegions(ctx context.Context, key, endKey []byte, limit int) ([]*pd.Region, error) {
	return c.regions, nil
}

type mockGrpcErr struct{}

func (e mockGrpcErr) GRPCStatus() *status.Status {
	return status.New(codes.Unimplemented, "unimplemented")
}

func (e mockGrpcErr) Error() string {
	return "unimplemented"
}

type mockImportClient struct {
	sst.ImportSSTClient
	store              *metapb.Store
	resp               *sst.IngestResponse
	err                error
	retry              int
	cnt                int
	multiIngestCheckFn func(s *metapb.Store) bool
	apiInvokeRecorder  map[string][]uint64
}

func newMockImportClient() *mockImportClient {
	return &mockImportClient{
		multiIngestCheckFn: func(s *metapb.Store) bool {
			return true
		},
	}
}

func (c *mockImportClient) MultiIngest(context.Context, *sst.MultiIngestRequest, ...grpc.CallOption) (*sst.IngestResponse, error) {
	defer func() {
		c.cnt++
	}()
	if c.apiInvokeRecorder != nil {
		c.apiInvokeRecorder["MultiIngest"] = append(c.apiInvokeRecorder["MultiIngest"], c.store.GetId())
	}
	if c.cnt < c.retry && (c.err != nil || c.resp != nil) {
		return c.resp, c.err
	}

	if !c.multiIngestCheckFn(c.store) {
		return nil, mockGrpcErr{}
	}
	return &sst.IngestResponse{}, nil
}

type mockWriteClient struct {
	sst.ImportSST_WriteClient
	writeResp *sst.WriteResponse
}

func (m mockWriteClient) Send(request *sst.WriteRequest) error {
	return nil
}

func (m mockWriteClient) CloseAndRecv() (*sst.WriteResponse, error) {
	return m.writeResp, nil
}

func (c *mockImportClient) Write(ctx context.Context, opts ...grpc.CallOption) (sst.ImportSST_WriteClient, error) {
	if c.apiInvokeRecorder != nil {
		c.apiInvokeRecorder["Write"] = append(c.apiInvokeRecorder["Write"], c.store.GetId())
	}
	return mockWriteClient{writeResp: &sst.WriteResponse{Metas: []*sst.SSTMeta{
		{}, {}, {},
	}}}, nil
}

type mockImportClientFactory struct {
	stores            []*metapb.Store
	createClientFn    func(store *metapb.Store) sst.ImportSSTClient
	apiInvokeRecorder map[string][]uint64
}

func (f *mockImportClientFactory) Create(_ context.Context, storeID uint64) (sst.ImportSSTClient, error) {
	for _, store := range f.stores {
		if store.Id == storeID {
			return f.createClientFn(store), nil
		}
	}
	return nil, fmt.Errorf("store %d not found", storeID)
}

func (f *mockImportClientFactory) Close() {}

func TestMultiIngest(t *testing.T) {
	allStores := []*metapb.Store{
		{
			Id:    1,
			State: metapb.StoreState_Offline,
		},
		{
			Id:    2,
			State: metapb.StoreState_Tombstone,
			Labels: []*metapb.StoreLabel{
				{
					Key:   "test",
					Value: "tiflash",
				},
			},
		},
		{
			Id:    3,
			State: metapb.StoreState_Up,
			Labels: []*metapb.StoreLabel{
				{
					Key:   "test",
					Value: "123",
				},
			},
		},
		{
			Id:    4,
			State: metapb.StoreState_Tombstone,
			Labels: []*metapb.StoreLabel{
				{
					Key:   "engine",
					Value: "test",
				},
			},
		},
		{
			Id:    5,
			State: metapb.StoreState_Tombstone,
			Labels: []*metapb.StoreLabel{
				{
					Key:   "engine",
					Value: "test123",
				},
			},
		},
		{
			Id:    6,
			State: metapb.StoreState_Offline,
			Labels: []*metapb.StoreLabel{
				{
					Key:   "engine",
					Value: "tiflash",
				},
			},
		},
		{
			Id:    7,
			State: metapb.StoreState_Up,
			Labels: []*metapb.StoreLabel{
				{
					Key:   "test",
					Value: "123",
				},
				{
					Key:   "engine",
					Value: "tiflash",
				},
			},
		},
		{
			Id:    8,
			State: metapb.StoreState_Up,
		},
	}
	cases := []struct {
		filter             func(store *metapb.Store) bool
		multiIngestSupport func(s *metapb.Store) bool
		retry              int
		err                error
		supportMutliIngest bool
		retErr             string
	}{
		// test up stores with all support multiIngest
		{
			func(store *metapb.Store) bool {
				return store.State == metapb.StoreState_Up
			},
			func(s *metapb.Store) bool {
				return true
			},
			0,
			nil,
			true,
			"",
		},
		// test all up stores with tiflash not support multi ingest
		{
			func(store *metapb.Store) bool {
				return store.State == metapb.StoreState_Up
			},
			func(s *metapb.Store) bool {
				return !engine.IsTiFlash(s)
			},
			0,
			nil,
			true,
			"",
		},
		// test all up stores with only tiflash support multi ingest
		{
			func(store *metapb.Store) bool {
				return store.State == metapb.StoreState_Up
			},
			func(s *metapb.Store) bool {
				return engine.IsTiFlash(s)
			},
			0,
			nil,
			false,
			"",
		},
		// test all up stores with some non-tiflash store support multi ingest
		{
			func(store *metapb.Store) bool {
				return store.State == metapb.StoreState_Up
			},
			func(s *metapb.Store) bool {
				return len(s.Labels) > 0
			},
			0,
			nil,
			false,
			"",
		},
		// test all stores with all states
		{
			func(store *metapb.Store) bool {
				return true
			},
			func(s *metapb.Store) bool {
				return true
			},
			0,
			nil,
			true,
			"",
		},
		// test all non-tiflash stores that support multi ingests
		{
			func(store *metapb.Store) bool {
				return !engine.IsTiFlash(store)
			},
			func(s *metapb.Store) bool {
				return !engine.IsTiFlash(s)
			},
			0,
			nil,
			true,
			"",
		},
		// test only up stores support multi ingest
		{
			func(store *metapb.Store) bool {
				return true
			},
			func(s *metapb.Store) bool {
				return s.State == metapb.StoreState_Up
			},
			0,
			nil,
			true,
			"",
		},
		// test only offline/tombstore stores support multi ingest
		{
			func(store *metapb.Store) bool {
				return true
			},
			func(s *metapb.Store) bool {
				return s.State != metapb.StoreState_Up
			},
			0,
			nil,
			false,
			"",
		},
		// test grpc return error but no tiflash
		{
			func(store *metapb.Store) bool {
				return !engine.IsTiFlash(store)
			},
			func(s *metapb.Store) bool {
				return true
			},
			math.MaxInt32,
			errors.New("mock error"),
			false,
			"",
		},
		// test grpc return error and contains offline tiflash
		{
			func(store *metapb.Store) bool {
				return !engine.IsTiFlash(store) || store.State != metapb.StoreState_Up
			},
			func(s *metapb.Store) bool {
				return true
			},
			math.MaxInt32,
			errors.New("mock error"),
			false,
			"",
		},
		// test grpc return error
		{
			func(store *metapb.Store) bool {
				return true
			},
			func(s *metapb.Store) bool {
				return true
			},
			math.MaxInt32,
			errors.New("mock error"),
			false,
			"mock error",
		},
		// test grpc return error only once
		{
			func(store *metapb.Store) bool {
				return true
			},
			func(s *metapb.Store) bool {
				return true
			},
			1,
			errors.New("mock error"),
			true,
			"",
		},
	}

	for _, testCase := range cases {
		stores := make([]*metapb.Store, 0, len(allStores))
		for _, s := range allStores {
			if testCase.filter(s) {
				stores = append(stores, s)
			}
		}

		importCli := &mockImportClient{
			cnt:                0,
			retry:              testCase.retry,
			err:                testCase.err,
			multiIngestCheckFn: testCase.multiIngestSupport,
		}
		pdCtl := &pdutil.PdController{}
		pdCtl.SetPDClient(&mockPdClient{stores: stores})

		local := &local{
			pdCtl: pdCtl,
			importClientFactory: &mockImportClientFactory{
				stores: allStores,
				createClientFn: func(store *metapb.Store) sst.ImportSSTClient {
					importCli.store = store
					return importCli
				},
			},
			logger: log.L(),
		}
		err := local.checkMultiIngestSupport(context.Background())
		if err != nil {
			require.Contains(t, err.Error(), testCase.retErr)
		} else {
			require.Equal(t, testCase.supportMutliIngest, local.supportMultiIngest)
		}
	}
}

func TestLocalWriteAndIngestPairsFailFast(t *testing.T) {
	bak := local{}
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/br/pkg/lightning/backend/local/WriteToTiKVNotEnoughDiskSpace", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/br/pkg/lightning/backend/local/WriteToTiKVNotEnoughDiskSpace"))
	}()
	jobCh := make(chan *regionJob, 1)
	jobCh <- &regionJob{}
	jobOutCh := make(chan *regionJob, 1)
	err := bak.startWorker(context.Background(), jobCh, jobOutCh)
	require.Error(t, err)
	require.Regexp(t, "The available disk of TiKV.*", err.Error())
	require.Len(t, jobCh, 0)
}

func TestGetRegionSplitSizeKeys(t *testing.T) {
	allStores := []*metapb.Store{
		{
			Address:       "172.16.102.1:20160",
			StatusAddress: "0.0.0.0:20180",
		},
		{
			Address:       "172.16.102.2:20160",
			StatusAddress: "0.0.0.0:20180",
		},
		{
			Address:       "172.16.102.3:20160",
			StatusAddress: "0.0.0.0:20180",
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli := utils.FakePDClient{Stores: allStores}
	defer func() {
		getSplitConfFromStoreFunc = getSplitConfFromStore
	}()
	getSplitConfFromStoreFunc = func(ctx context.Context, host string, tls *common.TLS) (int64, int64, error) {
		if strings.Contains(host, "172.16.102.3:20180") {
			return int64(1), int64(2), nil
		}
		return 0, 0, errors.New("invalid connection")
	}
	splitSize, splitKeys, err := getRegionSplitSizeKeys(ctx, cli, nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), splitSize)
	require.Equal(t, int64(2), splitKeys)
}

func TestLocalIsRetryableTiKVWriteError(t *testing.T) {
	l := local{}
	require.True(t, l.isRetryableImportTiKVError(io.EOF))
	require.True(t, l.isRetryableImportTiKVError(errors.Trace(io.EOF)))
}

func TestCheckPeersBusy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	apiInvokeRecorder := map[string][]uint64{}
	serverIsBusyResp := &sst.IngestResponse{
		Error: &errorpb.Error{
			ServerIsBusy: &errorpb.ServerIsBusy{},
		}}

	createTimeStore12 := 0
	local := &local{
		importClientFactory: &mockImportClientFactory{
			stores: []*metapb.Store{
				{Id: 11}, {Id: 12}, {Id: 13}, // region ["a", "b")
				{Id: 21}, {Id: 22}, {Id: 23}, // region ["b", "")
			},
			createClientFn: func(store *metapb.Store) sst.ImportSSTClient {
				importCli := newMockImportClient()
				importCli.store = store
				importCli.apiInvokeRecorder = apiInvokeRecorder
				if store.Id == 12 {
					createTimeStore12++
					// the second time is checkWriteStall, we mock a busy response
					if createTimeStore12 == 2 {
						importCli.retry = 1
						importCli.resp = serverIsBusyResp
					}
				}
				return importCli
			},
		},
		logger:                log.L(),
		writeLimiter:          noopStoreWriteLimiter{},
		bufferPool:            membuf.NewPool(),
		supportMultiIngest:    true,
		shouldCheckWriteStall: true,
		tikvCodec:             keyspace.CodecV1,
	}

	db, tmpPath := makePebbleDB(t, nil)
	_, engineUUID := backend.MakeUUID("ww", 0)
	engineCtx, cancel2 := context.WithCancel(context.Background())
	f := &Engine{
		db:           db,
		UUID:         engineUUID,
		sstDir:       tmpPath,
		ctx:          engineCtx,
		cancel:       cancel2,
		sstMetasChan: make(chan metaOrFlush, 64),
		keyAdapter:   noopKeyAdapter{},
		logger:       log.L(),
	}
	err := f.db.Set([]byte("a"), []byte("a"), nil)
	require.NoError(t, err)
	err = f.db.Set([]byte("b"), []byte("b"), nil)
	require.NoError(t, err)

	jobCh := make(chan *regionJob, 10)

	retryJob := &regionJob{
		keyRange: Range{start: []byte("a"), end: []byte("b")},
		region: &split.RegionInfo{
			Region: &metapb.Region{
				Id: 1,
				Peers: []*metapb.Peer{
					{Id: 1, StoreId: 11}, {Id: 2, StoreId: 12}, {Id: 3, StoreId: 13},
				},
				StartKey: []byte("a"),
				EndKey:   []byte("b"),
			},
			Leader: &metapb.Peer{Id: 1, StoreId: 11},
		},
		stage:      regionScanned,
		engine:     f,
		retryCount: 20,
		waitUntil:  time.Now().Add(-time.Second),
	}
	jobCh <- retryJob

	jobCh <- &regionJob{
		keyRange: Range{start: []byte("b"), end: []byte("")},
		region: &split.RegionInfo{
			Region: &metapb.Region{
				Id: 4,
				Peers: []*metapb.Peer{
					{Id: 4, StoreId: 21}, {Id: 5, StoreId: 22}, {Id: 6, StoreId: 23},
				},
				StartKey: []byte("b"),
				EndKey:   []byte(""),
			},
			Leader: &metapb.Peer{Id: 4, StoreId: 21},
		},
		stage:      regionScanned,
		engine:     f,
		retryCount: 20,
		waitUntil:  time.Now().Add(-time.Second),
	}

	retryJobs := make([]*regionJob, 0, 1)

	var wg sync.WaitGroup
	wg.Add(1)
	jobOutCh := make(chan *regionJob)
	go func() {
		job := <-jobOutCh
		job.retryCount++
		retryJobs = append(retryJobs, job)
		<-jobOutCh
		wg.Done()
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := local.startWorker(ctx, jobCh, jobOutCh)
		require.NoError(t, err)
	}()

	// retryJob will be retried once and worker will sleep 30s before processing the
	// job again, we simply hope below check is happened when worker is sleeping
	time.Sleep(5 * time.Second)
	require.Len(t, retryJobs, 1)
	require.Same(t, retryJob, retryJobs[0])
	require.Equal(t, 21, retryJob.retryCount)
	require.Equal(t, wrote, retryJob.stage)

	cancel()
	wg.Wait()

	require.Equal(t, []uint64{11, 12, 13, 21, 22, 23}, apiInvokeRecorder["Write"])
	// store 12 has a follower busy, so it will break the workflow for region (11, 12, 13)
	require.Equal(t, []uint64{11, 12, 21, 22, 23, 21}, apiInvokeRecorder["MultiIngest"])
	// region (11, 12, 13) has key range ["a", "b"), it's not finished.
	require.Equal(t, []Range{{start: []byte("b"), end: []byte("")}}, f.finishedRanges.ranges)
}
