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
	"path"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	_ "unsafe"

	"github.com/cockroachdb/pebble"
	"github.com/docker/go-units"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/errorpb"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/keyspace"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/engine"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/http"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/status"
)

var GetSplitConfFromStore = getSplitConfFromStore

func SetGetSplitConfFromStoreFunc(
	fn func(ctx context.Context, host string, tls *common.TLS) (splitSize int64, regionSplitKeys int64, err error),
) {
	getSplitConfFromStoreFunc = fn
}

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

	stmtCtx := stmtctx.NewStmtCtx()
	for _, datums := range testDatums {
		keyBytes, err := codec.EncodeKey(stmtCtx.TimeZone(), nil, types.NewIntDatum(123), datums[0])
		require.NoError(t, err)
		h, err := tidbkv.NewCommonHandle(keyBytes)
		require.NoError(t, err)
		key := tablecodec.EncodeRowKeyWithHandle(1, h)
		nextKeyBytes, err := codec.EncodeKey(stmtCtx.TimeZone(), nil, types.NewIntDatum(123), datums[1])
		require.NoError(t, err)
		nextHdl, err := tidbkv.NewCommonHandle(nextKeyBytes)
		require.NoError(t, err)
		nextValidKey := []byte(tablecodec.EncodeRowKeyWithHandle(1, nextHdl))
		// nextKey may return a key that can't be decoded, but it must not be larger than the valid next key.
		require.True(t, bytes.Compare(nextKey(key), nextValidKey) <= 0, "datums: %v", datums)
	}

	// a special case that when len(string datum) % 8 == 7, nextKey twice should not panic.
	keyBytes, err := codec.EncodeKey(stmtCtx.TimeZone(), nil, types.NewStringDatum("1234567"))
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
			_ = collector.Add(pebble.InternalKey{UserKey: p.key, Trailer: uint64(pebble.InternalKeyKindSet)}, v)
		}
	}

	userProperties := make(map[string]string, 1)
	_ = collector.Finish(userProperties)

	props, err := decodeRangeProperties(hack.Slice(userProperties[propRangeIndex]), common.NoopKeyAdapter{})
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

	fullRange := common.Range{Start: []byte("a"), End: []byte("z")}
	ranges := splitRangeBySizeProps(fullRange, sizeProps, 2*defaultPropSizeIndexDistance, defaultPropKeysIndexDistance*5/2)

	require.Equal(t, []common.Range{
		{Start: []byte("a"), End: []byte("e")},
		{Start: []byte("e"), End: []byte("k")},
		{Start: []byte("k"), End: []byte("mm")},
		{Start: []byte("mm"), End: []byte("q")},
		{Start: []byte("q"), End: []byte("z")},
	}, ranges)

	ranges = splitRangeBySizeProps(fullRange, sizeProps, 2*defaultPropSizeIndexDistance, defaultPropKeysIndexDistance)
	require.Equal(t, []common.Range{
		{Start: []byte("a"), End: []byte("e")},
		{Start: []byte("e"), End: []byte("h")},
		{Start: []byte("h"), End: []byte("k")},
		{Start: []byte("k"), End: []byte("m")},
		{Start: []byte("m"), End: []byte("mm")},
		{Start: []byte("mm"), End: []byte("n")},
		{Start: []byte("n"), End: []byte("q")},
		{Start: []byte("q"), End: []byte("z")},
	}, ranges)
}

func TestRangePropertiesWithPebble(t *testing.T) {
	sizeDistance := uint64(500)
	keysDistance := uint64(20)
	opt := &pebble.Options{
		MemTableSize:             512 * units.MiB,
		MaxConcurrentCompactions: func() int { return 16 },
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
			err = collector.Add(pebble.InternalKey{UserKey: key, Trailer: uint64(pebble.InternalKeyKindSet)}, value[:valueLen])
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
		MaxConcurrentCompactions: func() int { return 16 },
		L0CompactionThreshold:    math.MaxInt32, // set to max try to disable compaction
		L0StopWritesThreshold:    math.MaxInt32, // set to max try to disable compaction
		DisableWAL:               true,
		ReadOnly:                 false,
	}
	db, tmpPath := makePebbleDB(t, opt)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	_, engineUUID := backend.MakeUUID("ww", 0)
	engineCtx, cancel := context.WithCancel(context.Background())
	f := &Engine{
		UUID:         engineUUID,
		sstDir:       tmpPath,
		ctx:          engineCtx,
		cancel:       cancel,
		sstMetasChan: make(chan metaOrFlush, 64),
		keyAdapter:   common.NoopKeyAdapter{},
		logger:       log.L(),
	}
	f.db.Store(db)
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
	err = w.AppendRows(ctx, []string{}, kv.MakeRowsFromKvPairs(rows1))
	require.NoError(t, err)
	err = w.AppendRows(ctx, []string{}, kv.MakeRowsFromKvPairs(rows2))
	require.NoError(t, err)
	err = w.AppendRows(ctx, []string{}, kv.MakeRowsFromKvPairs(rows3))
	require.NoError(t, err)
	flushStatus, err := w.Close(context.Background())
	require.NoError(t, err)
	require.NoError(t, f.flushEngineWithoutLock(ctx))
	require.True(t, flushStatus.Flushed())
	o := &pebble.IterOptions{}
	it, _ := db.NewIter(o)

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
	require.NoError(t, it.Close())
	close(f.sstMetasChan)
	f.wg.Wait()
}

func TestEngineLocalWriter(t *testing.T) {
	// test local writer with sort
	testLocalWriter(t, false, false)

	// test local writer with ingest
	testLocalWriter(t, true, false)

	// test local writer with ingest unsort
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

func (i testIngester) mergeSSTs(metas []*sstMeta, dir string, blockSize int) (*sstMeta, error) {
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
		MaxConcurrentCompactions: func() int { return 16 },
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
	f.db.Store(db)
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

func makeRanges(input []string) []common.Range {
	ranges := make([]common.Range, 0, len(input)/2)
	for i := 0; i < len(input)-1; i += 2 {
		ranges = append(ranges, common.Range{Start: []byte(input[i]), End: []byte(input[i+1])})
	}
	return ranges
}

func testMergeSSTs(t *testing.T, kvs [][]common.KvPair, meta *sstMeta) {
	opt := &pebble.Options{
		MemTableSize:             1024 * 1024,
		MaxConcurrentCompactions: func() int { return 16 },
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
	f.db.Store(db)

	createSSTWriter := func() (*sstWriter, error) {
		path := filepath.Join(f.sstDir, uuid.New().String()+".sst")
		writer, err := newSSTWriter(path, 16*1024)
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
	newMeta, err := i.mergeSSTs(metas, tmpPath, 16*1024)
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

func (c *mockPdClient) ScanRegions(ctx context.Context, key, endKey []byte, limit int, opts ...pd.GetRegionOption) ([]*pd.Region, error) {
	return c.regions, nil
}

func (c *mockPdClient) GetTS(ctx context.Context) (int64, int64, error) {
	return 1, 2, nil
}

func (c *mockPdClient) GetClusterID(ctx context.Context) uint64 {
	return 1
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
	onceResp           *atomic.Pointer[sst.IngestResponse]
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

func (c *mockImportClient) MultiIngest(_ context.Context, req *sst.MultiIngestRequest, _ ...grpc.CallOption) (*sst.IngestResponse, error) {
	defer func() {
		c.cnt++
	}()
	for _, meta := range req.Ssts {
		if meta.RegionId != c.store.GetId() {
			return &sst.IngestResponse{Error: &errorpb.Error{Message: "The file which would be ingested doest not exist."}}, nil
		}
	}
	if c.apiInvokeRecorder != nil {
		c.apiInvokeRecorder["MultiIngest"] = append(c.apiInvokeRecorder["MultiIngest"], c.store.GetId())
	}
	if c.cnt < c.retry {
		if c.err != nil {
			return c.resp, c.err
		}
		if c.onceResp != nil {
			resp := c.onceResp.Swap(&sst.IngestResponse{})
			return resp, nil
		}
		if c.resp != nil {
			return c.resp, nil
		}
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

type baseCodec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

//go:linkname newContextWithRPCInfo google.golang.org/grpc.newContextWithRPCInfo
func newContextWithRPCInfo(ctx context.Context, failfast bool, codec baseCodec, cp grpc.Compressor, comp encoding.Compressor) context.Context

type mockCodec struct{}

func (m mockCodec) Marshal(v any) ([]byte, error) {
	return nil, nil
}

func (m mockCodec) Unmarshal(data []byte, v any) error {
	return nil
}

func (m mockWriteClient) Context() context.Context {
	ctx := context.Background()
	return newContextWithRPCInfo(ctx, false, mockCodec{}, nil, nil)
}

func (m mockWriteClient) SendMsg(_ any) error {
	return nil
}

func (c *mockImportClient) Write(ctx context.Context, opts ...grpc.CallOption) (sst.ImportSST_WriteClient, error) {
	if c.apiInvokeRecorder != nil {
		c.apiInvokeRecorder["Write"] = append(c.apiInvokeRecorder["Write"], c.store.GetId())
	}
	return mockWriteClient{writeResp: &sst.WriteResponse{Metas: []*sst.SSTMeta{
		{RegionId: c.store.GetId()},
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

		local := &Backend{
			pdCli: &mockPdClient{stores: stores},
			importClientFactory: &mockImportClientFactory{
				stores: allStores,
				createClientFn: func(store *metapb.Store) sst.ImportSSTClient {
					importCli.store = store
					return importCli
				},
			},
		}
		supportMultiIngest, err := checkMultiIngestSupport(context.Background(), local.pdCli, local.importClientFactory)
		if err != nil {
			require.Contains(t, err.Error(), testCase.retErr)
		} else {
			require.Equal(t, testCase.supportMutliIngest, supportMultiIngest)
		}
	}
}

func TestLocalWriteAndIngestPairsFailFast(t *testing.T) {
	bak := Backend{}
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/lightning/backend/local/WriteToTiKVNotEnoughDiskSpace", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/lightning/backend/local/WriteToTiKVNotEnoughDiskSpace"))
	}()
	jobCh := make(chan *regionJob, 1)
	jobCh <- &regionJob{}
	jobOutCh := make(chan *regionJob, 1)
	err := bak.startWorker(context.Background(), jobCh, jobOutCh, nil)
	require.Error(t, err)
	require.Regexp(t, "the remaining storage capacity of TiKV.*", err.Error())
	require.Len(t, jobCh, 0)
}

func TestLocalIsRetryableTiKVWriteError(t *testing.T) {
	l := Backend{}
	require.True(t, l.isRetryableImportTiKVError(io.EOF))
	require.True(t, l.isRetryableImportTiKVError(errors.Trace(io.EOF)))
}

// mockIngestData must be ordered on the first element of each [2][]byte.
type mockIngestData [][2][]byte

func (m mockIngestData) GetFirstAndLastKey(lowerBound, upperBound []byte) ([]byte, []byte, error) {
	i, j := m.getFirstAndLastKeyIdx(lowerBound, upperBound)
	if i == -1 {
		return nil, nil, nil
	}
	return m[i][0], m[j][0], nil
}

func (m mockIngestData) getFirstAndLastKeyIdx(lowerBound, upperBound []byte) (int, int) {
	var first int
	if len(lowerBound) == 0 {
		first = 0
	} else {
		i, _ := sort.Find(len(m), func(i int) int {
			return bytes.Compare(lowerBound, m[i][0])
		})
		if i == len(m) {
			return -1, -1
		}
		first = i
	}

	var last int
	if len(upperBound) == 0 {
		last = len(m) - 1
	} else {
		i, _ := sort.Find(len(m), func(i int) int {
			return bytes.Compare(upperBound, m[i][1])
		})
		if i == 0 {
			return -1, -1
		}
		last = i - 1
	}
	return first, last
}

type mockIngestIter struct {
	data                     mockIngestData
	startIdx, endIdx, curIdx int
}

func (m *mockIngestIter) First() bool {
	m.curIdx = m.startIdx
	return true
}

func (m *mockIngestIter) Valid() bool { return m.curIdx < m.endIdx }

func (m *mockIngestIter) Next() bool {
	m.curIdx++
	return m.Valid()
}

func (m *mockIngestIter) Key() []byte { return m.data[m.curIdx][0] }

func (m *mockIngestIter) Value() []byte { return m.data[m.curIdx][1] }

func (m *mockIngestIter) Close() error { return nil }

func (m *mockIngestIter) Error() error { return nil }

func (m *mockIngestIter) ReleaseBuf() {}

func (m mockIngestData) NewIter(_ context.Context, lowerBound, upperBound []byte, _ *membuf.Pool) common.ForwardIter {
	i, j := m.getFirstAndLastKeyIdx(lowerBound, upperBound)
	return &mockIngestIter{data: m, startIdx: i, endIdx: j, curIdx: i}
}

func (m mockIngestData) GetTS() uint64 { return 0 }

func (m mockIngestData) IncRef() {}

func (m mockIngestData) DecRef() {}

func (m mockIngestData) Finish(_, _ int64) {}

func TestCheckPeersBusy(t *testing.T) {
	backup := maxRetryBackoffSecond
	maxRetryBackoffSecond = 300
	t.Cleanup(func() {
		maxRetryBackoffSecond = backup
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	apiInvokeRecorder := map[string][]uint64{}
	serverIsBusyResp := &sst.IngestResponse{
		Error: &errorpb.Error{
			ServerIsBusy: &errorpb.ServerIsBusy{},
		}}

	createTimeStore12 := 0
	local := &Backend{
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
		logger:             log.L(),
		writeLimiter:       noopStoreWriteLimiter{},
		supportMultiIngest: true,
		BackendConfig: BackendConfig{
			ShouldCheckWriteStall: true,
			LocalStoreDir:         path.Join(t.TempDir(), "sorted-kv"),
		},
		tikvCodec: keyspace.CodecV1,
	}
	var err error
	local.engineMgr, err = newEngineManager(local.BackendConfig, local, local.logger)
	require.NoError(t, err)

	data := mockIngestData{{[]byte("a"), []byte("a")}, {[]byte("b"), []byte("b")}}

	jobCh := make(chan *regionJob, 10)

	retryJob := &regionJob{
		keyRange: common.Range{Start: []byte("a"), End: []byte("b")},
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
		ingestData: data,
		retryCount: 20,
		waitUntil:  time.Now().Add(-time.Second),
	}
	jobCh <- retryJob

	jobCh <- &regionJob{
		keyRange: common.Range{Start: []byte("b"), End: []byte("")},
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
		ingestData: data,
		retryCount: 20,
		waitUntil:  time.Now().Add(-time.Second),
	}

	retryJobs := make(chan *regionJob, 1)

	var wg sync.WaitGroup
	wg.Add(1)
	jobOutCh := make(chan *regionJob)
	go func() {
		job := <-jobOutCh
		job.retryCount++
		retryJobs <- job
		<-jobOutCh
		wg.Done()
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := local.startWorker(ctx, jobCh, jobOutCh, nil)
		require.NoError(t, err)
	}()

	require.Eventually(t, func() bool {
		return len(retryJobs) == 1
	}, 300*time.Second, time.Second)
	j := <-retryJobs
	require.Same(t, retryJob, j)
	require.Equal(t, 21, retryJob.retryCount)
	require.Equal(t, wrote, retryJob.stage)

	cancel()
	wg.Wait()

	require.Equal(t, []uint64{11, 12, 13, 21, 22, 23}, apiInvokeRecorder["Write"])
	// store 12 has a follower busy, so it will break the workflow for region (11, 12, 13)
	require.Equal(t, []uint64{11, 12, 21, 22, 23, 21}, apiInvokeRecorder["MultiIngest"])
	// region (11, 12, 13) has key range ["a", "b"), it's not finished.
	require.Equal(t, []byte("a"), retryJob.keyRange.Start)
	require.Equal(t, []byte("b"), retryJob.keyRange.End)
}

func TestNotLeaderErrorNeedUpdatePeers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// test lightning using stale region info (1,2,3), now the region is (11,12,13)
	apiInvokeRecorder := map[string][]uint64{}
	notLeaderResp := &sst.IngestResponse{
		Error: &errorpb.Error{
			NotLeader: &errorpb.NotLeader{Leader: &metapb.Peer{StoreId: 11}},
		}}

	local := &Backend{
		splitCli: initTestSplitClient3Replica([][]byte{{}, {'a'}, {}}, nil),
		importClientFactory: &mockImportClientFactory{
			stores: []*metapb.Store{
				{Id: 1}, {Id: 2}, {Id: 3},
				{Id: 11}, {Id: 12}, {Id: 13},
			},
			createClientFn: func(store *metapb.Store) sst.ImportSSTClient {
				importCli := newMockImportClient()
				importCli.store = store
				importCli.apiInvokeRecorder = apiInvokeRecorder
				if store.Id == 1 {
					importCli.retry = 1
					importCli.resp = notLeaderResp
				}
				return importCli
			},
		},
		logger:             log.L(),
		writeLimiter:       noopStoreWriteLimiter{},
		supportMultiIngest: true,
		BackendConfig: BackendConfig{
			ShouldCheckWriteStall: true,
			LocalStoreDir:         path.Join(t.TempDir(), "sorted-kv"),
		},
		tikvCodec: keyspace.CodecV1,
	}
	var err error
	local.engineMgr, err = newEngineManager(local.BackendConfig, local, local.logger)
	require.NoError(t, err)

	data := mockIngestData{{[]byte("a"), []byte("a")}}

	jobCh := make(chan *regionJob, 10)

	staleJob := &regionJob{
		keyRange: common.Range{Start: []byte("a"), End: []byte("")},
		region: &split.RegionInfo{
			Region: &metapb.Region{
				Id: 1,
				Peers: []*metapb.Peer{
					{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2}, {Id: 3, StoreId: 3},
				},
				StartKey: []byte("a"),
				EndKey:   []byte(""),
			},
			Leader: &metapb.Peer{Id: 1, StoreId: 1},
		},
		stage:      regionScanned,
		ingestData: data,
	}
	var jobWg sync.WaitGroup
	jobWg.Add(1)
	jobCh <- staleJob

	var wg sync.WaitGroup
	wg.Add(1)
	jobOutCh := make(chan *regionJob)
	go func() {
		defer wg.Done()
		for {
			job := <-jobOutCh
			if job.stage == ingested {
				jobWg.Done()
				return
			}
			jobCh <- job
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := local.startWorker(ctx, jobCh, jobOutCh, &jobWg)
		require.NoError(t, err)
	}()

	jobWg.Wait()
	cancel()
	wg.Wait()

	// "ingest" to test peers busy of stale region: 1,2,3
	// then "write" to stale region: 1,2,3
	// then "ingest" to stale leader: 1
	// then meet NotLeader error, scanned new region (11,12,13)
	// repeat above for 11,12,13
	require.Equal(t, []uint64{1, 2, 3, 11, 12, 13}, apiInvokeRecorder["Write"])
	require.Equal(t, []uint64{1, 2, 3, 1, 11, 12, 13, 11}, apiInvokeRecorder["MultiIngest"])
}

func TestPartialWriteIngestErrorWontPanic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// let lightning meet any error that will call convertStageTo(needRescan)
	apiInvokeRecorder := map[string][]uint64{}
	notLeaderResp := &sst.IngestResponse{
		Error: &errorpb.Error{
			NotLeader: &errorpb.NotLeader{Leader: &metapb.Peer{StoreId: 11}},
		}}

	local := &Backend{
		splitCli: initTestSplitClient3Replica([][]byte{{}, {'c'}}, nil),
		importClientFactory: &mockImportClientFactory{
			stores: []*metapb.Store{
				{Id: 1}, {Id: 2}, {Id: 3},
			},
			createClientFn: func(store *metapb.Store) sst.ImportSSTClient {
				importCli := newMockImportClient()
				importCli.store = store
				importCli.apiInvokeRecorder = apiInvokeRecorder
				if store.Id == 1 {
					importCli.retry = 1
					importCli.resp = notLeaderResp
				}
				return importCli
			},
		},
		logger:             log.L(),
		writeLimiter:       noopStoreWriteLimiter{},
		supportMultiIngest: true,
		tikvCodec:          keyspace.CodecV1,
		BackendConfig: BackendConfig{
			LocalStoreDir: path.Join(t.TempDir(), "sorted-kv"),
		},
	}
	var err error
	local.engineMgr, err = newEngineManager(local.BackendConfig, local, local.logger)
	require.NoError(t, err)

	data := mockIngestData{{[]byte("a"), []byte("a")}, {[]byte("a2"), []byte("a2")}}

	jobCh := make(chan *regionJob, 10)

	partialWriteJob := &regionJob{
		keyRange: common.Range{Start: []byte("a"), End: []byte("c")},
		region: &split.RegionInfo{
			Region: &metapb.Region{
				Id: 1,
				Peers: []*metapb.Peer{
					{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2}, {Id: 3, StoreId: 3},
				},
				StartKey: []byte("a"),
				EndKey:   []byte("c"),
			},
			Leader: &metapb.Peer{Id: 1, StoreId: 1},
		},
		stage:      regionScanned,
		ingestData: data,
		// use small regionSplitSize to trigger partial write
		regionSplitSize: 1,
	}
	var jobWg sync.WaitGroup
	jobWg.Add(1)
	jobCh <- partialWriteJob

	var wg sync.WaitGroup
	wg.Add(1)
	jobOutCh := make(chan *regionJob)
	go func() {
		defer wg.Done()
		for {
			job := <-jobOutCh
			if job.stage == regionScanned {
				jobWg.Done()
				return
			}
			require.Fail(t, "job stage %s is not expected", job.stage)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := local.startWorker(ctx, jobCh, jobOutCh, &jobWg)
		require.NoError(t, err)
	}()

	jobWg.Wait()
	cancel()
	wg.Wait()

	require.Equal(t, []uint64{1, 2, 3}, apiInvokeRecorder["Write"])
	require.Equal(t, []uint64{1}, apiInvokeRecorder["MultiIngest"])
}

func TestPartialWriteIngestBusy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	apiInvokeRecorder := map[string][]uint64{}
	notLeaderResp := &sst.IngestResponse{
		Error: &errorpb.Error{
			ServerIsBusy: &errorpb.ServerIsBusy{},
		}}
	onceResp := &atomic.Pointer[sst.IngestResponse]{}
	onceResp.Store(notLeaderResp)

	local := &Backend{
		splitCli: initTestSplitClient3Replica([][]byte{{}, {'c'}}, nil),
		importClientFactory: &mockImportClientFactory{
			stores: []*metapb.Store{
				{Id: 1}, {Id: 2}, {Id: 3},
			},
			createClientFn: func(store *metapb.Store) sst.ImportSSTClient {
				importCli := newMockImportClient()
				importCli.store = store
				importCli.apiInvokeRecorder = apiInvokeRecorder
				if store.Id == 1 {
					importCli.retry = 1
					importCli.onceResp = onceResp
				}
				return importCli
			},
		},
		logger:             log.L(),
		writeLimiter:       noopStoreWriteLimiter{},
		supportMultiIngest: true,
		tikvCodec:          keyspace.CodecV1,
		BackendConfig: BackendConfig{
			LocalStoreDir: path.Join(t.TempDir(), "sorted-kv"),
		},
	}
	var err error
	local.engineMgr, err = newEngineManager(local.BackendConfig, local, local.logger)
	require.NoError(t, err)

	db, tmpPath := makePebbleDB(t, nil)
	_, engineUUID := backend.MakeUUID("ww", 0)
	engineCtx, cancel2 := context.WithCancel(context.Background())
	f := &Engine{
		UUID:         engineUUID,
		sstDir:       tmpPath,
		ctx:          engineCtx,
		cancel:       cancel2,
		sstMetasChan: make(chan metaOrFlush, 64),
		keyAdapter:   common.NoopKeyAdapter{},
		logger:       log.L(),
	}
	f.db.Store(db)
	err = db.Set([]byte("a"), []byte("a"), nil)
	require.NoError(t, err)
	err = db.Set([]byte("a2"), []byte("a2"), nil)
	require.NoError(t, err)

	jobCh := make(chan *regionJob, 10)

	partialWriteJob := &regionJob{
		keyRange: common.Range{Start: []byte("a"), End: []byte("c")},
		region: &split.RegionInfo{
			Region: &metapb.Region{
				Id: 1,
				Peers: []*metapb.Peer{
					{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2}, {Id: 3, StoreId: 3},
				},
				StartKey: []byte("a"),
				EndKey:   []byte("c"),
			},
			Leader: &metapb.Peer{Id: 1, StoreId: 1},
		},
		stage:      regionScanned,
		ingestData: f,
		// use small regionSplitSize to trigger partial write
		regionSplitSize: 1,
	}
	var jobWg sync.WaitGroup
	jobWg.Add(1)
	jobCh <- partialWriteJob

	var wg sync.WaitGroup
	wg.Add(1)
	jobOutCh := make(chan *regionJob)
	go func() {
		defer wg.Done()
		for {
			job := <-jobOutCh
			switch job.stage {
			case wrote:
				// mimic retry later
				jobCh <- job
			case ingested:
				// partially write will change the start key
				require.Equal(t, []byte("a2"), job.keyRange.Start)
				require.Equal(t, []byte("c"), job.keyRange.End)
				jobWg.Done()
				return
			default:
				require.Fail(t, "job stage %s is not expected, job: %v", job.stage, job)
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := local.startWorker(ctx, jobCh, jobOutCh, &jobWg)
		require.NoError(t, err)
	}()

	jobWg.Wait()
	cancel()
	wg.Wait()

	require.Equal(t, int64(2), f.importedKVCount.Load())

	require.Equal(t, []uint64{1, 2, 3, 1, 2, 3}, apiInvokeRecorder["Write"])
	require.Equal(t, []uint64{1, 1, 1}, apiInvokeRecorder["MultiIngest"])

	require.NoError(t, f.Close())
}

// mockGetSizeProperties mocks that 50MB * 20 SST file.
func mockGetSizeProperties(log.Logger, *pebble.DB, common.KeyAdapter) (*sizeProperties, error) {
	props := newSizeProperties()
	// keys starts with 0 is meta keys, so we start with 1.
	for i := byte(1); i <= 10; i++ {
		rangeProps := &rangeProperty{
			Key: []byte{i},
			rangeOffsets: rangeOffsets{
				Size: 50 * units.MiB,
				Keys: 100_000,
			},
		}
		props.add(rangeProps)
		rangeProps = &rangeProperty{
			Key: []byte{i, 1},
			rangeOffsets: rangeOffsets{
				Size: 50 * units.MiB,
				Keys: 100_000,
			},
		}
		props.add(rangeProps)
	}
	return props, nil
}

type panicSplitRegionClient struct{}

func (p panicSplitRegionClient) BeforeSplitRegion(context.Context, *split.RegionInfo, [][]byte) (*split.RegionInfo, [][]byte) {
	panic("should not be called")
}

func (p panicSplitRegionClient) AfterSplitRegion(context.Context, *split.RegionInfo, [][]byte, []*split.RegionInfo, error) ([]*split.RegionInfo, error) {
	panic("should not be called")
}

func (p panicSplitRegionClient) BeforeScanRegions(ctx context.Context, key, endKey []byte, limit int) ([]byte, []byte, int) {
	return key, endKey, limit
}

func (p panicSplitRegionClient) AfterScanRegions(infos []*split.RegionInfo, err error) ([]*split.RegionInfo, error) {
	return infos, err
}

func TestSplitRangeAgain4BigRegion(t *testing.T) {
	backup := getSizePropertiesFn
	getSizePropertiesFn = mockGetSizeProperties
	t.Cleanup(func() {
		getSizePropertiesFn = backup
	})

	local := &Backend{
		splitCli: initTestSplitClient(
			[][]byte{{1}, {11}},      // we have one big region
			panicSplitRegionClient{}, // make sure no further split region
		),
	}
	local.BackendConfig.WorkerConcurrency = 1
	db, tmpPath := makePebbleDB(t, nil)
	_, engineUUID := backend.MakeUUID("ww", 0)
	ctx := context.Background()
	engineCtx, cancel := context.WithCancel(context.Background())
	f := &Engine{
		UUID:         engineUUID,
		sstDir:       tmpPath,
		ctx:          engineCtx,
		cancel:       cancel,
		sstMetasChan: make(chan metaOrFlush, 64),
		keyAdapter:   common.NoopKeyAdapter{},
		logger:       log.L(),
	}
	f.db.Store(db)
	// keys starts with 0 is meta keys, so we start with 1.
	for i := byte(1); i <= 10; i++ {
		err := db.Set([]byte{i}, []byte{i}, nil)
		require.NoError(t, err)
		err = db.Set([]byte{i, 1}, []byte{i, 1}, nil)
		require.NoError(t, err)
	}

	bigRegionRange := []common.Range{{Start: []byte{1}, End: []byte{11}}}
	jobCh := make(chan *regionJob, 10)
	jobWg := sync.WaitGroup{}
	err := local.generateAndSendJob(
		ctx,
		f,
		bigRegionRange,
		10*units.GB,
		1<<30,
		jobCh,
		&jobWg,
	)
	require.NoError(t, err)
	require.Len(t, jobCh, 10)
	for i := 0; i < 10; i++ {
		job := <-jobCh
		require.Equal(t, []byte{byte(i + 1)}, job.keyRange.Start)
		require.Equal(t, []byte{byte(i + 2)}, job.keyRange.End)
		jobWg.Done()
	}
	jobWg.Wait()
	require.NoError(t, f.Close())
}

func TestSplitRangeAgain4BigRegionExternalEngine(t *testing.T) {
	t.Skip("skip due to the delay of dynamic region feature, and external engine changed its behaviour")
	backup := external.LargeRegionSplitDataThreshold
	external.LargeRegionSplitDataThreshold = 1
	t.Cleanup(func() {
		external.LargeRegionSplitDataThreshold = backup
	})

	ctx := context.Background()
	local := &Backend{
		splitCli: initTestSplitClient(
			[][]byte{{1}, {11}},      // we have one big region
			panicSplitRegionClient{}, // make sure no further split region
		),
	}
	local.BackendConfig.WorkerConcurrency = 1
	bigRegionRange := []common.Range{{Start: []byte{1}, End: []byte{11}}}

	keys := make([][]byte, 0, 10)
	value := make([][]byte, 0, 10)
	for i := byte(1); i <= 10; i++ {
		keys = append(keys, []byte{i})
		value = append(value, []byte{i})
	}
	memStore := storage.NewMemStorage()

	dataFiles, statFiles, err := external.MockExternalEngine(memStore, keys, value)
	require.NoError(t, err)

	extEngine := external.NewExternalEngine(
		memStore,
		dataFiles,
		statFiles,
		[]byte{1},
		[]byte{10},
		[][]byte{{1}, {11}},
		1<<30,
		common.NoopKeyAdapter{},
		false,
		nil,
		common.DupDetectOpt{},
		10,
		123,
		456,
		789,
		true,
	)

	jobCh := make(chan *regionJob, 10)
	jobWg := sync.WaitGroup{}
	err = local.generateAndSendJob(
		ctx,
		extEngine,
		bigRegionRange,
		10*units.GB,
		1<<30,
		jobCh,
		&jobWg,
	)
	require.NoError(t, err)
	require.Len(t, jobCh, 10)
	for i := 0; i < 10; i++ {
		job := <-jobCh
		require.Equal(t, []byte{byte(i + 1)}, job.keyRange.Start)
		require.Equal(t, []byte{byte(i + 2)}, job.keyRange.End)
		firstKey, lastKey, err := job.ingestData.GetFirstAndLastKey(nil, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{byte(i + 1)}, firstKey)
		require.Equal(t, []byte{byte(i + 1)}, lastKey)
		jobWg.Done()
	}
	jobWg.Wait()
}

func getSuccessInjectedBehaviour() []injectedBehaviour {
	return []injectedBehaviour{
		{
			write: injectedWriteBehaviour{
				result: &tikvWriteResult{
					remainingStartKey: nil,
				},
			},
		},
		{
			ingest: injectedIngestBehaviour{
				nextStage: ingested,
			},
		},
	}
}

func getNeedRescanWhenIngestBehaviour() []injectedBehaviour {
	return []injectedBehaviour{
		{
			write: injectedWriteBehaviour{
				result: &tikvWriteResult{
					remainingStartKey: nil,
				},
			},
		},
		{
			ingest: injectedIngestBehaviour{
				nextStage: needRescan,
				err:       common.ErrKVEpochNotMatch,
			},
		},
	}
}

func TestDoImport(t *testing.T) {
	backup := maxRetryBackoffSecond
	maxRetryBackoffSecond = 1
	t.Cleanup(func() {
		maxRetryBackoffSecond = backup
	})

	_ = failpoint.Enable("github.com/pingcap/tidb/pkg/lightning/backend/local/skipSplitAndScatter", "return()")
	_ = failpoint.Enable("github.com/pingcap/tidb/pkg/lightning/backend/local/fakeRegionJobs", "return()")
	t.Cleanup(func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/pkg/lightning/backend/local/skipSplitAndScatter")
		_ = failpoint.Disable("github.com/pingcap/tidb/pkg/lightning/backend/local/fakeRegionJobs")
	})

	// test that
	// - one job need rescan when ingest
	// - one job need retry when write

	initRanges := []common.Range{
		{Start: []byte{'a'}, End: []byte{'b'}},
		{Start: []byte{'b'}, End: []byte{'c'}},
		{Start: []byte{'c'}, End: []byte{'d'}},
	}
	fakeRegionJobs = map[[2]string]struct {
		jobs []*regionJob
		err  error
	}{
		{"a", "b"}: {
			jobs: []*regionJob{
				{
					keyRange:   common.Range{Start: []byte{'a'}, End: []byte{'b'}},
					ingestData: &Engine{},
					injected:   getSuccessInjectedBehaviour(),
				},
			},
		},
		{"b", "c"}: {
			jobs: []*regionJob{
				{
					keyRange:   common.Range{Start: []byte{'b'}, End: []byte{'c'}},
					ingestData: &Engine{},
					injected: []injectedBehaviour{
						{
							write: injectedWriteBehaviour{
								result: &tikvWriteResult{
									remainingStartKey: []byte{'b', '2'},
								},
							},
						},
						{
							ingest: injectedIngestBehaviour{
								nextStage: ingested,
							},
						},
						{
							write: injectedWriteBehaviour{
								result: &tikvWriteResult{
									remainingStartKey: nil,
								},
							},
						},
						{
							ingest: injectedIngestBehaviour{
								nextStage: ingested,
							},
						},
					},
				},
			},
		},
		{"c", "d"}: {
			jobs: []*regionJob{
				{
					keyRange:   common.Range{Start: []byte{'c'}, End: []byte{'c', '2'}},
					ingestData: &Engine{},
					injected:   getNeedRescanWhenIngestBehaviour(),
				},
				{
					keyRange:   common.Range{Start: []byte{'c', '2'}, End: []byte{'d'}},
					ingestData: &Engine{},
					injected: []injectedBehaviour{
						{
							write: injectedWriteBehaviour{
								// a retryable error
								err: status.Error(codes.Unknown, "is not fully replicated"),
							},
						},
					},
				},
			},
		},
		{"c", "c2"}: {
			jobs: []*regionJob{
				{
					keyRange:   common.Range{Start: []byte{'c'}, End: []byte{'c', '2'}},
					ingestData: &Engine{},
					injected:   getSuccessInjectedBehaviour(),
				},
			},
		},
		{"c2", "d"}: {
			jobs: []*regionJob{
				{
					keyRange:   common.Range{Start: []byte{'c', '2'}, End: []byte{'d'}},
					ingestData: &Engine{},
					injected:   getSuccessInjectedBehaviour(),
				},
			},
		},
	}

	ctx := context.Background()
	l := &Backend{
		BackendConfig: BackendConfig{
			WorkerConcurrency: 2,
		},
	}
	e := &Engine{}
	err := l.doImport(ctx, e, initRanges, int64(config.SplitRegionSize), int64(config.SplitRegionKeys))
	require.NoError(t, err)
	for _, v := range fakeRegionJobs {
		for _, job := range v.jobs {
			require.Len(t, job.injected, 0)
		}
	}

	// test first call to generateJobForRange meet error

	fakeRegionJobs = map[[2]string]struct {
		jobs []*regionJob
		err  error
	}{
		{"a", "b"}: {
			jobs: []*regionJob{
				{
					keyRange:   common.Range{Start: []byte{'a'}, End: []byte{'b'}},
					ingestData: &Engine{},
					injected:   getSuccessInjectedBehaviour(),
				},
			},
		},
		{"b", "c"}: {
			err: errors.New("meet error when generateJobForRange"),
		},
	}
	err = l.doImport(ctx, e, initRanges, int64(config.SplitRegionSize), int64(config.SplitRegionKeys))
	require.ErrorContains(t, err, "meet error when generateJobForRange")

	// test second call to generateJobForRange (needRescan) meet error

	fakeRegionJobs = map[[2]string]struct {
		jobs []*regionJob
		err  error
	}{
		{"a", "b"}: {
			jobs: []*regionJob{
				{
					keyRange:   common.Range{Start: []byte{'a'}, End: []byte{'a', '2'}},
					ingestData: &Engine{},
					injected:   getNeedRescanWhenIngestBehaviour(),
				},
				{
					keyRange:   common.Range{Start: []byte{'a', '2'}, End: []byte{'b'}},
					ingestData: &Engine{},
					injected:   getSuccessInjectedBehaviour(),
				},
			},
		},
		{"b", "c"}: {
			jobs: []*regionJob{
				{
					keyRange:   common.Range{Start: []byte{'b'}, End: []byte{'c'}},
					ingestData: &Engine{},
					injected:   getSuccessInjectedBehaviour(),
				},
			},
		},
		{"c", "d"}: {
			jobs: []*regionJob{
				{
					keyRange:   common.Range{Start: []byte{'c'}, End: []byte{'d'}},
					ingestData: &Engine{},
					injected:   getSuccessInjectedBehaviour(),
				},
			},
		},
		{"a", "a2"}: {
			err: errors.New("meet error when generateJobForRange again"),
		},
	}
	err = l.doImport(ctx, e, initRanges, int64(config.SplitRegionSize), int64(config.SplitRegionKeys))
	require.ErrorContains(t, err, "meet error when generateJobForRange again")

	// test write meet unretryable error
	maxRetryBackoffSecond = 100
	l.WorkerConcurrency = 1
	fakeRegionJobs = map[[2]string]struct {
		jobs []*regionJob
		err  error
	}{
		{"a", "b"}: {
			jobs: []*regionJob{
				{
					keyRange:   common.Range{Start: []byte{'a'}, End: []byte{'b'}},
					ingestData: &Engine{},
					retryCount: maxWriteAndIngestRetryTimes - 1,
					injected:   getSuccessInjectedBehaviour(),
				},
			},
		},
		{"b", "c"}: {
			jobs: []*regionJob{
				{
					keyRange:   common.Range{Start: []byte{'b'}, End: []byte{'c'}},
					ingestData: &Engine{},
					retryCount: maxWriteAndIngestRetryTimes - 1,
					injected:   getSuccessInjectedBehaviour(),
				},
			},
		},
		{"c", "d"}: {
			jobs: []*regionJob{
				{
					keyRange:   common.Range{Start: []byte{'c'}, End: []byte{'d'}},
					ingestData: &Engine{},
					retryCount: maxWriteAndIngestRetryTimes - 2,
					injected: []injectedBehaviour{
						{
							write: injectedWriteBehaviour{
								// unretryable error
								err: errors.New("fatal error"),
							},
						},
					},
				},
			},
		},
	}
	err = l.doImport(ctx, e, initRanges, int64(config.SplitRegionSize), int64(config.SplitRegionKeys))
	require.ErrorContains(t, err, "fatal error")
	for _, v := range fakeRegionJobs {
		for _, job := range v.jobs {
			require.Len(t, job.injected, 0)
		}
	}
}

func TestRegionJobResetRetryCounter(t *testing.T) {
	backup := maxRetryBackoffSecond
	maxRetryBackoffSecond = 1
	t.Cleanup(func() {
		maxRetryBackoffSecond = backup
	})

	_ = failpoint.Enable("github.com/pingcap/tidb/pkg/lightning/backend/local/skipSplitAndScatter", "return()")
	_ = failpoint.Enable("github.com/pingcap/tidb/pkg/lightning/backend/local/fakeRegionJobs", "return()")
	t.Cleanup(func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/pkg/lightning/backend/local/skipSplitAndScatter")
		_ = failpoint.Disable("github.com/pingcap/tidb/pkg/lightning/backend/local/fakeRegionJobs")
	})

	// test that job need rescan when ingest

	initRanges := []common.Range{
		{Start: []byte{'c'}, End: []byte{'d'}},
	}
	fakeRegionJobs = map[[2]string]struct {
		jobs []*regionJob
		err  error
	}{
		{"c", "d"}: {
			jobs: []*regionJob{
				{
					keyRange:   common.Range{Start: []byte{'c'}, End: []byte{'c', '2'}},
					ingestData: &Engine{},
					injected:   getNeedRescanWhenIngestBehaviour(),
					retryCount: maxWriteAndIngestRetryTimes,
				},
				{
					keyRange:   common.Range{Start: []byte{'c', '2'}, End: []byte{'d'}},
					ingestData: &Engine{},
					injected:   getSuccessInjectedBehaviour(),
					retryCount: maxWriteAndIngestRetryTimes,
				},
			},
		},
		{"c", "c2"}: {
			jobs: []*regionJob{
				{
					keyRange:   common.Range{Start: []byte{'c'}, End: []byte{'c', '2'}},
					ingestData: &Engine{},
					injected:   getSuccessInjectedBehaviour(),
				},
			},
		},
	}

	ctx := context.Background()
	l := &Backend{
		BackendConfig: BackendConfig{
			WorkerConcurrency: 2,
		},
	}
	e := &Engine{}
	err := l.doImport(ctx, e, initRanges, int64(config.SplitRegionSize), int64(config.SplitRegionKeys))
	require.NoError(t, err)
	for _, v := range fakeRegionJobs {
		for _, job := range v.jobs {
			require.Len(t, job.injected, 0)
		}
	}
}

func TestCtxCancelIsIgnored(t *testing.T) {
	backup := maxRetryBackoffSecond
	maxRetryBackoffSecond = 1
	t.Cleanup(func() {
		maxRetryBackoffSecond = backup
	})

	_ = failpoint.Enable("github.com/pingcap/tidb/pkg/lightning/backend/local/skipSplitAndScatter", "return()")
	_ = failpoint.Enable("github.com/pingcap/tidb/pkg/lightning/backend/local/fakeRegionJobs", "return()")
	_ = failpoint.Enable("github.com/pingcap/tidb/pkg/lightning/backend/local/beforeGenerateJob", "sleep(1000)")
	_ = failpoint.Enable("github.com/pingcap/tidb/pkg/lightning/backend/local/WriteToTiKVNotEnoughDiskSpace", "return()")
	t.Cleanup(func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/pkg/lightning/backend/local/skipSplitAndScatter")
		_ = failpoint.Disable("github.com/pingcap/tidb/pkg/lightning/backend/local/fakeRegionJobs")
		_ = failpoint.Disable("github.com/pingcap/tidb/pkg/lightning/backend/local/beforeGenerateJob")
		_ = failpoint.Disable("github.com/pingcap/tidb/pkg/lightning/backend/local/WriteToTiKVNotEnoughDiskSpace")
	})

	initRanges := []common.Range{
		{Start: []byte{'c'}, End: []byte{'d'}},
		{Start: []byte{'d'}, End: []byte{'e'}},
	}
	fakeRegionJobs = map[[2]string]struct {
		jobs []*regionJob
		err  error
	}{
		{"c", "d"}: {
			jobs: []*regionJob{
				{
					keyRange:   common.Range{Start: []byte{'c'}, End: []byte{'d'}},
					ingestData: &Engine{},
					injected:   getSuccessInjectedBehaviour(),
				},
			},
		},
		{"d", "e"}: {
			jobs: []*regionJob{
				{
					keyRange:   common.Range{Start: []byte{'d'}, End: []byte{'e'}},
					ingestData: &Engine{},
					injected:   getSuccessInjectedBehaviour(),
				},
			},
		},
	}

	ctx := context.Background()
	l := &Backend{
		BackendConfig: BackendConfig{
			WorkerConcurrency: 1,
		},
	}
	e := &Engine{}
	err := l.doImport(ctx, e, initRanges, int64(config.SplitRegionSize), int64(config.SplitRegionKeys))
	require.ErrorContains(t, err, "the remaining storage capacity of TiKV")
}

func TestWorkerFailedWhenGeneratingJobs(t *testing.T) {
	backup := maxRetryBackoffSecond
	maxRetryBackoffSecond = 1
	t.Cleanup(func() {
		maxRetryBackoffSecond = backup
	})

	_ = failpoint.Enable("github.com/pingcap/tidb/pkg/lightning/backend/local/skipSplitAndScatter", "return()")
	_ = failpoint.Enable("github.com/pingcap/tidb/pkg/lightning/backend/local/sendDummyJob", "return()")
	_ = failpoint.Enable("github.com/pingcap/tidb/pkg/lightning/backend/local/mockGetFirstAndLastKey", "return()")
	_ = failpoint.Enable("github.com/pingcap/tidb/pkg/lightning/backend/local/WriteToTiKVNotEnoughDiskSpace", "return()")
	t.Cleanup(func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/pkg/lightning/backend/local/skipSplitAndScatter")
		_ = failpoint.Disable("github.com/pingcap/tidb/pkg/lightning/backend/local/sendDummyJob")
		_ = failpoint.Disable("github.com/pingcap/tidb/pkg/lightning/backend/local/mockGetFirstAndLastKey")
		_ = failpoint.Disable("github.com/pingcap/tidb/pkg/lightning/backend/local/WriteToTiKVNotEnoughDiskSpace")
	})

	initRanges := []common.Range{
		{Start: []byte{'c'}, End: []byte{'d'}},
	}

	ctx := context.Background()
	l := &Backend{
		BackendConfig: BackendConfig{
			WorkerConcurrency: 1,
		},
		splitCli: initTestSplitClient(
			[][]byte{{1}, {11}},
			panicSplitRegionClient{},
		),
	}
	e := &Engine{}
	err := l.doImport(ctx, e, initRanges, int64(config.SplitRegionSize), int64(config.SplitRegionKeys))
	require.ErrorContains(t, err, "the remaining storage capacity of TiKV")
}

func TestExternalEngine(t *testing.T) {
	_ = failpoint.Enable("github.com/pingcap/tidb/pkg/lightning/backend/local/skipSplitAndScatter", "return()")
	_ = failpoint.Enable("github.com/pingcap/tidb/pkg/lightning/backend/local/skipStartWorker", "return()")
	_ = failpoint.Enable("github.com/pingcap/tidb/pkg/lightning/backend/local/injectVariables", "return()")
	_ = failpoint.Enable("github.com/pingcap/tidb/pkg/lightning/backend/external/LoadIngestDataBatchSize", "return(2)")
	t.Cleanup(func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/pkg/lightning/backend/local/skipSplitAndScatter")
		_ = failpoint.Disable("github.com/pingcap/tidb/pkg/lightning/backend/local/skipStartWorker")
		_ = failpoint.Disable("github.com/pingcap/tidb/pkg/lightning/backend/local/injectVariables")
		_ = failpoint.Disable("github.com/pingcap/tidb/pkg/lightning/backend/external/LoadIngestDataBatchSize")
	})
	ctx := context.Background()
	dir := t.TempDir()
	storageURI := "file://" + filepath.ToSlash(dir)
	storeBackend, err := storage.ParseBackend(storageURI, nil)
	require.NoError(t, err)
	extStorage, err := storage.New(ctx, storeBackend, nil)
	require.NoError(t, err)
	keys := make([][]byte, 100)
	values := make([][]byte, 100)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key%06d", i))
		values[i] = []byte(fmt.Sprintf("value%06d", i))
	}
	// simple append 0x00
	endKey := make([]byte, len(keys[99])+1)
	copy(endKey, keys[99])

	dataFiles, statFiles, err := external.MockExternalEngine(extStorage, keys, values)
	require.NoError(t, err)

	externalCfg := &backend.ExternalEngineConfig{
		StorageURI:    storageURI,
		DataFiles:     dataFiles,
		StatFiles:     statFiles,
		StartKey:      keys[0],
		EndKey:        endKey,
		SplitKeys:     [][]byte{keys[20], keys[30], keys[50], keys[60], keys[80], keys[90]},
		TotalFileSize: int64(config.SplitRegionSize) + 1,
		TotalKVCount:  int64(config.SplitRegionKeys) + 1,
	}
	engineUUID := uuid.New()
	local := &Backend{
		BackendConfig: BackendConfig{
			WorkerConcurrency: 2,
			LocalStoreDir:     path.Join(t.TempDir(), "sorted-kv"),
		},
		splitCli: initTestSplitClient([][]byte{
			keys[0], keys[50], endKey,
		}, nil),
		pdCli: &mockPdClient{},
	}
	local.engineMgr, err = newEngineManager(local.BackendConfig, local, local.logger)
	require.NoError(t, err)
	jobs := make([]*regionJob, 0, 5)

	jobToWorkerCh := make(chan *regionJob, 10)
	testJobToWorkerCh = jobToWorkerCh

	done := make(chan struct{})
	go func() {
		for i := 0; i < 5; i++ {
			jobs = append(jobs, <-jobToWorkerCh)
			testJobWg.Done()
		}
	}()
	go func() {
		err2 := local.CloseEngine(
			ctx,
			&backend.EngineConfig{External: externalCfg},
			engineUUID,
		)
		require.NoError(t, err2)
		err2 = local.ImportEngine(ctx, engineUUID, int64(config.SplitRegionSize), int64(config.SplitRegionKeys))
		require.NoError(t, err2)
		close(done)
	}()

	<-done

	// no jobs left in the channel
	require.Len(t, jobToWorkerCh, 0)

	sort.Slice(jobs, func(i, j int) bool {
		return bytes.Compare(jobs[i].keyRange.Start, jobs[j].keyRange.Start) < 0
	})
	expectedKeyRanges := []common.Range{
		{Start: keys[0], End: keys[30]},
		{Start: keys[30], End: keys[50]},
		{Start: keys[50], End: keys[60]},
		{Start: keys[60], End: keys[90]},
		{Start: keys[90], End: endKey},
	}
	kvIdx := 0
	for i, job := range jobs {
		require.Equal(t, expectedKeyRanges[i], job.keyRange)
		iter := job.ingestData.NewIter(ctx, job.keyRange.Start, job.keyRange.End, nil)
		for iter.First(); iter.Valid(); iter.Next() {
			require.Equal(t, keys[kvIdx], iter.Key())
			require.Equal(t, values[kvIdx], iter.Value())
			kvIdx++
		}
		require.NoError(t, iter.Error())
		require.NoError(t, iter.Close())
	}
	require.Equal(t, 100, kvIdx)
}

func TestCheckDiskAvail(t *testing.T) {
	store := &http.StoreInfo{Status: http.StoreStatus{Capacity: "100 GB", Available: "50 GB"}}
	ctx := context.Background()
	err := checkDiskAvail(ctx, store)
	require.NoError(t, err)

	// pd may return this StoreInfo before the store reports heartbeat
	store = &http.StoreInfo{Status: http.StoreStatus{LeaderWeight: 1.0, RegionWeight: 1.0}}
	err = checkDiskAvail(ctx, store)
	require.NoError(t, err)
}
