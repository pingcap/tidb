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
	"encoding/base64"
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
	_ "unsafe"

	"github.com/cockroachdb/pebble"
	"github.com/docker/go-units"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/errorpb"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/br/pkg/storage"
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
	"google.golang.org/grpc/encoding"
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

func (c *mockPdClient) GetTS(ctx context.Context) (int64, int64, error) {
	return 1, 2, nil
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
	Marshal(v interface{}) ([]byte, error)
	Unmarshal(data []byte, v interface{}) error
}

//go:linkname newContextWithRPCInfo google.golang.org/grpc.newContextWithRPCInfo
func newContextWithRPCInfo(ctx context.Context, failfast bool, codec baseCodec, cp grpc.Compressor, comp encoding.Compressor) context.Context

type mockCodec struct{}

func (m mockCodec) Marshal(v interface{}) ([]byte, error) {
	return nil, nil
}

func (m mockCodec) Unmarshal(data []byte, v interface{}) error {
	return nil
}

func (m mockWriteClient) Context() context.Context {
	ctx := context.Background()
	return newContextWithRPCInfo(ctx, false, mockCodec{}, nil, nil)
}

func (m mockWriteClient) SendMsg(_ interface{}) error {
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
		pdCtl := &pdutil.PdController{}
		pdCtl.SetPDClient(&mockPdClient{stores: stores})

		local := &Backend{
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
	bak := Backend{}
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/br/pkg/lightning/backend/local/WriteToTiKVNotEnoughDiskSpace", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/br/pkg/lightning/backend/local/WriteToTiKVNotEnoughDiskSpace"))
	}()
	jobCh := make(chan *regionJob, 1)
	jobCh <- &regionJob{}
	jobOutCh := make(chan *regionJob, 1)
	err := bak.startWorker(context.Background(), jobCh, jobOutCh, nil)
	require.Error(t, err)
	require.Regexp(t, "the remaining storage capacity of TiKV.*", err.Error())
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

func (m mockIngestData) NewIter(ctx context.Context, lowerBound, upperBound []byte) common.ForwardIter {
	i, j := m.getFirstAndLastKeyIdx(lowerBound, upperBound)
	return &mockIngestIter{data: m, startIdx: i, endIdx: j, curIdx: i}
}

func (m mockIngestData) GetTS() uint64 { return 0 }

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
		bufferPool:         membuf.NewPool(),
		supportMultiIngest: true,
		BackendConfig: BackendConfig{
			ShouldCheckWriteStall: true,
		},
		tikvCodec: keyspace.CodecV1,
	}

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
		bufferPool:         membuf.NewPool(),
		supportMultiIngest: true,
		BackendConfig: BackendConfig{
			ShouldCheckWriteStall: true,
		},
		tikvCodec: keyspace.CodecV1,
	}

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
		bufferPool:         membuf.NewPool(),
		supportMultiIngest: true,
		tikvCodec:          keyspace.CodecV1,
	}

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
		bufferPool:         membuf.NewPool(),
		supportMultiIngest: true,
		tikvCodec:          keyspace.CodecV1,
	}

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
	err := db.Set([]byte("a"), []byte("a"), nil)
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

	_ = failpoint.Enable("github.com/pingcap/tidb/br/pkg/lightning/backend/local/skipSplitAndScatter", "return()")
	_ = failpoint.Enable("github.com/pingcap/tidb/br/pkg/lightning/backend/local/fakeRegionJobs", "return()")
	t.Cleanup(func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/br/pkg/lightning/backend/local/skipSplitAndScatter")
		_ = failpoint.Disable("github.com/pingcap/tidb/br/pkg/lightning/backend/local/fakeRegionJobs")
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

	_ = failpoint.Enable("github.com/pingcap/tidb/br/pkg/lightning/backend/local/skipSplitAndScatter", "return()")
	_ = failpoint.Enable("github.com/pingcap/tidb/br/pkg/lightning/backend/local/fakeRegionJobs", "return()")
	t.Cleanup(func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/br/pkg/lightning/backend/local/skipSplitAndScatter")
		_ = failpoint.Disable("github.com/pingcap/tidb/br/pkg/lightning/backend/local/fakeRegionJobs")
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

	_ = failpoint.Enable("github.com/pingcap/tidb/br/pkg/lightning/backend/local/skipSplitAndScatter", "return()")
	_ = failpoint.Enable("github.com/pingcap/tidb/br/pkg/lightning/backend/local/fakeRegionJobs", "return()")
	_ = failpoint.Enable("github.com/pingcap/tidb/br/pkg/lightning/backend/local/beforeGenerateJob", "sleep(1000)")
	_ = failpoint.Enable("github.com/pingcap/tidb/br/pkg/lightning/backend/local/WriteToTiKVNotEnoughDiskSpace", "return()")
	t.Cleanup(func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/br/pkg/lightning/backend/local/skipSplitAndScatter")
		_ = failpoint.Disable("github.com/pingcap/tidb/br/pkg/lightning/backend/local/fakeRegionJobs")
		_ = failpoint.Disable("github.com/pingcap/tidb/br/pkg/lightning/backend/local/beforeGenerateJob")
		_ = failpoint.Disable("github.com/pingcap/tidb/br/pkg/lightning/backend/local/WriteToTiKVNotEnoughDiskSpace")
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

func TestExternalEngine(t *testing.T) {
	_ = failpoint.Enable("github.com/pingcap/tidb/br/pkg/lightning/backend/local/skipSplitAndScatter", "return()")
	_ = failpoint.Enable("github.com/pingcap/tidb/br/pkg/lightning/backend/local/skipStartWorker", "return()")
	_ = failpoint.Enable("github.com/pingcap/tidb/br/pkg/lightning/backend/local/injectVariables", "return()")
	t.Cleanup(func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/br/pkg/lightning/backend/local/skipSplitAndScatter")
		_ = failpoint.Disable("github.com/pingcap/tidb/br/pkg/lightning/backend/local/skipStartWorker")
		_ = failpoint.Disable("github.com/pingcap/tidb/br/pkg/lightning/backend/local/injectVariables")
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
		MinKey:        keys[0],
		MaxKey:        keys[99],
		SplitKeys:     [][]byte{keys[30], keys[60], keys[90]},
		TotalFileSize: int64(config.SplitRegionSize) + 1,
		TotalKVCount:  int64(config.SplitRegionKeys) + 1,
	}
	engineUUID := uuid.New()
	pdCtl := &pdutil.PdController{}
	pdCtl.SetPDClient(&mockPdClient{})
	local := &Backend{
		BackendConfig: BackendConfig{
			WorkerConcurrency: 2,
		},
		splitCli: initTestSplitClient([][]byte{
			keys[0], keys[50], endKey,
		}, nil),
		pdCtl:          pdCtl,
		externalEngine: map[uuid.UUID]common.Engine{},
		keyAdapter:     common.NoopKeyAdapter{},
	}
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
		iter := job.ingestData.NewIter(ctx, job.keyRange.Start, job.keyRange.End)
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

func TestLance(t *testing.T) {
	ctx := context.Background()
	b, err := storage.ParseBackend(s3URL, nil)
	require.NoError(t, err)
	store, err := storage.New(ctx, b, nil)
	require.NoError(t, err)

	dataFiles := []string{
		"181/30065/f719a8b1-6514-48be-af1b-2e26f94fa303/0",
		"181/30065/f719a8b1-6514-48be-af1b-2e26f94fa303/1",
		"181/30066/41bfb300-376d-44dd-a9e3-23b269771cb2/0",
		"181/30066/41bfb300-376d-44dd-a9e3-23b269771cb2/1",
		"181/30067/413073d9-2412-4db6-ba64-5a7828b4877b/0",
		"181/30067/413073d9-2412-4db6-ba64-5a7828b4877b/1",
		"181/30068/f7b77ffd-8c98-4925-ba12-0d22375ff987/0",
		"181/30068/f7b77ffd-8c98-4925-ba12-0d22375ff987/1",
		"181/30069/6e414bf9-cb15-4658-b1de-589dc03b1033/0",
		"181/30069/6e414bf9-cb15-4658-b1de-589dc03b1033/1",
		"181/30070/0824eeda-8648-48d1-a47b-ed2333280357/0",
		"181/30070/0824eeda-8648-48d1-a47b-ed2333280357/1",
		"181/30071/6834ba45-bde2-4d14-9d83-8a56093a0f16/0",
		"181/30071/6834ba45-bde2-4d14-9d83-8a56093a0f16/1",
		"181/30072/3a3a4c24-0c29-4ca4-93bf-f468177ebc04/0",
		"181/30072/3a3a4c24-0c29-4ca4-93bf-f468177ebc04/1",
		"181/30073/3c52bd22-fc62-445e-9e97-f7c05c11c361/0",
		"181/30073/3c52bd22-fc62-445e-9e97-f7c05c11c361/1",
		"181/30074/24f18b3d-1d25-4aba-9620-931fee5f7d37/0",
		"181/30074/24f18b3d-1d25-4aba-9620-931fee5f7d37/1",
		"181/30075/010634e2-1258-4e8e-b13b-e9728c446920/0",
		"181/30075/010634e2-1258-4e8e-b13b-e9728c446920/1",
		"181/30076/1d806883-a939-43f5-bd1e-532b93040cb2/0",
		"181/30076/1d806883-a939-43f5-bd1e-532b93040cb2/1",
		"181/30077/82eb6723-e185-4132-965a-7170104e5f07/0",
		"181/30077/82eb6723-e185-4132-965a-7170104e5f07/1",
		"181/30078/f707d839-db67-4beb-9e0f-dcaf04e2031a/0",
		"181/30078/f707d839-db67-4beb-9e0f-dcaf04e2031a/1",
		"181/30079/585e1f5e-8fe4-419a-a4ac-5fd3186aa1b4/0",
		"181/30079/585e1f5e-8fe4-419a-a4ac-5fd3186aa1b4/1",
		"181/30080/82015db6-1287-42ed-8e46-6331ea797ca2/0",
		"181/30080/82015db6-1287-42ed-8e46-6331ea797ca2/1",
		"181/30081/41046798-c82c-45e0-953e-c44919808a48/0",
		"181/30081/41046798-c82c-45e0-953e-c44919808a48/1",
		"181/30082/e40e73c6-05f3-46cc-8267-b26c1baba308/0",
		"181/30082/e40e73c6-05f3-46cc-8267-b26c1baba308/1",
		"181/30083/f49f82bb-089d-487e-9630-5de2beb182b6/0",
		"181/30083/f49f82bb-089d-487e-9630-5de2beb182b6/1",
		"181/30084/77058fee-ad16-40f9-8067-7ce2a799bd38/0",
		"181/30084/77058fee-ad16-40f9-8067-7ce2a799bd38/1",
		"181/30085/56598d03-d57c-4fba-82e4-5733277efb45/0",
		"181/30085/56598d03-d57c-4fba-82e4-5733277efb45/1",
		"181/30086/a8fdbf46-e384-43ce-bb01-d625b6bb7113/0",
		"181/30086/a8fdbf46-e384-43ce-bb01-d625b6bb7113/1",
		"181/30087/f825e8d7-83e1-4067-b547-383a14207c4a/0",
		"181/30087/f825e8d7-83e1-4067-b547-383a14207c4a/1",
		"181/30088/1ff719da-2aa1-4098-835a-795a49c60fcc/0",
		"181/30088/1ff719da-2aa1-4098-835a-795a49c60fcc/1",
		"181/30089/cd10c7d1-bc62-4c87-ac8f-3f4a568f7a39/0",
		"181/30089/cd10c7d1-bc62-4c87-ac8f-3f4a568f7a39/1",
		"181/30090/18e5032a-88b6-4396-a9a2-5df74121db24/0",
		"181/30090/18e5032a-88b6-4396-a9a2-5df74121db24/1",
		"181/30091/0a4f7a0d-a108-42a3-b71a-1d8178cd844c/0",
		"181/30091/0a4f7a0d-a108-42a3-b71a-1d8178cd844c/1",
		"181/30092/d2f9e06b-b5dc-472b-b231-5c2e7b0601aa/0",
		"181/30092/d2f9e06b-b5dc-472b-b231-5c2e7b0601aa/1",
		"181/30093/d4feaf87-a7cb-485f-bdf6-87939a497f78/0",
		"181/30093/d4feaf87-a7cb-485f-bdf6-87939a497f78/1",
		"181/30094/eafcb790-7234-4dfb-870f-0cb5720f6f91/0",
		"181/30094/eafcb790-7234-4dfb-870f-0cb5720f6f91/1",
		"181/30095/2c626a9f-645f-4e66-bd36-d5c7ed574c86/0",
		"181/30095/2c626a9f-645f-4e66-bd36-d5c7ed574c86/1",
		"181/30096/e17c262f-f986-4b23-86fc-ef19549e3786/0",
		"181/30096/e17c262f-f986-4b23-86fc-ef19549e3786/1",
		"181/30097/39e1a47d-8df2-43dd-93e5-0612d0a16405/0",
		"181/30097/39e1a47d-8df2-43dd-93e5-0612d0a16405/1",
		"181/30098/5987ecdc-aa3a-43b3-be62-b9e647bea16b/0",
		"181/30098/5987ecdc-aa3a-43b3-be62-b9e647bea16b/1",
		"181/30099/468453c7-9bd3-4e39-8f67-617f7b7a6cec/0",
		"181/30099/468453c7-9bd3-4e39-8f67-617f7b7a6cec/1",
		"181/30100/ab302572-97c0-4e65-b3f5-c43f4b2f9604/0",
		"181/30100/ab302572-97c0-4e65-b3f5-c43f4b2f9604/1",
		"181/30101/83e9a74e-11f9-4b43-9963-4e1a9ea84098/0",
		"181/30101/83e9a74e-11f9-4b43-9963-4e1a9ea84098/1",
		"181/30102/a924bf2b-fa02-4037-8f3c-503b7af85b5f/0",
		"181/30102/a924bf2b-fa02-4037-8f3c-503b7af85b5f/1",
		"181/30103/bed93dd2-79a6-4919-9006-d5c63542ec7c/0",
		"181/30103/bed93dd2-79a6-4919-9006-d5c63542ec7c/1",
		"181/30104/ba961f3e-b0fa-4777-b1b2-f7dec0ebf075/0",
		"181/30104/ba961f3e-b0fa-4777-b1b2-f7dec0ebf075/1",
		"181/30105/3758ba70-d164-48e0-ab28-764ebd2a5e9d/0",
		"181/30105/3758ba70-d164-48e0-ab28-764ebd2a5e9d/1",
		"181/30106/55b02fe1-0284-41e9-ab6b-8aca6cea8598/0",
		"181/30106/55b02fe1-0284-41e9-ab6b-8aca6cea8598/1",
		"181/30107/14d16229-8cd5-4100-beb2-75c49184af07/0",
		"181/30107/14d16229-8cd5-4100-beb2-75c49184af07/1",
		"181/30108/ed5a0ba3-07b1-4d07-bc50-61874efd8f65/0",
		"181/30108/ed5a0ba3-07b1-4d07-bc50-61874efd8f65/1",
		"181/30109/645ff0e3-f4a8-42f4-9512-b34c905d9708/0",
		"181/30109/645ff0e3-f4a8-42f4-9512-b34c905d9708/1",
		"181/30110/034f0938-4707-46a5-ae5e-9907e029697a/0",
		"181/30110/034f0938-4707-46a5-ae5e-9907e029697a/1",
		"181/30111/f5c09533-ee2d-41c8-9319-87c2fb3f9525/0",
		"181/30111/f5c09533-ee2d-41c8-9319-87c2fb3f9525/1",
		"181/30112/d3f82ba6-c4dc-4a67-b4a4-f58472b32e57/0",
		"181/30112/d3f82ba6-c4dc-4a67-b4a4-f58472b32e57/1",
		"181/30113/28392f53-b683-4870-bc4a-67bf41d32484/0",
		"181/30113/28392f53-b683-4870-bc4a-67bf41d32484/1",
		"181/30114/23778449-b383-4620-9614-c3b9e163e69c/0",
		"181/30114/23778449-b383-4620-9614-c3b9e163e69c/1",
		"181/30115/cde77aaf-63d4-45ef-bc1a-381ab00f8663/0",
		"181/30115/cde77aaf-63d4-45ef-bc1a-381ab00f8663/1",
		"181/30116/f0487e6c-1ce4-4530-a08a-f7244a73566a/0",
		"181/30116/f0487e6c-1ce4-4530-a08a-f7244a73566a/1",
		"181/30117/e3f1da3d-8def-40b7-b75f-b93afd8b6fe7/0",
		"181/30117/e3f1da3d-8def-40b7-b75f-b93afd8b6fe7/1",
		"181/30118/b06dcce3-f92d-4942-92b4-0cb3910a1cb4/0",
		"181/30118/b06dcce3-f92d-4942-92b4-0cb3910a1cb4/1",
		"181/30119/90386586-1177-43ac-a83a-ed4f133695a3/0",
		"181/30119/90386586-1177-43ac-a83a-ed4f133695a3/1",
		"181/30120/fc0181cb-673a-4f77-af33-d2aecb34147c/0",
		"181/30120/fc0181cb-673a-4f77-af33-d2aecb34147c/1",
		"181/30121/cd1f6a24-5ca2-4394-8e79-d28a1d5a72a3/0",
		"181/30121/cd1f6a24-5ca2-4394-8e79-d28a1d5a72a3/1",
		"181/30122/1ba9e555-3ebc-45a2-81a0-08a2c1213fe6/0",
		"181/30122/1ba9e555-3ebc-45a2-81a0-08a2c1213fe6/1",
		"181/30123/22a28061-aa49-4b07-959d-82f0f5b9c0ac/0",
		"181/30123/22a28061-aa49-4b07-959d-82f0f5b9c0ac/1",
		"181/30124/8c0e8586-0397-4fc2-9a13-eefdebf5f9ae/0",
		"181/30124/8c0e8586-0397-4fc2-9a13-eefdebf5f9ae/1",
		"181/30125/a8f881ce-5220-4bd0-906c-e561df54489e/0",
		"181/30125/a8f881ce-5220-4bd0-906c-e561df54489e/1",
		"181/30126/7ec6c7cd-4a5e-4d2b-b162-6a4b7d4b476d/0",
		"181/30126/7ec6c7cd-4a5e-4d2b-b162-6a4b7d4b476d/1",
		"181/30127/72424840-315c-47de-acd3-f66e3ba3cfca/0",
		"181/30127/72424840-315c-47de-acd3-f66e3ba3cfca/1",
		"181/30128/59d8491d-d491-4bab-af93-94552712f5bb/0",
		"181/30128/59d8491d-d491-4bab-af93-94552712f5bb/1",
		"181/30129/a5a07925-7920-4870-9822-53776cb26946/0",
		"181/30129/a5a07925-7920-4870-9822-53776cb26946/1",
		"181/30130/8529481c-35d7-4f79-b20f-0c50b16a6efa/0",
	}

	statFiles := []string{
		"181/30065/f719a8b1-6514-48be-af1b-2e26f94fa303_stat/0",
		"181/30065/f719a8b1-6514-48be-af1b-2e26f94fa303_stat/1",
		"181/30066/41bfb300-376d-44dd-a9e3-23b269771cb2_stat/0",
		"181/30066/41bfb300-376d-44dd-a9e3-23b269771cb2_stat/1",
		"181/30067/413073d9-2412-4db6-ba64-5a7828b4877b_stat/0",
		"181/30067/413073d9-2412-4db6-ba64-5a7828b4877b_stat/1",
		"181/30068/f7b77ffd-8c98-4925-ba12-0d22375ff987_stat/0",
		"181/30068/f7b77ffd-8c98-4925-ba12-0d22375ff987_stat/1",
		"181/30069/6e414bf9-cb15-4658-b1de-589dc03b1033_stat/0",
		"181/30069/6e414bf9-cb15-4658-b1de-589dc03b1033_stat/1",
		"181/30070/0824eeda-8648-48d1-a47b-ed2333280357_stat/0",
		"181/30070/0824eeda-8648-48d1-a47b-ed2333280357_stat/1",
		"181/30071/6834ba45-bde2-4d14-9d83-8a56093a0f16_stat/0",
		"181/30071/6834ba45-bde2-4d14-9d83-8a56093a0f16_stat/1",
		"181/30072/3a3a4c24-0c29-4ca4-93bf-f468177ebc04_stat/0",
		"181/30072/3a3a4c24-0c29-4ca4-93bf-f468177ebc04_stat/1",
		"181/30073/3c52bd22-fc62-445e-9e97-f7c05c11c361_stat/0",
		"181/30073/3c52bd22-fc62-445e-9e97-f7c05c11c361_stat/1",
		"181/30074/24f18b3d-1d25-4aba-9620-931fee5f7d37_stat/0",
		"181/30074/24f18b3d-1d25-4aba-9620-931fee5f7d37_stat/1",
		"181/30075/010634e2-1258-4e8e-b13b-e9728c446920_stat/0",
		"181/30075/010634e2-1258-4e8e-b13b-e9728c446920_stat/1",
		"181/30076/1d806883-a939-43f5-bd1e-532b93040cb2_stat/0",
		"181/30076/1d806883-a939-43f5-bd1e-532b93040cb2_stat/1",
		"181/30077/82eb6723-e185-4132-965a-7170104e5f07_stat/0",
		"181/30077/82eb6723-e185-4132-965a-7170104e5f07_stat/1",
		"181/30078/f707d839-db67-4beb-9e0f-dcaf04e2031a_stat/0",
		"181/30078/f707d839-db67-4beb-9e0f-dcaf04e2031a_stat/1",
		"181/30079/585e1f5e-8fe4-419a-a4ac-5fd3186aa1b4_stat/0",
		"181/30079/585e1f5e-8fe4-419a-a4ac-5fd3186aa1b4_stat/1",
		"181/30080/82015db6-1287-42ed-8e46-6331ea797ca2_stat/0",
		"181/30080/82015db6-1287-42ed-8e46-6331ea797ca2_stat/1",
		"181/30081/41046798-c82c-45e0-953e-c44919808a48_stat/0",
		"181/30081/41046798-c82c-45e0-953e-c44919808a48_stat/1",
		"181/30082/e40e73c6-05f3-46cc-8267-b26c1baba308_stat/0",
		"181/30082/e40e73c6-05f3-46cc-8267-b26c1baba308_stat/1",
		"181/30083/f49f82bb-089d-487e-9630-5de2beb182b6_stat/0",
		"181/30083/f49f82bb-089d-487e-9630-5de2beb182b6_stat/1",
		"181/30084/77058fee-ad16-40f9-8067-7ce2a799bd38_stat/0",
		"181/30084/77058fee-ad16-40f9-8067-7ce2a799bd38_stat/1",
		"181/30085/56598d03-d57c-4fba-82e4-5733277efb45_stat/0",
		"181/30085/56598d03-d57c-4fba-82e4-5733277efb45_stat/1",
		"181/30086/a8fdbf46-e384-43ce-bb01-d625b6bb7113_stat/0",
		"181/30086/a8fdbf46-e384-43ce-bb01-d625b6bb7113_stat/1",
		"181/30087/f825e8d7-83e1-4067-b547-383a14207c4a_stat/0",
		"181/30087/f825e8d7-83e1-4067-b547-383a14207c4a_stat/1",
		"181/30088/1ff719da-2aa1-4098-835a-795a49c60fcc_stat/0",
		"181/30088/1ff719da-2aa1-4098-835a-795a49c60fcc_stat/1",
		"181/30089/cd10c7d1-bc62-4c87-ac8f-3f4a568f7a39_stat/0",
		"181/30089/cd10c7d1-bc62-4c87-ac8f-3f4a568f7a39_stat/1",
		"181/30090/18e5032a-88b6-4396-a9a2-5df74121db24_stat/0",
		"181/30090/18e5032a-88b6-4396-a9a2-5df74121db24_stat/1",
		"181/30091/0a4f7a0d-a108-42a3-b71a-1d8178cd844c_stat/0",
		"181/30091/0a4f7a0d-a108-42a3-b71a-1d8178cd844c_stat/1",
		"181/30092/d2f9e06b-b5dc-472b-b231-5c2e7b0601aa_stat/0",
		"181/30092/d2f9e06b-b5dc-472b-b231-5c2e7b0601aa_stat/1",
		"181/30093/d4feaf87-a7cb-485f-bdf6-87939a497f78_stat/0",
		"181/30093/d4feaf87-a7cb-485f-bdf6-87939a497f78_stat/1",
		"181/30094/eafcb790-7234-4dfb-870f-0cb5720f6f91_stat/0",
		"181/30094/eafcb790-7234-4dfb-870f-0cb5720f6f91_stat/1",
		"181/30095/2c626a9f-645f-4e66-bd36-d5c7ed574c86_stat/0",
		"181/30095/2c626a9f-645f-4e66-bd36-d5c7ed574c86_stat/1",
		"181/30096/e17c262f-f986-4b23-86fc-ef19549e3786_stat/0",
		"181/30096/e17c262f-f986-4b23-86fc-ef19549e3786_stat/1",
		"181/30097/39e1a47d-8df2-43dd-93e5-0612d0a16405_stat/0",
		"181/30097/39e1a47d-8df2-43dd-93e5-0612d0a16405_stat/1",
		"181/30098/5987ecdc-aa3a-43b3-be62-b9e647bea16b_stat/0",
		"181/30098/5987ecdc-aa3a-43b3-be62-b9e647bea16b_stat/1",
		"181/30099/468453c7-9bd3-4e39-8f67-617f7b7a6cec_stat/0",
		"181/30099/468453c7-9bd3-4e39-8f67-617f7b7a6cec_stat/1",
		"181/30100/ab302572-97c0-4e65-b3f5-c43f4b2f9604_stat/0",
		"181/30100/ab302572-97c0-4e65-b3f5-c43f4b2f9604_stat/1",
		"181/30101/83e9a74e-11f9-4b43-9963-4e1a9ea84098_stat/0",
		"181/30101/83e9a74e-11f9-4b43-9963-4e1a9ea84098_stat/1",
		"181/30102/a924bf2b-fa02-4037-8f3c-503b7af85b5f_stat/0",
		"181/30102/a924bf2b-fa02-4037-8f3c-503b7af85b5f_stat/1",
		"181/30103/bed93dd2-79a6-4919-9006-d5c63542ec7c_stat/0",
		"181/30103/bed93dd2-79a6-4919-9006-d5c63542ec7c_stat/1",
		"181/30104/ba961f3e-b0fa-4777-b1b2-f7dec0ebf075_stat/0",
		"181/30104/ba961f3e-b0fa-4777-b1b2-f7dec0ebf075_stat/1",
		"181/30105/3758ba70-d164-48e0-ab28-764ebd2a5e9d_stat/0",
		"181/30105/3758ba70-d164-48e0-ab28-764ebd2a5e9d_stat/1",
		"181/30106/55b02fe1-0284-41e9-ab6b-8aca6cea8598_stat/0",
		"181/30106/55b02fe1-0284-41e9-ab6b-8aca6cea8598_stat/1",
		"181/30107/14d16229-8cd5-4100-beb2-75c49184af07_stat/0",
		"181/30107/14d16229-8cd5-4100-beb2-75c49184af07_stat/1",
		"181/30108/ed5a0ba3-07b1-4d07-bc50-61874efd8f65_stat/0",
		"181/30108/ed5a0ba3-07b1-4d07-bc50-61874efd8f65_stat/1",
		"181/30109/645ff0e3-f4a8-42f4-9512-b34c905d9708_stat/0",
		"181/30109/645ff0e3-f4a8-42f4-9512-b34c905d9708_stat/1",
		"181/30110/034f0938-4707-46a5-ae5e-9907e029697a_stat/0",
		"181/30110/034f0938-4707-46a5-ae5e-9907e029697a_stat/1",
		"181/30111/f5c09533-ee2d-41c8-9319-87c2fb3f9525_stat/0",
		"181/30111/f5c09533-ee2d-41c8-9319-87c2fb3f9525_stat/1",
		"181/30112/d3f82ba6-c4dc-4a67-b4a4-f58472b32e57_stat/0",
		"181/30112/d3f82ba6-c4dc-4a67-b4a4-f58472b32e57_stat/1",
		"181/30113/28392f53-b683-4870-bc4a-67bf41d32484_stat/0",
		"181/30113/28392f53-b683-4870-bc4a-67bf41d32484_stat/1",
		"181/30114/23778449-b383-4620-9614-c3b9e163e69c_stat/0",
		"181/30114/23778449-b383-4620-9614-c3b9e163e69c_stat/1",
		"181/30115/cde77aaf-63d4-45ef-bc1a-381ab00f8663_stat/0",
		"181/30115/cde77aaf-63d4-45ef-bc1a-381ab00f8663_stat/1",
		"181/30116/f0487e6c-1ce4-4530-a08a-f7244a73566a_stat/0",
		"181/30116/f0487e6c-1ce4-4530-a08a-f7244a73566a_stat/1",
		"181/30117/e3f1da3d-8def-40b7-b75f-b93afd8b6fe7_stat/0",
		"181/30117/e3f1da3d-8def-40b7-b75f-b93afd8b6fe7_stat/1",
		"181/30118/b06dcce3-f92d-4942-92b4-0cb3910a1cb4_stat/0",
		"181/30118/b06dcce3-f92d-4942-92b4-0cb3910a1cb4_stat/1",
		"181/30119/90386586-1177-43ac-a83a-ed4f133695a3_stat/0",
		"181/30119/90386586-1177-43ac-a83a-ed4f133695a3_stat/1",
		"181/30120/fc0181cb-673a-4f77-af33-d2aecb34147c_stat/0",
		"181/30120/fc0181cb-673a-4f77-af33-d2aecb34147c_stat/1",
		"181/30121/cd1f6a24-5ca2-4394-8e79-d28a1d5a72a3_stat/0",
		"181/30121/cd1f6a24-5ca2-4394-8e79-d28a1d5a72a3_stat/1",
		"181/30122/1ba9e555-3ebc-45a2-81a0-08a2c1213fe6_stat/0",
		"181/30122/1ba9e555-3ebc-45a2-81a0-08a2c1213fe6_stat/1",
		"181/30123/22a28061-aa49-4b07-959d-82f0f5b9c0ac_stat/0",
		"181/30123/22a28061-aa49-4b07-959d-82f0f5b9c0ac_stat/1",
		"181/30124/8c0e8586-0397-4fc2-9a13-eefdebf5f9ae_stat/0",
		"181/30124/8c0e8586-0397-4fc2-9a13-eefdebf5f9ae_stat/1",
		"181/30125/a8f881ce-5220-4bd0-906c-e561df54489e_stat/0",
		"181/30125/a8f881ce-5220-4bd0-906c-e561df54489e_stat/1",
		"181/30126/7ec6c7cd-4a5e-4d2b-b162-6a4b7d4b476d_stat/0",
		"181/30126/7ec6c7cd-4a5e-4d2b-b162-6a4b7d4b476d_stat/1",
		"181/30127/72424840-315c-47de-acd3-f66e3ba3cfca_stat/0",
		"181/30127/72424840-315c-47de-acd3-f66e3ba3cfca_stat/1",
		"181/30128/59d8491d-d491-4bab-af93-94552712f5bb_stat/0",
		"181/30128/59d8491d-d491-4bab-af93-94552712f5bb_stat/1",
		"181/30129/a5a07925-7920-4870-9822-53776cb26946_stat/0",
		"181/30129/a5a07925-7920-4870-9822-53776cb26946_stat/1",
		"181/30130/8529481c-35d7-4f79-b20f-0c50b16a6efa_stat/0",
	}

	offsets := []uint64{165838848, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	_ = offsets

	decode := func(s string) []byte {
		b, err := base64.StdEncoding.DecodeString(s)
		require.NoError(t, err)
		return b
	}

	splitKeysStr := []string{
		"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",
	}
	minKey := decode("dIAAAAAAAACyX2mAAAAAAAAACAEAAAAAAAAAAPcEAAAAAABQAAAEAAABgO7R8b8EAAAAAAAA3Jk=")
	maxKey := decode("dIAAAAAAAACyX2mAAAAAAAAACAEAAAAAAAAAAPcEAAAAAACgAAAEAAABgO7RkQMEAAAAAAAAUOI=")

	splitKeys := make([][]byte, 0, len(splitKeysStr))
	for i := range splitKeysStr {
		splitKeys = append(splitKeys, decode(splitKeysStr[i]))
	}
	engine := external.NewExternalEngine(
		store, dataFiles, statFiles, minKey, maxKey, splitKeys,
		common.NoopKeyAdapter{}, false, nil, common.DupDetectOpt{}, 0, 0, 0)

	ranges, err := engine.SplitRanges(minKey, maxKey, int64(config.SplitRegionSize), int64(config.SplitRegionKeys), log.L())
	require.NoError(t, err)

	fmt.Printf("length of ranges %d\n", len(ranges))

	lb := Backend{
		BackendConfig: BackendConfig{
			WorkerConcurrency: 8,
		},
	}

	jobCh := make(chan *regionJob, 128)
	jobWg := &sync.WaitGroup{}
	err = lb.generateAndSendJob(
		ctx,
		engine,
		ranges,
		int64(config.SplitRegionSize),
		int64(config.SplitRegionKeys),
		jobCh,
		jobWg,
	)
	require.NoError(t, err)
}
