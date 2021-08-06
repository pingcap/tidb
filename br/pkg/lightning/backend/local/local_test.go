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
// See the License for the specific language governing permissions and
// limitations under the License.

package local

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/coreos/go-semver/semver"
	"github.com/docker/go-units"
	"github.com/golang/mock/gomock"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/errorpb"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/mock"
	"github.com/pingcap/tidb/br/pkg/restore"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/hack"
)

type localSuite struct{}

var _ = Suite(&localSuite{})

func Test(t *testing.T) {
	TestingT(t)
}

func (s *localSuite) TestNextKey(c *C) {
	c.Assert(nextKey([]byte{}), DeepEquals, []byte{})

	cases := [][]byte{
		{0},
		{255},
		{1, 255},
	}
	for _, b := range cases {
		next := nextKey(b)
		c.Assert(next, DeepEquals, append(b, 0))
	}

	// in the old logic, this should return []byte{} which is not the actually smallest eky
	next := nextKey([]byte{1, 255})
	c.Assert(bytes.Compare(next, []byte{2}), Equals, -1)

	// another test case, nextkey()'s return should be smaller than key with a prefix of the origin key
	next = nextKey([]byte{1, 255})
	c.Assert(bytes.Compare(next, []byte{1, 255, 0, 1, 2}), Equals, -1)

	// test recode key
	// key with int handle
	for _, handleID := range []int64{math.MinInt64, 1, 255, math.MaxInt32 - 1} {
		key := tablecodec.EncodeRowKeyWithHandle(1, tidbkv.IntHandle(handleID))
		c.Assert(nextKey(key), DeepEquals, []byte(tablecodec.EncodeRowKeyWithHandle(1, tidbkv.IntHandle(handleID+1))))
	}

	// overflowed
	key := tablecodec.EncodeRowKeyWithHandle(1, tidbkv.IntHandle(math.MaxInt64))
	next = tablecodec.EncodeTablePrefix(2)
	c.Assert([]byte(key), Less, next)
	c.Assert(nextKey(key), DeepEquals, next)

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
		c.Assert(err, IsNil)
		h, err := tidbkv.NewCommonHandle(keyBytes)
		c.Assert(err, IsNil)
		key := tablecodec.EncodeRowKeyWithHandle(1, h)
		nextKeyBytes, err := codec.EncodeKey(stmtCtx, nil, types.NewIntDatum(123), datums[1])
		c.Assert(err, IsNil)
		nextHdl, err := tidbkv.NewCommonHandle(nextKeyBytes)
		c.Assert(err, IsNil)
		expectNextKey := []byte(tablecodec.EncodeRowKeyWithHandle(1, nextHdl))
		c.Assert(nextKey(key), DeepEquals, expectNextKey)
	}

	// dIAAAAAAAAD/PV9pgAAAAAD/AAABA4AAAAD/AAAAAQOAAAD/AAAAAAEAAAD8
	// a index key with: table: 61, index: 1, int64: 1, int64: 1
	a := []byte{116, 128, 0, 0, 0, 0, 0, 0, 255, 61, 95, 105, 128, 0, 0, 0, 0, 255, 0, 0, 1, 3, 128, 0, 0, 0, 255, 0, 0, 0, 1, 3, 128, 0, 0, 255, 0, 0, 0, 0, 1, 0, 0, 0, 252}
	c.Assert(nextKey(a), DeepEquals, append(a, 0))
}

// The first half of this test is same as the test in tikv:
// https://github.com/tikv/tikv/blob/dbfe7730dd0fddb34cb8c3a7f8a079a1349d2d41/components/engine_rocks/src/properties.rs#L572
func (s *localSuite) TestRangeProperties(c *C) {
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
			_ = collector.Add(pebble.InternalKey{UserKey: p.key}, v)
		}
	}

	userProperties := make(map[string]string, 1)
	_ = collector.Finish(userProperties)

	props, err := decodeRangeProperties(hack.Slice(userProperties[propRangeIndex]))
	c.Assert(err, IsNil)

	// Smallest key in props.
	c.Assert(props[0].Key, DeepEquals, cases[0].key)
	// Largest key in props.
	c.Assert(props[len(props)-1].Key, DeepEquals, cases[len(cases)-1].key)
	c.Assert(len(props), Equals, 7)

	a := props.get([]byte("a"))
	c.Assert(a.Size, Equals, uint64(1))
	e := props.get([]byte("e"))
	c.Assert(e.Size, Equals, uint64(defaultPropSizeIndexDistance+5))
	i := props.get([]byte("i"))
	c.Assert(i.Size, Equals, uint64(defaultPropSizeIndexDistance/8*17+9))
	k := props.get([]byte("k"))
	c.Assert(k.Size, Equals, uint64(defaultPropSizeIndexDistance/8*25+11))
	m := props.get([]byte("m"))
	c.Assert(m.Keys, Equals, uint64(defaultPropKeysIndexDistance+11))
	n := props.get([]byte("n"))
	c.Assert(n.Keys, Equals, uint64(defaultPropKeysIndexDistance*2+11))
	o := props.get([]byte("o"))
	c.Assert(o.Keys, Equals, uint64(defaultPropKeysIndexDistance*2+12))

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

	c.Assert(sizeProps.indexHandles.Len(), Equals, 12)
	idx := 0
	sizeProps.iter(func(p *rangeProperty) bool {
		c.Assert(p, DeepEquals, res[idx])
		idx++
		return true
	})

	fullRange := Range{start: []byte("a"), end: []byte("z")}
	ranges := splitRangeBySizeProps(fullRange, sizeProps, 2*defaultPropSizeIndexDistance, defaultPropKeysIndexDistance*5/2)

	c.Assert(ranges, DeepEquals, []Range{
		{start: []byte("a"), end: []byte("e")},
		{start: []byte("e"), end: []byte("k")},
		{start: []byte("k"), end: []byte("mm")},
		{start: []byte("mm"), end: []byte("q")},
		{start: []byte("q"), end: []byte("z")},
	})

	ranges = splitRangeBySizeProps(fullRange, sizeProps, 2*defaultPropSizeIndexDistance, defaultPropKeysIndexDistance)
	c.Assert(ranges, DeepEquals, []Range{
		{start: []byte("a"), end: []byte("e")},
		{start: []byte("e"), end: []byte("h")},
		{start: []byte("h"), end: []byte("k")},
		{start: []byte("k"), end: []byte("m")},
		{start: []byte("m"), end: []byte("mm")},
		{start: []byte("mm"), end: []byte("n")},
		{start: []byte("n"), end: []byte("q")},
		{start: []byte("q"), end: []byte("z")},
	})
}

func (s *localSuite) TestRangePropertiesWithPebble(c *C) {
	dir := c.MkDir()

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
	db, err := pebble.Open(filepath.Join(dir, "test"), opt)
	c.Assert(err, IsNil)
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
			err = wb.Set(key, value[:valueLen], writeOpt)
			c.Assert(err, IsNil)
			err = collector.Add(pebble.InternalKey{UserKey: key}, value[:valueLen])
			c.Assert(err, IsNil)
		}
		c.Assert(wb.Commit(writeOpt), IsNil)
	}
	// flush one sst
	c.Assert(db.Flush(), IsNil)

	props := make(map[string]string, 1)
	c.Assert(collector.Finish(props), IsNil)

	sstMetas, err := db.SSTables(pebble.WithProperties())
	c.Assert(err, IsNil)
	for i, level := range sstMetas {
		if i == 0 {
			c.Assert(len(level), Equals, 1)
		} else {
			c.Assert(len(level), Equals, 0)
		}
	}

	c.Assert(sstMetas[0][0].Properties.UserProperties, DeepEquals, props)
}

func testLocalWriter(c *C, needSort bool, partitialSort bool) {
	dir := c.MkDir()
	opt := &pebble.Options{
		MemTableSize:             1024 * 1024,
		MaxConcurrentCompactions: 16,
		L0CompactionThreshold:    math.MaxInt32, // set to max try to disable compaction
		L0StopWritesThreshold:    math.MaxInt32, // set to max try to disable compaction
		DisableWAL:               true,
		ReadOnly:                 false,
	}
	db, err := pebble.Open(filepath.Join(dir, "test"), opt)
	c.Assert(err, IsNil)
	defer db.Close()
	tmpPath := filepath.Join(dir, "test.sst")
	err = os.Mkdir(tmpPath, 0o755)
	c.Assert(err, IsNil)

	_, engineUUID := backend.MakeUUID("ww", 0)
	engineCtx, cancel := context.WithCancel(context.Background())
	f := &File{
		db:           db,
		UUID:         engineUUID,
		sstDir:       tmpPath,
		ctx:          engineCtx,
		cancel:       cancel,
		sstMetasChan: make(chan metaOrFlush, 64),
		keyAdapter:   noopKeyAdapter{},
	}
	f.sstIngester = dbSSTIngester{e: f}
	f.wg.Add(1)
	go f.ingestSSTLoop()
	sorted := needSort && !partitialSort
	w, err := openLocalWriter(context.Background(), &backend.LocalWriterConfig{IsKVSorted: sorted}, f, 1<<20)
	c.Assert(err, IsNil)

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
	c.Assert(err, IsNil)
	err = w.AppendRows(ctx, "", []string{}, kv.MakeRowsFromKvPairs(rows2))
	c.Assert(err, IsNil)
	err = w.AppendRows(ctx, "", []string{}, kv.MakeRowsFromKvPairs(rows3))
	c.Assert(err, IsNil)
	flushStatus, err := w.Close(context.Background())
	c.Assert(err, IsNil)
	c.Assert(f.flushEngineWithoutLock(ctx), IsNil)
	c.Assert(flushStatus.Flushed(), IsTrue)
	o := &pebble.IterOptions{}
	it := db.NewIter(o)

	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})
	c.Assert(int(f.Length.Load()), Equals, 20000)
	c.Assert(int(f.TotalSize.Load()), Equals, 144*20000)
	valid := it.SeekGE(keys[0])
	c.Assert(valid, IsTrue)
	for _, k := range keys {
		c.Assert(it.Key(), DeepEquals, k)
		it.Next()
	}
	close(f.sstMetasChan)
	f.wg.Wait()
}

func (s *localSuite) TestLocalWriterWithSort(c *C) {
	testLocalWriter(c, false, false)
}

func (s *localSuite) TestLocalWriterWithIngest(c *C) {
	testLocalWriter(c, true, false)
}

func (s *localSuite) TestLocalWriterWithIngestUnsort(c *C) {
	testLocalWriter(c, true, true)
}

type mockSplitClient struct {
	restore.SplitClient
}

func (c *mockSplitClient) GetRegion(ctx context.Context, key []byte) (*restore.RegionInfo, error) {
	return &restore.RegionInfo{
		Leader: &metapb.Peer{Id: 1},
		Region: &metapb.Region{
			Id:       1,
			StartKey: key,
		},
	}, nil
}

func (s *localSuite) TestIsIngestRetryable(c *C) {
	local := &local{
		splitCli: &mockSplitClient{},
	}

	resp := &sst.IngestResponse{
		Error: &errorpb.Error{
			NotLeader: &errorpb.NotLeader{
				Leader: &metapb.Peer{Id: 2},
			},
		},
	}
	ctx := context.Background()
	region := &restore.RegionInfo{
		Leader: &metapb.Peer{Id: 1},
		Region: &metapb.Region{
			Id:       1,
			StartKey: []byte{1},
			EndKey:   []byte{3},
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: 1,
				Version: 1,
			},
		},
	}
	metas := []*sst.SSTMeta{
		{
			Range: &sst.Range{
				Start: []byte{1},
				End:   []byte{2},
			},
		},
		{
			Range: &sst.Range{
				Start: []byte{1, 1},
				End:   []byte{2},
			},
		},
	}
	retryType, newRegion, err := local.isIngestRetryable(ctx, resp, region, metas)
	c.Assert(retryType, Equals, retryWrite)
	c.Assert(newRegion.Leader.Id, Equals, uint64(2))
	c.Assert(err, NotNil)

	resp.Error = &errorpb.Error{
		EpochNotMatch: &errorpb.EpochNotMatch{
			CurrentRegions: []*metapb.Region{
				{
					Id:       1,
					StartKey: []byte{1},
					EndKey:   []byte{3},
					RegionEpoch: &metapb.RegionEpoch{
						ConfVer: 1,
						Version: 2,
					},
					Peers: []*metapb.Peer{{Id: 1}},
				},
			},
		},
	}
	retryType, newRegion, err = local.isIngestRetryable(ctx, resp, region, metas)
	c.Assert(retryType, Equals, retryWrite)
	c.Assert(newRegion.Region.RegionEpoch.Version, Equals, uint64(2))
	c.Assert(err, NotNil)

	resp.Error = &errorpb.Error{Message: "raft: proposal dropped"}
	retryType, _, err = local.isIngestRetryable(ctx, resp, region, metas)
	c.Assert(retryType, Equals, retryWrite)
	c.Assert(err, NotNil)

	resp.Error = &errorpb.Error{Message: "unknown error"}
	retryType, _, err = local.isIngestRetryable(ctx, resp, region, metas)
	c.Assert(retryType, Equals, retryNone)
	c.Assert(err, ErrorMatches, "non-retryable error: unknown error")
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

func (s *localSuite) TestLocalIngestLoop(c *C) {
	dir := c.MkDir()
	opt := &pebble.Options{
		MemTableSize:             1024 * 1024,
		MaxConcurrentCompactions: 16,
		L0CompactionThreshold:    math.MaxInt32, // set to max try to disable compaction
		L0StopWritesThreshold:    math.MaxInt32, // set to max try to disable compaction
		DisableWAL:               true,
		ReadOnly:                 false,
	}
	db, err := pebble.Open(filepath.Join(dir, "test"), opt)
	c.Assert(err, IsNil)
	defer db.Close()
	tmpPath := filepath.Join(dir, "test.sst")
	err = os.Mkdir(tmpPath, 0o755)
	c.Assert(err, IsNil)
	_, engineUUID := backend.MakeUUID("ww", 0)
	engineCtx, cancel := context.WithCancel(context.Background())
	f := File{
		db:           db,
		UUID:         engineUUID,
		sstDir:       "",
		ctx:          engineCtx,
		cancel:       cancel,
		sstMetasChan: make(chan metaOrFlush, 64),
		config: backend.LocalEngineConfig{
			Compact:            true,
			CompactThreshold:   100,
			CompactConcurrency: 4,
		},
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
				c.Assert(err, IsNil)
				if int32(i) >= flushCnt {
					f.mutex.RLock()
					err = f.flushEngineWithoutLock(engineCtx)
					c.Assert(err, IsNil)
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
	err = f.flushEngineWithoutLock(engineCtx)
	c.Assert(err, IsNil)
	f.mutex.RUnlock()

	close(f.sstMetasChan)
	f.wg.Wait()
	c.Assert(f.ingestErr.Get(), IsNil)
	c.Assert(totalSize, Equals, f.TotalSize.Load())
	c.Assert(f.Length.Load(), Equals, int64(concurrency*count))
	c.Assert(f.finishedMetaSeq.Load(), Equals, atomic.LoadInt32(&maxMetaSeq))
}

func (s *localSuite) TestCheckRequirementsTiFlash(c *C) {
	controller := gomock.NewController(c)
	defer controller.Finish()
	glue := mock.NewMockGlue(controller)
	exec := mock.NewMockSQLExecutor(controller)
	ctx := context.Background()

	dbMetas := []*mydump.MDDatabaseMeta{
		{
			Name: "test",
			Tables: []*mydump.MDTableMeta{
				{
					DB:        "test",
					Name:      "t1",
					DataFiles: []mydump.FileInfo{{}},
				},
				{
					DB:        "test",
					Name:      "tbl",
					DataFiles: []mydump.FileInfo{{}},
				},
			},
		},
		{
			Name: "test1",
			Tables: []*mydump.MDTableMeta{
				{
					DB:        "test1",
					Name:      "t",
					DataFiles: []mydump.FileInfo{{}},
				},
				{
					DB:        "test1",
					Name:      "tbl",
					DataFiles: []mydump.FileInfo{{}},
				},
			},
		},
	}
	checkCtx := &backend.CheckCtx{DBMetas: dbMetas}

	glue.EXPECT().GetSQLExecutor().Return(exec)
	exec.EXPECT().QueryStringsWithLog(ctx, tiFlashReplicaQuery, gomock.Any(), gomock.Any()).
		Return([][]string{{"db", "tbl"}, {"test", "t1"}, {"test1", "tbl"}}, nil)

	err := checkTiFlashVersion(ctx, glue, checkCtx, *semver.New("4.0.2"))
	c.Assert(err, ErrorMatches, "lightning local backend doesn't support TiFlash in this TiDB version. conflict tables: \\[`test`.`t1`, `test1`.`tbl`\\].*")
}

func makeRanges(input []string) []Range {
	ranges := make([]Range, 0, len(input)/2)
	for i := 0; i < len(input)-1; i += 2 {
		ranges = append(ranges, Range{start: []byte(input[i]), end: []byte(input[i+1])})
	}
	return ranges
}

func (s *localSuite) TestDedupAndMergeRanges(c *C) {
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

		c.Assert(sortAndMergeRanges(input), DeepEquals, output)
	}
}

func (s *localSuite) TestFilterOverlapRange(c *C) {
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

		c.Assert(filterOverlapRange(input, finished), DeepEquals, output)
	}
}
