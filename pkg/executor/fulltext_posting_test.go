// Copyright 2026 PingCAP, Inc.
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

package executor

import (
	"bytes"
	"errors"
	"slices"
	"sort"
	"testing"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tidb/pkg/expression/fulltext"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikvrpc"
	clientutil "github.com/tikv/client-go/v2/util"
	rmclient "github.com/tikv/pd/client/resource_group/controller"
)

// memPostingSnapshot is a kv.Snapshot over sorted in-memory entries that
// serves Iter only, and counts the scans opened.
type memPostingSnapshot struct {
	kv.Snapshot
	keys   []kv.Key
	values map[string][]byte
	iters  int
}

func (s *memPostingSnapshot) put(key kv.Key, value []byte) {
	s.values[string(key)] = value
	i := sort.Search(len(s.keys), func(i int) bool { return s.keys[i].Cmp(key) >= 0 })
	s.keys = slices.Insert(s.keys, i, key)
}

func (s *memPostingSnapshot) Iter(start, end kv.Key) (kv.Iterator, error) {
	s.iters++
	i := sort.Search(len(s.keys), func(i int) bool { return s.keys[i].Cmp(start) >= 0 })
	return &memPostingIter{snapshot: s, i: i, end: end}, nil
}

type memPostingIter struct {
	snapshot *memPostingSnapshot
	i        int
	end      kv.Key
}

func (it *memPostingIter) Valid() bool {
	return it.i < len(it.snapshot.keys) && (len(it.end) == 0 || it.snapshot.keys[it.i].Cmp(it.end) < 0)
}
func (it *memPostingIter) Key() kv.Key   { return it.snapshot.keys[it.i] }
func (it *memPostingIter) Value() []byte { return it.snapshot.values[string(it.snapshot.keys[it.i])] }
func (it *memPostingIter) Next() error   { it.i++; return nil }
func (*memPostingIter) Close()           {}

const testPostingTableID, testPostingIndexID = 100, 1

func testPostingIndex() *model.IndexInfo {
	return &model.IndexInfo{
		ID:      testPostingIndexID,
		Columns: []*model.IndexColumn{{Offset: 0}},
		TiKVFullText: &model.TiKVFullTextIndexInfo{
			ParserType: model.FullTextParserTypeStandardV1, MinTokenSize: 3, MaxTokenSize: 84, EnableStopword: true,
		},
	}
}

// putPosting writes the entry of term for handle the way the write path
// encodes it.
func putPosting(t *testing.T, s *memPostingSnapshot, term string, handle kv.Handle, positions ...int) {
	encoded, err := codec.EncodeKey(nil, nil, types.NewBytesDatum([]byte(term)))
	require.NoError(t, err)
	key := appendHandleKey(tablecodec.EncodeIndexSeekKey(testPostingTableID, testPostingIndexID, encoded), handle)
	s.put(key, tablecodec.EncodeTiKVFullTextIndexValue(nil, tablecodec.EncodeTiKVFullTextPositions(nil, positions), false))
}

func newTestPostingSource(s *memPostingSnapshot) *tikvPostingSource {
	return &tikvPostingSource{
		snapshot:        s,
		physicalTableID: testPostingTableID,
		index:           testPostingIndex(),
		stats:           &fullTextScanRuntimeStats{},
	}
}

func searchPostings(t *testing.T, src *tikvPostingSource, search string) []kv.Handle {
	query, err := fulltext.CompileBooleanQuery(search, fulltext.AnalyzerConfigFromTiKVFullTextIndex(src.index.TiKVFullText))
	require.NoError(t, err)
	iter, err := query.OpenPostings(src)
	require.NoError(t, err)
	var handles []kv.Handle
	for {
		handle, ok, err := iter.Next()
		require.NoError(t, err)
		if !ok {
			break
		}
		handles = append(handles, handle)
	}
	require.NoError(t, iter.Close())
	return handles
}

// TestTiKVPostingSeek covers the posting cursors seeking: reading forward to
// a near handle, reopening the scan at a far one, and, with handle ranges,
// skipping the ranges below the target; and an intersection reading a common
// term's postings only around the rows a rare term proposes.
func TestTiKVPostingSeek(t *testing.T) {
	const rows = 5000
	for _, intHandle := range []bool{true, false} {
		handleOf := func(i int) kv.Handle {
			if intHandle {
				return kv.IntHandle(i)
			}
			encoded, err := codec.EncodeKey(nil, nil, types.NewStringDatum("tenant"), types.NewIntDatum(int64(i)))
			require.NoError(t, err)
			h, err := kv.NewCommonHandle(encoded)
			require.NoError(t, err)
			return h
		}
		s := &memPostingSnapshot{values: make(map[string][]byte)}
		var rare []kv.Handle
		for i := 1; i <= rows; i++ {
			putPosting(t, s, "common", handleOf(i), 0)
			if i%1000 == 7 {
				putPosting(t, s, "rare", handleOf(i), 1)
				rare = append(rare, handleOf(i))
			}
		}

		src := newTestPostingSource(s)
		require.Equal(t, rare, searchPostings(t, src, "+rare +common"))
		require.Less(t, src.stats.entries, int64(len(rare)*(seekReadLimit+2)+len(rare)+1), "entries read")
		require.Equal(t, rare, searchPostings(t, src, `+"common rare"`))
		require.Empty(t, searchPostings(t, src, `+"rare common"`))
		require.Empty(t, searchPostings(t, src, "+rare -common"))

		// A near target is read forward to; a far one reopens the scan.
		src = newTestPostingSource(s)
		cursor, err := src.Term("common")
		require.NoError(t, err)
		seeker := cursor.(fulltext.PostingSeeker)
		posting, ok, err := seeker.Seek(handleOf(100))
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, handleOf(100), posting.Handle)
		require.Equal(t, []int{0}, posting.Positions)
		require.Equal(t, int64(1), src.stats.scans)
		posting, ok, err = seeker.Seek(handleOf(4000))
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, handleOf(4000), posting.Handle)
		require.Equal(t, int64(2), src.stats.scans)
		posting, ok, err = cursor.Next()
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, handleOf(4001), posting.Handle)
		// A target at or behind the cursor returns its next posting.
		posting, ok, err = seeker.Seek(handleOf(1))
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, handleOf(4002), posting.Handle)
		_, ok, err = seeker.Seek(handleOf(rows + 1))
		require.NoError(t, err)
		require.False(t, ok)
		require.NoError(t, cursor.Close())

		// Within handle ranges a seek skips the ranges below its target.
		if intHandle {
			src = newTestPostingSource(s)
			for _, r := range [][2]int64{{100, 200}, {3000, 3100}} {
				low, err := codec.EncodeKey(nil, nil, types.NewIntDatum(r[0]))
				require.NoError(t, err)
				high, err := codec.EncodeKey(nil, nil, types.NewIntDatum(r[1]))
				require.NoError(t, err)
				src.handleRanges = append(src.handleRanges, fullTextHandleRange{low: low, high: high})
			}
			cursor, err = src.Term("common")
			require.NoError(t, err)
			seeker = cursor.(fulltext.PostingSeeker)
			for _, c := range []struct{ target, want int64 }{{50, 100}, {150, 150}, {250, 3000}, {3100, 3100}} {
				posting, ok, err = seeker.Seek(kv.IntHandle(c.target))
				require.NoError(t, err)
				require.True(t, ok)
				require.Equal(t, kv.IntHandle(c.want), posting.Handle, "seek %d", c.target)
			}
			_, ok, err = seeker.Seek(kv.IntHandle(3050))
			require.NoError(t, err)
			require.False(t, ok)
			require.NoError(t, cursor.Close())
			require.Equal(t, []kv.Handle{kv.IntHandle(3007)}, searchPostings(t, src, "+rare +common"))
		}
	}
}

// TestTiKVPostingInterrupted covers a posting scan stopping on a kill, and on
// a runaway rule its reads broke, while it reads rather than once it ends.
func TestTiKVPostingInterrupted(t *testing.T) {
	s := &memPostingSnapshot{values: make(map[string][]byte)}
	for i := 1; i <= 5000; i++ {
		putPosting(t, s, "common", kv.IntHandle(i), 0)
	}
	src := newTestPostingSource(s)
	src.killer = &sqlkiller.SQLKiller{}
	cursor, err := src.Term("common")
	require.NoError(t, err)
	for range 10 {
		_, ok, err := cursor.Next()
		require.NoError(t, err)
		require.True(t, ok)
	}
	src.killer.SendKillSignal(sqlkiller.QueryInterrupted)
	read := 0
	for ; read <= postingInterruptCheckInterval; read++ {
		if _, _, err = cursor.Next(); err != nil {
			break
		}
	}
	require.Error(t, err)
	require.LessOrEqual(t, read, postingInterruptCheckInterval)
	require.NoError(t, cursor.Close())
	// A new scan is refused outright.
	_, err = src.Term("common")
	require.Error(t, err)

	src = newTestPostingSource(s)
	src.reads = &fullTextReadAccount{}
	runawayErr := errors.New("runaway")
	src.reads.runawayErr.Store(&runawayErr)
	query, err := fulltext.CompileBooleanQuery("+common", fulltext.AnalyzerConfigFromTiKVFullTextIndex(src.index.TiKVFullText))
	require.NoError(t, err)
	_, err = query.OpenPostings(src)
	require.ErrorIs(t, err, runawayErr)
}

// TestTiKVPostingPrefixMemory covers a prefix search charging the statement's
// memory tracker for the rows it collects, and releasing them when closed.
func TestTiKVPostingPrefixMemory(t *testing.T) {
	s := &memPostingSnapshot{values: make(map[string][]byte)}
	for i := 1; i <= 3000; i++ {
		putPosting(t, s, []string{"region", "regular", "remote"}[i%3], kv.IntHandle(i), 0)
	}
	src := newTestPostingSource(s)
	src.memTracker = memory.NewTracker(1, -1)
	query, err := fulltext.CompileBooleanQuery("re*", fulltext.AnalyzerConfigFromTiKVFullTextIndex(src.index.TiKVFullText))
	require.NoError(t, err)
	iter, err := query.OpenPostings(src)
	require.NoError(t, err)
	require.GreaterOrEqual(t, src.memTracker.BytesConsumed(), int64(3000*8))
	handle, ok, err := iter.Next()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, kv.IntHandle(1), handle)
	require.NoError(t, iter.Close())
	require.Zero(t, src.memTracker.BytesConsumed())
	require.Equal(t, int64(1), src.stats.scans)

	// A quota below what the prefix collects stops it.
	src = newTestPostingSource(s)
	src.memTracker = memory.NewTracker(1, 1024)
	src.memTracker.SetActionOnExceed(&memory.PanicOnExceed{})
	require.Panics(t, func() { _, _ = query.OpenPostings(src) })
}

// fakeRunawayChecker records what the posting reads report to a runaway
// checker, and breaks its rule past a number of keys.
type fakeRunawayChecker struct {
	before    int
	keys      int64
	limitKeys int64
}

func (*fakeRunawayChecker) BeforeExecutor() (string, error) { return "", nil }
func (c *fakeRunawayChecker) BeforeCopRequest(*tikvrpc.Request) error {
	c.before++
	return nil
}
func (c *fakeRunawayChecker) CheckThresholds(_ *clientutil.RUDetails, keys int64, err error) error {
	c.keys += keys
	if c.limitKeys > 0 && c.keys >= c.limitKeys {
		return errors.New("runaway: too many keys")
	}
	return err
}
func (*fakeRunawayChecker) ResetTotalProcessedKeys()            {}
func (*fakeRunawayChecker) CheckAction() rmpb.RunawayAction     { return rmpb.RunawayAction_NoneAction }
func (*fakeRunawayChecker) CheckRuleKillAction() (string, bool) { return "", false }

// TestFullTextReadAccount covers the interceptor that accounts posting scans
// to the statement: the RU the resource controller's calculator gives each
// scan, and the keys it read to the runaway checker, only while a reader is
// open and only for scans.
func TestFullTextReadAccount(t *testing.T) {
	runaway := &fakeRunawayChecker{limitKeys: 5}
	cfg := rmclient.DefaultRUConfig()
	a := &fullTextReadAccount{calculator: &rmclient.KVCalculator{RUConfig: cfg}, runaway: runaway}
	scanResp := &kvrpcpb.ScanResponse{Pairs: []*kvrpcpb.KvPair{{Key: []byte("k1"), Value: bytes.Repeat([]byte("v"), 100)}, {Key: []byte("k2"), Value: []byte("v")}}}
	sent := 0
	send := a.intercept(func(string, *tikvrpc.Request) (*tikvrpc.Response, error) {
		sent++
		return &tikvrpc.Response{Resp: scanResp}, nil
	})
	scan := tikvrpc.NewRequest(tikvrpc.CmdScan, &kvrpcpb.ScanRequest{})

	// Not counted while no reader is open.
	ruDetails := clientutil.NewRUDetails()
	_, err := send("store", scan)
	require.NoError(t, err)
	require.Zero(t, runaway.before)

	a.open(ruDetails)
	_, err = send("store", scan)
	require.NoError(t, err)
	want := float64(cfg.ReadBaseCost) + float64(cfg.ReadPerBatchBaseCost)*0.7 + float64(cfg.ReadBytesCost)*float64(scanResp.Size())
	require.InDelta(t, want, ruDetails.RRU(), 1e-9)
	require.Zero(t, ruDetails.WRU())
	require.Equal(t, 1, runaway.before)
	require.Equal(t, int64(2), runaway.keys)
	require.NoError(t, a.interrupted())

	// Only scans are counted.
	_, err = send("store", tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{}))
	require.NoError(t, err)
	require.InDelta(t, want, ruDetails.RRU(), 1e-9)
	require.Equal(t, 1, runaway.before)

	// A broken runaway rule does not fail the RPC, which client-go would
	// retry, but is raised by the cursors.
	for range 2 {
		_, err = send("store", scan)
		require.NoError(t, err)
	}
	require.ErrorContains(t, a.interrupted(), "too many keys")

	a.close()
	_, err = send("store", scan)
	require.NoError(t, err)
	require.InDelta(t, 3*want, ruDetails.RRU(), 1e-9)
	require.Equal(t, 6, sent)

	// Without resource control no RU is recorded, as client-go records none.
	b := &fullTextReadAccount{}
	b.open(ruDetails)
	_, err = b.intercept(func(string, *tikvrpc.Request) (*tikvrpc.Response, error) {
		return &tikvrpc.Response{Resp: scanResp}, nil
	})("store", scan)
	require.NoError(t, err)
	require.InDelta(t, 3*want, ruDetails.RRU(), 1e-9)
}
