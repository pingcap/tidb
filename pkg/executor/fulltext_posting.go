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
	"fmt"
	"slices"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression/fulltext"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/resourcegroup"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/tikvrpc/interceptor"
	clientutil "github.com/tikv/client-go/v2/util"
	rmclient "github.com/tikv/pd/client/resource_group/controller"
)

// tikvPostingSource opens the posting lists of a FULLTEXT index built in TiKV.
// A term's posting list is the key range of that term's entries under the
// key-column values the scan is confined to, read from the statement's
// snapshot. Like a coprocessor index scan it does not see the transaction's
// own writes; the UnionScan the planner places above a reader of a table the
// transaction has changed merges those in, re-evaluating the MATCH on each
// changed row. That is also why the index need not rewrite untouched entries
// when another column of a row is updated.
type tikvPostingSource struct {
	snapshot        kv.Snapshot
	physicalTableID int64
	index           *model.IndexInfo
	// keyPrefix is the encoded values of the index's key columns, which
	// every entry of the index carries ahead of its term; empty when the
	// index has none.
	keyPrefix []byte
	// handleRanges confine the postings of each exact term to ranges of the
	// clustered handle, which every entry carries after its term; empty when
	// a term's postings are read whole. A prefix scan is never confined, see
	// Prefix.
	handleRanges []fullTextHandleRange
	// stats counts the scans opened and the entries read, for EXPLAIN
	// ANALYZE; nil when runtime statistics are not collected.
	stats *fullTextScanRuntimeStats
	// killer and reads let a long posting scan stop on KILL,
	// MAX_EXECUTION_TIME, the memory quota or a runaway rule while it
	// reads, not only once it hands its rows on; either may be nil.
	killer *sqlkiller.SQLKiller
	reads  *fullTextReadAccount
	// memTracker is charged for the rows a prefix collects; nil when
	// memory is not tracked.
	memTracker *memory.Tracker
}

// ConsumeMemory implements fulltext.PostingMemoryConsumer.
func (s *tikvPostingSource) ConsumeMemory(bytes int64) {
	if s.memTracker != nil {
		s.memTracker.Consume(bytes)
	}
}

// postingInterruptCheckInterval is how many entries a posting scan reads
// between checks for a kill. A check is an atomic load or two, and a TiKV scan
// batch holds 256 entries by default.
const postingInterruptCheckInterval = 256

// checkInterrupted reports a pending kill of the statement, or a runaway rule
// the posting reads broke.
func (s *tikvPostingSource) checkInterrupted() error {
	if s.killer != nil {
		if err := s.killer.HandleSignal(); err != nil {
			return err
		}
	}
	return s.reads.interrupted()
}

// fullTextScanRuntimeStats is what reading a FULLTEXT index built in TiKV
// costs: the posting entries read, which every posting list the search opens
// contributes in full whatever the size of the result, and the posting scans
// opened, each at least one kv_scan request. A partial worker owns it while it
// runs and registers it when done, so it needs no locking.
type fullTextScanRuntimeStats struct {
	entries int64
	scans   int64
}

// String implements execdetails.RuntimeStats.
func (s *fullTextScanRuntimeStats) String() string {
	return fmt.Sprintf("fulltext:{posting_entries:%d, posting_scans:%d}", s.entries, s.scans)
}

// Merge implements execdetails.RuntimeStats.
func (s *fullTextScanRuntimeStats) Merge(other execdetails.RuntimeStats) {
	o, ok := other.(*fullTextScanRuntimeStats)
	if !ok {
		return
	}
	s.entries += o.entries
	s.scans += o.scans
}

// Clone implements execdetails.RuntimeStats.
func (s *fullTextScanRuntimeStats) Clone() execdetails.RuntimeStats {
	cloned := *s
	return &cloned
}

// Tp implements execdetails.RuntimeStats.
func (*fullTextScanRuntimeStats) Tp() int {
	return execdetails.TpFullTextScanRuntimeStats
}

// fullTextHandleRange is a range of the clustered handle, with its bounds
// encoded as the handle is after the term in an index key. Whether a bound
// is inclusive is applied to the whole key of a term, of which the bound is
// a suffix, since the next key after a suffix is not the suffix of the next
// key.
type fullTextHandleRange struct {
	low, high               []byte
	lowExclude, highExclude bool
}

// Term implements fulltext.PostingSource. With handle ranges the term's
// postings are read within each range in turn: the ranges are sorted and
// disjoint, and a term's entries are stored in handle order, so the result
// is in ascending handle order like the whole posting list.
func (s *tikvPostingSource) Term(term string) (fulltext.PostingCursor, error) {
	start, err := s.termKey(term)
	if err != nil {
		return nil, err
	}
	if len(s.handleRanges) == 0 {
		return s.openTerm(start, start, start.PrefixNext())
	}
	return &rangedPostingCursor{source: s, termKey: start}, nil
}

// Prefix implements fulltext.PostingSource. Terms are stored in byte order
// under the key-column values, so every term with the prefix sits in one
// contiguous range starting at the prefix itself; the cursor stops at the
// first term outside it, and the range ends with the key-column values.
// The handle ranges do not apply: within the range the entries are grouped
// by term, and each term's handles would need a seek of their own. For the
// same reason the cursor cannot seek, and is not a fulltext.PostingSeeker.
func (s *tikvPostingSource) Prefix(prefix string) (fulltext.PostingCursor, error) {
	start, err := s.termKey(prefix)
	if err != nil {
		return nil, err
	}
	end := tablecodec.EncodeIndexSeekKey(s.physicalTableID, s.index.ID, s.keyPrefix).PrefixNext()
	cursor, err := s.open(start, end)
	if err != nil {
		return nil, err
	}
	cursor.prefix = prefix
	return prefixPostingCursor{cursor: cursor}, nil
}

// termKey is the key prefix shared by every entry of a term: the index
// prefix, the key-column values, and the term, encoded exactly as the write
// path encodes them.
func (s *tikvPostingSource) termKey(term string) (kv.Key, error) {
	encoded, err := codec.EncodeKey(time.UTC, append([]byte(nil), s.keyPrefix...), types.NewBytesDatum([]byte(term)))
	if err != nil {
		return nil, errors.Trace(err)
	}
	return tablecodec.EncodeIndexSeekKey(s.physicalTableID, s.index.ID, encoded), nil
}

// openTerm opens the postings of the term whose entries start with termKey,
// in [start, end), with a cursor that can seek within that range.
func (s *tikvPostingSource) openTerm(termKey, start, end kv.Key) (*tikvPostingCursor, error) {
	cursor, err := s.open(start, end)
	if err != nil {
		return nil, err
	}
	cursor.termKey, cursor.start, cursor.end = termKey, start, end
	return cursor, nil
}

func (s *tikvPostingSource) open(start, end kv.Key) (*tikvPostingCursor, error) {
	iter, err := s.iter(start, end)
	if err != nil {
		return nil, err
	}
	return &tikvPostingCursor{source: s, iter: iter}, nil
}

func (s *tikvPostingSource) iter(start, end kv.Key) (kv.Iterator, error) {
	if err := s.checkInterrupted(); err != nil {
		return nil, err
	}
	iter, err := s.snapshot.Iter(start, end)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if s.stats != nil {
		s.stats.scans++
	}
	return iter, nil
}

// appendHandleKey appends handle encoded the way an entry of a non-unique
// index ends with it, see tablecodec.GenIndexKey. FULLTEXT indexes are never
// global, so the handle carries no partition.
func appendHandleKey(key []byte, handle kv.Handle) []byte {
	if handle.IsInt() {
		key = append(key, codec.IntHandleFlag)
		return codec.EncodeInt(key, handle.IntValue())
	}
	return append(key, handle.Encoded()...)
}

// seekReadLimit is how many entries a seek reads forward before it reopens
// the scan at its target instead. A scan fetches entries from TiKV in batches
// of 256 by default, so reading forward costs at most about one batch more
// than reopening would, while reopening skips everything in between; a seek
// into a dense posting list therefore costs about what a scan of the rows
// near the target does, not of the whole gap.
const seekReadLimit = 256

type tikvPostingCursor struct {
	source *tikvPostingSource
	iter   kv.Iterator
	// prefix, when set, ends the cursor at the first term without it.
	prefix string
	// termKey, start and end are the term's key prefix and the range the
	// cursor reads; a seek reopens the scan within them. termKey is nil for
	// a prefix cursor, which does not seek.
	termKey, start, end kv.Key
	done                bool
	read                int
}

// advance moves past the current entry, counting it and checking for a
// kill every postingInterruptCheckInterval entries.
func (c *tikvPostingCursor) advance() error {
	if err := c.iter.Next(); err != nil {
		return errors.Trace(err)
	}
	if c.source.stats != nil {
		c.source.stats.entries++
	}
	c.read++
	if c.read%postingInterruptCheckInterval == 0 {
		return c.source.checkInterrupted()
	}
	return nil
}

// current returns the handle of the entry the iterator is on, skipping
// entries with no value, and false at the end of the cursor's range.
func (c *tikvPostingCursor) current() (kv.Handle, bool, error) {
	for !c.done && c.iter.Valid() {
		key, value := c.iter.Key(), c.iter.Value()
		if len(value) == 0 {
			if err := c.advance(); err != nil {
				return nil, false, err
			}
			continue
		}
		if c.prefix != "" {
			_, term, err := tables.DecodeTiKVFullTextIndexKey(c.source.index, key)
			if err != nil {
				return nil, false, errors.Trace(err)
			}
			if !bytes.HasPrefix(term, []byte(c.prefix)) {
				c.done = true
				break
			}
		}
		handle, err := tablecodec.DecodeIndexHandle(key, value, len(c.source.index.Columns))
		if err != nil {
			return nil, false, errors.Trace(err)
		}
		return handle, true, nil
	}
	return nil, false, nil
}

// Next implements fulltext.PostingCursor.
func (c *tikvPostingCursor) Next() (fulltext.Posting, bool, error) {
	handle, ok, err := c.current()
	if err != nil || !ok {
		return fulltext.Posting{}, false, err
	}
	positions, err := tablecodec.DecodeTiKVFullTextIndexValue(c.iter.Value())
	if err != nil {
		return fulltext.Posting{}, false, errors.Trace(err)
	}
	if err := c.advance(); err != nil {
		return fulltext.Posting{}, false, err
	}
	return fulltext.Posting{Handle: handle, Positions: positions}, true, nil
}

// Seek implements fulltext.PostingSeeker. It reads forward while the target
// is near, decoding only the handles, and past seekReadLimit entries reopens
// the scan at the target's key within the cursor's range.
func (c *tikvPostingCursor) Seek(target kv.Handle) (fulltext.Posting, bool, error) {
	for steps := 0; ; steps++ {
		handle, ok, err := c.current()
		if err != nil || !ok {
			return fulltext.Posting{}, false, err
		}
		if handle.Compare(target) >= 0 {
			return c.Next()
		}
		if steps < seekReadLimit || c.termKey == nil {
			if err := c.advance(); err != nil {
				return fulltext.Posting{}, false, err
			}
			continue
		}
		start := kv.Key(appendHandleKey(slices.Clone(c.termKey), target))
		if start.Cmp(c.start) < 0 {
			start = c.start
		}
		if start.Cmp(c.end) >= 0 {
			c.done = true
			return fulltext.Posting{}, false, nil
		}
		iter, err := c.source.iter(start, c.end)
		if err != nil {
			return fulltext.Posting{}, false, err
		}
		c.iter.Close()
		c.iter, steps = iter, 0
	}
}

// Close implements fulltext.PostingCursor.
func (c *tikvPostingCursor) Close() error {
	c.iter.Close()
	return nil
}

// prefixPostingCursor hides Seek from a prefix cursor, whose entries are
// grouped by term and so not in one handle order.
type prefixPostingCursor struct {
	cursor *tikvPostingCursor
}

// Next implements fulltext.PostingCursor.
func (c prefixPostingCursor) Next() (fulltext.Posting, bool, error) { return c.cursor.Next() }

// Close implements fulltext.PostingCursor.
func (c prefixPostingCursor) Close() error { return c.cursor.Close() }

// rangedPostingCursor reads one term's postings within each handle range of
// the source in turn, opening a range only when the one before it is
// exhausted, and a seek skips the ranges wholly below its target.
type rangedPostingCursor struct {
	source  *tikvPostingSource
	termKey kv.Key
	next    int
	current *tikvPostingCursor
}

// openNext opens the next range that ends past from, a key under the term,
// starting at from when it falls inside the range; from is nil to open the
// next range whole. It reports false when no range is left.
func (c *rangedPostingCursor) openNext(from kv.Key) (bool, error) {
	for c.next < len(c.source.handleRanges) {
		ran := c.source.handleRanges[c.next]
		c.next++
		start := kv.Key(slices.Concat(c.termKey, ran.low))
		if ran.lowExclude {
			start = start.PrefixNext()
		}
		end := kv.Key(slices.Concat(c.termKey, ran.high))
		if !ran.highExclude {
			end = end.PrefixNext()
		}
		if from != nil && from.Cmp(start) > 0 {
			start = from
		}
		if start.Cmp(end) >= 0 {
			continue
		}
		cursor, err := c.source.openTerm(c.termKey, start, end)
		if err != nil {
			return false, err
		}
		c.current = cursor
		return true, nil
	}
	return false, nil
}

func (c *rangedPostingCursor) closeCurrent() error {
	err := c.current.Close()
	c.current = nil
	return err
}

// Next implements fulltext.PostingCursor.
func (c *rangedPostingCursor) Next() (fulltext.Posting, bool, error) {
	for {
		if c.current == nil {
			ok, err := c.openNext(nil)
			if err != nil || !ok {
				return fulltext.Posting{}, false, err
			}
		}
		posting, ok, err := c.current.Next()
		if err != nil || ok {
			return posting, ok, err
		}
		if err := c.closeCurrent(); err != nil {
			return fulltext.Posting{}, false, err
		}
	}
}

// Seek implements fulltext.PostingSeeker.
func (c *rangedPostingCursor) Seek(target kv.Handle) (fulltext.Posting, bool, error) {
	for {
		if c.current == nil {
			ok, err := c.openNext(appendHandleKey(slices.Clone(c.termKey), target))
			if err != nil || !ok {
				return fulltext.Posting{}, false, err
			}
		}
		posting, ok, err := c.current.Seek(target)
		if err != nil || ok {
			return posting, ok, err
		}
		if err := c.closeCurrent(); err != nil {
			return fulltext.Posting{}, false, err
		}
	}
}

// Close implements fulltext.PostingCursor.
func (c *rangedPostingCursor) Close() error {
	if c.current == nil {
		return nil
	}
	return c.closeCurrent()
}

// fullTextReadAccount accounts the posting reads of a statement to it. The
// reads are issued through kv.Snapshot.Iter, whose RPCs carry no statement
// context, so the RU they cost reaches the resource group but not the
// statement, and runaway rules never see them. An RPC interceptor on the
// snapshot adds them back: the RU of each scan to the statement's RU details,
// computed with the resource controller's own calculator, and the scan to
// the runaway checker.
//
// The snapshot may be the transaction's own, shared with later statements and
// with other readers of this one, and an RPC does not say which reader sent
// it. So the interceptor is installed under one name, which replaces the one
// before it rather than adding a second, and it counts only while a posting
// worker of its reader runs. Two FULLTEXT readers of one statement running at
// once on one snapshot may thus leave some reads of the first uncounted once
// the second ends, but no read is ever counted twice, or against a statement
// that has finished.
type fullTextReadAccount struct {
	active    atomic.Int32
	ruDetails atomic.Pointer[clientutil.RUDetails]
	// calculator is nil when resource control is off, as client-go then
	// records no RU for any request.
	calculator *rmclient.KVCalculator
	runaway    resourcegroup.RunawayChecker
	// runawayErr holds the error of a runaway rule a scan broke, raised by
	// the posting cursors rather than by the RPC, which client-go would
	// retry.
	runawayErr atomic.Pointer[error]
}

const fullTextReadAccountInterceptor = "fulltext-posting-reads"

// newFullTextReadAccount builds the account of a reader in sctx's statement.
func newFullTextReadAccount(sctx sessionctx.Context) *fullTextReadAccount {
	sessVars := sctx.GetSessionVars()
	a := &fullTextReadAccount{runaway: sessVars.StmtCtx.RunawayChecker}
	// Like client-go, record no RU when resource control is off or for
	// internal SQL.
	if vardef.EnableResourceControl.Load() && !sessVars.InRestrictedSQL {
		if do := domain.GetDomain(sctx); do != nil {
			if controller := do.ResourceGroupsController(); controller != nil {
				a.calculator = &rmclient.KVCalculator{RUConfig: controller.GetConfig()}
			}
		}
	}
	return a
}

// attach installs the account's interceptor on snapshot.
func (a *fullTextReadAccount) attach(snapshot kv.Snapshot) {
	snapshot.SetOption(kv.RPCInterceptor, interceptor.NewRPCInterceptor(fullTextReadAccountInterceptor, a.intercept))
}

// open starts counting for a reader, into the statement's ruDetails, which
// may be nil; close stops it.
func (a *fullTextReadAccount) open(ruDetails *clientutil.RUDetails) {
	if a == nil {
		return
	}
	if ruDetails != nil {
		a.ruDetails.Store(ruDetails)
	}
	a.active.Add(1)
}

func (a *fullTextReadAccount) close() {
	if a != nil {
		a.active.Add(-1)
	}
}

func (a *fullTextReadAccount) interrupted() error {
	if a == nil {
		return nil
	}
	if err := a.runawayErr.Load(); err != nil {
		return *err
	}
	return nil
}

func (a *fullTextReadAccount) intercept(next interceptor.RPCInterceptorFunc) interceptor.RPCInterceptorFunc {
	return func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
		if req.Type != tikvrpc.CmdScan || a.active.Load() <= 0 {
			return next(target, req)
		}
		if a.runaway != nil {
			if err := a.runaway.BeforeCopRequest(req); err != nil {
				a.runawayErr.CompareAndSwap(nil, &err)
			}
		}
		resp, err := next(target, req)
		ruDetails := a.ruDetails.Load()
		var processedKeys int64
		var scan *kvrpcpb.ScanResponse
		if resp != nil {
			scan, _ = resp.Resp.(*kvrpcpb.ScanResponse)
		}
		if scan != nil {
			processedKeys = int64(len(scan.Pairs))
			if a.calculator != nil && ruDetails != nil {
				consumption := &rmpb.Consumption{}
				info := scanRequestInfo{size: uint64(req.GetSize()), storeID: req.Context.GetPeer().GetStoreId()}
				a.calculator.BeforeKVRequest(consumption, info)
				a.calculator.AfterKVRequest(consumption, info, scanResponseInfo{readBytes: uint64(scan.Size()), size: uint64(resp.GetSize())})
				ruDetails.Update(consumption, 0)
			}
		}
		if a.runaway != nil {
			if err := a.runaway.CheckThresholds(ruDetails, processedKeys, nil); err != nil {
				a.runawayErr.CompareAndSwap(nil, &err)
			}
		}
		return resp, err
	}
}

// scanRequestInfo and scanResponseInfo describe a scan to the resource
// controller's calculator the way client-go does for the requests it
// accounts: a read whose bytes are its response's size, with no KV CPU time
// reported.
type scanRequestInfo struct {
	size    uint64
	storeID uint64
}

func (scanRequestInfo) IsWrite() bool         { return false }
func (scanRequestInfo) WriteBytes() uint64    { return 0 }
func (scanRequestInfo) ReplicaNumber() int64  { return 0 }
func (r scanRequestInfo) StoreID() uint64     { return r.storeID }
func (r scanRequestInfo) RequestSize() uint64 { return r.size }
func (scanRequestInfo) AccessLocationType() rmclient.AccessLocationType {
	return rmclient.AccessUnknown
}

type scanResponseInfo struct {
	readBytes, size uint64
}

func (r scanResponseInfo) ReadBytes() uint64    { return r.readBytes }
func (scanResponseInfo) KVCPU() time.Duration   { return 0 }
func (scanResponseInfo) Succeed() bool          { return true }
func (r scanResponseInfo) ResponseSize() uint64 { return r.size }
