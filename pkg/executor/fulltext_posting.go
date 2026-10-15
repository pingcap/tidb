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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression/fulltext"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/execdetails"
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
		return s.open(start, start.PrefixNext(), "")
	}
	return &rangedPostingCursor{source: s, termKey: start}, nil
}

// Prefix implements fulltext.PostingSource. Terms are stored in byte order
// under the key-column values, so every term with the prefix sits in one
// contiguous range starting at the prefix itself; the cursor stops at the
// first term outside it, and the range ends with the key-column values.
// The handle ranges do not apply: within the range the entries are grouped
// by term, and each term's handles would need a seek of their own.
func (s *tikvPostingSource) Prefix(prefix string) (fulltext.PostingCursor, error) {
	start, err := s.termKey(prefix)
	if err != nil {
		return nil, err
	}
	end := tablecodec.EncodeIndexSeekKey(s.physicalTableID, s.index.ID, s.keyPrefix).PrefixNext()
	return s.open(start, end, prefix)
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

func (s *tikvPostingSource) open(start, end kv.Key, prefix string) (fulltext.PostingCursor, error) {
	iter, err := s.snapshot.Iter(start, end)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if s.stats != nil {
		s.stats.scans++
	}
	return &tikvPostingCursor{iter: iter, index: s.index, prefix: prefix, stats: s.stats}, nil
}

type tikvPostingCursor struct {
	iter   kv.Iterator
	index  *model.IndexInfo
	prefix string
	done   bool
	stats  *fullTextScanRuntimeStats
}

// Next implements fulltext.PostingCursor.
func (c *tikvPostingCursor) Next() (fulltext.Posting, bool, error) {
	for !c.done && c.iter.Valid() {
		key, value := c.iter.Key(), c.iter.Value()
		if err := c.iter.Next(); err != nil {
			return fulltext.Posting{}, false, errors.Trace(err)
		}
		if c.stats != nil {
			c.stats.entries++
		}
		if len(value) == 0 {
			continue
		}
		if c.prefix != "" {
			_, term, err := tables.DecodeTiKVFullTextIndexKey(c.index, key)
			if err != nil {
				return fulltext.Posting{}, false, errors.Trace(err)
			}
			if !bytes.HasPrefix(term, []byte(c.prefix)) {
				c.done = true
				break
			}
		}
		handle, err := tablecodec.DecodeIndexHandle(key, value, len(c.index.Columns))
		if err != nil {
			return fulltext.Posting{}, false, errors.Trace(err)
		}
		positions, err := tablecodec.DecodeTiKVFullTextIndexValue(value)
		if err != nil {
			return fulltext.Posting{}, false, errors.Trace(err)
		}
		return fulltext.Posting{Handle: handle, Positions: positions}, true, nil
	}
	return fulltext.Posting{}, false, nil
}

// Close implements fulltext.PostingCursor.
func (c *tikvPostingCursor) Close() error {
	c.iter.Close()
	return nil
}

// rangedPostingCursor reads one term's postings within each handle range of
// the source in turn, opening a range only when the one before it is
// exhausted.
type rangedPostingCursor struct {
	source  *tikvPostingSource
	termKey kv.Key
	next    int
	current fulltext.PostingCursor
}

// Next implements fulltext.PostingCursor.
func (c *rangedPostingCursor) Next() (fulltext.Posting, bool, error) {
	for {
		if c.current == nil {
			if c.next >= len(c.source.handleRanges) {
				return fulltext.Posting{}, false, nil
			}
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
			cursor, err := c.source.open(start, end, "")
			if err != nil {
				return fulltext.Posting{}, false, err
			}
			c.current = cursor
		}
		posting, ok, err := c.current.Next()
		if err != nil {
			return fulltext.Posting{}, false, err
		}
		if ok {
			return posting, true, nil
		}
		if err := c.current.Close(); err != nil {
			return fulltext.Posting{}, false, err
		}
		c.current = nil
	}
}

// Close implements fulltext.PostingCursor.
func (c *rangedPostingCursor) Close() error {
	if c.current == nil {
		return nil
	}
	err := c.current.Close()
	c.current = nil
	return err
}
