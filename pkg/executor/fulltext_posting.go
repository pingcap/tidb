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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression/fulltext"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
)

// tikvPostingSource opens the posting lists of a FULLTEXT index built in TiKV.
// A term's posting list is the key range of that term's entries, read from
// the statement's snapshot. Like a coprocessor index scan it does not see the
// transaction's own writes; the UnionScan the planner places above a reader
// of a table the transaction has changed merges those in, re-evaluating the
// MATCH on each changed row. That is also why the index need not rewrite
// untouched entries when another column of a row is updated.
type tikvPostingSource struct {
	snapshot        kv.Snapshot
	physicalTableID int64
	indexID         int64
}

// Term implements fulltext.PostingSource.
func (s *tikvPostingSource) Term(term string) (fulltext.PostingCursor, error) {
	start, err := s.termKey(term)
	if err != nil {
		return nil, err
	}
	return s.open(start, start.PrefixNext(), "")
}

// Prefix implements fulltext.PostingSource. Terms are stored in byte order,
// so every term with the prefix sits in one contiguous range starting at the
// prefix itself; the cursor stops at the first term outside it.
func (s *tikvPostingSource) Prefix(prefix string) (fulltext.PostingCursor, error) {
	start, err := s.termKey(prefix)
	if err != nil {
		return nil, err
	}
	end := tablecodec.EncodeTableIndexPrefix(s.physicalTableID, s.indexID).PrefixNext()
	return s.open(start, end, prefix)
}

// termKey is the key prefix shared by every entry of a term: the index prefix
// followed by the term encoded exactly as the write path encodes it.
func (s *tikvPostingSource) termKey(term string) (kv.Key, error) {
	encoded, err := codec.EncodeKey(time.UTC, nil, types.NewBytesDatum([]byte(term)))
	if err != nil {
		return nil, errors.Trace(err)
	}
	return tablecodec.EncodeIndexSeekKey(s.physicalTableID, s.indexID, encoded), nil
}

func (s *tikvPostingSource) open(start, end kv.Key, prefix string) (fulltext.PostingCursor, error) {
	iter, err := s.snapshot.Iter(start, end)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &tikvPostingCursor{iter: iter, prefix: prefix}, nil
}

type tikvPostingCursor struct {
	iter   kv.Iterator
	prefix string
	done   bool
}

// Next implements fulltext.PostingCursor.
func (c *tikvPostingCursor) Next() (fulltext.Posting, bool, error) {
	for !c.done && c.iter.Valid() {
		key, value := c.iter.Key(), c.iter.Value()
		if err := c.iter.Next(); err != nil {
			return fulltext.Posting{}, false, errors.Trace(err)
		}
		if len(value) == 0 {
			continue
		}
		if c.prefix != "" {
			term, err := tables.DecodeTiKVFullTextIndexKey(key)
			if err != nil {
				return fulltext.Posting{}, false, errors.Trace(err)
			}
			if !bytes.HasPrefix(term, []byte(c.prefix)) {
				c.done = true
				break
			}
		}
		handle, err := tablecodec.DecodeIndexHandle(key, value, 1)
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
