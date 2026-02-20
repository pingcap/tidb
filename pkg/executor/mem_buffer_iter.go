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

package executor

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx"
	transaction "github.com/pingcap/tidb/pkg/store/driver/txn"
)

// txnMemBufferIter implements a kv.Iterator, it is an iterator that combines the membuffer data and snapshot data.
type txnMemBufferIter struct {
	sctx       sessionctx.Context
	kvRanges   []kv.KeyRange
	cacheTable kv.MemBuffer
	txn        kv.Transaction
	idx        int
	curr       kv.Iterator
	reverse    bool
	err        error
}

func newTxnMemBufferIter(sctx sessionctx.Context, cacheTable kv.MemBuffer, kvRanges []kv.KeyRange, reverse bool) (*txnMemBufferIter, error) {
	txn, err := sctx.Txn(true)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &txnMemBufferIter{
		sctx:       sctx,
		txn:        txn,
		kvRanges:   kvRanges,
		cacheTable: cacheTable,
		reverse:    reverse,
	}, nil
}

func (iter *txnMemBufferIter) Valid() bool {
	if iter.curr != nil {
		if iter.curr.Valid() {
			return true
		}
		iter.idx++
	}
	for iter.idx < len(iter.kvRanges) {
		rg := iter.kvRanges[iter.idx]
		var tmp kv.Iterator
		if !iter.reverse {
			tmp = iter.txn.GetMemBuffer().SnapshotIter(rg.StartKey, rg.EndKey)
		} else {
			tmp = iter.txn.GetMemBuffer().SnapshotIterReverse(rg.EndKey, rg.StartKey)
		}
		snapCacheIter, err := getSnapIter(iter.sctx, iter.cacheTable, rg, iter.reverse)
		if err != nil {
			iter.err = errors.Trace(err)
			return true
		}
		if snapCacheIter != nil {
			tmp, err = transaction.NewUnionIter(tmp, snapCacheIter, iter.reverse)
			if err != nil {
				iter.err = errors.Trace(err)
				return true
			}
		}
		iter.curr = tmp
		if iter.curr.Valid() {
			return true
		}
		iter.idx++
	}
	return false
}

func (iter *txnMemBufferIter) Next() error {
	if iter.err != nil {
		return errors.Trace(iter.err)
	}
	if iter.curr != nil {
		if iter.curr.Valid() {
			return iter.curr.Next()
		}
	}
	return nil
}

func (iter *txnMemBufferIter) Key() kv.Key {
	return iter.curr.Key()
}

func (iter *txnMemBufferIter) Value() []byte {
	return iter.curr.Value()
}

func (iter *txnMemBufferIter) Close() {
	if iter.curr != nil {
		iter.curr.Close()
	}
}
type processKVFunc func(key, value []byte) error

func iterTxnMemBuffer(ctx sessionctx.Context, cacheTable kv.MemBuffer, kvRanges []kv.KeyRange, reverse bool, fn processKVFunc) error {
	txn, err := ctx.Txn(true)
	if err != nil {
		return err
	}

	for _, rg := range kvRanges {
		var iter kv.Iterator
		if !reverse {
			iter = txn.GetMemBuffer().SnapshotIter(rg.StartKey, rg.EndKey)
		} else {
			iter = txn.GetMemBuffer().SnapshotIterReverse(rg.EndKey, rg.StartKey)
		}
		snapCacheIter, err := getSnapIter(ctx, cacheTable, rg, reverse)
		if err != nil {
			return err
		}
		if snapCacheIter != nil {
			iter, err = transaction.NewUnionIter(iter, snapCacheIter, reverse)
			if err != nil {
				return err
			}
		}
		for ; iter.Valid(); err = iter.Next() {
			if err != nil {
				return err
			}
			// check whether the key was been deleted.
			if len(iter.Value()) == 0 {
				continue
			}
			err = fn(iter.Key(), iter.Value())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func getSnapIter(ctx sessionctx.Context, cacheTable kv.MemBuffer, rg kv.KeyRange, reverse bool) (snapCacheIter kv.Iterator, err error) {
	var cacheIter, snapIter kv.Iterator
	tempTableData := ctx.GetSessionVars().TemporaryTableData
	if tempTableData != nil {
		if !reverse {
			snapIter, err = tempTableData.Iter(rg.StartKey, rg.EndKey)
		} else {
			snapIter, err = tempTableData.IterReverse(rg.EndKey, rg.StartKey)
		}
		if err != nil {
			return nil, err
		}
		snapCacheIter = snapIter
	} else if cacheTable != nil {
		if !reverse {
			cacheIter, err = cacheTable.Iter(rg.StartKey, rg.EndKey)
		} else {
			cacheIter, err = cacheTable.IterReverse(rg.EndKey, rg.StartKey)
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
		snapCacheIter = cacheIter
	}
	return snapCacheIter, nil
}
