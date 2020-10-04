// Copyright 2020 PingCAP, Inc.
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

package tikv

import (
	"bytes"
	"context"
	"math"
	"sort"
	"sync/atomic"

	"github.com/pingcap/errors"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

func (c *twoPhaseCommitter) initCommitterStates(ctx context.Context) error {
	txn := c.txn
	c.isPessimistic = txn.IsPessimistic()
	bo := NewBackofferWithVars(ctx, PrewriteMaxBackoff, c.txn.vars)

	// Sanity check for startTS.
	if txn.StartTS() == math.MaxUint64 {
		err := errors.Errorf("try to commit with invalid txnStartTS: %d", txn.StartTS())
		logutil.BgLogger().Error("commit failed",
			zap.Uint64("conn", c.connID),
			zap.Error(err))
		return errors.Trace(err)
	}

	var (
		firstKey      []byte
		regionSizeCal regionTxnSizeCalculator
		txnDetailsCal txnDetailsCalculator
		preSplitCal   preSplitCalculator
		it            = c.mapWithRegion(bo, committerTxnMutations{c, true}.Iter(nil, nil))
	)
	for {
		m, err := it.Next()
		if err != nil {
			return errors.Trace(err)
		}
		if m.key == nil {
			break
		}
		if firstKey == nil {
			firstKey = m.key
		}

		if len(c.primaryKey) == 0 && m.op != pb.Op_CheckNotExists {
			c.primaryKey = m.key
		}

		regionSizeCal.Process(m)
		txnDetailsCal.Process(m)
		preSplitCal.Process(m)
	}
	if len(c.primaryKey) == 0 {
		c.primaryKey = firstKey
	}

	if err := txnDetailsCal.Finish(c); err != nil {
		return errors.Trace(err)
	}
	regionSizeCal.Finish(c)
	splitKeys, splitRegions := preSplitCal.Finish()

	if c.txnSize == 0 {
		return nil
	}

	if c.trySplitRegions(splitKeys, splitRegions) {
		if err := c.reCalRegionTxnSize(bo); err != nil {
			return err
		}
	}

	mutationsIt := it.src.(*txnMutationsIter)
	c.prewriteOnlyKeys = mutationsIt.prewriteOnlyKeys
	c.ignoredKeys = mutationsIt.ignoredKeys

	c.lockTTL = txnLockTTL(txn.startTime, txn.Size())
	c.priority = getTxnPriority(txn)
	c.syncLog = getTxnSyncLog(txn)
	return nil
}

type mutation struct {
	key               []byte
	value             []byte
	isPessimisticLock bool
	op                pb.Op
}

type mutationWithRegion struct {
	mutation
	region RegionVerID
}

type mutations interface {
	Iter(start, end []byte) mutationsIter
	Len() int
}

type mutationsIter interface {
	Next() mutation
	WithFilter(func([]byte) bool)
}

type regionTxnSizeCalculator struct {
	result     map[uint64]int
	prevRegion RegionVerID
	numKeys    int
}

func (c *regionTxnSizeCalculator) Process(m mutationWithRegion) {
	if c.result == nil {
		c.result = make(map[uint64]int)
	}
	if m.region.id != c.prevRegion.id {
		if c.prevRegion.id != 0 {
			c.result[c.prevRegion.id] = c.numKeys
		}
		c.prevRegion = m.region
		c.numKeys = 0
	}
	c.numKeys++
}

func (c *regionTxnSizeCalculator) Finish(committer *twoPhaseCommitter) {
	if c.prevRegion.id != 0 {
		c.result[c.prevRegion.id] = c.numKeys
	}
	committer.regionTxnSize = c.result
	committer.getDetail().PrewriteRegionNum = int32(len(c.result))
}

type txnDetailsCalculator struct {
	writeKeys int
	writeSize int
	putCnt    int
	delCnt    int
	lockCnt   int
	checkCnt  int
}

func (c *txnDetailsCalculator) Process(m mutationWithRegion) {
	c.writeKeys++
	c.writeSize += len(m.key) + len(m.value)
	switch m.op {
	case pb.Op_CheckNotExists:
		c.checkCnt++
	case pb.Op_Del:
		c.delCnt++
	case pb.Op_Put, pb.Op_Insert:
		c.putCnt++
	case pb.Op_Lock:
		c.lockCnt++
	}
}

func (c *txnDetailsCalculator) Finish(committer *twoPhaseCommitter) error {
	if c.writeSize > int(kv.TxnTotalSizeLimit) {
		return kv.ErrTxnTooLarge.GenWithStackByArgs(c.writeSize)
	}

	const logEntryCount = 10000
	const logSize = 4 * 1024 * 1024 // 4MB
	if c.writeKeys > logEntryCount || c.writeSize > logSize {
		tableID := tablecodec.DecodeTableID(committer.primaryKey)
		logutil.BgLogger().Info("[BIG_TXN]",
			zap.Uint64("con", committer.connID),
			zap.Int64("table ID", tableID),
			zap.Int("size", c.writeSize),
			zap.Int("keys", c.writeKeys),
			zap.Int("puts", c.putCnt),
			zap.Int("dels", c.delCnt),
			zap.Int("locks", c.lockCnt),
			zap.Int("checks", c.checkCnt),
			zap.Uint64("txnStartTS", committer.txn.startTS))
	}

	committer.txnSize = c.writeSize
	details := committer.getDetail()
	details.WriteKeys = c.writeKeys
	details.WriteSize = c.writeSize

	metrics.TiKVTxnWriteKVCountHistogram.Observe(float64(c.writeKeys))
	metrics.TiKVTxnWriteSizeHistogram.Observe(float64(c.writeSize))

	return nil
}

type preSplitCalculator struct {
	limit        uint32
	size         int
	splitKeys    [][]byte
	splitRegions []RegionVerID
}

func (c *preSplitCalculator) Process(m mutationWithRegion) {
	if c.limit == 0 {
		c.limit = atomic.LoadUint32(&preSplitSizeThreshold)
	}
	c.size += len(m.key) + len(m.value)
	if uint32(c.size) >= c.limit {
		c.splitKeys = append(c.splitKeys, m.key)
		if len(c.splitRegions) == 0 || c.splitRegions[len(c.splitRegions)-1] != m.region {
			c.splitRegions = append(c.splitRegions, m.region)
		}
		c.size = 0
	}
}

func (c *preSplitCalculator) Finish() ([][]byte, []RegionVerID) {
	return c.splitKeys, c.splitRegions
}

func (c *twoPhaseCommitter) trySplitRegions(splitKeys [][]byte, splitRegions []RegionVerID) bool {
	if len(splitKeys) == 0 {
		return false
	}
	ctx := context.Background()
	regions := make([]uint64, len(splitRegions))
	for i := range regions {
		regions[i] = splitRegions[i].id
	}
	logutil.BgLogger().Info("2PC detect large amount of mutations on some region", zap.Uint64s("regions", regions))
	newRegions, err := c.store.SplitRegions(ctx, splitKeys, true, nil)
	if err != nil {
		logutil.BgLogger().Warn("2PC split regions failed", zap.Uint64s("regions", regions), zap.Error(err))
		return false
	}

	for _, regionID := range newRegions {
		err := c.store.WaitScatterRegionFinish(ctx, regionID, 0)
		if err != nil {
			logutil.BgLogger().Warn("2PC wait scatter region failed", zap.Uint64("regionID", regionID), zap.Error(err))
		}
	}

	for _, region := range splitRegions {
		c.store.regionCache.InvalidateCachedRegion(region)
	}
	return true
}

func (c *twoPhaseCommitter) reCalRegionTxnSize(bo *Backoffer) error {
	it := c.mapWithRegion(bo, committerTxnMutations{c, true}.Iter(nil, nil))
	var regionSizeCal regionTxnSizeCalculator
	for {
		m, err := it.Next()
		if err != nil {
			return errors.Trace(err)
		}
		if m.key == nil {
			break
		}
		regionSizeCal.Process(m)
	}
	regionSizeCal.Finish(c)
	return nil
}

type lockKeysMutations struct {
	keys [][]byte
}

func (m lockKeysMutations) Iter(start, end []byte) mutationsIter {
	it := &lockKeysMutationsIter{
		keys: m.keys,
		end:  end,
	}
	if len(start) != 0 {
		it.idx = sort.Search(len(m.keys), func(i int) bool {
			return bytes.Compare(m.keys[i], start) >= 0
		})
	}
	return it
}

func (m lockKeysMutations) Len() int {
	return len(m.keys)
}

type lockKeysMutationsIter struct {
	keys [][]byte
	idx  int
	end  []byte
	keep func([]byte) bool
}

func (it *lockKeysMutationsIter) Next() mutation {
	for ; it.idx < len(it.keys); it.idx++ {
		key := it.keys[it.idx]
		if it.keep != nil && !it.keep(key) {
			continue
		}

		if len(it.end) > 0 && bytes.Compare(key, it.end) >= 0 {
			return mutation{}
		}

		it.idx++
		return mutation{key: key}
	}
	return mutation{}
}

func (it *lockKeysMutationsIter) WithFilter(f func([]byte) bool) {
	it.keep = f
}

type staticMutations struct {
	mutations CommitterMutations
}

func (m staticMutations) Iter(start, end []byte) mutationsIter {
	it := &staticMutationsIter{
		mutations: m.mutations,
		end:       end,
	}
	if len(start) != 0 {
		it.idx = sort.Search(len(m.mutations.keys), func(i int) bool {
			return bytes.Compare(m.mutations.keys[i], start) >= 0
		})
	}
	return it
}

func (m staticMutations) Len() int {
	return m.mutations.len()
}

type staticMutationsIter struct {
	mutations CommitterMutations
	idx       int
	end       []byte
	keep      func([]byte) bool
}

func (it *staticMutationsIter) Next() mutation {
	for ; it.idx < len(it.mutations.keys); it.idx++ {
		m := mutation{
			key: it.mutations.keys[it.idx],
		}
		if len(it.end) > 0 && bytes.Compare(m.key, it.end) >= 0 {
			return mutation{}
		}
		if it.idx < len(it.mutations.values) {
			m.value = it.mutations.values[it.idx]
			m.op = it.mutations.ops[it.idx]
			m.isPessimisticLock = it.mutations.isPessimisticLock[it.idx]
		}
		if it.keep != nil && !it.keep(m.key) {
			continue
		}

		it.idx++
		return m
	}
	return mutation{}
}

func (it *staticMutationsIter) WithFilter(f func([]byte) bool) {
	it.keep = f
}

type committerTxnMutations struct {
	*twoPhaseCommitter
	isPrewrite bool
}

func (m committerTxnMutations) Iter(start, end []byte) mutationsIter {
	return m.newMutationsIter(m.txn.GetMemBuffer().IterWithFlags(start, end), m.isPrewrite)
}

func (m committerTxnMutations) Len() int {
	base := m.txn.Len() - m.ignoredKeys
	if m.isPrewrite {
		return base
	}
	return base - m.prewriteOnlyKeys
}

type txnMutationsIter struct {
	src           kv.MemBufferIterator
	isPrewrite    bool
	isPessimistic bool
	keep          func(key []byte) bool

	prewriteOnlyKeys int
	ignoredKeys      int
}

func (c *twoPhaseCommitter) newMutationsIter(src kv.MemBufferIterator, isPrewrite bool) *txnMutationsIter {
	return &txnMutationsIter{
		src:           src,
		isPrewrite:    isPrewrite,
		isPessimistic: c.txn.IsPessimistic(),
	}
}

func (it *txnMutationsIter) WithFilter(f func(key []byte) bool) {
	it.keep = f
}

func (it *txnMutationsIter) Next() (m mutation) {
	var err error
	for src := it.src; src.Valid(); err = src.Next() {
		_ = err
		m.key = src.Key()
		m.value = nil
		m.isPessimisticLock = false
		flags := src.Flags()

		if it.keep != nil && !it.keep(m.key) {
			continue
		}

		if flags.HasIgnoredIn2PC() {
			continue
		}

		if it.isPrewrite {
			if ignored := it.fillMutationForPrewrite(&m, src); ignored {
				// Because we will have discarded values in Commit phase,
				// we must record the `ignore` decision in MemBuffer at here.
				src.UpdateFlags(kv.SetIgnoredIn2PC)
				it.ignoredKeys++
				continue
			}
		} else {
			// For commit and cleanup, we only need keys.
			if flags.HasPrewriteOnly() {
				continue
			}
		}

		err = src.Next()
		_ = err
		return
	}
	return mutation{}
}

func (it *txnMutationsIter) fillMutationForPrewrite(m *mutation, src kv.MemBufferIterator) bool {
	flags := src.Flags()

	if !src.HasValue() {
		if flags.HasLocked() {
			m.op = pb.Op_Lock
		} else {
			// 2PC don't care other flags.
			return true
		}
	} else {
		m.value = src.Value()
		if kv.IsTombstone(m.value) {
			if !it.isPessimistic && flags.HasPresumeKeyNotExists() {
				// delete-your-writes keys in optimistic txn need check not exists in prewrite-phase
				// due to `Op_CheckNotExists` doesn't prewrite lock, so mark those keys should not be used in commit-phase.
				m.op = pb.Op_CheckNotExists
				src.UpdateFlags(kv.SetPrewriteOnly)
				it.prewriteOnlyKeys++
			} else {
				// normal delete keys in optimistic txn can be delete without not exists checking
				// delete-your-writes keys in pessimistic txn can ensure must be no exists so can directly delete them
				m.op = pb.Op_Del
			}
		} else {
			if tablecodec.IsUntouchedIndexKValue(m.key, m.value) {
				return true
			}
			m.op = pb.Op_Put
			if flags.HasPresumeKeyNotExists() {
				m.op = pb.Op_Insert
			}
		}
	}
	if flags.HasLocked() {
		m.isPessimisticLock = it.isPessimistic
	}

	return false
}

type mutationWithRegionIter struct {
	src mutationsIter

	rc  *RegionCache
	loc *KeyLocation
	bo  *Backoffer
}

func (c *twoPhaseCommitter) mapWithRegion(bo *Backoffer, src mutationsIter) *mutationWithRegionIter {
	return &mutationWithRegionIter{
		src: src,
		rc:  c.store.regionCache,
		bo:  bo,
	}
}

func (it *mutationWithRegionIter) Next() (mutationWithRegion, error) {
	m := it.src.Next()
	if m.key == nil {
		return mutationWithRegion{}, nil
	}

	var err error
	if it.loc == nil || !it.loc.Contains(m.key) {
		it.loc, err = it.rc.LocateKey(it.bo, m.key)
		if err != nil {
			return mutationWithRegion{}, errors.Trace(err)
		}
	}

	return mutationWithRegion{m, it.loc.Region}, nil
}

type mutationBatchCollector struct {
	src            *mutationWithRegionIter
	primaryKey     []byte
	limit          int
	done           bool
	onlyCollectKey bool

	curr    mutationWithRegion
	lenHint int
}

func (c *twoPhaseCommitter) newBatchCollector(bo *Backoffer, src mutationsIter, limit int, isPrewrite bool) (*mutationBatchCollector, error) {
	mutations := c.mapWithRegion(bo, src)
	m, err := mutations.Next()
	if err != nil {
		return nil, err
	}

	return &mutationBatchCollector{
		src:            mutations,
		curr:           m,
		limit:          txnCommitBatchSize,
		primaryKey:     c.primaryKey,
		onlyCollectKey: !isPrewrite,
	}, nil
}

func (c *mutationBatchCollector) Collect() (*batchMutations, error) {
	if c.done {
		return nil, nil
	}

	var (
		mutations = NewCommiterMutations(c.lenHint)
		region    = c.curr.region
		m         = c.curr
		isPrimary bool
		size      int
		err       error
	)

	for {
		if m.key == nil {
			c.done = true
			break
		}

		if c.onlyCollectKey {
			mutations.keys = append(mutations.keys, m.key)
			size += len(m.key)
		} else {
			mutations.Push(m.op, m.key, m.value, m.isPessimisticLock)
			size += len(m.key) + len(m.value)
		}

		if !isPrimary {
			isPrimary = bytes.Equal(m.key, c.primaryKey)
		}

		m, err = c.src.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}

		if size >= c.limit || m.region.id != region.id {
			c.curr = m
			break
		}
	}

	var ret *batchMutations
	if mutations.len() != 0 {
		c.lenHint = mutations.len()
		ret = &batchMutations{
			mutations: mutations,
			region:    region,
			isPrimary: isPrimary,
		}
	}

	return ret, nil
}

func (c *mutationBatchCollector) Finished() bool {
	return c.curr.key == nil
}
