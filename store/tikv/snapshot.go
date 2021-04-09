// Copyright 2015 PingCAP, Inc.
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
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/kv"
	"github.com/pingcap/tidb/store/tikv/logutil"
	"github.com/pingcap/tidb/store/tikv/metrics"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/store/tikv/util"
	"github.com/pingcap/tidb/util/execdetails"
	"go.uber.org/zap"
)

const (
	scanBatchSize = 256
	batchGetSize  = 5120
	maxTimestamp  = math.MaxUint64
)

// KVSnapshot implements the tidbkv.Snapshot interface.
type KVSnapshot struct {
	store           *KVStore
	version         uint64
	isolationLevel  kv.IsoLevel
	priority        pb.CommandPri
	notFillCache    bool
	syncLog         bool
	keyOnly         bool
	vars            *tidbkv.Variables
	replicaReadSeed uint32
	resolvedLocks   *util.TSSet

	// Cache the result of BatchGet.
	// The invariance is that calling BatchGet multiple times using the same start ts,
	// the result should not change.
	// NOTE: This representation here is different from the BatchGet API.
	// cached use len(value)=0 to represent a key-value entry doesn't exist (a reliable truth from TiKV).
	// In the BatchGet API, it use no key-value entry to represent non-exist.
	// It's OK as long as there are no zero-byte values in the protocol.
	mu struct {
		sync.RWMutex
		hitCnt      int64
		cached      map[string][]byte
		cachedSize  int
		stats       *SnapshotRuntimeStats
		replicaRead kv.ReplicaReadType
		taskID      uint64
		isStaleness bool
		// MatchStoreLabels indicates the labels the store should be matched
		matchStoreLabels []*metapb.StoreLabel
	}
	sampleStep uint32
	txnScope   string
}

// newTiKVSnapshot creates a snapshot of an TiKV store.
func newTiKVSnapshot(store *KVStore, ts uint64, replicaReadSeed uint32) *KVSnapshot {
	// Sanity check for snapshot version.
	if ts >= math.MaxInt64 && ts != math.MaxUint64 {
		err := errors.Errorf("try to get snapshot with a large ts %d", ts)
		panic(err)
	}
	return &KVSnapshot{
		store:           store,
		version:         ts,
		priority:        pb.CommandPri_Normal,
		vars:            tidbkv.DefaultVars,
		replicaReadSeed: replicaReadSeed,
		resolvedLocks:   util.NewTSSet(5),
	}
}

func (s *KVSnapshot) setSnapshotTS(ts uint64) {
	// Sanity check for snapshot version.
	if ts >= math.MaxInt64 && ts != math.MaxUint64 {
		err := errors.Errorf("try to get snapshot with a large ts %d", ts)
		panic(err)
	}
	// Invalidate cache if the snapshotTS change!
	s.version = ts
	s.mu.Lock()
	s.mu.cached = nil
	s.mu.Unlock()
	// And also the minCommitTS pushed information.
	s.resolvedLocks = util.NewTSSet(5)
}

// BatchGet gets all the keys' value from kv-server and returns a map contains key/value pairs.
// The map will not contain nonexistent keys.
func (s *KVSnapshot) BatchGet(ctx context.Context, keys []tidbkv.Key) (map[string][]byte, error) {
	// Check the cached value first.
	m := make(map[string][]byte)
	s.mu.RLock()
	if s.mu.cached != nil {
		tmp := make([]tidbkv.Key, 0, len(keys))
		for _, key := range keys {
			if val, ok := s.mu.cached[string(key)]; ok {
				atomic.AddInt64(&s.mu.hitCnt, 1)
				if len(val) > 0 {
					m[string(key)] = val
				}
			} else {
				tmp = append(tmp, key)
			}
		}
		keys = tmp
	}
	s.mu.RUnlock()

	if len(keys) == 0 {
		return m, nil
	}

	// We want [][]byte instead of []tidbkv.Key, use some magic to save memory.
	bytesKeys := *(*[][]byte)(unsafe.Pointer(&keys))
	ctx = context.WithValue(ctx, TxnStartKey, s.version)
	bo := NewBackofferWithVars(ctx, batchGetMaxBackoff, s.vars)

	// Create a map to collect key-values from region servers.
	var mu sync.Mutex
	err := s.batchGetKeysByRegions(bo, bytesKeys, func(k, v []byte) {
		if len(v) == 0 {
			return
		}

		mu.Lock()
		m[string(k)] = v
		mu.Unlock()
	})
	s.recordBackoffInfo(bo)
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = s.store.CheckVisibility(s.version)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Update the cache.
	s.mu.Lock()
	if s.mu.cached == nil {
		s.mu.cached = make(map[string][]byte, len(m))
	}
	for _, key := range keys {
		val := m[string(key)]
		s.mu.cachedSize += len(key) + len(val)
		s.mu.cached[string(key)] = val
	}

	const cachedSizeLimit = 10 << 30
	if s.mu.cachedSize >= cachedSizeLimit {
		for k, v := range s.mu.cached {
			if _, needed := m[k]; needed {
				continue
			}
			delete(s.mu.cached, k)
			s.mu.cachedSize -= len(k) + len(v)
			if s.mu.cachedSize < cachedSizeLimit {
				break
			}
		}
	}
	s.mu.Unlock()

	return m, nil
}

type batchKeys struct {
	region RegionVerID
	keys   [][]byte
}

// appendBatchKeysBySize appends keys to b. It may split the keys to make
// sure each batch's size does not exceed the limit.
func appendBatchKeysBySize(b []batchKeys, region RegionVerID, keys [][]byte, sizeFn func([]byte) int, limit int) []batchKeys {
	var start, end int
	for start = 0; start < len(keys); start = end {
		var size int
		for end = start; end < len(keys) && size < limit; end++ {
			size += sizeFn(keys[end])
		}
		b = append(b, batchKeys{
			region: region,
			keys:   keys[start:end],
		})
	}
	return b
}

func (s *KVSnapshot) batchGetKeysByRegions(bo *Backoffer, keys [][]byte, collectF func(k, v []byte)) error {
	defer func(start time.Time) {
		metrics.TxnCmdHistogramWithBatchGet.Observe(time.Since(start).Seconds())
	}(time.Now())
	groups, _, err := s.store.regionCache.GroupKeysByRegion(bo, keys, nil)
	if err != nil {
		return errors.Trace(err)
	}

	metrics.TxnRegionsNumHistogramWithSnapshot.Observe(float64(len(groups)))

	var batches []batchKeys
	for id, g := range groups {
		batches = appendBatchKeysBySize(batches, id, g, func([]byte) int { return 1 }, batchGetSize)
	}

	if len(batches) == 0 {
		return nil
	}
	if len(batches) == 1 {
		return errors.Trace(s.batchGetSingleRegion(bo, batches[0], collectF))
	}
	ch := make(chan error)
	for _, batch1 := range batches {
		batch := batch1
		go func() {
			backoffer, cancel := bo.Fork()
			defer cancel()
			ch <- s.batchGetSingleRegion(backoffer, batch, collectF)
		}()
	}
	for i := 0; i < len(batches); i++ {
		if e := <-ch; e != nil {
			logutil.BgLogger().Debug("snapshot batchGet failed",
				zap.Error(e),
				zap.Uint64("txnStartTS", s.version))
			err = e
		}
	}
	return errors.Trace(err)
}

func (s *KVSnapshot) batchGetSingleRegion(bo *Backoffer, batch batchKeys, collectF func(k, v []byte)) error {
	cli := NewClientHelper(s.store, s.resolvedLocks)
	s.mu.RLock()
	if s.mu.stats != nil {
		cli.Stats = make(map[tikvrpc.CmdType]*RPCRuntimeStats)
		defer func() {
			s.mergeRegionRequestStats(cli.Stats)
		}()
	}
	s.mu.RUnlock()

	pending := batch.keys
	for {
		isStaleness := false
		var matchStoreLabels []*metapb.StoreLabel
		s.mu.RLock()
		req := tikvrpc.NewReplicaReadRequest(tikvrpc.CmdBatchGet, &pb.BatchGetRequest{
			Keys:    pending,
			Version: s.version,
		}, s.mu.replicaRead, &s.replicaReadSeed, pb.Context{
			Priority:     s.priority,
			NotFillCache: s.notFillCache,
			TaskId:       s.mu.taskID,
		})
		isStaleness = s.mu.isStaleness
		matchStoreLabels = s.mu.matchStoreLabels
		s.mu.RUnlock()
		var ops []StoreSelectorOption
		if isStaleness {
			req.EnableStaleRead()
		}
		if len(matchStoreLabels) > 0 {
			ops = append(ops, WithMatchLabels(matchStoreLabels))
		}
		resp, _, _, err := cli.SendReqCtx(bo, req, batch.region, ReadTimeoutMedium, tikvrpc.TiKV, "", ops...)

		if err != nil {
			return errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return errors.Trace(err)
		}
		if regionErr != nil {
			err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return errors.Trace(err)
			}
			err = s.batchGetKeysByRegions(bo, pending, collectF)
			return errors.Trace(err)
		}
		if resp.Resp == nil {
			return errors.Trace(kv.ErrBodyMissing)
		}
		batchGetResp := resp.Resp.(*pb.BatchGetResponse)
		var (
			lockedKeys [][]byte
			locks      []*Lock
		)
		if keyErr := batchGetResp.GetError(); keyErr != nil {
			// If a response-level error happens, skip reading pairs.
			lock, err := extractLockFromKeyErr(keyErr)
			if err != nil {
				return errors.Trace(err)
			}
			lockedKeys = append(lockedKeys, lock.Key)
			locks = append(locks, lock)
		} else {
			for _, pair := range batchGetResp.Pairs {
				keyErr := pair.GetError()
				if keyErr == nil {
					collectF(pair.GetKey(), pair.GetValue())
					continue
				}
				lock, err := extractLockFromKeyErr(keyErr)
				if err != nil {
					return errors.Trace(err)
				}
				lockedKeys = append(lockedKeys, lock.Key)
				locks = append(locks, lock)
			}
		}
		if batchGetResp.ExecDetailsV2 != nil {
			s.mergeExecDetail(batchGetResp.ExecDetailsV2)
		}
		if len(lockedKeys) > 0 {
			msBeforeExpired, err := cli.ResolveLocks(bo, s.version, locks)
			if err != nil {
				return errors.Trace(err)
			}
			if msBeforeExpired > 0 {
				err = bo.BackoffWithMaxSleep(BoTxnLockFast, int(msBeforeExpired), errors.Errorf("batchGet lockedKeys: %d", len(lockedKeys)))
				if err != nil {
					return errors.Trace(err)
				}
			}
			// Only reduce pending keys when there is no response-level error. Otherwise,
			// lockedKeys may be incomplete.
			if batchGetResp.GetError() == nil {
				pending = lockedKeys
			}
			continue
		}
		return nil
	}
}

// Get gets the value for key k from snapshot.
func (s *KVSnapshot) Get(ctx context.Context, k tidbkv.Key) ([]byte, error) {

	defer func(start time.Time) {
		metrics.TxnCmdHistogramWithGet.Observe(time.Since(start).Seconds())
	}(time.Now())

	ctx = context.WithValue(ctx, TxnStartKey, s.version)
	bo := NewBackofferWithVars(ctx, getMaxBackoff, s.vars)
	val, err := s.get(ctx, bo, k)
	s.recordBackoffInfo(bo)
	if err != nil {
		return nil, errors.Trace(err)
	}
	err = s.store.CheckVisibility(s.version)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(val) == 0 {
		return nil, tidbkv.ErrNotExist
	}
	return val, nil
}

func (s *KVSnapshot) get(ctx context.Context, bo *Backoffer, k tidbkv.Key) ([]byte, error) {
	// Check the cached values first.
	s.mu.RLock()
	if s.mu.cached != nil {
		if value, ok := s.mu.cached[string(k)]; ok {
			atomic.AddInt64(&s.mu.hitCnt, 1)
			s.mu.RUnlock()
			return value, nil
		}
	}
	s.mu.RUnlock()
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("tikvSnapshot.get", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		opentracing.ContextWithSpan(ctx, span1)
	}
	failpoint.Inject("snapshot-get-cache-fail", func(_ failpoint.Value) {
		if bo.ctx.Value("TestSnapshotCache") != nil {
			panic("cache miss")
		}
	})

	cli := NewClientHelper(s.store, s.resolvedLocks)

	// Secondary locks or async commit locks cannot be ignored when getting using the max version.
	// So we concurrently get a TS from PD and use it in retries to avoid unnecessary blocking.
	var tsFuture oracle.Future
	if s.version == maxTimestamp {
		tsFuture = s.store.oracle.GetTimestampAsync(ctx, &oracle.Option{TxnScope: s.txnScope})
	}
	failpoint.Inject("snapshotGetTSAsync", nil)

	isStaleness := false
	var matchStoreLabels []*metapb.StoreLabel
	s.mu.RLock()
	if s.mu.stats != nil {
		cli.Stats = make(map[tikvrpc.CmdType]*RPCRuntimeStats)
		defer func() {
			s.mergeRegionRequestStats(cli.Stats)
		}()
	}
	req := tikvrpc.NewReplicaReadRequest(tikvrpc.CmdGet,
		&pb.GetRequest{
			Key:     k,
			Version: s.version,
		}, s.mu.replicaRead, &s.replicaReadSeed, pb.Context{
			Priority:     s.priority,
			NotFillCache: s.notFillCache,
			TaskId:       s.mu.taskID,
		})
	isStaleness = s.mu.isStaleness
	matchStoreLabels = s.mu.matchStoreLabels
	s.mu.RUnlock()
	var ops []StoreSelectorOption
	if isStaleness {
		req.EnableStaleRead()
	}
	if len(matchStoreLabels) > 0 {
		ops = append(ops, WithMatchLabels(matchStoreLabels))
	}
	for {
		loc, err := s.store.regionCache.LocateKey(bo, k)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resp, _, _, err := cli.SendReqCtx(bo, req, loc.Region, ReadTimeoutShort, tikvrpc.TiKV, "", ops...)
		if err != nil {
			return nil, errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if regionErr != nil {
			err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return nil, errors.Trace(err)
			}
			continue
		}
		if resp.Resp == nil {
			return nil, errors.Trace(kv.ErrBodyMissing)
		}
		cmdGetResp := resp.Resp.(*pb.GetResponse)
		if cmdGetResp.ExecDetailsV2 != nil {
			s.mergeExecDetail(cmdGetResp.ExecDetailsV2)
		}
		val := cmdGetResp.GetValue()
		if keyErr := cmdGetResp.GetError(); keyErr != nil {
			lock, err := extractLockFromKeyErr(keyErr)
			if err != nil {
				return nil, errors.Trace(err)
			}

			snapVer := s.version
			if s.version == maxTimestamp {
				newTS, err := tsFuture.Wait()
				if err != nil {
					return nil, errors.Trace(err)
				}
				s.version = newTS
				req.Req.(*pb.GetRequest).Version = newTS
				// skip lock resolving and backoff if the lock does not block the read
				if newTS < lock.TxnID || newTS < lock.MinCommitTS {
					continue
				}
			}

			// Use the original snapshot version to resolve locks so we can use MaxUint64
			// as the callerStartTS if it's an auto-commit point get. This could save us
			// one write at TiKV by not pushing forward the minCommitTS.
			msBeforeExpired, err := cli.ResolveLocks(bo, snapVer, []*Lock{lock})
			if err != nil {
				return nil, errors.Trace(err)
			}
			if msBeforeExpired > 0 {
				err = bo.BackoffWithMaxSleep(BoTxnLockFast, int(msBeforeExpired), errors.New(keyErr.String()))
				if err != nil {
					return nil, errors.Trace(err)
				}
			}
			continue
		}
		return val, nil
	}
}

func (s *KVSnapshot) mergeExecDetail(detail *pb.ExecDetailsV2) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if detail == nil || s.mu.stats == nil {
		return
	}
	if s.mu.stats.scanDetail == nil {
		s.mu.stats.scanDetail = &execdetails.ScanDetail{}
	}
	if s.mu.stats.timeDetail == nil {
		s.mu.stats.timeDetail = &execdetails.TimeDetail{}
	}
	s.mu.stats.scanDetail.MergeFromScanDetailV2(detail.ScanDetailV2)
	s.mu.stats.timeDetail.MergeFromTimeDetail(detail.TimeDetail)
}

// Iter return a list of key-value pair after `k`.
func (s *KVSnapshot) Iter(k tidbkv.Key, upperBound tidbkv.Key) (tidbkv.Iterator, error) {
	scanner, err := newScanner(s, k, upperBound, scanBatchSize, false)
	return scanner, errors.Trace(err)
}

// IterReverse creates a reversed Iterator positioned on the first entry which key is less than k.
func (s *KVSnapshot) IterReverse(k tidbkv.Key) (tidbkv.Iterator, error) {
	scanner, err := newScanner(s, nil, k, scanBatchSize, true)
	return scanner, errors.Trace(err)
}

// SetOption sets an option with a value, when val is nil, uses the default
// value of this option. Only ReplicaRead is supported for snapshot
func (s *KVSnapshot) SetOption(opt int, val interface{}) {
	switch opt {
	case kv.IsolationLevel:
		s.isolationLevel = val.(kv.IsoLevel)
	case kv.Priority:
		s.priority = PriorityToPB(val.(int))
	case kv.NotFillCache:
		s.notFillCache = val.(bool)
	case kv.SyncLog:
		s.syncLog = val.(bool)
	case kv.KeyOnly:
		s.keyOnly = val.(bool)
	case kv.SnapshotTS:
		s.setSnapshotTS(val.(uint64))
	case kv.ReplicaRead:
		s.mu.Lock()
		s.mu.replicaRead = val.(kv.ReplicaReadType)
		s.mu.Unlock()
	case kv.TaskID:
		s.mu.Lock()
		s.mu.taskID = val.(uint64)
		s.mu.Unlock()
	case kv.CollectRuntimeStats:
		s.mu.Lock()
		s.mu.stats = val.(*SnapshotRuntimeStats)
		s.mu.Unlock()
	case kv.SampleStep:
		s.sampleStep = val.(uint32)
	case kv.IsStalenessReadOnly:
		s.mu.Lock()
		s.mu.isStaleness = val.(bool)
		s.mu.Unlock()
	case kv.MatchStoreLabels:
		s.mu.Lock()
		s.mu.matchStoreLabels = val.([]*metapb.StoreLabel)
		s.mu.Unlock()
	case kv.TxnScope:
		s.txnScope = val.(string)
	}
}

// DelOption deletes an option.
func (s *KVSnapshot) DelOption(opt int) {
	switch opt {
	case kv.ReplicaRead:
		s.mu.Lock()
		s.mu.replicaRead = kv.ReplicaReadLeader
		s.mu.Unlock()
	case kv.CollectRuntimeStats:
		s.mu.Lock()
		s.mu.stats = nil
		s.mu.Unlock()
	}
}

// SnapCacheHitCount gets the snapshot cache hit count. Only for test.
func (s *KVSnapshot) SnapCacheHitCount() int {
	return int(atomic.LoadInt64(&s.mu.hitCnt))
}

// SnapCacheSize gets the snapshot cache size. Only for test.
func (s *KVSnapshot) SnapCacheSize() int {
	s.mu.RLock()
	defer s.mu.RLock()
	return len(s.mu.cached)
}

func extractLockFromKeyErr(keyErr *pb.KeyError) (*Lock, error) {
	if locked := keyErr.GetLocked(); locked != nil {
		return NewLock(locked), nil
	}
	return nil, extractKeyErr(keyErr)
}

func extractKeyErr(keyErr *pb.KeyError) error {
	if val, err := util.MockRetryableErrorResp.Eval(); err == nil {
		if val.(bool) {
			keyErr.Conflict = nil
			keyErr.Retryable = "mock retryable error"
		}
	}

	if keyErr.Conflict != nil {
		return &kv.ErrWriteConflict{WriteConflict: keyErr.GetConflict()}
	}

	if keyErr.Retryable != "" {
		return &kv.ErrRetryable{Retryable: keyErr.Retryable}
	}

	if keyErr.Abort != "" {
		err := errors.Errorf("tikv aborts txn: %s", keyErr.GetAbort())
		logutil.BgLogger().Warn("2PC failed", zap.Error(err))
		return errors.Trace(err)
	}
	if keyErr.CommitTsTooLarge != nil {
		err := errors.Errorf("commit TS %v is too large", keyErr.CommitTsTooLarge.CommitTs)
		logutil.BgLogger().Warn("2PC failed", zap.Error(err))
		return errors.Trace(err)
	}
	if keyErr.TxnNotFound != nil {
		err := errors.Errorf("txn %d not found", keyErr.TxnNotFound.StartTs)
		return errors.Trace(err)
	}
	return errors.Errorf("unexpected KeyError: %s", keyErr.String())
}

func (s *KVSnapshot) recordBackoffInfo(bo *Backoffer) {
	s.mu.RLock()
	if s.mu.stats == nil || bo.totalSleep == 0 {
		s.mu.RUnlock()
		return
	}
	s.mu.RUnlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.stats == nil {
		return
	}
	if s.mu.stats.backoffSleepMS == nil {
		s.mu.stats.backoffSleepMS = bo.backoffSleepMS
		s.mu.stats.backoffTimes = bo.backoffTimes
		return
	}
	for k, v := range bo.backoffSleepMS {
		s.mu.stats.backoffSleepMS[k] += v
	}
	for k, v := range bo.backoffTimes {
		s.mu.stats.backoffTimes[k] += v
	}
}

func (s *KVSnapshot) mergeRegionRequestStats(stats map[tikvrpc.CmdType]*RPCRuntimeStats) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.stats == nil {
		return
	}
	if s.mu.stats.rpcStats.Stats == nil {
		s.mu.stats.rpcStats.Stats = stats
		return
	}
	for k, v := range stats {
		stat, ok := s.mu.stats.rpcStats.Stats[k]
		if !ok {
			s.mu.stats.rpcStats.Stats[k] = v
			continue
		}
		stat.Count += v.Count
		stat.Consume += v.Consume
	}
}

// SnapshotRuntimeStats records the runtime stats of snapshot.
type SnapshotRuntimeStats struct {
	rpcStats       RegionRequestRuntimeStats
	backoffSleepMS map[BackoffType]int
	backoffTimes   map[BackoffType]int
	scanDetail     *execdetails.ScanDetail
	timeDetail     *execdetails.TimeDetail
}

// Tp implements the RuntimeStats interface.
func (rs *SnapshotRuntimeStats) Tp() int {
	return execdetails.TpSnapshotRuntimeStats
}

// Clone implements the RuntimeStats interface.
func (rs *SnapshotRuntimeStats) Clone() execdetails.RuntimeStats {
	newRs := SnapshotRuntimeStats{rpcStats: NewRegionRequestRuntimeStats()}
	if rs.rpcStats.Stats != nil {
		for k, v := range rs.rpcStats.Stats {
			newRs.rpcStats.Stats[k] = v
		}
	}
	if len(rs.backoffSleepMS) > 0 {
		newRs.backoffSleepMS = make(map[BackoffType]int)
		newRs.backoffTimes = make(map[BackoffType]int)
		for k, v := range rs.backoffSleepMS {
			newRs.backoffSleepMS[k] += v
		}
		for k, v := range rs.backoffTimes {
			newRs.backoffTimes[k] += v
		}
	}
	return &newRs
}

// Merge implements the RuntimeStats interface.
func (rs *SnapshotRuntimeStats) Merge(other execdetails.RuntimeStats) {
	tmp, ok := other.(*SnapshotRuntimeStats)
	if !ok {
		return
	}
	if tmp.rpcStats.Stats != nil {
		if rs.rpcStats.Stats == nil {
			rs.rpcStats.Stats = make(map[tikvrpc.CmdType]*RPCRuntimeStats, len(tmp.rpcStats.Stats))
		}
		rs.rpcStats.Merge(tmp.rpcStats)
	}
	if len(tmp.backoffSleepMS) > 0 {
		if rs.backoffSleepMS == nil {
			rs.backoffSleepMS = make(map[BackoffType]int)
		}
		if rs.backoffTimes == nil {
			rs.backoffTimes = make(map[BackoffType]int)
		}
		for k, v := range tmp.backoffSleepMS {
			rs.backoffSleepMS[k] += v
		}
		for k, v := range tmp.backoffTimes {
			rs.backoffTimes[k] += v
		}
	}
}

// String implements fmt.Stringer interface.
func (rs *SnapshotRuntimeStats) String() string {
	var buf bytes.Buffer
	buf.WriteString(rs.rpcStats.String())
	for k, v := range rs.backoffTimes {
		if buf.Len() > 0 {
			buf.WriteByte(',')
		}
		ms := rs.backoffSleepMS[k]
		d := time.Duration(ms) * time.Millisecond
		buf.WriteString(fmt.Sprintf("%s_backoff:{num:%d, total_time:%s}", k.String(), v, execdetails.FormatDuration(d)))
	}
	timeDetail := rs.timeDetail.String()
	if timeDetail != "" {
		buf.WriteString(", ")
		buf.WriteString(timeDetail)
	}
	scanDetail := rs.scanDetail.String()
	if scanDetail != "" {
		buf.WriteString(", ")
		buf.WriteString(scanDetail)
	}
	return buf.String()
}
