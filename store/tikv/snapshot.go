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
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var (
	_ kv.Snapshot = (*tikvSnapshot)(nil)
)

const (
	scanBatchSize = 256
	batchGetSize  = 5120
)

var (
	tikvTxnRegionsNumHistogramWithSnapshot = metrics.TiKVTxnRegionsNumHistogram.WithLabelValues("snapshot")
)

// tikvSnapshot implements the kv.Snapshot interface.
type tikvSnapshot struct {
	store           *tikvStore
	version         kv.Version
	isolationLevel  kv.IsoLevel
	priority        pb.CommandPri
	notFillCache    bool
	syncLog         bool
	keyOnly         bool
	vars            *kv.Variables
	replicaRead     kv.ReplicaReadType
	replicaReadSeed uint32
	taskID          uint64
	minCommitTSPushed

	// Cache the result of BatchGet.
	// The invariance is that calling BatchGet multiple times using the same start ts,
	// the result should not change.
	// NOTE: This representation here is different from the BatchGet API.
	// cached use len(value)=0 to represent a key-value entry doesn't exist (a reliable truth from TiKV).
	// In the BatchGet API, it use no key-value entry to represent non-exist.
	// It's OK as long as there are no zero-byte values in the protocol.
	mu struct {
		sync.RWMutex
		hitCnt int64
		cached map[string][]byte
		stats  *SnapshotRuntimeStats
	}
	sampleStep uint32
}

// newTiKVSnapshot creates a snapshot of an TiKV store.
func newTiKVSnapshot(store *tikvStore, ver kv.Version, replicaReadSeed uint32) *tikvSnapshot {
	return &tikvSnapshot{
		store:           store,
		version:         ver,
		priority:        pb.CommandPri_Normal,
		vars:            kv.DefaultVars,
		replicaReadSeed: replicaReadSeed,
		minCommitTSPushed: minCommitTSPushed{
			data: make(map[uint64]struct{}, 5),
		},
	}
}

func (s *tikvSnapshot) setSnapshotTS(ts uint64) {
	// Invalidate cache if the snapshotTS change!
	s.version.Ver = ts
	s.mu.Lock()
	s.mu.cached = nil
	s.mu.Unlock()
	// And also the minCommitTS pushed information.
	s.minCommitTSPushed.data = make(map[uint64]struct{}, 5)
}

// BatchGet gets all the keys' value from kv-server and returns a map contains key/value pairs.
// The map will not contain nonexistent keys.
func (s *tikvSnapshot) BatchGet(ctx context.Context, keys []kv.Key) (map[string][]byte, error) {
	// Check the cached value first.
	m := make(map[string][]byte)
	s.mu.RLock()
	if s.mu.cached != nil {
		tmp := make([]kv.Key, 0, len(keys))
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

	// We want [][]byte instead of []kv.Key, use some magic to save memory.
	bytesKeys := *(*[][]byte)(unsafe.Pointer(&keys))
	ctx = context.WithValue(ctx, txnStartKey, s.version.Ver)
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

	err = s.store.CheckVisibility(s.version.Ver)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Update the cache.
	s.mu.Lock()
	if s.mu.cached == nil {
		s.mu.cached = make(map[string][]byte, len(m))
	}
	for _, key := range keys {
		s.mu.cached[string(key)] = m[string(key)]
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

func (s *tikvSnapshot) batchGetKeysByRegions(bo *Backoffer, keys [][]byte, collectF func(k, v []byte)) error {
	defer func(start time.Time) {
		tikvTxnCmdHistogramWithBatchGet.Observe(time.Since(start).Seconds())
	}(time.Now())
	groups, _, err := s.store.regionCache.GroupKeysByRegion(bo, keys, nil)
	if err != nil {
		return errors.Trace(err)
	}

	tikvTxnRegionsNumHistogramWithSnapshot.Observe(float64(len(groups)))

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
				zap.Uint64("txnStartTS", s.version.Ver))
			err = e
		}
	}
	return errors.Trace(err)
}

func (s *tikvSnapshot) batchGetSingleRegion(bo *Backoffer, batch batchKeys, collectF func(k, v []byte)) error {
	cli := clientHelper{
		LockResolver:      s.store.lockResolver,
		RegionCache:       s.store.regionCache,
		minCommitTSPushed: &s.minCommitTSPushed,
		Client:            s.store.client,
	}
	if s.mu.stats != nil {
		cli.Stats = make(map[tikvrpc.CmdType]*RPCRuntimeStats)
		defer func() {
			s.mergeRegionRequestStats(cli.Stats)
		}()
	}

	pending := batch.keys
	for {
		req := tikvrpc.NewReplicaReadRequest(tikvrpc.CmdBatchGet, &pb.BatchGetRequest{
			Keys:    pending,
			Version: s.version.Ver,
		}, s.replicaRead, &s.replicaReadSeed, pb.Context{
			Priority:     s.priority,
			NotFillCache: s.notFillCache,
			TaskId:       s.taskID,
		})

		resp, _, _, err := cli.SendReqCtx(bo, req, batch.region, ReadTimeoutMedium, kv.TiKV, "")

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
			return errors.Trace(ErrBodyMissing)
		}
		batchGetResp := resp.Resp.(*pb.BatchGetResponse)
		var (
			lockedKeys [][]byte
			locks      []*Lock
		)
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
		if len(lockedKeys) > 0 {
			msBeforeExpired, err := cli.ResolveLocks(bo, s.version.Ver, locks)
			if err != nil {
				return errors.Trace(err)
			}
			if msBeforeExpired > 0 {
				err = bo.BackoffWithMaxSleep(boTxnLockFast, int(msBeforeExpired), errors.Errorf("batchGet lockedKeys: %d", len(lockedKeys)))
				if err != nil {
					return errors.Trace(err)
				}
			}
			pending = lockedKeys
			continue
		}
		return nil
	}
}

// Get gets the value for key k from snapshot.
func (s *tikvSnapshot) Get(ctx context.Context, k kv.Key) ([]byte, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("tikvSnapshot.get", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	defer func(start time.Time) {
		tikvTxnCmdHistogramWithGet.Observe(time.Since(start).Seconds())
	}(time.Now())

	ctx = context.WithValue(ctx, txnStartKey, s.version.Ver)
	bo := NewBackofferWithVars(ctx, getMaxBackoff, s.vars)
	val, err := s.get(bo, k)
	s.recordBackoffInfo(bo)
	if err != nil {
		return nil, errors.Trace(err)
	}
	err = s.store.CheckVisibility(s.version.Ver)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(val) == 0 {
		return nil, kv.ErrNotExist
	}
	return val, nil
}

func (s *tikvSnapshot) get(bo *Backoffer, k kv.Key) ([]byte, error) {
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

	failpoint.Inject("snapshot-get-cache-fail", func(_ failpoint.Value) {
		if bo.ctx.Value("TestSnapshotCache") != nil {
			panic("cache miss")
		}
	})

	cli := clientHelper{
		LockResolver:      s.store.lockResolver,
		RegionCache:       s.store.regionCache,
		minCommitTSPushed: &s.minCommitTSPushed,
		Client:            s.store.client,
		resolveLite:       true,
	}
	if s.mu.stats != nil {
		cli.Stats = make(map[tikvrpc.CmdType]*RPCRuntimeStats)
		defer func() {
			s.mergeRegionRequestStats(cli.Stats)
		}()
	}

	req := tikvrpc.NewReplicaReadRequest(tikvrpc.CmdGet,
		&pb.GetRequest{
			Key:     k,
			Version: s.version.Ver,
		}, s.replicaRead, &s.replicaReadSeed, pb.Context{
			Priority:     s.priority,
			NotFillCache: s.notFillCache,
			TaskId:       s.taskID,
		})
	for {
		loc, err := s.store.regionCache.LocateKey(bo, k)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resp, _, _, err := cli.SendReqCtx(bo, req, loc.Region, readTimeoutShort, kv.TiKV, "")
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
			return nil, errors.Trace(ErrBodyMissing)
		}
		cmdGetResp := resp.Resp.(*pb.GetResponse)
		val := cmdGetResp.GetValue()
		if keyErr := cmdGetResp.GetError(); keyErr != nil {
			lock, err := extractLockFromKeyErr(keyErr)
			if err != nil {
				return nil, errors.Trace(err)
			}
			msBeforeExpired, err := cli.ResolveLocks(bo, s.version.Ver, []*Lock{lock})
			if err != nil {
				return nil, errors.Trace(err)
			}
			if msBeforeExpired > 0 {
				err = bo.BackoffWithMaxSleep(boTxnLockFast, int(msBeforeExpired), errors.New(keyErr.String()))
				if err != nil {
					return nil, errors.Trace(err)
				}
			}
			continue
		}
		return val, nil
	}
}

// Iter return a list of key-value pair after `k`.
func (s *tikvSnapshot) Iter(k kv.Key, upperBound kv.Key) (kv.Iterator, error) {
	scanner, err := newScanner(s, k, upperBound, scanBatchSize, false)
	return scanner, errors.Trace(err)
}

// IterReverse creates a reversed Iterator positioned on the first entry which key is less than k.
func (s *tikvSnapshot) IterReverse(k kv.Key) (kv.Iterator, error) {
	scanner, err := newScanner(s, nil, k, scanBatchSize, true)
	return scanner, errors.Trace(err)
}

// SetOption sets an option with a value, when val is nil, uses the default
// value of this option. Only ReplicaRead is supported for snapshot
func (s *tikvSnapshot) SetOption(opt kv.Option, val interface{}) {
	switch opt {
	case kv.IsolationLevel:
		s.isolationLevel = val.(kv.IsoLevel)
	case kv.Priority:
		s.priority = kvPriorityToCommandPri(val.(int))
	case kv.NotFillCache:
		s.notFillCache = val.(bool)
	case kv.SyncLog:
		s.syncLog = val.(bool)
	case kv.KeyOnly:
		s.keyOnly = val.(bool)
	case kv.SnapshotTS:
		s.setSnapshotTS(val.(uint64))
	case kv.ReplicaRead:
		s.replicaRead = val.(kv.ReplicaReadType)
	case kv.TaskID:
		s.taskID = val.(uint64)
	case kv.CollectRuntimeStats:
		s.mu.Lock()
		s.mu.stats = val.(*SnapshotRuntimeStats)
		s.mu.Unlock()
	case kv.SampleStep:
		s.sampleStep = val.(uint32)
	}
}

// ClearFollowerRead disables follower read on current transaction
func (s *tikvSnapshot) DelOption(opt kv.Option) {
	switch opt {
	case kv.ReplicaRead:
		s.replicaRead = kv.ReplicaReadLeader
	case kv.CollectRuntimeStats:
		s.mu.Lock()
		s.mu.stats = nil
		s.mu.Unlock()
	}
}

// SnapCacheHitCount gets the snapshot cache hit count.
func SnapCacheHitCount(snap kv.Snapshot) int {
	tikvSnap, ok := snap.(*tikvSnapshot)
	if !ok {
		return 0
	}
	return int(atomic.LoadInt64(&tikvSnap.mu.hitCnt))
}

// SnapCacheSize gets the snapshot cache size.
func SnapCacheSize(snap kv.Snapshot) int {
	tikvSnap, ok := snap.(*tikvSnapshot)
	if !ok {
		return 0
	}
	tikvSnap.mu.RLock()
	defer tikvSnap.mu.RLock()
	return len(tikvSnap.mu.cached)
}

func extractLockFromKeyErr(keyErr *pb.KeyError) (*Lock, error) {
	if locked := keyErr.GetLocked(); locked != nil {
		return NewLock(locked), nil
	}
	return nil, extractKeyErr(keyErr)
}

func extractKeyErr(keyErr *pb.KeyError) error {
	failpoint.Inject("ErrMockRetryableOnly", func(val failpoint.Value) {
		if val.(bool) {
			keyErr.Conflict = nil
			keyErr.Retryable = "mock retryable error"
		}
	})

	if keyErr.Conflict != nil {
		return newWriteConflictError(keyErr.Conflict)
	}
	if keyErr.Retryable != "" {
		notFoundDetail := prettyLockNotFoundKey(keyErr.GetRetryable())
		return kv.ErrTxnRetryable.GenWithStackByArgs(keyErr.GetRetryable() + " " + notFoundDetail)
	}
	if keyErr.Abort != "" {
		err := errors.Errorf("tikv aborts txn: %s", keyErr.GetAbort())
		logutil.BgLogger().Warn("2PC failed", zap.Error(err))
		return errors.Trace(err)
	}
	return errors.Errorf("unexpected KeyError: %s", keyErr.String())
}

func prettyLockNotFoundKey(rawRetry string) string {
	if !strings.Contains(rawRetry, "TxnLockNotFound") {
		return ""
	}
	start := strings.Index(rawRetry, "[")
	if start == -1 {
		return ""
	}
	rawRetry = rawRetry[start:]
	end := strings.Index(rawRetry, "]")
	if end == -1 {
		return ""
	}
	rawRetry = rawRetry[:end+1]
	var key []byte
	err := json.Unmarshal([]byte(rawRetry), &key)
	if err != nil {
		return ""
	}
	var buf bytes.Buffer
	prettyWriteKey(&buf, key)
	return buf.String()
}

func newWriteConflictError(conflict *pb.WriteConflict) error {
	var buf bytes.Buffer
	prettyWriteKey(&buf, conflict.Key)
	buf.WriteString(" primary=")
	prettyWriteKey(&buf, conflict.Primary)
	return kv.ErrWriteConflict.FastGenByArgs(conflict.StartTs, conflict.ConflictTs, conflict.ConflictCommitTs, buf.String())
}

func prettyWriteKey(buf *bytes.Buffer, key []byte) {
	tableID, indexID, indexValues, err := tablecodec.DecodeIndexKey(key)
	if err == nil {
		_, err1 := fmt.Fprintf(buf, "{tableID=%d, indexID=%d, indexValues={", tableID, indexID)
		if err1 != nil {
			logutil.BgLogger().Error("error", zap.Error(err1))
		}
		for _, v := range indexValues {
			_, err2 := fmt.Fprintf(buf, "%s, ", v)
			if err2 != nil {
				logutil.BgLogger().Error("error", zap.Error(err2))
			}
		}
		buf.WriteString("}}")
		return
	}

	tableID, handle, err := tablecodec.DecodeRecordKey(key)
	if err == nil {
		_, err3 := fmt.Fprintf(buf, "{tableID=%d, handle=%d}", tableID, handle)
		if err3 != nil {
			logutil.BgLogger().Error("error", zap.Error(err3))
		}
		return
	}

	mKey, mField, err := tablecodec.DecodeMetaKey(key)
	if err == nil {
		_, err3 := fmt.Fprintf(buf, "{metaKey=true, key=%s, field=%s}", string(mKey), string(mField))
		if err3 != nil {
			logutil.Logger(context.Background()).Error("error", zap.Error(err3))
		}
		return
	}

	_, err4 := fmt.Fprintf(buf, "%#v", key)
	if err4 != nil {
		logutil.BgLogger().Error("error", zap.Error(err4))
	}
}

func (s *tikvSnapshot) recordBackoffInfo(bo *Backoffer) {
	if s.mu.stats == nil || bo.totalSleep == 0 {
		return
	}
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

func (s *tikvSnapshot) mergeRegionRequestStats(stats map[tikvrpc.CmdType]*RPCRuntimeStats) {
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
	backoffSleepMS map[backoffType]int
	backoffTimes   map[backoffType]int
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
		buf.WriteString(fmt.Sprintf("%s_backoff:{num:%d, total_time:%d ms}", k.String(), v, ms))
	}
	return buf.String()
}
