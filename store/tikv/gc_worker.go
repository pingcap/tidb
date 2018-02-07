// Copyright 2016 PingCAP, Inc.
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
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/coreos/etcd/clientv3"
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/terror"
	goctx "golang.org/x/net/context"
)

// GCWorker periodically triggers GC process on tikv server.
type GCWorker struct {
	uuid        string
	desc        string
	store       *tikvStore
	gcIsRunning bool
	lastFinish  time.Time
	cancel      goctx.CancelFunc
	done        chan error

	session tidb.Session
}

// SafePointKV is used for a seamingless integration for mockTest and runtime.
type SafePointKV interface {
	Put(k string, v string) error
	Get(k string) (string, error)
}

// MockSafePointKV implements SafePointKV at mock test
type MockSafePointKV struct {
	store    map[string]string
	mockLock sync.RWMutex
}

// NewMockSafePointKV creates an instance of MockSafePointKV
func NewMockSafePointKV() *MockSafePointKV {
	return &MockSafePointKV{
		store: make(map[string]string),
	}
}

// Put implements the Put method for SafePointKV
func (w *MockSafePointKV) Put(k string, v string) error {
	w.mockLock.Lock()
	defer w.mockLock.Unlock()
	w.store[k] = v
	return nil
}

// Get implements the Get method for SafePointKV
func (w *MockSafePointKV) Get(k string) (string, error) {
	w.mockLock.RLock()
	defer w.mockLock.RUnlock()
	elem, _ := w.store[k]
	return elem, nil
}

// EtcdSafePointKV implements SafePointKV at runtime
type EtcdSafePointKV struct {
	cli *clientv3.Client
}

// NewEtcdSafePointKV creates an instance of EtcdSafePointKV
func NewEtcdSafePointKV(addrs []string) (*EtcdSafePointKV, error) {
	etcdCli, err := createEtcdKV(addrs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &EtcdSafePointKV{cli: etcdCli}, nil
}

// Put implements the Put method for SafePointKV
func (w *EtcdSafePointKV) Put(k string, v string) error {
	ctx, cancel := goctx.WithTimeout(goctx.Background(), time.Second*5)
	_, err := w.cli.Put(ctx, k, v)
	cancel()
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// Get implements the Get method for SafePointKV
func (w *EtcdSafePointKV) Get(k string) (string, error) {
	ctx, cancel := goctx.WithTimeout(goctx.Background(), time.Second*5)
	resp, err := w.cli.Get(ctx, k)
	cancel()
	if err != nil {
		return "", errors.Trace(err)
	}
	if len(resp.Kvs) > 0 {
		return string(resp.Kvs[0].Value), nil
	}
	return "", nil
}

// StartSafePointChecker triggers SafePoint Checker on each tidb
func (w *GCWorker) StartSafePointChecker() {
	go func() {
		d := gcSafePointUpdateInterval
		for {
			select {
			case spCachedTime := <-time.After(d):
				cachedSafePoint, err := w.loadSafePoint(gcSavedSafePoint)
				if err == nil {
					gcWorkerCounter.WithLabelValues("check_safepoint_ok").Inc()
					w.store.UpdateSPCache(cachedSafePoint, spCachedTime)
					d = gcSafePointUpdateInterval
				} else {
					gcWorkerCounter.WithLabelValues("check_safepoint_fail").Inc()
					log.Errorf("[gc worker] fail to load safepoint: %v", err)
					d = gcSafePointQuickRepeatInterval
				}
			case <-w.store.closed:
				return
			}
		}
	}()
}

// NewGCWorker creates a GCWorker instance.
func NewGCWorker(store kv.Storage, enableGC bool) (*GCWorker, error) {
	ver, err := store.CurrentVersion()
	if err != nil {
		return nil, errors.Trace(err)
	}
	hostName, err := os.Hostname()
	if err != nil {
		hostName = "unknown"
	}
	worker := &GCWorker{
		uuid:        strconv.FormatUint(ver.Ver, 16),
		desc:        fmt.Sprintf("host:%s, pid:%d, start at %s", hostName, os.Getpid(), time.Now()),
		store:       store.(*tikvStore),
		gcIsRunning: false,
		lastFinish:  time.Now(),
		done:        make(chan error),
	}

	worker.StartSafePointChecker()

	var ctx goctx.Context
	ctx, worker.cancel = goctx.WithCancel(goctx.Background())
	var wg sync.WaitGroup

	if enableGC {
		wg.Add(1)
		go worker.start(ctx, &wg)
		wg.Wait() // Wait create session finish in worker, some test code depend on this to avoid race.
	}

	return worker, nil
}

// Close stops background goroutines.
func (w *GCWorker) Close() {
	w.cancel()
}

const (
	gcTimeFormat         = "20060102-15:04:05 -0700 MST"
	gcWorkerTickInterval = time.Minute
	gcJobLogTickInterval = time.Minute * 10
	gcWorkerLease        = time.Minute * 2
	gcLeaderUUIDKey      = "tikv_gc_leader_uuid"
	gcLeaderDescKey      = "tikv_gc_leader_desc"
	gcLeaderLeaseKey     = "tikv_gc_leader_lease"

	gcLastRunTimeKey     = "tikv_gc_last_run_time"
	gcRunIntervalKey     = "tikv_gc_run_interval"
	gcDefaultRunInterval = time.Minute * 10
	gcWaitTime           = time.Minute * 10

	gcLifeTimeKey                  = "tikv_gc_life_time"
	gcDefaultLifeTime              = time.Minute * 10
	gcSafePointKey                 = "tikv_gc_safe_point"
	gcSavedSafePoint               = "/tidb/store/gcworker/saved_safe_point"
	gcSafePointCacheInterval       = time.Second * 100
	gcSafePointUpdateInterval      = time.Second * 10
	gcSafePointQuickRepeatInterval = time.Second
	gcCPUTimeInaccuracyBound       = time.Second
)

var gcVariableComments = map[string]string{
	gcLeaderUUIDKey:  "Current GC worker leader UUID. (DO NOT EDIT)",
	gcLeaderDescKey:  "Host name and pid of current GC leader. (DO NOT EDIT)",
	gcLeaderLeaseKey: "Current GC worker leader lease. (DO NOT EDIT)",
	gcLastRunTimeKey: "The time when last GC starts. (DO NOT EDIT)",
	gcRunIntervalKey: "GC run interval, at least 10m, in Go format.",
	gcLifeTimeKey:    "All versions within life time will not be collected by GC, at least 10m, in Go format.",
	gcSafePointKey:   "All versions after safe point can be accessed. (DO NOT EDIT)",
}

func (w *GCWorker) start(ctx goctx.Context, wg *sync.WaitGroup) {
	log.Infof("[gc worker] %s start.", w.uuid)

	w.session = createSession(w.store)

	w.tick(ctx) // Immediately tick once to initialize configs.
	wg.Done()

	ticker := time.NewTicker(gcWorkerTickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			w.tick(ctx)
		case err := <-w.done:
			w.gcIsRunning = false
			w.lastFinish = time.Now()
			if err != nil {
				log.Errorf("[gc worker] runGCJob error: %v", err)
				break
			}
		case <-ctx.Done():
			log.Infof("[gc worker] (%s) quit.", w.uuid)
			return
		}
	}
}

func createSession(store kv.Storage) tidb.Session {
	for {
		session, err := tidb.CreateSession(store)
		if err != nil {
			log.Warnf("[gc worker] create session err: %v", err)
			continue
		}
		// Disable privilege check for gc worker session.
		privilege.BindPrivilegeManager(session, nil)
		session.GetSessionVars().InRestrictedSQL = true
		return session
	}
}

func (w *GCWorker) tick(ctx goctx.Context) {
	isLeader, err := w.checkLeader()
	if err != nil {
		log.Warnf("[gc worker] check leader err: %v", err)
		return
	}
	if isLeader {
		err = w.leaderTick(ctx)
		if err != nil {
			log.Warnf("[gc worker] leader tick err: %v", err)
		}
	} else {
		// Config metrics should always be updated by leader, set them to 0 when current instance is not leader.
		gcConfigGauge.WithLabelValues(gcRunIntervalKey).Set(0)
		gcConfigGauge.WithLabelValues(gcLifeTimeKey).Set(0)
	}
}

const notBootstrappedVer = 0

func (w *GCWorker) storeIsBootstrapped() bool {
	var ver int64
	err := kv.RunInNewTxn(w.store, false, func(txn kv.Transaction) error {
		var err error
		t := meta.NewMeta(txn)
		ver, err = t.GetBootstrapVersion()
		return errors.Trace(err)
	})
	if err != nil {
		log.Errorf("[gc worker] check bootstrapped error %v", err)
		return false
	}
	return ver > notBootstrappedVer
}

// Leader of GC worker checks if it should start a GC job every tick.
func (w *GCWorker) leaderTick(ctx goctx.Context) error {
	if w.gcIsRunning {
		return nil
	}

	ok, safePoint, err := w.prepare()
	if err != nil || !ok {
		w.gcIsRunning = false
		return errors.Trace(err)
	}
	// When the worker is just started, or an old GC job has just finished,
	// wait a while before starting a new job.
	if time.Since(w.lastFinish) < gcWaitTime {
		w.gcIsRunning = false
		return nil
	}

	w.gcIsRunning = true
	log.Infof("[gc worker] %s starts the whole job, safePoint: %v", w.uuid, safePoint)
	go w.runGCJob(ctx, safePoint)
	return nil
}

// prepare checks required conditions for starting a GC job. It returns a bool
// that indicates whether the GC job should start and the new safePoint.
func (w *GCWorker) prepare() (bool, uint64, error) {
	now, err := w.getOracleTime()
	if err != nil {
		return false, 0, errors.Trace(err)
	}
	ok, err := w.checkGCInterval(now)
	if err != nil || !ok {
		return false, 0, errors.Trace(err)
	}
	newSafePoint, err := w.calculateNewSafePoint(now)
	if err != nil || newSafePoint == nil {
		return false, 0, errors.Trace(err)
	}
	err = w.saveTime(gcLastRunTimeKey, now, w.session)
	if err != nil {
		return false, 0, errors.Trace(err)
	}
	err = w.saveTime(gcSafePointKey, *newSafePoint, w.session)
	if err != nil {
		return false, 0, errors.Trace(err)
	}
	return true, oracle.ComposeTS(oracle.GetPhysical(*newSafePoint), 0), nil
}

func (w *GCWorker) getOracleTime() (time.Time, error) {
	currentVer, err := w.store.CurrentVersion()
	if err != nil {
		return time.Time{}, errors.Trace(err)
	}
	physical := oracle.ExtractPhysical(currentVer.Ver)
	sec, nsec := physical/1e3, (physical%1e3)*1e6
	return time.Unix(sec, nsec), nil
}

func (w *GCWorker) checkGCInterval(now time.Time) (bool, error) {
	runInterval, err := w.loadDurationWithDefault(gcRunIntervalKey, gcDefaultRunInterval)
	if err != nil {
		return false, errors.Trace(err)
	}
	gcConfigGauge.WithLabelValues(gcRunIntervalKey).Set(runInterval.Seconds())
	lastRun, err := w.loadTime(gcLastRunTimeKey, w.session)
	if err != nil {
		return false, errors.Trace(err)
	}

	if lastRun != nil && lastRun.Add(*runInterval).After(now) {
		return false, nil
	}

	return true, nil
}

func (w *GCWorker) calculateNewSafePoint(now time.Time) (*time.Time, error) {
	lifeTime, err := w.loadDurationWithDefault(gcLifeTimeKey, gcDefaultLifeTime)
	if err != nil {
		return nil, errors.Trace(err)
	}
	gcConfigGauge.WithLabelValues(gcLifeTimeKey).Set(lifeTime.Seconds())
	lastSafePoint, err := w.loadTime(gcSafePointKey, w.session)
	if err != nil {
		return nil, errors.Trace(err)
	}
	safePoint := now.Add(-*lifeTime)
	// We should never decrease safePoint.
	if lastSafePoint != nil && safePoint.Before(*lastSafePoint) {
		return nil, nil
	}
	return &safePoint, nil
}

// RunGCJob sends GC command to KV. it is exported for testing purpose, do not use it with GCWorker at the same time.
func RunGCJob(ctx goctx.Context, store kv.Storage, safePoint uint64, identifier string) error {
	s, ok := store.(*tikvStore)
	if !ok {
		return errors.New("should use tikv driver")
	}
	err := resolveLocks(ctx, s, safePoint, identifier)
	if err != nil {
		return errors.Trace(err)
	}
	err = doGC(ctx, s, safePoint, identifier)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (w *GCWorker) runGCJob(ctx goctx.Context, safePoint uint64) {
	gcWorkerCounter.WithLabelValues("run_job").Inc()
	err := resolveLocks(ctx, w.store, safePoint, w.uuid)
	if err != nil {
		log.Errorf("[gc worker] %s resolve locks returns an error %v", w.uuid, err)
		gcJobFailureCounter.WithLabelValues("resolve_lock").Inc()
		w.done <- errors.Trace(err)
		return
	}
	err = w.deleteRanges(ctx, safePoint)
	if err != nil {
		log.Errorf("[gc worker] %s delete range returns an error %v", w.uuid, err)
		gcJobFailureCounter.WithLabelValues("delete_range").Inc()
		w.done <- errors.Trace(err)
		return
	}
	err = doGC(ctx, w.store, safePoint, w.uuid)
	if err != nil {
		log.Errorf("[gc worker] %s do GC returns an error %v", w.uuid, err)
		w.gcIsRunning = false
		gcJobFailureCounter.WithLabelValues("gc").Inc()
		w.done <- errors.Trace(err)
		return
	}
	w.done <- nil
}

func (w *GCWorker) deleteRanges(ctx goctx.Context, safePoint uint64) error {
	gcWorkerCounter.WithLabelValues("delete_range").Inc()

	session := createSession(w.store)
	ranges, err := util.LoadDeleteRanges(session, safePoint)
	session.Close()
	if err != nil {
		return errors.Trace(err)
	}

	bo := NewBackoffer(gcDeleteRangeMaxBackoff, ctx)
	log.Infof("[gc worker] %s start delete %v ranges", w.uuid, len(ranges))
	startTime := time.Now()
	regions := 0
	for _, r := range ranges {
		startKey, rangeEndKey := r.Range()
		for {
			select {
			case <-ctx.Done():
				return errors.New("[gc worker] gc job canceled")
			default:
			}

			loc, err := w.store.regionCache.LocateKey(bo, startKey)
			if err != nil {
				return errors.Trace(err)
			}

			endKey := loc.EndKey
			if loc.Contains(rangeEndKey) {
				endKey = rangeEndKey
			}

			req := &tikvrpc.Request{
				Type: tikvrpc.CmdDeleteRange,
				DeleteRange: &kvrpcpb.DeleteRangeRequest{
					StartKey: startKey,
					EndKey:   endKey,
				},
			}

			resp, err := w.store.SendReq(bo, req, loc.Region, readTimeoutMedium)
			if err != nil {
				return errors.Trace(err)
			}
			regionErr, err := resp.GetRegionError()
			if err != nil {
				return errors.Trace(err)
			}
			if regionErr != nil {
				err = bo.Backoff(boRegionMiss, errors.New(regionErr.String()))
				if err != nil {
					return errors.Trace(err)
				}
				continue
			}
			deleteRangeResp := resp.DeleteRange
			if deleteRangeResp == nil {
				return errors.Trace(errBodyMissing)
			}
			if err := deleteRangeResp.GetError(); err != "" {
				return errors.Errorf("unexpected delete range err: %v", err)
			}
			regions++
			if bytes.Equal(endKey, rangeEndKey) {
				break
			}
			startKey = endKey
		}
		session := createSession(w.store)
		err := util.CompleteDeleteRange(session, r)
		session.Close()
		if err != nil {
			return errors.Trace(err)
		}
	}
	log.Infof("[gc worker] %s finish delete %v ranges, regions: %v, cost time: %s", w.uuid, len(ranges), regions, time.Since(startTime))
	gcHistogram.WithLabelValues("delete_ranges").Observe(time.Since(startTime).Seconds())
	return nil
}

func resolveLocks(ctx goctx.Context, store *tikvStore, safePoint uint64, identifier string) error {
	gcWorkerCounter.WithLabelValues("resolve_locks").Inc()
	req := &tikvrpc.Request{
		Type: tikvrpc.CmdScanLock,
		ScanLock: &kvrpcpb.ScanLockRequest{
			MaxVersion: safePoint,
		},
	}
	bo := NewBackoffer(gcResolveLockMaxBackoff, ctx)

	log.Infof("[gc worker] %s start resolve locks, safePoint: %v.", identifier, safePoint)
	startTime := time.Now()
	regions, totalResolvedLocks := 0, 0

	var key []byte
	for {
		select {
		case <-ctx.Done():
			return errors.New("[gc worker] gc job canceled")
		default:
		}

		loc, err := store.regionCache.LocateKey(bo, key)
		if err != nil {
			return errors.Trace(err)
		}
		resp, err := store.SendReq(bo, req, loc.Region, readTimeoutMedium)
		if err != nil {
			return errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return errors.Trace(err)
		}
		if regionErr != nil {
			err = bo.Backoff(boRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return errors.Trace(err)
			}
			continue
		}
		locksResp := resp.ScanLock
		if locksResp == nil {
			return errors.Trace(errBodyMissing)
		}
		if locksResp.GetError() != nil {
			return errors.Errorf("unexpected scanlock error: %s", locksResp)
		}
		locksInfo := locksResp.GetLocks()
		locks := make([]*Lock, len(locksInfo))
		for i := range locksInfo {
			locks[i] = newLock(locksInfo[i])
		}
		ok, err1 := store.lockResolver.ResolveLocks(bo, locks)
		if err1 != nil {
			return errors.Trace(err1)
		}
		if !ok {
			err = bo.Backoff(boTxnLock, errors.Errorf("remain locks: %d", len(locks)))
			if err != nil {
				return errors.Trace(err)
			}
			continue
		}
		regions++
		totalResolvedLocks += len(locks)
		key = loc.EndKey
		if len(key) == 0 {
			break
		}
	}
	log.Infof("[gc worker] %s finish resolve locks, safePoint: %v, regions: %v, total resolved: %v, cost time: %s", identifier, safePoint, regions, totalResolvedLocks, time.Since(startTime))
	gcHistogram.WithLabelValues("resolve_locks").Observe(time.Since(startTime).Seconds())
	return nil
}

func doGC(ctx goctx.Context, store *tikvStore, safePoint uint64, identifier string) error {
	gcWorkerCounter.WithLabelValues("do_gc").Inc()

	err := store.gcWorker.saveSafePoint(gcSavedSafePoint, safePoint)
	if err != nil {
		return errors.Trace(err)
	}

	// Sleep to wait for all other tidb instances update their safepoint cache.
	time.Sleep(gcSafePointCacheInterval)

	log.Infof("[gc worker] %s start gc, safePoint: %v.", identifier, safePoint)
	startTime := time.Now()
	successRegions := 0
	failedRegions := 0

	ticker := time.NewTicker(gcJobLogTickInterval)
	defer ticker.Stop()

	bo := NewBackoffer(gcOneRegionMaxBackoff, ctx)
	var key []byte
	for {
		select {
		case <-ctx.Done():
			return errors.New("[gc worker] gc job canceled")
		case <-ticker.C:
			log.Infof("[gc worker] %s gc in process, safePoint: %v, successful regions: %v, failed regions: %v, cost time: %s",
				identifier, safePoint, successRegions, failedRegions, time.Since(startTime))
		default:
		}

		loc, err := store.regionCache.LocateKey(bo, key)
		if err != nil {
			return errors.Trace(err)
		}

		var regionErr *errorpb.Error
		regionErr, err = doGCForOneRegion(bo, store, safePoint, loc.Region)

		// we check regionErr here first, because we know 'regionErr' and 'err' should not return together, to keep it to
		// make the process correct.
		if regionErr != nil {
			err = bo.Backoff(boRegionMiss, errors.New(regionErr.String()))
			if err == nil {
				continue
			}
		}

		if err != nil {
			failedRegions++
			gcActionRegionResultCounter.WithLabelValues("fail").Inc()
			log.Warnf("[gc worker] %s failed to do gc on region(%s, %s), ignore it", identifier, string(loc.StartKey), string(loc.EndKey))
		} else {
			successRegions++
			gcActionRegionResultCounter.WithLabelValues("success").Inc()
		}

		key = loc.EndKey
		if len(key) == 0 {
			break
		}
		bo = NewBackoffer(gcOneRegionMaxBackoff, ctx)
	}
	log.Infof("[gc worker] %s finish gc, safePoint: %v, successful regions: %v, failed regions: %v, cost time: %s",
		identifier, safePoint, successRegions, failedRegions, time.Since(startTime))
	gcHistogram.WithLabelValues("do_gc").Observe(time.Since(startTime).Seconds())
	return nil
}

// these two errors should not return together, for more, see the func 'doGC'
func doGCForOneRegion(bo *Backoffer, store *tikvStore, safePoint uint64, region RegionVerID) (*errorpb.Error, error) {
	req := &tikvrpc.Request{
		Type: tikvrpc.CmdGC,
		GC: &kvrpcpb.GCRequest{
			SafePoint: safePoint,
		},
	}

	resp, err := store.SendReq(bo, req, region, gcTimeout)
	if err != nil {
		return nil, errors.Trace(err)
	}
	regionErr, err := resp.GetRegionError()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if regionErr != nil {
		return regionErr, nil
	}

	gcResp := resp.GC
	if gcResp == nil {
		return nil, errors.Trace(errBodyMissing)
	}
	if gcResp.GetError() != nil {
		return nil, errors.Errorf("unexpected gc error: %s", gcResp.GetError())
	}

	return nil, nil
}

func (w *GCWorker) checkLeader() (bool, error) {
	gcWorkerCounter.WithLabelValues("check_leader").Inc()
	session := createSession(w.store)
	defer session.Close()

	_, err := session.Execute("BEGIN")
	if err != nil {
		return false, errors.Trace(err)
	}
	leader, err := w.loadValueFromSysTable(gcLeaderUUIDKey, session)
	if err != nil {
		_, err1 := session.Execute("ROLLBACK")
		terror.Log(errors.Trace(err1))
		return false, errors.Trace(err)
	}
	log.Debugf("[gc worker] got leader: %s", leader)
	if leader == w.uuid {
		err = w.saveTime(gcLeaderLeaseKey, time.Now().Add(gcWorkerLease), session)
		if err != nil {
			_, err1 := session.Execute("ROLLBACK")
			terror.Log(errors.Trace(err1))
			return false, errors.Trace(err)
		}
		_, err = session.Execute("COMMIT")
		if err != nil {
			return false, errors.Trace(err)
		}
		return true, nil
	}

	_, err = session.Execute("BEGIN")
	if err != nil {
		return false, errors.Trace(err)
	}
	lease, err := w.loadTime(gcLeaderLeaseKey, session)
	if err != nil {
		return false, errors.Trace(err)
	}
	if lease == nil || lease.Before(time.Now()) {
		log.Debugf("[gc worker] register %s as leader", w.uuid)
		gcWorkerCounter.WithLabelValues("register_leader").Inc()

		err = w.saveValueToSysTable(gcLeaderUUIDKey, w.uuid, session)
		if err != nil {
			_, err1 := session.Execute("ROLLBACK")
			terror.Log(errors.Trace(err1))
			return false, errors.Trace(err)
		}
		err = w.saveValueToSysTable(gcLeaderDescKey, w.desc, session)
		if err != nil {
			_, err1 := session.Execute("ROLLBACK")
			terror.Log(errors.Trace(err1))
			return false, errors.Trace(err)
		}
		err = w.saveTime(gcLeaderLeaseKey, time.Now().Add(gcWorkerLease), session)
		if err != nil {
			_, err1 := session.Execute("ROLLBACK")
			terror.Log(errors.Trace(err1))
			return false, errors.Trace(err)
		}
		_, err = session.Execute("COMMIT")
		if err != nil {
			return false, errors.Trace(err)
		}
		return true, nil
	}
	_, err1 := session.Execute("ROLLBACK")
	terror.Log(errors.Trace(err1))
	return false, nil
}

func (w *GCWorker) saveSafePoint(key string, t uint64) error {
	s := strconv.FormatUint(t, 10)
	err := w.store.kv.Put(gcSavedSafePoint, s)
	if err != nil {
		log.Error("save safepoint failed:", err)
		return errors.Trace(err)
	}
	return nil
}

func (w *GCWorker) loadSafePoint(key string) (uint64, error) {
	str, err := w.store.kv.Get(gcSavedSafePoint)

	if err != nil {
		return 0, errors.Trace(err)
	}

	if str == "" {
		return 0, nil
	}

	t, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return t, nil
}

func (w *GCWorker) saveTime(key string, t time.Time, s tidb.Session) error {
	err := w.saveValueToSysTable(key, t.Format(gcTimeFormat), s)
	return errors.Trace(err)
}

func (w *GCWorker) loadTime(key string, s tidb.Session) (*time.Time, error) {
	str, err := w.loadValueFromSysTable(key, s)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if str == "" {
		return nil, nil
	}
	t, err := time.Parse(gcTimeFormat, str)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &t, nil
}

func (w *GCWorker) saveDuration(key string, d time.Duration) error {
	err := w.saveValueToSysTable(key, d.String(), w.session)
	return errors.Trace(err)
}

func (w *GCWorker) loadDuration(key string) (*time.Duration, error) {
	str, err := w.loadValueFromSysTable(key, w.session)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if str == "" {
		return nil, nil
	}
	d, err := time.ParseDuration(str)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &d, nil
}

func (w *GCWorker) loadDurationWithDefault(key string, def time.Duration) (*time.Duration, error) {
	d, err := w.loadDuration(key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if d == nil {
		err = w.saveDuration(key, def)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return &def, nil
	}
	return d, nil
}

func (w *GCWorker) loadValueFromSysTable(key string, s tidb.Session) (string, error) {
	stmt := fmt.Sprintf(`SELECT (variable_value) FROM mysql.tidb WHERE variable_name='%s' FOR UPDATE`, key)
	rs, err := s.Execute(stmt)
	if err != nil {
		return "", errors.Trace(err)
	}
	row, err := rs[0].Next()
	if err != nil {
		return "", errors.Trace(err)
	}
	if row == nil {
		log.Debugf("[gc worker] load kv, %s:nil", key)
		return "", nil
	}
	value := row.Data[0].GetString()
	log.Debugf("[gc worker] load kv, %s:%s", key, value)
	return value, nil
}

func (w *GCWorker) saveValueToSysTable(key, value string, s tidb.Session) error {
	stmt := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
			       ON DUPLICATE KEY
			       UPDATE variable_value = '%[2]s', comment = '%[3]s'`,
		key, value, gcVariableComments[key])
	if s == nil {
		return errors.New("[saveValueToSysTable session is nil]")
	}
	_, err := s.Execute(stmt)
	log.Debugf("[gc worker] save kv, %s:%s %v", key, value, err)
	return errors.Trace(err)
}

// MockGCWorker is for test.
type MockGCWorker struct {
	worker *GCWorker
}

// NewMockGCWorker creates a MockGCWorker instance ONLY for test.
func NewMockGCWorker(store kv.Storage) (*MockGCWorker, error) {
	ver, err := store.CurrentVersion()
	if err != nil {
		return nil, errors.Trace(err)
	}
	hostName, err := os.Hostname()
	if err != nil {
		hostName = "unknown"
	}
	worker := &GCWorker{
		uuid:        strconv.FormatUint(ver.Ver, 16),
		desc:        fmt.Sprintf("host:%s, pid:%d, start at %s", hostName, os.Getpid(), time.Now()),
		store:       store.(*tikvStore),
		gcIsRunning: false,
		lastFinish:  time.Now(),
		done:        make(chan error),
	}
	worker.session, err = tidb.CreateSession(worker.store)
	if err != nil {
		log.Errorf("initialize MockGCWorker session fail: %s", err)
		return nil, errors.Trace(err)
	}
	privilege.BindPrivilegeManager(worker.session, nil)
	worker.session.GetSessionVars().InRestrictedSQL = true
	return &MockGCWorker{worker: worker}, nil
}

// DeleteRanges call deleteRanges internally, just for test.
func (w *MockGCWorker) DeleteRanges(ctx goctx.Context, safePoint uint64) error {
	log.Errorf("deleteRanges is called")
	return w.worker.deleteRanges(ctx, safePoint)
}
