// Copyright 2017 PingCAP, Inc.
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

package gcworker

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/terror"
	tidbutil "github.com/pingcap/tidb/util"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

// GCWorker periodically triggers GC process on tikv server.
type GCWorker struct {
	uuid        string
	desc        string
	store       tikv.Storage
	gcIsRunning bool
	lastFinish  time.Time
	cancel      context.CancelFunc
	done        chan error

	session session.Session
}

// NewGCWorker creates a GCWorker instance.
func NewGCWorker(store tikv.Storage) (tikv.GCHandler, error) {
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
		store:       store,
		gcIsRunning: false,
		lastFinish:  time.Now(),
		done:        make(chan error),
	}
	return worker, nil
}

// Start starts the worker.
func (w *GCWorker) Start() {
	var ctx context.Context
	ctx, w.cancel = context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go w.start(ctx, &wg)
	wg.Wait() // Wait create session finish in worker, some test code depend on this to avoid race.
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
	gcWaitTime           = time.Minute * 1

	gcLifeTimeKey        = "tikv_gc_life_time"
	gcDefaultLifeTime    = time.Minute * 10
	gcSafePointKey       = "tikv_gc_safe_point"
	gcConcurrencyKey     = "tikv_gc_concurrency"
	gcDefaultConcurrency = 2
	gcMinConcurrency     = 1
	gcMaxConcurrency     = 128
	// We don't want gc to sweep out the cached info belong to other processes, like coprocessor.
	gcScanLockLimit = tikv.ResolvedCacheSize / 2
)

var gcSafePointCacheInterval = tikv.GcSafePointCacheInterval

var gcVariableComments = map[string]string{
	gcLeaderUUIDKey:  "Current GC worker leader UUID. (DO NOT EDIT)",
	gcLeaderDescKey:  "Host name and pid of current GC leader. (DO NOT EDIT)",
	gcLeaderLeaseKey: "Current GC worker leader lease. (DO NOT EDIT)",
	gcLastRunTimeKey: "The time when last GC starts. (DO NOT EDIT)",
	gcRunIntervalKey: "GC run interval, at least 10m, in Go format.",
	gcLifeTimeKey:    "All versions within life time will not be collected by GC, at least 10m, in Go format.",
	gcSafePointKey:   "All versions after safe point can be accessed. (DO NOT EDIT)",
	gcConcurrencyKey: "How many go routines used to do GC parallel, [1, 128], default 2",
}

func (w *GCWorker) start(ctx context.Context, wg *sync.WaitGroup) {
	log.Infof("[gc worker] %s start.", w.uuid)

	w.session = createSession(w.store)

	w.tick(ctx) // Immediately tick once to initialize configs.
	wg.Done()

	ticker := time.NewTicker(gcWorkerTickInterval)
	defer ticker.Stop()
	defer func() {
		r := recover()
		if r != nil {
			buf := tidbutil.GetStack()
			log.Errorf("gcWorker %v %s", r, buf)
			metrics.PanicCounter.WithLabelValues(metrics.LabelGCWorker).Inc()
		}
	}()
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
			log.Infof("[gc worker] %s quit.", w.uuid)
			return
		}
	}
}

func createSession(store kv.Storage) session.Session {
	for {
		se, err := session.CreateSession(store)
		if err != nil {
			log.Warnf("[gc worker] create session err: %v", err)
			continue
		}
		// Disable privilege check for gc worker session.
		privilege.BindPrivilegeManager(se, nil)
		se.GetSessionVars().InRestrictedSQL = true
		return se
	}
}

func (w *GCWorker) tick(ctx context.Context) {
	isLeader, err := w.checkLeader()
	if err != nil {
		log.Warnf("[gc worker] check leader err: %v", err)
		gcJobFailureCounter.WithLabelValues("check_leader").Inc()
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
func (w *GCWorker) leaderTick(ctx context.Context) error {
	if w.gcIsRunning {
		return nil
	}

	ok, safePoint, err := w.prepare()
	if err != nil || !ok {
		if err != nil {
			gcJobFailureCounter.WithLabelValues("prepare").Inc()
		}
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

// prepare checks preconditions for starting a GC job. It returns a bool
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
	err = w.saveTime(gcLastRunTimeKey, now)
	if err != nil {
		return false, 0, errors.Trace(err)
	}
	err = w.saveTime(gcSafePointKey, *newSafePoint)
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
	lastRun, err := w.loadTime(gcLastRunTimeKey)
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
	lastSafePoint, err := w.loadTime(gcSafePointKey)
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

func (w *GCWorker) runGCJob(ctx context.Context, safePoint uint64) {
	gcWorkerCounter.WithLabelValues("run_job").Inc()
	err := w.resolveLocks(ctx, safePoint)
	if err != nil {
		log.Errorf("[gc worker] %s resolve locks returns an error %v", w.uuid, errors.ErrorStack(err))
		gcJobFailureCounter.WithLabelValues("resolve_lock").Inc()
		w.done <- errors.Trace(err)
		return
	}
	err = w.deleteRanges(ctx, safePoint)
	if err != nil {
		log.Errorf("[gc worker] %s delete range returns an error %v", w.uuid, errors.ErrorStack(err))
		gcJobFailureCounter.WithLabelValues("delete_range").Inc()
		w.done <- errors.Trace(err)
		return
	}
	err = w.doGC(ctx, safePoint)
	if err != nil {
		log.Errorf("[gc worker] %s do GC returns an error %v", w.uuid, errors.ErrorStack(err))
		w.gcIsRunning = false
		gcJobFailureCounter.WithLabelValues("gc").Inc()
		w.done <- errors.Trace(err)
		return
	}
	w.done <- nil
}

func (w *GCWorker) deleteRanges(ctx context.Context, safePoint uint64) error {
	gcWorkerCounter.WithLabelValues("delete_range").Inc()

	se := createSession(w.store)
	ranges, err := util.LoadDeleteRanges(se, safePoint)
	se.Close()
	if err != nil {
		return errors.Trace(err)
	}

	log.Infof("[gc worker] %s start delete %v ranges", w.uuid, len(ranges))
	startTime := time.Now()
	regions := 0
	for _, r := range ranges {
		startKey, rangeEndKey := r.Range()

		deleteRangeTask := tikv.NewDeleteRangeTask(ctx, w.store, startKey, rangeEndKey)
		err := deleteRangeTask.Execute()

		if err != nil {
			return errors.Trace(err)
		}
		if deleteRangeTask.IsCanceled() {
			return errors.New("[gc worker] gc job canceled")
		}

		regions += deleteRangeTask.CompletedRegions()
		se := createSession(w.store)
		err = util.CompleteDeleteRange(se, r)
		se.Close()
		if err != nil {
			return errors.Trace(err)
		}
	}
	log.Infof("[gc worker] %s finish delete %v ranges, regions: %v, cost time: %s", w.uuid, len(ranges), regions, time.Since(startTime))
	gcHistogram.WithLabelValues("delete_ranges").Observe(time.Since(startTime).Seconds())
	return nil
}

func (w *GCWorker) loadGCConcurrencyWithDefault() (int, error) {
	str, err := w.loadValueFromSysTable(gcConcurrencyKey)
	if err != nil {
		return gcDefaultConcurrency, errors.Trace(err)
	}
	if str == "" {
		err = w.saveValueToSysTable(gcConcurrencyKey, strconv.Itoa(gcDefaultConcurrency))
		if err != nil {
			return gcDefaultConcurrency, errors.Trace(err)
		}
		return gcDefaultConcurrency, nil
	}

	jobConcurrency, err := strconv.Atoi(str)
	if err != nil {
		return gcDefaultConcurrency, err
	}

	if jobConcurrency < gcMinConcurrency {
		jobConcurrency = gcMinConcurrency
	}

	if jobConcurrency > gcMaxConcurrency {
		jobConcurrency = gcMaxConcurrency
	}

	return jobConcurrency, nil
}

func (w *GCWorker) resolveLocks(ctx context.Context, safePoint uint64) error {
	gcWorkerCounter.WithLabelValues("resolve_locks").Inc()

	// for scan lock request, we must return all locks even if they are generated
	// by the same transaction. because gc worker need to make sure all locks have been
	// cleaned.
	req := &tikvrpc.Request{
		Type: tikvrpc.CmdScanLock,
		ScanLock: &kvrpcpb.ScanLockRequest{
			MaxVersion: safePoint,
			Limit:      gcScanLockLimit,
		},
	}

	log.Infof("[gc worker] %s start resolve locks, safePoint: %v.", w.uuid, safePoint)
	startTime := time.Now()
	regions, totalResolvedLocks := 0, 0

	var key []byte
	for {
		select {
		case <-ctx.Done():
			return errors.New("[gc worker] gc job canceled")
		default:
		}

		bo := tikv.NewBackoffer(ctx, tikv.GcResolveLockMaxBackoff)

		req.ScanLock.StartKey = key
		loc, err := w.store.GetRegionCache().LocateKey(bo, key)
		if err != nil {
			return errors.Trace(err)
		}
		resp, err := w.store.SendReq(bo, req, loc.Region, tikv.ReadTimeoutMedium)
		if err != nil {
			return errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return errors.Trace(err)
		}
		if regionErr != nil {
			err = bo.Backoff(tikv.BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return errors.Trace(err)
			}
			continue
		}
		locksResp := resp.ScanLock
		if locksResp == nil {
			return errors.Trace(tikv.ErrBodyMissing)
		}
		if locksResp.GetError() != nil {
			return errors.Errorf("unexpected scanlock error: %s", locksResp)
		}
		locksInfo := locksResp.GetLocks()
		locks := make([]*tikv.Lock, len(locksInfo))
		for i := range locksInfo {
			locks[i] = tikv.NewLock(locksInfo[i])
		}

		ok, err1 := w.store.GetLockResolver().BatchResolveLocks(bo, locks, loc.Region)
		if err1 != nil {
			return errors.Trace(err1)
		}
		if !ok {
			err = bo.Backoff(tikv.BoTxnLock, errors.Errorf("remain locks: %d", len(locks)))
			if err != nil {
				return errors.Trace(err)
			}
			continue
		}

		totalResolvedLocks += len(locks)
		if len(locks) < gcScanLockLimit {
			regions++
			key = loc.EndKey
			if len(key) == 0 {
				break
			}
		} else {
			log.Infof("[gc worker] %s, region %d has more than %d locks", w.uuid, loc.Region.GetID(), gcScanLockLimit)
			gcRegionTooManyLocksCounter.Inc()
			key = locks[len(locks)-1].Key
		}
	}
	log.Infof("[gc worker] %s finish resolve locks, safePoint: %v, regions: %v, total resolved: %v, cost time: %s",
		w.uuid, safePoint, regions, totalResolvedLocks, time.Since(startTime))
	gcHistogram.WithLabelValues("resolve_locks").Observe(time.Since(startTime).Seconds())
	return nil
}

type gcTask struct {
	startKey  []byte
	endKey    []byte
	safePoint uint64
}

type gcTaskWorker struct {
	identifier string
	store      tikv.Storage
	taskCh     chan *gcTask
	wg         *sync.WaitGroup
	// use atomic to read and set
	successRegions *int32
	failedRegions  *int32
}

func newGCTaskWorker(store tikv.Storage, taskCh chan *gcTask, wg *sync.WaitGroup, identifer string, successRegions *int32, failedRegions *int32) *gcTaskWorker {
	return &gcTaskWorker{
		identifer,
		store,
		taskCh,
		wg,
		successRegions,
		failedRegions,
	}
}

func (w *gcTaskWorker) run() {
	defer w.wg.Done()
	for task := range w.taskCh {
		err := w.doGCForRange(task.startKey, task.endKey, task.safePoint)
		if err != nil {
			log.Errorf("[gc worker] %s, gc interupted because get region(%v, %v) error, err %v",
				w.identifier, task.startKey, task.endKey, errors.Trace(err))
		}
	}
}

func (w *gcTaskWorker) doGCForRange(startKey []byte, endKey []byte, safePoint uint64) error {
	var successRegions int32
	var failedRegions int32
	defer func() {
		atomic.AddInt32(w.successRegions, successRegions)
		atomic.AddInt32(w.failedRegions, failedRegions)
		gcActionRegionResultCounter.WithLabelValues("success").Add(float64(successRegions))
		gcActionRegionResultCounter.WithLabelValues("fail").Add(float64(failedRegions))
	}()
	key := startKey
	for {
		bo := tikv.NewBackoffer(context.Background(), tikv.GcOneRegionMaxBackoff)
		loc, err := w.store.GetRegionCache().LocateKey(bo, key)
		if err != nil {
			return errors.Trace(err)
		}

		var regionErr *errorpb.Error
		regionErr, err = w.doGCForRegion(bo, safePoint, loc.Region)

		// we check regionErr here first, because we know 'regionErr' and 'err' should not return together, to keep it to
		// make the process correct.
		if regionErr != nil {
			err = bo.Backoff(tikv.BoRegionMiss, errors.New(regionErr.String()))
			if err == nil {
				continue
			}
		}

		if err != nil {
			log.Warnf("[gc worker] %s gc for range [%v, %v) safepoint: %v, failed, err: %v", w.identifier, startKey, endKey, safePoint, err)
			failedRegions++
		} else {
			successRegions++
		}

		key = loc.EndKey
		if len(key) == 0 || bytes.Compare(key, endKey) >= 0 {
			break
		}
	}

	return nil
}

// these two errors should not return together, for more, see the func 'doGC'
func (w *gcTaskWorker) doGCForRegion(bo *tikv.Backoffer, safePoint uint64, region tikv.RegionVerID) (*errorpb.Error, error) {
	req := &tikvrpc.Request{
		Type: tikvrpc.CmdGC,
		GC: &kvrpcpb.GCRequest{
			SafePoint: safePoint,
		},
	}

	resp, err := w.store.SendReq(bo, req, region, tikv.GCTimeout)
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
		return nil, errors.Trace(tikv.ErrBodyMissing)
	}
	if gcResp.GetError() != nil {
		return nil, errors.Errorf("unexpected gc error: %s", gcResp.GetError())
	}

	return nil, nil
}

func (w *GCWorker) genNextGCTask(bo *tikv.Backoffer, safePoint uint64, key kv.Key) (*gcTask, error) {
	loc, err := w.store.GetRegionCache().LocateKey(bo, key)
	if err != nil {
		return nil, errors.Trace(err)
	}

	task := &gcTask{
		startKey:  key,
		endKey:    loc.EndKey,
		safePoint: safePoint,
	}
	return task, nil
}

func (w *GCWorker) doGC(ctx context.Context, safePoint uint64) error {
	concurrency, err := w.loadGCConcurrencyWithDefault()
	if err != nil {
		log.Errorf("[gc worker] %s failed to load gcConcurrency, err %s", w.uuid, err)
		concurrency = gcDefaultConcurrency
	}

	return w.doGCInternal(ctx, safePoint, concurrency)
}

func (w *GCWorker) doGCInternal(ctx context.Context, safePoint uint64, concurrency int) error {
	gcWorkerCounter.WithLabelValues("do_gc").Inc()

	err := w.saveSafePoint(w.store.GetSafePointKV(), tikv.GcSavedSafePoint, safePoint)
	if err != nil {
		return errors.Trace(err)
	}

	// Sleep to wait for all other tidb instances update their safepoint cache.
	time.Sleep(gcSafePointCacheInterval)

	log.Infof("[gc worker] %s start gc, concurrency %v, safePoint: %v.", w.uuid, concurrency, safePoint)
	startTime := time.Now()
	var successRegions int32
	var failedRegions int32

	ticker := time.NewTicker(gcJobLogTickInterval)
	defer ticker.Stop()

	// Create task queue and start task workers.
	gcTaskCh := make(chan *gcTask, concurrency)
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		w := newGCTaskWorker(w.store, gcTaskCh, &wg, w.uuid, &successRegions, &failedRegions)
		wg.Add(1)
		go w.run()
	}

	var key []byte
	defer func() {
		close(gcTaskCh)
		wg.Wait()
		log.Infof("[gc worker] %s finish gc, safePoint: %v, successful regions: %v, failed regions: %v, total cost time: %s",
			w.uuid, safePoint, atomic.LoadInt32(&successRegions), atomic.LoadInt32(&failedRegions), time.Since(startTime))
		gcHistogram.WithLabelValues("do_gc").Observe(time.Since(startTime).Seconds())
	}()

	for {
		select {
		case <-ctx.Done():
			return errors.New("[gc worker] gc job canceled")
		case <-ticker.C:
			log.Infof("[gc worker] %s gc in process, safePoint: %v, successful regions: %v, failed regions: %v, total cost time: %s",
				w.uuid, safePoint, atomic.LoadInt32(&successRegions), atomic.LoadInt32(&failedRegions), time.Since(startTime))
		default:
		}

		bo := tikv.NewBackoffer(ctx, tikv.GcOneRegionMaxBackoff)
		task, err := w.genNextGCTask(bo, safePoint, key)
		if err != nil {
			return errors.Trace(err)
		}
		if task != nil {
			gcTaskCh <- task
			key = task.endKey
		}

		if len(key) == 0 {
			return nil
		}
	}
}

func (w *GCWorker) checkLeader() (bool, error) {
	gcWorkerCounter.WithLabelValues("check_leader").Inc()
	se := createSession(w.store)
	defer se.Close()

	ctx := context.Background()
	_, err := se.Execute(ctx, "BEGIN")
	if err != nil {
		return false, errors.Trace(err)
	}
	w.session = se
	leader, err := w.loadValueFromSysTable(gcLeaderUUIDKey)
	if err != nil {
		_, err1 := se.Execute(ctx, "ROLLBACK")
		terror.Log(errors.Trace(err1))
		return false, errors.Trace(err)
	}
	log.Debugf("[gc worker] got leader: %s", leader)
	if leader == w.uuid {
		err = w.saveTime(gcLeaderLeaseKey, time.Now().Add(gcWorkerLease))
		if err != nil {
			_, err1 := se.Execute(ctx, "ROLLBACK")
			terror.Log(errors.Trace(err1))
			return false, errors.Trace(err)
		}
		_, err = se.Execute(ctx, "COMMIT")
		if err != nil {
			return false, errors.Trace(err)
		}
		return true, nil
	}

	_, err = se.Execute(ctx, "ROLLBACK")
	terror.Log(errors.Trace(err))

	_, err = se.Execute(ctx, "BEGIN")
	if err != nil {
		return false, errors.Trace(err)
	}
	lease, err := w.loadTime(gcLeaderLeaseKey)
	if err != nil {
		return false, errors.Trace(err)
	}
	if lease == nil || lease.Before(time.Now()) {
		log.Debugf("[gc worker] register %s as leader", w.uuid)
		gcWorkerCounter.WithLabelValues("register_leader").Inc()

		err = w.saveValueToSysTable(gcLeaderUUIDKey, w.uuid)
		if err != nil {
			_, err1 := se.Execute(ctx, "ROLLBACK")
			terror.Log(errors.Trace(err1))
			return false, errors.Trace(err)
		}
		err = w.saveValueToSysTable(gcLeaderDescKey, w.desc)
		if err != nil {
			_, err1 := se.Execute(ctx, "ROLLBACK")
			terror.Log(errors.Trace(err1))
			return false, errors.Trace(err)
		}
		err = w.saveTime(gcLeaderLeaseKey, time.Now().Add(gcWorkerLease))
		if err != nil {
			_, err1 := se.Execute(ctx, "ROLLBACK")
			terror.Log(errors.Trace(err1))
			return false, errors.Trace(err)
		}
		_, err = se.Execute(ctx, "COMMIT")
		if err != nil {
			return false, errors.Trace(err)
		}
		return true, nil
	}
	_, err1 := se.Execute(ctx, "ROLLBACK")
	terror.Log(errors.Trace(err1))
	return false, nil
}

func (w *GCWorker) saveSafePoint(kv tikv.SafePointKV, key string, t uint64) error {
	s := strconv.FormatUint(t, 10)
	err := kv.Put(tikv.GcSavedSafePoint, s)
	if err != nil {
		log.Error("save safepoint failed:", err)
		return errors.Trace(err)
	}
	return nil
}

func (w *GCWorker) saveTime(key string, t time.Time) error {
	err := w.saveValueToSysTable(key, t.Format(gcTimeFormat))
	return errors.Trace(err)
}

func (w *GCWorker) loadTime(key string) (*time.Time, error) {
	str, err := w.loadValueFromSysTable(key)
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
	err := w.saveValueToSysTable(key, d.String())
	return errors.Trace(err)
}

func (w *GCWorker) loadDuration(key string) (*time.Duration, error) {
	str, err := w.loadValueFromSysTable(key)
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

func (w *GCWorker) loadValueFromSysTable(key string) (string, error) {
	ctx := context.Background()
	stmt := fmt.Sprintf(`SELECT HIGH_PRIORITY (variable_value) FROM mysql.tidb WHERE variable_name='%s' FOR UPDATE`, key)
	rs, err := w.session.Execute(ctx, stmt)
	if len(rs) > 0 {
		defer terror.Call(rs[0].Close)
	}
	if err != nil {
		return "", errors.Trace(err)
	}
	chk := rs[0].NewChunk()
	err = rs[0].Next(ctx, chk)
	if err != nil {
		return "", errors.Trace(err)
	}
	if chk.NumRows() == 0 {
		log.Debugf("[gc worker] load kv, %s:nil", key)
		return "", nil
	}
	value := chk.GetRow(0).GetString(0)
	log.Debugf("[gc worker] load kv, %s:%s", key, value)
	return value, nil
}

func (w *GCWorker) saveValueToSysTable(key, value string) error {
	stmt := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
			       ON DUPLICATE KEY
			       UPDATE variable_value = '%[2]s', comment = '%[3]s'`,
		key, value, gcVariableComments[key])
	if w.session == nil {
		return errors.New("[saveValueToSysTable session is nil]")
	}
	_, err := w.session.Execute(context.Background(), stmt)
	log.Debugf("[gc worker] save kv, %s:%s %v", key, value, err)
	return errors.Trace(err)
}

// RunGCJob sends GC command to KV. it is exported for kv api, do not use it with GCWorker at the same time.
func RunGCJob(ctx context.Context, s tikv.Storage, safePoint uint64, identifier string, concurrency int) error {
	gcWorker := &GCWorker{
		store: s,
		uuid:  identifier,
	}

	err := gcWorker.resolveLocks(ctx, safePoint)
	if err != nil {
		return errors.Trace(err)
	}

	if concurrency <= 0 {
		return errors.Errorf("[gc worker] gc concurrency should greater than 0, current concurrency: %v", concurrency)
	}
	err = gcWorker.doGCInternal(ctx, safePoint, concurrency)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// MockGCWorker is for test.
type MockGCWorker struct {
	worker *GCWorker
}

// NewMockGCWorker creates a MockGCWorker instance ONLY for test.
func NewMockGCWorker(store tikv.Storage) (*MockGCWorker, error) {
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
		store:       store,
		gcIsRunning: false,
		lastFinish:  time.Now(),
		done:        make(chan error),
	}
	worker.session, err = session.CreateSession(worker.store)
	if err != nil {
		log.Errorf("initialize MockGCWorker session fail: %s", err)
		return nil, errors.Trace(err)
	}
	privilege.BindPrivilegeManager(worker.session, nil)
	worker.session.GetSessionVars().InRestrictedSQL = true
	return &MockGCWorker{worker: worker}, nil
}

// DeleteRanges call deleteRanges internally, just for test.
func (w *MockGCWorker) DeleteRanges(ctx context.Context, safePoint uint64) error {
	log.Errorf("deleteRanges is called")
	return w.worker.deleteRanges(ctx, safePoint)
}
