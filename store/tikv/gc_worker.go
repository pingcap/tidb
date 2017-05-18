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
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/util/sqlexec"
	goctx "golang.org/x/net/context"
)

// GCWorker periodically triggers GC process on tikv server.
type GCWorker struct {
	uuid        string
	desc        string
	store       *tikvStore
	session     tidb.Session
	gcIsRunning bool
	lastFinish  time.Time
	quit        chan struct{}
	done        chan error
}

// NewGCWorker creates a GCWorker instance.
func NewGCWorker(store kv.Storage) (*GCWorker, error) {
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
		quit:        make(chan struct{}),
		done:        make(chan error),
	}
	go worker.start()
	return worker, nil
}

// Close stops background goroutines.
func (w *GCWorker) Close() {
	close(w.quit)
}

const (
	gcTimeFormat = "20060102-15:04:05 -0700 MST"

	gcWorkerTickInterval = time.Minute
	gcWorkerLease        = time.Minute * 2
	gcLeaderUUIDKey      = "tikv_gc_leader_uuid"
	gcLeaderDescKey      = "tikv_gc_leader_desc"
	gcLeaderLeaseKey     = "tikv_gc_leader_lease"

	gcLastRunTimeKey     = "tikv_gc_last_run_time"
	gcRunIntervalKey     = "tikv_gc_run_interval"
	gcDefaultRunInterval = time.Minute * 10
	gcWaitTime           = time.Minute * 10

	gcLifeTimeKey     = "tikv_gc_life_time"
	gcDefaultLifeTime = time.Minute * 10
	gcSafePointKey    = "tikv_gc_safe_point"
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

func (w *GCWorker) start() {
	log.Infof("[gc worker] %s start.", w.uuid)
	ticker := time.NewTicker(gcWorkerTickInterval)
	for {
		select {
		case <-ticker.C:
			if w.session == nil {
				if !w.storeIsBootstrapped() {
					break
				}
				var err error
				w.session, err = tidb.CreateSession(w.store)
				if err != nil {
					log.Warnf("[gc worker] create session err: %v", err)
					break
				}
				// Disable privilege check for gc worker session.
				privilege.BindPrivilegeManager(w.session, nil)
			}

			isLeader, err := w.checkLeader()
			if err != nil {
				log.Warnf("[gc worker] check leader err: %v", err)
				break
			}
			if isLeader {
				err = w.leaderTick()
				if err != nil {
					log.Warnf("[gc worker] leader tick err: %v", err)
				}
			} else {
				// Config metrics should always be updated by leader.
				gcConfigGauge.WithLabelValues(gcRunIntervalKey).Set(0)
				gcConfigGauge.WithLabelValues(gcLifeTimeKey).Set(0)
			}
		case err := <-w.done:
			w.gcIsRunning = false
			w.lastFinish = time.Now()
			if err != nil {
				log.Errorf("[gc worker] runGCJob error: %v", err)
				break
			}
		case <-w.quit:
			log.Infof("[gc worker] (%s) quit.", w.uuid)
			return
		}
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
func (w *GCWorker) leaderTick() error {
	if w.gcIsRunning {
		return nil
	}
	// When the worker is just started, or an old GC job has just finished,
	// wait a while before starting a new job.
	if time.Since(w.lastFinish) < gcWaitTime {
		return nil
	}

	ok, safePoint, err := w.prepare()
	if err != nil || !ok {
		return errors.Trace(err)
	}

	w.gcIsRunning = true
	log.Infof("[gc worker] %s starts GC job, safePoint: %v", w.uuid, safePoint)
	go w.runGCJob(safePoint)
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
	gcConfigGauge.WithLabelValues(gcRunIntervalKey).Set(float64(runInterval.Seconds()))
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
	gcConfigGauge.WithLabelValues(gcLifeTimeKey).Set(float64(lifeTime.Seconds()))
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

func (w *GCWorker) runGCJob(safePoint uint64) {
	gcWorkerCounter.WithLabelValues("run_job").Inc()

	err := w.resolveLocks(safePoint)
	if err != nil {
		w.done <- errors.Trace(err)
		return
	}
	err = w.DoGC(safePoint)
	if err != nil {
		w.done <- errors.Trace(err)
	}
	w.done <- nil
}

func (w *GCWorker) resolveLocks(safePoint uint64) error {
	gcWorkerCounter.WithLabelValues("resolve_locks").Inc()

	req := &kvrpcpb.Request{
		Type: kvrpcpb.MessageType_CmdScanLock,
		CmdScanLockReq: &kvrpcpb.CmdScanLockRequest{
			MaxVersion: safePoint,
		},
	}
	bo := NewBackoffer(gcResolveLockMaxBackoff, goctx.Background())

	log.Infof("[gc worker] %s start resolve locks, safePoint: %v.", w.uuid, safePoint)
	startTime := time.Now()
	regions, totalResolvedLocks := 0, 0

	var key []byte
	for {
		select {
		case <-w.quit:
			return errors.New("[gc worker] gc job canceled")
		default:
		}

		loc, err := w.store.regionCache.LocateKey(bo, key)
		if err != nil {
			return errors.Trace(err)
		}
		resp, err := w.store.SendKVReq(bo, req, loc.Region, readTimeoutMedium)
		if err != nil {
			return errors.Trace(err)
		}
		if regionErr := resp.GetRegionError(); regionErr != nil {
			err = bo.Backoff(boRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return errors.Trace(err)
			}
			continue
		}
		locksResp := resp.GetCmdScanLockResp()
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
		ok, err1 := w.store.lockResolver.ResolveLocks(bo, locks)
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
	log.Infof("[gc worker] %s finish resolve locks, safePoint: %v, regions: %v, total resolved: %v, cost time: %s", w.uuid, safePoint, regions, totalResolvedLocks, time.Since(startTime))
	gcHistogram.WithLabelValues("resolve_locks").Observe(time.Since(startTime).Seconds())
	return nil
}

// DoGC sends GC command to KV, it is exported for testing purpose.
func (w *GCWorker) DoGC(safePoint uint64) error {
	gcWorkerCounter.WithLabelValues("do_gc").Inc()

	req := &kvrpcpb.Request{
		Type: kvrpcpb.MessageType_CmdGC,
		CmdGcReq: &kvrpcpb.CmdGCRequest{
			SafePoint: safePoint,
		},
	}
	bo := NewBackoffer(gcMaxBackoff, goctx.Background())

	log.Infof("[gc worker] %s start gc, safePoint: %v.", w.uuid, safePoint)
	startTime := time.Now()
	regions := 0

	var key []byte
	for {
		select {
		case <-w.quit:
			return errors.New("[gc worker] gc job canceled")
		default:
		}

		loc, err := w.store.regionCache.LocateKey(bo, key)
		if err != nil {
			return errors.Trace(err)
		}
		resp, err := w.store.SendKVReq(bo, req, loc.Region, readTimeoutLong)
		if err != nil {
			return errors.Trace(err)
		}
		if regionErr := resp.GetRegionError(); regionErr != nil {
			err = bo.Backoff(boRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return errors.Trace(err)
			}
			continue
		}
		gcResp := resp.GetCmdGcResp()
		if gcResp == nil {
			return errors.Trace(errBodyMissing)
		}
		if gcResp.GetError() != nil {
			return errors.Errorf("unexpected gc error: %s", gcResp.GetError())
		}
		regions++
		key = loc.EndKey
		if len(key) == 0 {
			break
		}
	}
	log.Infof("[gc worker] %s finish gc, safePoint: %v, regions: %v, cost time: %s", w.uuid, safePoint, regions, time.Since(startTime))
	gcHistogram.WithLabelValues("do_gc").Observe(time.Since(startTime).Seconds())
	return nil
}

func (w *GCWorker) checkLeader() (bool, error) {
	gcWorkerCounter.WithLabelValues("check_leader").Inc()

	_, err := w.session.Execute("BEGIN")
	if err != nil {
		return false, errors.Trace(err)
	}
	leader, err := w.loadValueFromSysTable(gcLeaderUUIDKey)
	if err != nil {
		w.session.Execute("ROLLBACK")
		return false, errors.Trace(err)
	}
	log.Debugf("[gc worker] got leader: %s", leader)
	if leader == w.uuid {
		err = w.saveTime(gcLeaderLeaseKey, time.Now().Add(gcWorkerLease))
		if err != nil {
			w.session.Execute("ROLLBACK")
			return false, errors.Trace(err)
		}
		_, err = w.session.Execute("COMMIT")
		if err != nil {
			return false, errors.Trace(err)
		}
		return true, nil
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
			w.session.Execute("ROLLBACK")
			return false, errors.Trace(err)
		}
		err = w.saveValueToSysTable(gcLeaderDescKey, w.desc)
		if err != nil {
			w.session.Execute("ROLLBACK")
			return false, errors.Trace(err)
		}
		err = w.saveTime(gcLeaderLeaseKey, time.Now().Add(gcWorkerLease))
		if err != nil {
			w.session.Execute("ROLLBACK")
			return false, errors.Trace(err)
		}
		_, err = w.session.Execute("COMMIT")
		if err != nil {
			return false, errors.Trace(err)
		}
		return true, nil
	}
	w.session.Execute("ROLLBACK")
	return false, nil
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
	stmt := fmt.Sprintf(`SELECT (variable_value) FROM mysql.tidb WHERE variable_name='%s' FOR UPDATE`, key)
	rs, err := w.session.(sqlexec.SQLExecutor).Execute(stmt)
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

func (w *GCWorker) saveValueToSysTable(key, value string) error {
	stmt := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
			       ON DUPLICATE KEY
			       UPDATE variable_value = '%[2]s', comment = '%[3]s'`,
		key, value, gcVariableComments[key])
	_, err := w.session.(sqlexec.SQLExecutor).Execute(stmt)
	log.Debugf("[gc worker] save kv, %s:%s %v", key, value, err)
	return errors.Trace(err)
}
