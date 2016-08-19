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
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb"
)

// GCWorker periodically triggers GC process on tikv server.
type GCWorker struct {
	uuid    string
	store   *tikvStore
	session tidb.Session
	quit    chan struct{}
	done    chan error
}

// NewGCWorker creates a GCWorker instance.
func NewGCWorker(store *tikvStore) (*GCWorker, error) {
	session, err := tidb.CreateSession(store)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ver, err := store.CurrentVersion()
	if err != nil {
		return nil, errors.Trace(err)
	}
	worker := &GCWorker{
		uuid:    fmt.Sprintf("gcworker_%d", ver.Ver),
		store:   store,
		session: session,
		quit:    make(chan struct{}),
		done:    make(chan error),
	}
	go worker.start(ver.Ver)
	return worker, nil
}

// Close stops backgroud goroutines.
func (w *GCWorker) Close() {
	close(w.quit)
}

const (
	gcWorkerTickInterval = time.Minute
	gcWorkerLease        = time.Minute * 2
	gcRunInterval        = time.Minute * 10
	gcLeaderUUIDKey      = "tikv_gc_leader_uuid"
	gcLeaderLeaseKey     = "tikv_gc_leader_lease"
	gcLeaderLeaseFormat  = "20060102-15:04:05 -0700 MST"
)

func (w *GCWorker) start(ver uint64) {
	log.Infof("[gc worker] %s start.", w.uuid)
	safePoint := ver
	gcIsRunning := false
	ticker := time.NewTicker(gcWorkerTickInterval)
	for {
		select {
		case <-ticker.C:
			isLeader, err := w.checkLeader()
			if err != nil {
				log.Warnf("[gc worker] check leader err: %v", err)
				break
			}
			if isLeader && !gcIsRunning {
				if w.store.oracle.IsExpired(safePoint, uint64(gcRunInterval/time.Millisecond)) {
					gcIsRunning = true
					go w.runGCJob(safePoint)
				}
			}
		case err := <-w.done:
			gcIsRunning = false
			if err != nil {
				log.Errorf("[gc worker] runGCJob error: %v", err)
				break
			}
			ver, err := w.store.CurrentVersion()
			if err != nil {
				log.Errorf("[gc worker] failed get current version: %v", err)
				break
			}
			safePoint = ver.Ver
		case <-w.quit:
			log.Infof("[gc worker] (%s) quit.", w.uuid)
			return
		}
	}
}

func (w *GCWorker) runGCJob(safePoint uint64) {
	log.Infof("[gc worker] %s starts GC job, safePoint: %v", w.uuid, safePoint)
	err := w.resolveLocks(safePoint)
	if err != nil {
		w.done <- errors.Trace(err)
	}
	err = w.doGC(safePoint)
	if err != nil {
		w.done <- errors.Trace(err)
	}
	w.done <- nil
}

func (w *GCWorker) resolveLocks(safePoint uint64) error {
	req := &kvrpcpb.Request{
		Type: kvrpcpb.MessageType_CmdScanLock.Enum(),
		CmdScanLockReq: &kvrpcpb.CmdScanLockRequest{
			MaxVersion: proto.Uint64(safePoint),
		},
	}
	bo := NewBackoffer(gcResolveLockMaxBackoff)

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

		region, err := w.store.regionCache.GetRegion(bo, key)
		if err != nil {
			return errors.Trace(err)
		}
		resp, err := w.store.SendKVReq(bo, req, region.VerID())
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
			return errors.Trace(err)
		}
		if !ok {
			err = bo.Backoff(boTxnLock, err1)
			if err != nil {
				return errors.Trace(err)
			}
			continue
		}
		regions++
		totalResolvedLocks += len(locks)
		key = region.EndKey()
		if len(key) == 0 {
			break
		}
	}
	log.Infof("[gc worker] %s finish resolve locks, safePoint: %v, regions: %v, total resolved: %v, cost time: %s", w.uuid, safePoint, regions, totalResolvedLocks, time.Now().Sub(startTime))
	return nil
}

func (w *GCWorker) doGC(safePoint uint64) error {
	req := &kvrpcpb.Request{
		Type: kvrpcpb.MessageType_CmdGC.Enum(),
		CmdGcReq: &kvrpcpb.CmdGCRequest{
			SafePoint: proto.Uint64(safePoint),
		},
	}
	bo := NewBackoffer(gcMaxBackoff)

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

		region, err := w.store.regionCache.GetRegion(bo, key)
		if err != nil {
			return errors.Trace(err)
		}
		resp, err := w.store.SendKVReq(bo, req, region.VerID())
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
		key = region.EndKey()
		if len(key) == 0 {
			break
		}
	}
	log.Infof("[gc worker] %s finish gc, safePoint: %v, regions: %v, cost time: %s", w.uuid, safePoint, regions, time.Now().Sub(startTime))
	return nil
}

func (w *GCWorker) checkLeader() (bool, error) {
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
		err = w.updateLease()
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
	lease, err := w.loadLease()
	if err != nil || lease.Before(time.Now()) {
		log.Debugf("[gc worker] register %s as leader", w.uuid)
		err = w.saveValueToSysTable(gcLeaderUUIDKey, w.uuid)
		if err != nil {
			w.session.Execute("ROLLBACK")
			return false, errors.Trace(err)
		}
		err = w.updateLease()
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

func (w *GCWorker) updateLease() error {
	lease := time.Now().Add(gcWorkerTickInterval * 2).Format(gcLeaderLeaseFormat)
	log.Debugf("[gc worker] update leader lease to %s for worker %s", lease, w.uuid)
	err := w.saveValueToSysTable(gcLeaderLeaseKey, lease)
	return errors.Trace(err)
}

func (w *GCWorker) loadLease() (time.Time, error) {
	var t time.Time
	str, err := w.loadValueFromSysTable(gcLeaderLeaseKey)
	if err != nil {
		return t, errors.Trace(err)
	}
	lease, err := time.Parse(gcLeaderLeaseFormat, str)
	if err != nil {
		return t, errors.Trace(err)
	}
	log.Debugf("[gc worker] load lease: %s", lease)
	return lease, nil
}

func (w *GCWorker) loadValueFromSysTable(key string) (string, error) {
	stmt := fmt.Sprintf(`SELECT (variable_value) FROM mysql.tidb WHERE variable_name='%s' FOR UPDATE`, key)
	rs, err := w.session.Execute(stmt)
	if err != nil {
		return "", errors.Trace(err)
	}
	row, err := rs[0].Next()
	if err != nil {
		return "", errors.Trace(err)
	}
	if row == nil {
		return "", nil
	}
	return row.Data[0].GetString(), nil
}

func (w *GCWorker) saveValueToSysTable(key, value string) error {
	stmt := fmt.Sprintf(`INSERT INTO mysql.tidb (variable_name, variable_value) VALUES ('%s', '%s') ON DUPLICATE KEY UPDATE variable_value = '%s'`, key, value, value)
	_, err := w.session.Execute(stmt)
	return errors.Trace(err)
}
