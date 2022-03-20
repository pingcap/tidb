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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kv

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

const (
	chanBufferSize             = 256
	undeletedStartTsBufferSize = 16
	// TimeToPrintLongTimeInternalTxn is the duration if the internal transaction lasts more than it, TiDB prints
	// a log message
	TimeToPrintLongTimeInternalTxn = time.Minute
)

var globalInnerTxnTsBox atomic.Value

type innerTxnStartTsBox struct {
	wg                  sync.WaitGroup
	chanToStoreStartTS  chan uint64
	chanToDeleteStartTS chan uint64

	innerTxnTsBoxRun          atomic.Value
	innerTxnTsBoxRoutineCount int32

	innerTSLock        sync.Mutex
	innerTxnStartTsMap map[uint64]uint64

	// `storeInnerTxnStartTsLoop` and `deleteInnerTxnStartTsLoop` run asynchronously,
	// It can't ensure `storeInnerTxnStartTsLoop` receives startTS before `deleteInnerTxnStartTsLoop`,
	// though `RunInNewTxn`send startTS to `storeInnerTxnStartTsLoop` firstly. If `deleteInnerTxnStartTsLoop`
	// recevied startTS before `storeInnerTxnStartTsLoop`, the `startTS` couldn't be deleted from `innerTxnStartTsMap`,
	// and GC safepoint can't be advanced properly.
	undeletedTsLock        sync.Mutex
	chanToProcUndelStartTS chan uint64
	undeletedStartTsMap    map[uint64]uint64
}

// InitInnerTxnStartTsBox initializes globalInnerTxnTsBox
func InitInnerTxnStartTsBox() {
	iTxnTsBox := &innerTxnStartTsBox{
		chanToStoreStartTS:     make(chan uint64, chanBufferSize),
		chanToDeleteStartTS:    make(chan uint64, chanBufferSize),
		innerTxnStartTsMap:     make(map[uint64]uint64, chanBufferSize),
		chanToProcUndelStartTS: make(chan uint64, undeletedStartTsBufferSize),
		undeletedStartTsMap:    make(map[uint64]uint64, undeletedStartTsBufferSize),
	}
	globalInnerTxnTsBox.Store(iTxnTsBox)
	iTxnTsBox.wg.Add(3)
	iTxnTsBox.innerTxnTsBoxRun.Store(true)
	go iTxnTsBox.storeInnerTxnStartTsLoop()
	go iTxnTsBox.deleteInnerTxnStartTsLoop()
	go iTxnTsBox.processUndeletedStartTsLoop()
}

func (ib *innerTxnStartTsBox) resetGlobalInnerTxnTsBox() {
	ib.innerTxnTsBoxRun.Store(false)
}

func (ib *innerTxnStartTsBox) isBoxRunning() bool {
	return ib.innerTxnTsBoxRun.Load().(bool)
}

func (ib *innerTxnStartTsBox) storeInnerTxnStartTsLoop() {
	logutil.BgLogger().Info("storeInnerTxnStartTsLoop started")
	defer ib.wg.Done()
	atomic.AddInt32(&ib.innerTxnTsBoxRoutineCount, 1)
	for startTS := range ib.chanToStoreStartTS {
		failpoint.Inject("mockDelayStoreInnerTxnStartTs", func() {
			logutil.BgLogger().Info("Enable mockDelayStoreInnerTxnStartTs ...")
			time.Sleep(100 * time.Millisecond)
		})

		ib.storeInnerTxnTS(startTS)
	}
	atomic.AddInt32(&ib.innerTxnTsBoxRoutineCount, -1)
	logutil.BgLogger().Info("storeInnerTxnStartTsLoop exited")
}

func (ib *innerTxnStartTsBox) deleteInnerTxnStartTsLoop() {
	logutil.BgLogger().Info("deleteInnerTxnStartTsLoop started")
	defer ib.wg.Done()
	atomic.AddInt32(&ib.innerTxnTsBoxRoutineCount, 1)
	for startTS := range ib.chanToDeleteStartTS {
		ib.deleteInnerTxnTS(startTS)
	}
	atomic.AddInt32(&ib.innerTxnTsBoxRoutineCount, -1)
	logutil.BgLogger().Info("deleteInnerTxnStartTsLoop exited")
}

func (ib *innerTxnStartTsBox) processUndeletedStartTsLoop() {
	logutil.BgLogger().Info("processUndeletedStartTsLoop started")
	defer ib.wg.Done()
	defer func() {
		atomic.AddInt32(&ib.innerTxnTsBoxRoutineCount, -1)
		logutil.BgLogger().Info("processUndeletedStartTsLoop exited")
	}()

	atomic.AddInt32(&ib.innerTxnTsBoxRoutineCount, 1)
	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()
	for {
		select {
		case startTS, ok := <-ib.chanToProcUndelStartTS:
			if !ok {
				return
			}
			ib.putToUndeletedTSMap(startTS)
		case <-ticker.C:
			ib.processUndeletedStartTS()
		}
	}
}

func (ib *innerTxnStartTsBox) processUndeletedStartTS() {
	ib.undeletedTsLock.Lock()
	for startTS := range ib.undeletedStartTsMap { // infinite loop ??
		if ib.realDeleteInnerTxnTS(startTS) {
			// modify the map when traverse it, refer to https://go.dev/doc/effective_go#for
			delete(ib.undeletedStartTsMap, startTS)
			logutil.BgLogger().Info("[processUndeletedStartTsLoop] processed an internal transaction start timestamp ",
				zap.Uint64("startTS", startTS))
		}
	}
	ib.undeletedTsLock.Unlock()
}

// close close chan chanToStoreStartTs and chanToDeleteStartTS
func (ib *innerTxnStartTsBox) close() {
	ib.resetGlobalInnerTxnTsBox()
	close(ib.chanToStoreStartTS)
	close(ib.chanToDeleteStartTS)
	close(ib.chanToProcUndelStartTS)
	ib.wg.Wait()
}

// CloseGlobalInnerTxnTsBox close chanels
func CloseGlobalInnerTxnTsBox() {
	logutil.BgLogger().Info("Begin to close GlobalInnerTxnTsBox")
	ib := getGlobalInnerTxnTsBox()
	if ib == nil {
		return
	}
	if !ib.isBoxRunning() {
		logutil.BgLogger().Info("GlobalInnerTxnTsBox is not running, doesn't need to close")
		return
	}
	ib.close()
}

func getGlobalInnerTxnTsBox() *innerTxnStartTsBox {
	v := globalInnerTxnTsBox.Load()
	if v == nil {
		return nil
	}
	ib := v.(*innerTxnStartTsBox)
	return ib
}

// wrapStoreInterTxnTS is the entry function used to store the startTS of an internal transaction to globalInnerTxnTsBox
// @param startTS	the startTS of the internal transaction produced by `RunInNewTxn`
func wrapStoreInterTxnTS(startTS uint64) {
	ib := getGlobalInnerTxnTsBox()
	if ib == nil {
		logutil.BgLogger().Info("Fail to store internal txn startTS, globalInnerTxnTsBox is nil")
		return
	}
	if !ib.isBoxRunning() {
		return
	}
	ib.chanToStoreStartTS <- startTS
}

func (ib *innerTxnStartTsBox) storeInnerTxnTS(startTS uint64) {
	ib.innerTSLock.Lock()
	ib.innerTxnStartTsMap[startTS] = startTS
	ib.innerTSLock.Unlock()
}

// wrapDeleteInterTxnTS delete the startTS from globalInnerTxnTsBox when the transaciotn is commted
// @param startTS	the startTS of the internal transaction produced by `RunInNewTxn`
func wrapDeleteInterTxnTS(startTS uint64) {
	ib := getGlobalInnerTxnTsBox()
	if ib == nil {
		logutil.BgLogger().Info("Fail to delete internal txn startTS, globalInnerTxnTsBox is nil")
		return
	}
	if !ib.isBoxRunning() {
		return
	}
	ib.chanToDeleteStartTS <- startTS
}

func (ib *innerTxnStartTsBox) deleteInnerTxnTS(startTS uint64) {
	deleted := ib.realDeleteInnerTxnTS(startTS)
	if !deleted {
		ib.chanToProcUndelStartTS <- startTS
	}
}

func (ib *innerTxnStartTsBox) realDeleteInnerTxnTS(startTS uint64) bool {
	ib.innerTSLock.Lock()
	defer ib.innerTSLock.Unlock()
	if _, ok := ib.innerTxnStartTsMap[startTS]; ok {
		delete(ib.innerTxnStartTsMap, startTS)
		return true
	}
	return false
}

func (ib *innerTxnStartTsBox) putToUndeletedTSMap(startTS uint64) {
	ib.undeletedTsLock.Lock()
	ib.undeletedStartTsMap[startTS] = startTS
	ib.undeletedTsLock.Unlock()
}

// WrapGetMinStartTs get the min StatTS between startTSLowerLimit and curMinStartTS in globalInnerTxnTsBox
func WrapGetMinStartTs(now time.Time, startTSLowerLimit uint64,
	curMinStartTS uint64) uint64 {
	ib := getGlobalInnerTxnTsBox()
	if ib == nil {
		logutil.BgLogger().Info("Fail to get internal txn startTS, globalInnerTxnTsBox is nil")
		return curMinStartTS
	}
	if !ib.isBoxRunning() {
		return curMinStartTS
	}
	return ib.getMinStartTs(now, startTSLowerLimit, curMinStartTS)
}

func (ib *innerTxnStartTsBox) getMinStartTs(now time.Time, startTSLowerLimit uint64,
	curMinStartTS uint64) uint64 {
	minStartTS := curMinStartTS
	ib.innerTSLock.Lock()
	for _, innerTS := range ib.innerTxnStartTsMap {
		innerTxnStartTime := oracle.GetTimeFromTS(innerTS)
		if now.Sub(innerTxnStartTime) > TimeToPrintLongTimeInternalTxn {
			logutil.BgLogger().Info("An internal transaction running by RunInNewTxn lasts long time",
				zap.Duration("time", now.Sub(innerTxnStartTime)))
		}

		if innerTS > startTSLowerLimit && innerTS < minStartTS {
			minStartTS = innerTS
		}
	}
	ib.innerTSLock.Unlock()
	return minStartTS
}

// RunInNewTxn will run the f in a new transaction environment.
func RunInNewTxn(ctx context.Context, store Storage, retryable bool, f func(ctx context.Context, txn Transaction) error) error {
	var (
		err           error
		originalTxnTS uint64
		txn           Transaction
	)

	defer func() {
		wrapDeleteInterTxnTS(originalTxnTS)
	}()

	for i := uint(0); i < maxRetryCnt; i++ {
		txn, err = store.Begin()
		if err != nil {
			logutil.BgLogger().Error("RunInNewTxn", zap.Error(err))
			return err
		}

		// originalTxnTS is used to trace the original transaction when the function is retryable.
		if i == 0 {
			originalTxnTS = txn.StartTS()
			wrapStoreInterTxnTS(originalTxnTS)
		}

		err = f(ctx, txn)
		if err != nil {
			err1 := txn.Rollback()
			terror.Log(err1)
			if retryable && IsTxnRetryableError(err) {
				logutil.BgLogger().Warn("RunInNewTxn",
					zap.Uint64("retry txn", txn.StartTS()),
					zap.Uint64("original txn", originalTxnTS),
					zap.Error(err))
				continue
			}
			return err
		}

		failpoint.Inject("mockCommitErrorInNewTxn", func(val failpoint.Value) {
			if v := val.(string); len(v) > 0 {
				switch v {
				case "retry_once":
					if i == 0 {
						err = ErrTxnRetryable
					}
				case "no_retry":
					failpoint.Return(errors.New("mock commit error"))
				}
			}
		})

		if err == nil {
			err = txn.Commit(ctx)
			if err == nil {
				break
			}
		}
		if retryable && IsTxnRetryableError(err) {
			logutil.BgLogger().Warn("RunInNewTxn",
				zap.Uint64("retry txn", txn.StartTS()),
				zap.Uint64("original txn", originalTxnTS),
				zap.Error(err))
			BackOff(i)
			continue
		}
		return err
	}
	return err
}

var (
	// maxRetryCnt represents maximum retry times in RunInNewTxn.
	maxRetryCnt uint = 100
	// retryBackOffBase is the initial duration, in microsecond, a failed transaction stays dormancy before it retries
	retryBackOffBase = 1
	// retryBackOffCap is the max amount of duration, in microsecond, a failed transaction stays dormancy before it retries
	retryBackOffCap = 100
)

// BackOff Implements exponential backoff with full jitter.
// Returns real back off time in microsecond.
// See http://www.awsarchitectureblog.com/2015/03/backoff.html.
func BackOff(attempts uint) int {
	upper := int(math.Min(float64(retryBackOffCap), float64(retryBackOffBase)*math.Pow(2.0, float64(attempts))))
	sleep := time.Duration(rand.Intn(upper)) * time.Millisecond // #nosec G404
	time.Sleep(sleep)
	return int(sleep)
}
