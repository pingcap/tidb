// Copyright 2023 PingCAP, Inc.
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

package sqlkiller

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

type killSignal = uint32

// KillSignal types.
const (
	UnspecifiedKillSignal killSignal = iota
	QueryInterrupted
	MaxExecTimeExceeded
	QueryMemoryExceeded
	ServerMemoryExceeded
	RunawayQueryExceeded
	KilledByMemArbitrator
	// When you add a new signal, you should also modify store/driver/error/ToTiDBErr,
	// so that errors in client can be correctly converted to tidb errors.
)

// SQLKiller is used to kill a query.
type SQLKiller struct {
	Finish    func()
	killEvent struct {
		ch   chan struct{}
		desc string
		// Mutex serializes Signal writers with ch, desc, and triggered.
		sync.Mutex
		triggered bool
	}
	ConnID atomic.Uint64
	// FinishFuncLock is used to ensure that Finish is not called and modified at the same time.
	// An external call to the Finish function only allows when the main goroutine to be in the writeResultSet process.
	// When the main goroutine exits the writeResultSet process, the Finish function will be cleared.
	FinishFuncLock sync.Mutex
	Signal         killSignal
	// InWriteResultSet is used to indicate whether the query is currently calling clientConn.writeResultSet().
	// If the query is in writeResultSet and Finish() can acquire rs.finishLock, we can assume the query is waiting for the client to receive data from the server over network I/O.
	InWriteResultSet atomic.Bool

	lastCheckTime     atomic.Pointer[time.Time]
	IsConnectionAlive atomic.Pointer[func() bool]
	// ConnCancel cancels the current execution context so that in-flight RPCs
	// (e.g. a coprocessor request blocked in TiKV) are aborted promptly, which
	// sends a stream reset to TiKV instead of waiting for CoprReqTimeout. It is
	// installed together with IsConnectionAlive and invoked when the connection
	// is detected dead.
	ConnCancel atomic.Pointer[func()]
}

// GetKillEventChan returns a recv chan which will be closed when the kill signal is sent.
func (killer *SQLKiller) GetKillEventChan() <-chan struct{} {
	killer.killEvent.Lock()
	defer killer.killEvent.Unlock()

	if killer.killEvent.ch != nil {
		return killer.killEvent.ch
	}

	killer.killEvent.ch = make(chan struct{})
	if killer.killEvent.triggered {
		close(killer.killEvent.ch)
	}

	return killer.killEvent.ch
}

func (killer *SQLKiller) triggerKillEventLocked() {
	if killer.killEvent.triggered {
		return
	}

	if killer.killEvent.ch != nil {
		close(killer.killEvent.ch)
	}
	killer.killEvent.triggered = true
}

func (killer *SQLKiller) resetKillEventLocked() {
	if !killer.killEvent.triggered && killer.killEvent.ch != nil {
		close(killer.killEvent.ch)
	}
	killer.killEvent.ch = nil
	killer.killEvent.triggered = false
	killer.killEvent.desc = ""
}

// SendKillSignalWithKillEventReason sets the reason for the kill event and sends a kill signal.
func (killer *SQLKiller) SendKillSignalWithKillEventReason(killSignal killSignal, desc string) {
	killer.killEvent.Lock()
	killer.killEvent.desc = desc
	signalSent, eventDesc := killer.sendKillSignalLocked(killSignal)
	killer.triggerKillEventLocked()
	killer.killEvent.Unlock()

	if signalSent {
		killer.logKillSignal(killSignal, eventDesc)
	}
}

func (killer *SQLKiller) sendKillSignal(reason killSignal) {
	killer.killEvent.Lock()
	signalSent, eventDesc := killer.sendKillSignalLocked(reason)
	killer.killEvent.Unlock()

	if signalSent {
		killer.logKillSignal(reason, eventDesc)
	}
}

func (killer *SQLKiller) sendKillSignalLocked(reason killSignal) (bool, string) {
	if atomic.CompareAndSwapUint32(&killer.Signal, 0, reason) {
		// Cancel the execution context so that any in-flight RPC (e.g. a
		// coprocessor request already sent to TiKV) is aborted promptly via a
		// gRPC stream reset, instead of blocking until CoprReqTimeout. The
		// polled Killed flag only aborts at task/backoff boundaries, so a
		// request stuck inside a single RPC would otherwise not observe the
		// kill until the RPC deadline. This runs once, guarded by the CAS above.
		if cancel := killer.ConnCancel.Load(); cancel != nil {
			(*cancel)()
		}
		return true, killer.killEvent.desc
	}
	return false, ""
}

func (killer *SQLKiller) logKillSignal(reason killSignal, desc string) {
	err := killer.getKillError(reason, desc)
	logutil.BgLogger().Warn("kill initiated", zap.Uint64("connection ID", killer.ConnID.Load()), zap.String("reason", err.Error()))
}

// SendKillSignal sends a kill signal to the query.
func (killer *SQLKiller) SendKillSignal(reason killSignal) {
	killer.killEvent.Lock()
	signalSent, eventDesc := killer.sendKillSignalLocked(reason)
	killer.triggerKillEventLocked()
	killer.killEvent.Unlock()

	if signalSent {
		failpoint.InjectCall("beforeLogKillSignal")
		killer.logKillSignal(reason, eventDesc)
	}
}

// GetKillSignal gets the kill signal.
func (killer *SQLKiller) GetKillSignal() killSignal {
	return atomic.LoadUint32(&killer.Signal)
}

// getKillError gets the error according to the kill signal.
func (killer *SQLKiller) getKillError(status killSignal, desc string) error {
	switch status {
	case QueryInterrupted:
		return exeerrors.ErrQueryInterrupted.GenWithStackByArgs()
	case MaxExecTimeExceeded:
		return exeerrors.ErrMaxExecTimeExceeded.GenWithStackByArgs()
	case QueryMemoryExceeded:
		return exeerrors.ErrMemoryExceedForQuery.GenWithStackByArgs(killer.ConnID.Load())
	case ServerMemoryExceeded:
		return exeerrors.ErrMemoryExceedForInstance.GenWithStackByArgs(killer.ConnID.Load())
	case RunawayQueryExceeded:
		return exeerrors.ErrResourceGroupQueryRunawayInterrupted.FastGenByArgs("runaway exceed tidb side")
	case KilledByMemArbitrator:
		return exeerrors.ErrQueryExecStopped.GenWithStackByArgs(desc, killer.ConnID.Load())
	default:
	}
	return nil
}

// FinishResultSet is used to close the result set.
// If a kill signal is sent but the SQL query is stuck in the network stack while writing packets to the client,
// encountering some bugs that cause it to hang, or failing to detect the kill signal, we can call Finish to release resources used during the SQL execution process.
func (killer *SQLKiller) FinishResultSet() {
	killer.FinishFuncLock.Lock()
	defer killer.FinishFuncLock.Unlock()
	if killer.Finish != nil {
		killer.Finish()
	}
}

// SetFinishFunc sets the finish function.
func (killer *SQLKiller) SetFinishFunc(fn func()) {
	killer.FinishFuncLock.Lock()
	defer killer.FinishFuncLock.Unlock()
	killer.Finish = fn
}

// ClearFinishFunc clears the finish function.1
func (killer *SQLKiller) ClearFinishFunc() {
	killer.FinishFuncLock.Lock()
	defer killer.FinishFuncLock.Unlock()
	killer.Finish = nil
}

// HandleSignal handles the kill signal and return the error.
func (killer *SQLKiller) HandleSignal() error {
	failpoint.Inject("randomPanic", func(val failpoint.Value) {
		if p, ok := val.(int); ok {
			if rand.Float64() > (float64)(p)/1000 {
				if killer.ConnID.Load() != 0 {
					targetStatus := rand.Int31n(5)
					killer.killEvent.Lock()
					atomic.StoreUint32(&killer.Signal, uint32(targetStatus))
					killer.killEvent.Unlock()
				}
			}
		}
	})

	// Checks if the connection is alive.
	// For performance reasons, the check interval should be at least `checkConnectionAliveDur`(1 second).
	fn := killer.IsConnectionAlive.Load()
	if fn != nil {
		var checkConnectionAliveDur time.Duration = time.Second
		now := time.Now()
		if intest.InTest {
			checkConnectionAliveDur = time.Millisecond
		}
		lastCheckTime := killer.lastCheckTime.Load()
		if lastCheckTime == nil {
			killer.lastCheckTime.Store(&now)
		} else if now.Sub(*lastCheckTime) > checkConnectionAliveDur {
			killer.lastCheckTime.Store(&now)
			if !(*fn)() {
				killer.sendKillSignal(QueryInterrupted)
			}
		}
	}

	status := atomic.LoadUint32(&killer.Signal)
	var desc string
	if status == KilledByMemArbitrator {
		killer.killEvent.Lock()
		status = atomic.LoadUint32(&killer.Signal)
		desc = killer.killEvent.desc
		killer.killEvent.Unlock()
	}
	err := killer.getKillError(status, desc)
	if status == ServerMemoryExceeded {
		logutil.BgLogger().Warn("global memory controller, NeedKill signal is received successfully",
			zap.Uint64("conn", killer.ConnID.Load()))
	}
	return err
}

// CheckConnectionAlive checks whether the connection is alive immediately.
func (killer *SQLKiller) CheckConnectionAlive() {
	fn := killer.IsConnectionAlive.Load()
	if fn != nil && !(*fn)() {
		killer.sendKillSignal(QueryInterrupted)
	}
}

// Reset resets the SqlKiller.
func (killer *SQLKiller) Reset() {
	killer.killEvent.Lock()
	status := atomic.SwapUint32(&killer.Signal, UnspecifiedKillSignal)
	// Keep this hook immediately after the atomic clear. If Reset is ever split
	// into separate observation and clear operations, place it between them.
	failpoint.InjectCall("afterResetKillSignalSwap")
	killer.resetKillEventLocked()
	killer.killEvent.Unlock()

	if status != UnspecifiedKillSignal {
		logutil.BgLogger().Warn("kill finished", zap.Uint64("conn", killer.ConnID.Load()))
	}
	killer.lastCheckTime.Store(nil)
	killer.ConnCancel.Store(nil)
}
