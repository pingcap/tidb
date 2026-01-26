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

	"github.com/pingcap/errors"
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

var errKilled = errors.New("it has been killed by the sql killer")

// SQLKiller is used to kill a query.
type SQLKiller struct {
	Finish    func()
	killEvent struct {
		ch   chan struct{}
		desc string
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

func (killer *SQLKiller) triggerKillEvent() {
	killer.killEvent.Lock()
	defer killer.killEvent.Unlock()

	if killer.killEvent.triggered {
		return
	}

	if killer.killEvent.ch != nil {
		close(killer.killEvent.ch)
	}
	killer.killEvent.triggered = true
}

func (killer *SQLKiller) resetKillEvent() {
	killer.killEvent.Lock()
	defer killer.killEvent.Unlock()

	if !killer.killEvent.triggered && killer.killEvent.ch != nil {
		close(killer.killEvent.ch)
	}
	killer.killEvent.ch = nil
	killer.killEvent.triggered = false
	killer.killEvent.desc = ""
}

// SendKillSignalWithKillEventReason sets the reason for the kill event and sends a kill signal.
func (killer *SQLKiller) SendKillSignalWithKillEventReason(killSignal killSignal, desc string) {
	{
		killer.killEvent.Lock()
		killer.killEvent.desc = desc
		killer.killEvent.Unlock()
	}
	killer.sendKillSignal(killSignal)
	killer.triggerKillEvent()
}

func (killer *SQLKiller) sendKillSignal(reason killSignal) {
	if atomic.CompareAndSwapUint32(&killer.Signal, 0, reason) {
		status := atomic.LoadUint32(&killer.Signal)
		err := killer.getKillError(status)
		logutil.BgLogger().Warn("kill initiated", zap.Uint64("connection ID", killer.ConnID.Load()), zap.String("reason", err.Error()))
	}
}

// SendKillSignal sends a kill signal to the query.
func (killer *SQLKiller) SendKillSignal(reason killSignal) {
	killer.sendKillSignal(reason)
	killer.triggerKillEvent()
}

// GetKillSignal gets the kill signal.
func (killer *SQLKiller) GetKillSignal() killSignal {
	return atomic.LoadUint32(&killer.Signal)
}

func (killer *SQLKiller) getKillEventReason() (res string) {
	killer.killEvent.Lock()
	//
	res = killer.killEvent.desc
	//
	killer.killEvent.Unlock()
	return res
}

// getKillError gets the error according to the kill signal.
func (killer *SQLKiller) getKillError(status killSignal) error {
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
		return exeerrors.ErrQueryExecStopped.GenWithStackByArgs(killer.getKillEventReason(), killer.ConnID.Load())
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
					atomic.StoreUint32(&killer.Signal, uint32(targetStatus))
				}
			}
		}
	})

	// Checks if the connection is alive.
	// For performance reasons, the check interval should be at least `checkConnectionAliveDur`(1 second).
	fn := killer.IsConnectionAlive.Load()
	lastCheckTime := killer.lastCheckTime.Load()
	if fn != nil {
		var checkConnectionAliveDur time.Duration = time.Second
		now := time.Now()
		if intest.InTest {
			checkConnectionAliveDur = time.Millisecond
		}
		if lastCheckTime == nil {
			killer.lastCheckTime.Store(&now)
		} else if now.Sub(*lastCheckTime) > checkConnectionAliveDur {
			killer.lastCheckTime.Store(&now)
			if !(*fn)() {
				atomic.CompareAndSwapUint32(&killer.Signal, 0, QueryInterrupted)
			}
		}
	}

	status := atomic.LoadUint32(&killer.Signal)
	err := killer.getKillError(status)
	if status == ServerMemoryExceeded {
		logutil.BgLogger().Warn("global memory controller, NeedKill signal is received successfully",
			zap.Uint64("conn", killer.ConnID.Load()))
	}
	return err
}

// Reset resets the SqlKiller.
func (killer *SQLKiller) Reset() {
	if atomic.LoadUint32(&killer.Signal) != 0 {
		logutil.BgLogger().Warn("kill finished", zap.Uint64("conn", killer.ConnID.Load()))
	}

	atomic.StoreUint32(&killer.Signal, 0)
	killer.resetKillEvent()
	killer.lastCheckTime.Store(nil)
}
