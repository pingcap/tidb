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

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
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
	// When you add a new signal, you should also modify store/driver/error/ToTidbErr,
	// so that errors in client can be correctly converted to tidb errors.
)

// SQLKiller is used to kill a query.
type SQLKiller struct {
	Signal killSignal
	ConnID uint64
	// FinishFuncLock is used to ensure that Finish is not called and modified at the same time.
	// An external call to the Finish function only allows when the main goroutine to be in the writeResultSet process.
	// When the main goroutine exits the writeResultSet process, the Finish function will be cleared.
	FinishFuncLock sync.Mutex
	Finish         func()
	// InWriteResultSet is used to indicate whether the query is currently calling clientConn.writeResultSet().
	// If the query is in writeResultSet and Finish() can acquire rs.finishLock, we can assume the query is waiting for the client to receive data from the server over network I/O.
	InWriteResultSet atomic.Bool
}

// SendKillSignal sends a kill signal to the query.
func (killer *SQLKiller) SendKillSignal(reason killSignal) {
	if atomic.CompareAndSwapUint32(&killer.Signal, 0, reason) {
		status := atomic.LoadUint32(&killer.Signal)
		err := killer.getKillError(status)
		logutil.BgLogger().Warn("kill initiated", zap.Uint64("connection ID", killer.ConnID), zap.String("reason", err.Error()))
	}
}

// GetKillSignal gets the kill signal.
func (killer *SQLKiller) GetKillSignal() killSignal {
	return atomic.LoadUint32(&killer.Signal)
}

// getKillError gets the error according to the kill signal.
func (killer *SQLKiller) getKillError(status killSignal) error {
	switch status {
	case QueryInterrupted:
		return exeerrors.ErrQueryInterrupted.GenWithStackByArgs()
	case MaxExecTimeExceeded:
		return exeerrors.ErrMaxExecTimeExceeded.GenWithStackByArgs()
	case QueryMemoryExceeded:
		return exeerrors.ErrMemoryExceedForQuery.GenWithStackByArgs(killer.ConnID)
	case ServerMemoryExceeded:
		return exeerrors.ErrMemoryExceedForInstance.GenWithStackByArgs(killer.ConnID)
	case RunawayQueryExceeded:
		return exeerrors.ErrResourceGroupQueryRunawayInterrupted.GenWithStackByArgs()
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
				if killer.ConnID != 0 {
					targetStatus := rand.Int31n(5)
					atomic.StoreUint32(&killer.Signal, uint32(targetStatus))
				}
			}
		}
	})
	status := atomic.LoadUint32(&killer.Signal)
	err := killer.getKillError(status)
	if status == ServerMemoryExceeded {
		logutil.BgLogger().Warn("global memory controller, NeedKill signal is received successfully",
			zap.Uint64("conn", killer.ConnID))
	}
	return err
}

// Reset resets the SqlKiller.
func (killer *SQLKiller) Reset() {
	if atomic.LoadUint32(&killer.Signal) != 0 {
		logutil.BgLogger().Warn("kill finished", zap.Uint64("conn", killer.ConnID))
	}
	atomic.StoreUint32(&killer.Signal, 0)
}
