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
	// When you add a new signal, you should also modify store/driver/error/ToTidbErr,
	// so that errors in client can be correctly converted to tidb errors.
)

// SQLKiller is used to kill a query.
type SQLKiller struct {
	Signal killSignal
	ConnID uint64
}

// SendKillSignal sends a kill signal to the query.
func (killer *SQLKiller) SendKillSignal(reason killSignal) {
	atomic.CompareAndSwapUint32(&killer.Signal, 0, reason)
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
	switch status {
	case QueryInterrupted:
		return exeerrors.ErrQueryInterrupted.GenWithStackByArgs()
	case MaxExecTimeExceeded:
		return exeerrors.ErrMaxExecTimeExceeded.GenWithStackByArgs()
	case QueryMemoryExceeded:
		return exeerrors.ErrMemoryExceedForQuery.GenWithStackByArgs(killer.ConnID)
	case ServerMemoryExceeded:
		logutil.BgLogger().Warn("global memory controller, NeedKill signal is received successfully",
			zap.Uint64("conn", killer.ConnID))
		return exeerrors.ErrMemoryExceedForInstance.GenWithStackByArgs(killer.ConnID)
	}
	return nil
}

// Reset resets the SqlKiller.
func (killer *SQLKiller) Reset() {
	atomic.StoreUint32(&killer.Signal, 0)
}
