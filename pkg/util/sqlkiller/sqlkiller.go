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
	"fmt"
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// Kill reasons.
const (
	QueryInterrupted uint32 = iota + 1
	MaxExecTimeExceeded
	QueryMemoryExceeded
	ServerMemoryExceeded
)

// SQLKiller is used to kill a query.
type SQLKiller struct {
	Status uint32
	ConnID uint64
}

// SendKillSignal sends a kill signal to the query.
func (killer *SQLKiller) SendKillSignal(reason uint32) {
	atomic.CompareAndSwapUint32(&killer.Status, 0, reason)
}

// HandleSignal handles the kill signal and return the error.
func (killer *SQLKiller) HandleSignal() error {
	status := atomic.LoadUint32(&killer.Status)
	switch status {
	case QueryInterrupted:
		return exeerrors.ErrQueryInterrupted
	case MaxExecTimeExceeded:
		return exeerrors.ErrMaxExecTimeExceeded
	case QueryMemoryExceeded:
		return fmt.Errorf(PanicMemoryExceedWarnMsg + WarnMsgSuffixForSingleQuery + fmt.Sprintf("[conn=%d]", killer.ConnID))
	case ServerMemoryExceeded:
		logutil.BgLogger().Warn("global memory controller, NeedKill signal is received successfully",
			zap.Uint64("conn", killer.ConnID))
		return fmt.Errorf(PanicMemoryExceedWarnMsg + WarnMsgSuffixForInstance + fmt.Sprintf("[conn=%d]", killer.ConnID))
	}
	return nil
}

// NeedKill checks whether the sql need kill.
func (killer *SQLKiller) NeedKill() bool {
	return atomic.LoadUint32(&killer.Status) > 0
}

// Reset resets the SqlKiller.
func (killer *SQLKiller) Reset() {
	atomic.StoreUint32(&killer.Status, 0)
}

const (
	// PanicMemoryExceedWarnMsg represents the panic message when out of memory quota.
	PanicMemoryExceedWarnMsg string = "Your query has been cancelled due to exceeding the allowed memory limit"
	// WarnMsgSuffixForSingleQuery represents the suffix of the warning message when out of memory quota for a single query.
	WarnMsgSuffixForSingleQuery string = " for a single SQL query. Please try narrowing your query scope or increase the tidb_mem_quota_query limit and try again."
	// WarnMsgSuffixForInstance represents the suffix of the warning message when out of memory quota for the tidb-server instance.
	WarnMsgSuffixForInstance string = " for the tidb-server instance and this query is currently using the most memory. Please try narrowing your query scope or increase the tidb_server_memory_limit and try again."
)
