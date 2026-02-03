// Copyright 2026 PingCAP, Inc.
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

package sortexec

import (
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

var aqsortEnabled atomic.Bool

// SetAQSortEnabled enables/disables the experimental AQS-based in-memory sort path
// for SortExec.
//
// This switch is intended for benchmarking/experimentation only. The default is
// disabled.
func SetAQSortEnabled(enabled bool) { aqsortEnabled.Store(enabled) }

func isAQSortEnabled() bool { return aqsortEnabled.Load() }

type aqsortControl struct {
	enabled atomic.Bool
	warned  atomic.Bool

	connID     uint64
	executorID int
}

func newAQSortControl(enabled bool, connID uint64, executorID int) *aqsortControl {
	ctrl := &aqsortControl{
		connID:     connID,
		executorID: executorID,
	}
	ctrl.enabled.Store(enabled)
	return ctrl
}

func (c *aqsortControl) isEnabled() bool {
	if c == nil {
		return false
	}
	return c.enabled.Load()
}

func (c *aqsortControl) disableWithWarn(err error, extraFields ...zap.Field) {
	if c == nil {
		return
	}
	c.enabled.Store(false)
	if c.warned.CompareAndSwap(false, true) {
		fields := make([]zap.Field, 0, 3+len(extraFields))
		fields = append(fields,
			zap.Error(err),
			zap.Uint64("conn_id", c.connID),
			zap.Int("executor_id", c.executorID),
		)
		fields = append(fields, extraFields...)
		logutil.BgLogger().Warn("AQSort disabled for SortExec, falling back to std sort", fields...)
	}
}
