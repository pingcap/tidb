// Copyright 2018 PingCAP, Inc.
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

package memory

import (
	"fmt"
	"sync"

	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/v4/errno"
	"github.com/pingcap/tidb/v4/util/logutil"
	"go.uber.org/zap"
)

// ActionOnExceed is the action taken when memory usage exceeds memory quota.
// NOTE: All the implementors should be thread-safe.
type ActionOnExceed interface {
	// Action will be called when memory usage exceeds memory quota by the
	// corresponding Tracker.
	Action(t *Tracker)
	// SetLogHook binds a log hook which will be triggered and log an detailed
	// message for the out-of-memory sql.
	SetLogHook(hook func(uint64))
	// SetFallback sets a fallback action which will be triggered if itself has
	// already been triggered.
	SetFallback(a ActionOnExceed)
}

// LogOnExceed logs a warning only once when memory usage exceeds memory quota.
type LogOnExceed struct {
	mutex   sync.Mutex // For synchronization.
	acted   bool
	ConnID  uint64
	logHook func(uint64)
}

// SetLogHook sets a hook for LogOnExceed.
func (a *LogOnExceed) SetLogHook(hook func(uint64)) {
	a.logHook = hook
}

// Action logs a warning only once when memory usage exceeds memory quota.
func (a *LogOnExceed) Action(t *Tracker) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	if !a.acted {
		a.acted = true
		if a.logHook == nil {
			logutil.BgLogger().Warn("memory exceeds quota",
				zap.Error(errMemExceedThreshold.GenWithStackByArgs(t.label, t.BytesConsumed(), t.bytesLimit, t.String())))
			return
		}
		a.logHook(a.ConnID)
	}
}

// SetFallback sets a fallback action.
func (a *LogOnExceed) SetFallback(ActionOnExceed) {}

// PanicOnExceed panics when memory usage exceeds memory quota.
type PanicOnExceed struct {
	mutex   sync.Mutex // For synchronization.
	acted   bool
	ConnID  uint64
	logHook func(uint64)
}

// SetLogHook sets a hook for PanicOnExceed.
func (a *PanicOnExceed) SetLogHook(hook func(uint64)) {
	a.logHook = hook
}

// Action panics when memory usage exceeds memory quota.
func (a *PanicOnExceed) Action(t *Tracker) {
	a.mutex.Lock()
	if a.acted {
		a.mutex.Unlock()
		return
	}
	a.acted = true
	a.mutex.Unlock()
	if a.logHook != nil {
		a.logHook(a.ConnID)
	}
	panic(PanicMemoryExceed + fmt.Sprintf("[conn_id=%d]", a.ConnID))
}

// SetFallback sets a fallback action.
func (a *PanicOnExceed) SetFallback(ActionOnExceed) {}

var (
	errMemExceedThreshold = terror.ClassUtil.New(errno.ErrMemExceedThreshold, errno.MySQLErrName[errno.ErrMemExceedThreshold])
)

const (
	// PanicMemoryExceed represents the panic message when out of memory quota.
	PanicMemoryExceed string = "Out Of Memory Quota!"
)
