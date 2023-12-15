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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package memory

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// ActionOnExceed is the action taken when memory usage exceeds memory quota.
// NOTE: All the implementors should be thread-safe.
type ActionOnExceed interface {
	// Action will be called when memory usage exceeds memory quota by the
	// corresponding Tracker.
	Action(t *Tracker)
	// SetFallback sets a fallback action which will be triggered if itself has
	// already been triggered.
	SetFallback(a ActionOnExceed)
	// GetFallback get the fallback action of the Action.
	GetFallback() ActionOnExceed
	// GetPriority get the priority of the Action.
	GetPriority() int64
	// SetFinished sets the finished state of the Action.
	SetFinished()
	// IsFinished returns the finished state of the Action.
	IsFinished() bool
}

var _ ActionOnExceed = &actionWithPriority{}

type actionWithPriority struct {
	ActionOnExceed
	priority int64
}

// NewActionWithPriority wraps the action with a new priority
func NewActionWithPriority(action ActionOnExceed, priority int64) *actionWithPriority {
	return &actionWithPriority{
		action,
		priority,
	}
}

func (a *actionWithPriority) GetPriority() int64 {
	return a.priority
}

// ActionInvoker indicates the invoker of the Action.
type ActionInvoker byte

const (
	// SingleQuery indicates the Action is invoked by a tidb_mem_quota_query.
	SingleQuery ActionInvoker = iota
	// Instance indicates the Action is invoked by a tidb_server_memory_limit.
	Instance
)

// ActionCareInvoker is the interface for the Actions which need to be aware of the invoker.
type ActionCareInvoker interface {
	SetInvoker(invoker ActionInvoker)
}

// BaseOOMAction manages the fallback action for all Action.
type BaseOOMAction struct {
	fallbackAction ActionOnExceed
	finished       int32
}

// SetFallback sets a fallback action which will be triggered if itself has
// already been triggered.
func (b *BaseOOMAction) SetFallback(a ActionOnExceed) {
	b.fallbackAction = a
}

// SetFinished sets the finished state of the Action.
func (b *BaseOOMAction) SetFinished() {
	atomic.StoreInt32(&b.finished, 1)
}

// IsFinished returns the finished state of the Action.
func (b *BaseOOMAction) IsFinished() bool {
	return atomic.LoadInt32(&b.finished) == 1
}

// GetFallback get the fallback action and remove finished fallback.
func (b *BaseOOMAction) GetFallback() ActionOnExceed {
	for b.fallbackAction != nil && b.fallbackAction.IsFinished() {
		b.SetFallback(b.fallbackAction.GetFallback())
	}
	return b.fallbackAction
}

// Default OOM Action priority.
const (
	DefPanicPriority = iota
	DefLogPriority
	DefSpillPriority
	// DefCursorFetchSpillPriority is higher than normal disk spill, because it can release much more memory in the future.
	// And the performance impaction of it is less than other disk-spill action, because it's write-only in execution stage.
	DefCursorFetchSpillPriority
	DefRateLimitPriority
)

// LogOnExceed logs a warning only once when memory usage exceeds memory quota.
type LogOnExceed struct {
	logHook func(uint64)
	BaseOOMAction
	ConnID uint64
	mutex  sync.Mutex // For synchronization.
	acted  bool
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
				zap.Error(errMemExceedThreshold.GenWithStackByArgs(t.label, t.BytesConsumed(), t.GetBytesLimit(), t.String())))
			return
		}
		a.logHook(a.ConnID)
	}
}

// GetPriority get the priority of the Action
func (*LogOnExceed) GetPriority() int64 {
	return DefLogPriority
}

// PanicOnExceed panics when memory usage exceeds memory quota.
type PanicOnExceed struct {
	logHook func(uint64)
	BaseOOMAction
	ConnID  uint64
	mutex   sync.Mutex // For synchronization.
	acted   bool
	invoker ActionInvoker
}

// SetLogHook sets a hook for PanicOnExceed.
func (a *PanicOnExceed) SetLogHook(hook func(uint64)) {
	a.logHook = hook
}

// Action panics when memory usage exceeds memory quota.
func (a *PanicOnExceed) Action(t *Tracker) {
	a.mutex.Lock()
	defer func() {
		a.mutex.Unlock()
	}()
	if !a.acted {
		if a.logHook == nil {
			logutil.BgLogger().Warn("memory exceeds quota",
				zap.Uint64("conn", t.SessionID.Load()), zap.Error(errMemExceedThreshold.GenWithStackByArgs(t.label, t.BytesConsumed(), t.GetBytesLimit(), t.String())))
		} else {
			a.logHook(a.ConnID)
		}
	}
	a.acted = true
	if a.invoker == SingleQuery {
		panic(PanicMemoryExceedWarnMsg + WarnMsgSuffixForSingleQuery + fmt.Sprintf("[conn=%d]", a.ConnID))
	}
	panic(PanicMemoryExceedWarnMsg + WarnMsgSuffixForInstance + fmt.Sprintf("[conn=%d]", a.ConnID))
}

// GetPriority get the priority of the Action
func (*PanicOnExceed) GetPriority() int64 {
	return DefPanicPriority
}

// SetInvoker sets the invoker of the Action.
func (a *PanicOnExceed) SetInvoker(invoker ActionInvoker) {
	a.invoker = invoker
}

var (
	errMemExceedThreshold = dbterror.ClassUtil.NewStd(errno.ErrMemExceedThreshold)
)

const (
	// PanicMemoryExceedWarnMsg represents the panic message when out of memory quota.
	PanicMemoryExceedWarnMsg string = "Your query has been cancelled due to exceeding the allowed memory limit"
	// WarnMsgSuffixForSingleQuery represents the suffix of the warning message when out of memory quota for a single query.
	WarnMsgSuffixForSingleQuery string = " for a single SQL query. Please try narrowing your query scope or increase the tidb_mem_quota_query limit and try again."
	// WarnMsgSuffixForInstance represents the suffix of the warning message when out of memory quota for the tidb-server instance.
	WarnMsgSuffixForInstance string = " for the tidb-server instance and this query is currently using the most memory. Please try narrowing your query scope or increase the tidb_server_memory_limit and try again."
)
