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
	"bytes"
	"fmt"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	atomicutil "go.uber.org/atomic"
)

// TrackMemWhenExceeds is the threshold when memory usage needs to be tracked.
const TrackMemWhenExceeds = 104857600 // 100MB

// DefMemQuotaQuery is default memory quota for query.
const DefMemQuotaQuery = 1073741824 // 1GB

// Process global variables for memory limit.
var (
	ServerMemoryLimitOriginText  = atomicutil.NewString("0")
	ServerMemoryLimit            = atomicutil.NewUint64(0)
	ServerMemoryLimitSessMinSize = atomicutil.NewUint64(128 << 20)

	QueryForceDisk       = atomicutil.NewInt64(0)
	TriggerMemoryLimitGC = atomicutil.NewBool(false)
	MemoryLimitGCLast    = atomicutil.NewTime(time.Time{})
	MemoryLimitGCTotal   = atomicutil.NewInt64(0)
)

// Tracker is used to track the memory usage during query execution.
// It contains an optional limit and can be arranged into a tree structure
// such that the consumption tracked by a Tracker is also tracked by
// its ancestors. The main idea comes from Apache Impala:
//
// https://github.com/cloudera/Impala/blob/cdh5-trunk/be/src/runtime/mem-tracker.h
//
// By default, memory consumption is tracked via calls to "Consume()", either to
// the tracker itself or to one of its descendents. A typical sequence of calls
// for a single Tracker is:
// 1. tracker.SetLabel() / tracker.SetActionOnExceed() / tracker.AttachTo()
// 2. tracker.Consume() / tracker.ReplaceChild() / tracker.BytesConsumed()
//
// NOTE: We only protect concurrent access to "bytesConsumed" and "children",
// that is to say:
// 1. Only "BytesConsumed()", "Consume()" and "AttachTo()" are thread-safe.
// 2. Other operations of a Tracker tree is not thread-safe.
//
// We have two limits for the memory quota: soft limit and hard limit.
// If the soft limit is exceeded, we will trigger the action that alleviates the
// speed of memory growth. The soft limit is hard-coded as `0.8*hard limit`.
// The actions that could be triggered are: AggSpillDiskAction.
//
// If the hard limit is exceeded, we will trigger the action that immediately
// reduces memory usage. The hard limit is set by the system variable `tidb_mem_query_quota`.
// The actions that could be triggered are: SpillDiskAction, SortAndSpillDiskAction, rateLimitAction,
// PanicOnExceed, globalPanicOnExceed, LogOnExceed.
type Tracker struct {
	parent               atomic.Pointer[Tracker]
	MemArbitrator        *memArbitrator
	Killer               *sqlkiller.SQLKiller
	bytesLimit           atomic.Pointer[bytesLimits]
	actionMuForHardLimit actionMu
	actionMuForSoftLimit actionMu
	mu                   struct {
		// The children memory trackers. If the Tracker is the Global Tracker, like executor.GlobalDiskUsageTracker,
		// we wouldn't maintain its children in order to avoiding mutex contention.
		children map[int][]*Tracker
		sync.Mutex
	}
	label int // Label of this "Tracker".
	// following fields are used with atomic operations, so make them 64-byte aligned.
	bytesReleased       int64             // Released bytes.
	maxConsumed         atomicutil.Int64  // max number of bytes consumed during execution.
	SessionID           atomicutil.Uint64 // SessionID indicates the sessionID the tracker is bound.
	bytesConsumed       int64             // Consumed bytes.
	IsRootTrackerOfSess bool              // IsRootTrackerOfSess indicates whether this tracker is bound for session
	isGlobal            bool              // isGlobal indicates whether this tracker is global tracker
}

type actionMu struct {
	actionOnExceed ActionOnExceed
	sync.Mutex
}

// EnableGCAwareMemoryTrack is used to turn on/off the GC-aware memory track
var EnableGCAwareMemoryTrack = atomicutil.NewBool(false)

// https://golang.google.cn/pkg/runtime/#SetFinalizer
// It is not guaranteed that a finalizer will run if the size of *obj is zero bytes.
type finalizerRef struct {
	_ byte //nolint:unused
}

// softScale means the scale of the soft limit to the hard limit.
const softScale = 0.8

// bytesLimits holds limit config atomically.
type bytesLimits struct {
	bytesHardLimit int64 // bytesHardLimit <= 0 means no limit, used for actionMuForHardLimit.
	bytesSoftLimit int64 // bytesSoftLimit <= 0 means no limit, used for actionMuForSoftLimit.
}

var unlimitedBytesLimit = bytesLimits{
	bytesHardLimit: -1,
	bytesSoftLimit: -1,
}

var defaultQueryQuota = bytesLimits{
	bytesHardLimit: DefMemQuotaQuery,
	bytesSoftLimit: DefMemQuotaQuery * 8 / 10,
}

// MemUsageTop1Tracker record the use memory top1 session's tracker for kill.
var MemUsageTop1Tracker atomic.Pointer[Tracker]

var mockDebugInject func()

// InitTracker initializes a memory tracker.
//  1. "label" is the label used in the usage string.
//  2. "bytesLimit <= 0" means no limit.
//
// For the common tracker, isGlobal is default as false
func InitTracker(t *Tracker, label int, bytesLimit int64, action ActionOnExceed) {
	t.mu.children = nil
	t.actionMuForHardLimit.actionOnExceed = action
	t.actionMuForSoftLimit.actionOnExceed = nil
	t.parent.Store(nil)

	t.label = label
	if bytesLimit <= 0 {
		t.bytesLimit.Store(&unlimitedBytesLimit)
	} else if bytesLimit == DefMemQuotaQuery {
		t.bytesLimit.Store(&defaultQueryQuota)
	} else {
		t.bytesLimit.Store(&bytesLimits{
			bytesHardLimit: bytesLimit,
			bytesSoftLimit: int64(float64(bytesLimit) * softScale),
		})
	}
	t.maxConsumed.Store(0)
	t.isGlobal = false
	t.MemArbitrator = nil
}

// NewTracker creates a memory tracker.
//  1. "label" is the label used in the usage string.
//  2. "bytesLimit <= 0" means no limit.
//
// For the common tracker, isGlobal is default as false
func NewTracker(label int, bytesLimit int64) *Tracker {
	t := &Tracker{
		label: label,
	}
	t.bytesLimit.Store(&bytesLimits{
		bytesHardLimit: bytesLimit,
		bytesSoftLimit: int64(float64(bytesLimit) * softScale),
	})
	t.actionMuForHardLimit.actionOnExceed = &LogOnExceed{}
	t.isGlobal = false
	return t
}

// NewGlobalTracker creates a global tracker, its isGlobal is default as true
func NewGlobalTracker(label int, bytesLimit int64) *Tracker {
	t := &Tracker{
		label: label,
	}
	t.bytesLimit.Store(&bytesLimits{
		bytesHardLimit: bytesLimit,
		bytesSoftLimit: int64(float64(bytesLimit) * softScale),
	})
	t.actionMuForHardLimit.actionOnExceed = &LogOnExceed{}
	t.isGlobal = true
	return t
}

// CheckBytesLimit check whether the bytes limit of the tracker is equal to a value.
// Only used in test.
func (t *Tracker) CheckBytesLimit(val int64) bool {
	return t.bytesLimit.Load().bytesHardLimit == val
}

// SetBytesLimit sets the bytes limit for this tracker.
// "bytesHardLimit <= 0" means no limit.
func (t *Tracker) SetBytesLimit(bytesLimit int64) {
	if bytesLimit <= 0 {
		t.bytesLimit.Store(&unlimitedBytesLimit)
	} else if bytesLimit == DefMemQuotaQuery {
		t.bytesLimit.Store(&defaultQueryQuota)
	} else {
		t.bytesLimit.Store(&bytesLimits{
			bytesHardLimit: bytesLimit,
			bytesSoftLimit: int64(float64(bytesLimit) * softScale),
		})
	}
}

// GetBytesLimit gets the bytes limit for this tracker.
// "bytesHardLimit <= 0" means no limit.
func (t *Tracker) GetBytesLimit() int64 {
	return t.bytesLimit.Load().bytesHardLimit
}

// CheckExceed checks whether the consumed bytes is exceed for this tracker.
func (t *Tracker) CheckExceed() bool {
	bytesHardLimit := t.bytesLimit.Load().bytesHardLimit
	return atomic.LoadInt64(&t.bytesConsumed) >= bytesHardLimit && bytesHardLimit > 0
}

// SetActionOnExceed sets the action when memory usage exceeds bytesHardLimit.
func (t *Tracker) SetActionOnExceed(a ActionOnExceed) {
	t.actionMuForHardLimit.Lock()
	defer t.actionMuForHardLimit.Unlock()
	t.actionMuForHardLimit.actionOnExceed = a
}

// FallbackOldAndSetNewAction sets the action when memory usage exceeds bytesHardLimit
// and set the original action as its fallback.
func (t *Tracker) FallbackOldAndSetNewAction(a ActionOnExceed) {
	t.actionMuForHardLimit.Lock()
	defer t.actionMuForHardLimit.Unlock()
	t.actionMuForHardLimit.actionOnExceed = reArrangeFallback(a, t.actionMuForHardLimit.actionOnExceed)
}

// FallbackOldAndSetNewActionForSoftLimit sets the action when memory usage exceeds bytesSoftLimit
// and set the original action as its fallback.
func (t *Tracker) FallbackOldAndSetNewActionForSoftLimit(a ActionOnExceed) {
	t.actionMuForSoftLimit.Lock()
	defer t.actionMuForSoftLimit.Unlock()
	t.actionMuForSoftLimit.actionOnExceed = reArrangeFallback(a, t.actionMuForSoftLimit.actionOnExceed)
}

// GetFallbackForTest get the oom action used by test.
func (t *Tracker) GetFallbackForTest(ignoreFinishedAction bool) ActionOnExceed {
	t.actionMuForHardLimit.Lock()
	defer t.actionMuForHardLimit.Unlock()
	if t.actionMuForHardLimit.actionOnExceed != nil && t.actionMuForHardLimit.actionOnExceed.IsFinished() && ignoreFinishedAction {
		t.actionMuForHardLimit.actionOnExceed = t.actionMuForHardLimit.actionOnExceed.GetFallback()
	}
	return t.actionMuForHardLimit.actionOnExceed
}

// UnbindActions unbinds actionForHardLimit and actionForSoftLimit.
func (t *Tracker) UnbindActions() {
	t.actionMuForSoftLimit.Lock()
	defer t.actionMuForSoftLimit.Unlock()
	t.actionMuForSoftLimit.actionOnExceed = nil

	t.actionMuForHardLimit.Lock()
	defer t.actionMuForHardLimit.Unlock()
	// Currently this method is only called by ResetContextOfStmt, which then always calls SetActionOnExceed to set
	// actionForHardLimit.actionOnExceed properly, thus it's safe to set it nil here.
	t.actionMuForHardLimit.actionOnExceed = nil
}

// UnbindActionFromHardLimit unbinds action from hardLimit.
func (t *Tracker) UnbindActionFromHardLimit(actionToUnbind ActionOnExceed) {
	t.actionMuForHardLimit.Lock()
	defer t.actionMuForHardLimit.Unlock()

	var prev ActionOnExceed
	for current := t.actionMuForHardLimit.actionOnExceed; current != nil; current = current.GetFallback() {
		if current == actionToUnbind {
			if prev == nil {
				// actionToUnbind is the first element
				t.actionMuForHardLimit.actionOnExceed = current.GetFallback()
			} else {
				// actionToUnbind is not the first element
				prev.SetFallback(current.GetFallback())
			}
			break
		}
		prev = current
	}
}

// reArrangeFallback merge two action chains and rearrange them by priority in descending order.
func reArrangeFallback(a ActionOnExceed, b ActionOnExceed) ActionOnExceed {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if a.GetPriority() < b.GetPriority() {
		a, b = b, a
	}
	a.SetFallback(reArrangeFallback(a.GetFallback(), b))
	return a
}

// SetLabel sets the label of a Tracker.
func (t *Tracker) SetLabel(label int) {
	parent := t.getParent()
	t.Detach()
	t.label = label
	if parent != nil {
		t.AttachTo(parent)
	}
}

// Label gets the label of a Tracker.
func (t *Tracker) Label() int {
	return t.label
}

// AttachTo attaches this memory tracker as a child to another Tracker. If it
// already has a parent, this function will remove it from the old parent.
// Its consumed memory usage is used to update all its ancestors.
func (t *Tracker) AttachTo(parent *Tracker) {
	if parent.isGlobal {
		t.AttachToGlobalTracker(parent)
		return
	}
	oldParent := t.getParent()
	if oldParent != nil {
		oldParent.remove(t)
	}
	parent.mu.Lock()
	if parent.mu.children == nil {
		parent.mu.children = make(map[int][]*Tracker)
	}
	parent.mu.children[t.label] = append(parent.mu.children[t.label], t)
	parent.mu.Unlock()

	t.setParent(parent)
	parent.Consume(t.BytesConsumed())
}

// Detach de-attach the tracker child from its parent, then set its parent property as nil
func (t *Tracker) Detach() {
	if t == nil {
		return
	}
	t.DetachMemArbitrator()
	parent := t.getParent()
	if parent == nil {
		return
	}
	if parent.isGlobal {
		t.DetachFromGlobalTracker()
		return
	}
	if parent.IsRootTrackerOfSess && t.label != LabelForMemDB {
		parent.actionMuForHardLimit.Lock()
		parent.actionMuForHardLimit.actionOnExceed = nil
		parent.actionMuForHardLimit.Unlock()

		parent.actionMuForSoftLimit.Lock()
		parent.actionMuForSoftLimit.actionOnExceed = nil
		parent.actionMuForSoftLimit.Unlock()
		parent.Killer.Reset()
	}
	parent.remove(t)
	t.setParent(nil) //atomic operator
}

func (t *Tracker) remove(oldChild *Tracker) {
	found := false
	label := oldChild.label
	t.mu.Lock()
	if t.mu.children != nil {
		children := t.mu.children[label]
		for i, child := range children {
			if child == oldChild {
				children = slices.Delete(children, i, i+1)
				if len(children) > 0 {
					t.mu.children[label] = children
				} else {
					delete(t.mu.children, label)
				}
				found = true
				break
			}
		}
	}
	t.mu.Unlock()
	if found {
		oldChild.setParent(nil)
		t.Consume(-oldChild.BytesConsumed())
	}
}

// ReplaceChild removes the old child specified in "oldChild" and add a new
// child specified in "newChild". old child's memory consumption will be
// removed and new child's memory consumption will be added.
func (t *Tracker) ReplaceChild(oldChild, newChild *Tracker) {
	if newChild == nil {
		t.remove(oldChild)
		return
	}

	if oldChild.label != newChild.label {
		t.remove(oldChild)
		newChild.AttachTo(t)
		return
	}

	newConsumed := newChild.BytesConsumed()
	newChild.setParent(t)

	label := oldChild.label
	t.mu.Lock()
	if t.mu.children != nil {
		children := t.mu.children[label]
		for i, child := range children {
			if child != oldChild {
				continue
			}

			newConsumed -= oldChild.BytesConsumed()
			oldChild.setParent(nil)
			children[i] = newChild
			t.mu.children[label] = children
			break
		}
	}
	t.mu.Unlock()

	t.Consume(newConsumed)
}

// Consume is used to consume a memory usage. "bytes" can be a negative value,
// which means this is a memory release operation. When memory usage of a tracker
// exceeds its bytesSoftLimit/bytesHardLimit, the tracker calls its action, so does each of its ancestors.
func (t *Tracker) Consume(bs int64) {
	if bs == 0 {
		return
	}
	var rootExceed, rootExceedForSoftLimit, sessionRootTracker *Tracker
	for tracker := t; tracker != nil; tracker = tracker.getParent() {
		if tracker.IsRootTrackerOfSess {
			sessionRootTracker = tracker
		}
		if m := tracker.MemArbitrator; m != nil {
			if bs > 0 {
				if m.useBigBudget() {
					goto useBigBudget
				}
				{ // fast path for small budget
					if m.addSmallBudget(bs) > m.budget.smallLimit {
						m.addSmallBudget(-bs)
						goto initBigBudget
					}
					b := m.smallBudget()
					if t := m.approxUnixTimeSec(); b.getLastUsedTimeSec() != t {
						b.setLastUsedTimeSec(t)
					}
					if b.Used.Load() > b.approxCapacity() && b.PullFromUpstream() != nil {
						goto initBigBudget
					}
					goto endUseBudget
				}
			initBigBudget:
				m.initBigBudget()
			useBigBudget:
				if m.addBigBudgetUsed(bs) > m.bigBudgetGrowThreshold() {
					m.growBigBudget()
				}
			endUseBudget: // nop
			} else if m.useBigBudget() { // delta <= 0 && use big budget
				m.addBigBudgetUsed(bs)
			} else { // delta <= 0 && use small budget
				m.addSmallBudget(bs)
			}
		}
		bytesConsumed := atomic.AddInt64(&tracker.bytesConsumed, bs)
		bytesReleased := atomic.LoadInt64(&tracker.bytesReleased)
		limits := tracker.bytesLimit.Load()
		if bytesConsumed+bytesReleased >= limits.bytesHardLimit && limits.bytesHardLimit > 0 {
			rootExceed = tracker
		}
		if bytesConsumed+bytesReleased >= limits.bytesSoftLimit && limits.bytesSoftLimit > 0 {
			rootExceedForSoftLimit = tracker
		}

		for {
			maxNow := tracker.maxConsumed.Load()
			consumed := atomic.LoadInt64(&tracker.bytesConsumed)
			if consumed > maxNow && !tracker.maxConsumed.CompareAndSwap(maxNow, consumed) {
				continue
			}
			if tracker.label == LabelForGlobalAnalyzeMemory {
				// `LabelForGlobalAnalyzeMemory` represents in-use memory, which should never be negative.
				intest.Assert(consumed >= 0, fmt.Sprintf("global analyze memory usage negative: %d", consumed))
			}
			if label, ok := MetricsTypes[tracker.label]; ok {
				metrics.MemoryUsage.WithLabelValues(label[0], label[1]).Set(float64(consumed))
			}
			break
		}
	}

	tryAction := func(mu *actionMu, tracker *Tracker) {
		mu.Lock()
		defer mu.Unlock()
		for mu.actionOnExceed != nil && mu.actionOnExceed.IsFinished() {
			mu.actionOnExceed = mu.actionOnExceed.GetFallback()
		}
		if mu.actionOnExceed != nil {
			mu.actionOnExceed.Action(tracker)
		}
	}

	if bs > 0 && !UsingGlobalMemArbitration() && sessionRootTracker != nil {
		// Update the Top1 session
		memUsage := sessionRootTracker.BytesConsumed()
		limitSessMinSize := ServerMemoryLimitSessMinSize.Load()
		if uint64(memUsage) >= limitSessMinSize {
			oldTracker := MemUsageTop1Tracker.Load()
			for oldTracker.LessThan(sessionRootTracker) {
				if MemUsageTop1Tracker.CompareAndSwap(oldTracker, sessionRootTracker) {
					break
				}
				oldTracker = MemUsageTop1Tracker.Load()
			}
		}
	}

	if bs > 0 && sessionRootTracker != nil {
		err := sessionRootTracker.Killer.HandleSignal()
		if err != nil {
			panic(err)
		}
	}

	if bs > 0 && rootExceed != nil {
		tryAction(&rootExceed.actionMuForHardLimit, rootExceed)
	}

	if bs > 0 && rootExceedForSoftLimit != nil {
		tryAction(&rootExceedForSoftLimit.actionMuForSoftLimit, rootExceedForSoftLimit)
	}
}

// HandleKillSignal checks if a kill signal has been sent to the session root tracker.
// If a kill signal is detected, it panics with the error returned by the signal handler.
func (t *Tracker) HandleKillSignal() {
	var sessionRootTracker *Tracker
	for tracker := t; tracker != nil; tracker = tracker.getParent() {
		if tracker.IsRootTrackerOfSess {
			sessionRootTracker = tracker
		}
	}
	if sessionRootTracker != nil {
		err := sessionRootTracker.Killer.HandleSignal()
		if err != nil {
			panic(err)
		}
	}
}

// BufferedConsume is used to buffer memory usage and do late consume
// not thread-safe, should be called in one goroutine
func (t *Tracker) BufferedConsume(bufferedMemSize *int64, bytes int64) {
	*bufferedMemSize += bytes
	if *bufferedMemSize >= int64(TrackMemWhenExceeds) {
		t.Consume(*bufferedMemSize)
		*bufferedMemSize = int64(0)
	}
}

// Release is used to release memory tracked, track the released memory until GC triggered if needed
// If you want your track to be GC-aware, please use Release(bytes) instead of Consume(-bytes), and pass the memory size of the real object.
// Only Analyze is integrated with Release so far.
func (t *Tracker) Release(bytes int64) {
	if bytes == 0 {
		return
	}
	defer t.Consume(-bytes)
	for tracker := t; tracker != nil; tracker = tracker.getParent() {
		if tracker.shouldRecordRelease() {
			// use fake ref instead of obj ref, otherwise obj will be reachable again and gc in next cycle
			newRef := &finalizerRef{}
			finalizer := func(tracker *Tracker) func(ref *finalizerRef) {
				return func(*finalizerRef) {
					tracker.release(bytes) // finalizer func is called async
				}
			}
			runtime.SetFinalizer(newRef, finalizer(tracker))
			tracker.recordRelease(bytes)
			return
		}
	}
}

// BufferedRelease is used to buffer memory release and do late release
// not thread-safe, should be called in one goroutine
func (t *Tracker) BufferedRelease(bufferedMemSize *int64, bytes int64) {
	*bufferedMemSize += bytes
	if *bufferedMemSize >= int64(TrackMemWhenExceeds) {
		t.Release(*bufferedMemSize)
		*bufferedMemSize = int64(0)
	}
}

func (t *Tracker) shouldRecordRelease() bool {
	return EnableGCAwareMemoryTrack.Load() && t.label == LabelForGlobalAnalyzeMemory
}

func (t *Tracker) recordRelease(bytes int64) {
	for tracker := t; tracker != nil; tracker = tracker.getParent() {
		bytesReleased := atomic.AddInt64(&tracker.bytesReleased, bytes)
		if label, ok := MetricsTypes[tracker.label]; ok {
			metrics.MemoryUsage.WithLabelValues(label[0], label[2]).Set(float64(bytesReleased))
		}
	}
}

func (t *Tracker) release(bytes int64) {
	for tracker := t; tracker != nil; tracker = tracker.getParent() {
		bytesReleased := atomic.AddInt64(&tracker.bytesReleased, -bytes)
		if label, ok := MetricsTypes[tracker.label]; ok {
			metrics.MemoryUsage.WithLabelValues(label[0], label[2]).Set(float64(bytesReleased))
		}
	}
}

// BytesConsumed returns the consumed memory usage value in bytes.
func (t *Tracker) BytesConsumed() int64 {
	return atomic.LoadInt64(&t.bytesConsumed)
}

// BytesReleased returns the released memory value in bytes.
func (t *Tracker) BytesReleased() int64 {
	return atomic.LoadInt64(&t.bytesReleased)
}

// MaxConsumed returns max number of bytes consumed during execution.
// Note: Don't make this method return -1 for special meanings in the future. Because binary plan has used -1 to
// distinguish between "0 bytes" and "N/A". ref: binaryOpFromFlatOp()
func (t *Tracker) MaxConsumed() int64 {
	return t.maxConsumed.Load()
}

// ResetMaxConsumed should be invoked before executing a new statement in a session.
func (t *Tracker) ResetMaxConsumed() {
	t.maxConsumed.Store(t.BytesConsumed())
}

// SearchTrackerWithoutLock searches the specific tracker under this tracker without lock.
func (t *Tracker) SearchTrackerWithoutLock(label int) *Tracker {
	if t.label == label {
		return t
	}
	children := t.mu.children[label]
	if len(children) > 0 {
		return children[0]
	}
	return nil
}

// SearchTrackerConsumedMoreThanNBytes searches the specific tracker that consumes more than NBytes.
func (t *Tracker) SearchTrackerConsumedMoreThanNBytes(limit int64) (res []*Tracker) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, childSlice := range t.mu.children {
		for _, tracker := range childSlice {
			if tracker.BytesConsumed() > limit {
				res = append(res, tracker)
			}
		}
	}
	return
}

// String returns the string representation of this Tracker tree.
func (t *Tracker) String() string {
	buffer := bytes.NewBufferString("\n")
	t.toString("", buffer)
	return buffer.String()
}

func (t *Tracker) toString(indent string, buffer *bytes.Buffer) {
	fmt.Fprintf(buffer, "%s\"%d\"{\n", indent, t.label)
	bytesLimit := t.GetBytesLimit()
	if bytesLimit > 0 {
		fmt.Fprintf(buffer, "%s  \"quota\": %s\n", indent, t.FormatBytes(bytesLimit))
	}
	fmt.Fprintf(buffer, "%s  \"consumed\": %s\n", indent, t.FormatBytes(t.BytesConsumed()))

	t.mu.Lock()
	labels := make([]int, 0, len(t.mu.children))
	for label := range t.mu.children {
		labels = append(labels, label)
	}
	slices.Sort(labels)
	for _, label := range labels {
		children := t.mu.children[label]
		for _, child := range children {
			child.toString(indent+"  ", buffer)
		}
	}
	t.mu.Unlock()
	buffer.WriteString(indent + "}\n")
}

// FormatBytes uses to format bytes, this function will prune precision before format bytes.
func (*Tracker) FormatBytes(numBytes int64) string {
	return FormatBytes(numBytes)
}

// LessThan indicates whether t byteConsumed is less than t2 byteConsumed.
func (t *Tracker) LessThan(t2 *Tracker) bool {
	if t == nil {
		return true
	}
	if t2 == nil {
		return false
	}
	return t.BytesConsumed() < t2.BytesConsumed()
}

// BytesToString converts the memory consumption to a readable string.
func BytesToString(numBytes int64) string {
	gb := float64(numBytes) / float64(byteSizeGB)
	if gb > 1 {
		return fmt.Sprintf("%v GB", gb)
	}

	mb := float64(numBytes) / float64(byteSizeMB)
	if mb > 1 {
		return fmt.Sprintf("%v MB", mb)
	}

	kb := float64(numBytes) / float64(byteSizeKB)
	if kb > 1 {
		return fmt.Sprintf("%v KB", kb)
	}

	return fmt.Sprintf("%v Bytes", numBytes)
}

// FormatBytes uses to format bytes, this function will prune precision before format bytes.
func FormatBytes(numBytes int64) string {
	if numBytes <= byteSizeKB {
		return BytesToString(numBytes)
	}
	unit, unitStr := getByteUnit(numBytes)
	if unit == byteSize {
		return BytesToString(numBytes)
	}
	v := float64(numBytes) / float64(unit)
	decimal := 1
	if numBytes%unit == 0 {
		decimal = 0
	} else if v < 10 {
		decimal = 2
	}
	return fmt.Sprintf("%v %s", strconv.FormatFloat(v, 'f', decimal, 64), unitStr)
}

func getByteUnit(b int64) (int64, string) {
	if b > byteSizeGB {
		return byteSizeGB, "GB"
	} else if b > byteSizeMB {
		return byteSizeMB, "MB"
	} else if b > byteSizeKB {
		return byteSizeKB, "KB"
	}
	return byteSize, "Bytes"
}

// AttachToGlobalTracker attach the tracker to the global tracker
// AttachToGlobalTracker should be called at the initialization for the session executor's tracker
func (t *Tracker) AttachToGlobalTracker(globalTracker *Tracker) {
	if globalTracker == nil {
		return
	}
	if !globalTracker.isGlobal {
		panic("Attach to a non-GlobalTracker")
	}
	parent := t.getParent()
	if parent != nil {
		if parent.isGlobal {
			parent.Consume(-t.BytesConsumed())
		} else {
			parent.remove(t)
		}
	}
	t.setParent(globalTracker)
	globalTracker.Consume(t.BytesConsumed())
}

// DetachFromGlobalTracker detach itself from its parent
// Note that only the parent of this tracker is Global Tracker could call this function
// Otherwise it should use Detach
func (t *Tracker) DetachFromGlobalTracker() {
	parent := t.getParent()
	if parent == nil {
		return
	}
	if !parent.isGlobal {
		panic("Detach from a non-GlobalTracker")
	}
	parent.Consume(-t.BytesConsumed())
	t.setParent(nil)
}

// ReplaceBytesUsed replace bytesConsume for the tracker
func (t *Tracker) ReplaceBytesUsed(bytes int64) {
	t.Consume(bytes - t.BytesConsumed())
}

// Reset detach the tracker from the old parent and clear the old children. The label and byteLimit would not be reset.
func (t *Tracker) Reset() {
	t.Detach()
	t.ReplaceBytesUsed(0)
	t.mu.children = nil
	t.resetMemArbitrator()
}

func (t *Tracker) getParent() *Tracker {
	return t.parent.Load()
}

func (t *Tracker) setParent(parent *Tracker) {
	t.parent.Store(parent)
}

// CountAllChildrenMemUse return memory used tree for the tracker
func (t *Tracker) CountAllChildrenMemUse() map[string]int64 {
	trackerMemUseMap := make(map[string]int64, 1024)
	countChildMem(t, "", trackerMemUseMap)
	return trackerMemUseMap
}

// GetChildrenForTest returns children trackers
func (t *Tracker) GetChildrenForTest() []*Tracker {
	t.mu.Lock()
	defer t.mu.Unlock()
	trackers := make([]*Tracker, 0)
	for _, list := range t.mu.children {
		trackers = append(trackers, list...)
	}
	return trackers
}

func countChildMem(t *Tracker, familyTreeName string, trackerMemUseMap map[string]int64) {
	if len(familyTreeName) > 0 {
		familyTreeName += " <- "
	}
	familyTreeName += "[" + strconv.Itoa(t.Label()) + "]"
	trackerMemUseMap[familyTreeName] += t.BytesConsumed()
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, sli := range t.mu.children {
		for _, tracker := range sli {
			countChildMem(tracker, familyTreeName, trackerMemUseMap)
		}
	}
}

const (
	// LabelForSQLText represents the label of the SQL Text
	LabelForSQLText int = -1
	// LabelForIndexWorker represents the label of the index worker
	LabelForIndexWorker int = -2
	// LabelForInnerList represents the label of the inner list
	LabelForInnerList int = -3
	// LabelForInnerTable represents the label of the inner table
	LabelForInnerTable int = -4
	// LabelForOuterTable represents the label of the outer table
	LabelForOuterTable int = -5
	// LabelForCoprocessor represents the label of the coprocessor
	LabelForCoprocessor int = -6
	// LabelForChunkList represents the label of the chunk list
	LabelForChunkList int = -7
	// LabelForGlobalSimpleLRUCache represents the label of the Global SimpleLRUCache
	LabelForGlobalSimpleLRUCache int = -8
	// LabelForChunkDataInDiskByRows represents the label of the chunk list in disk
	LabelForChunkDataInDiskByRows int = -9
	// LabelForRowContainer represents the label of the row container
	LabelForRowContainer int = -10
	// LabelForGlobalStorage represents the label of the Global Storage
	LabelForGlobalStorage int = -11
	// LabelForGlobalMemory represents the label of the Global Memory
	LabelForGlobalMemory int = -12
	// LabelForBuildSideResult represents the label of the BuildSideResult
	LabelForBuildSideResult int = -13
	// LabelForRowChunks represents the label of the row chunks
	LabelForRowChunks int = -14
	// LabelForStatsCache represents the label of the stats cache
	LabelForStatsCache int = -15
	// LabelForOuterList represents the label of the outer list
	LabelForOuterList int = -16
	// LabelForApplyCache represents the label of the apply cache
	LabelForApplyCache int = -17
	// LabelForSimpleTask represents the label of the simple task
	LabelForSimpleTask int = -18
	// LabelForCTEStorage represents the label of CTE storage
	LabelForCTEStorage int = -19
	// LabelForIndexJoinInnerWorker represents the label of IndexJoin InnerWorker
	LabelForIndexJoinInnerWorker int = -20
	// LabelForIndexJoinOuterWorker represents the label of IndexJoin OuterWorker
	LabelForIndexJoinOuterWorker int = -21
	// LabelForBindCache represents the label of the bind cache
	LabelForBindCache int = -22
	// LabelForNonTransactionalDML represents the label of the non-transactional DML
	LabelForNonTransactionalDML = -23
	// LabelForAnalyzeMemory represents the label of the memory of each analyze job
	LabelForAnalyzeMemory int = -24
	// LabelForGlobalAnalyzeMemory represents the label of the global memory of all analyze jobs
	LabelForGlobalAnalyzeMemory int = -25
	// LabelForPreparedPlanCache represents the label of the prepared plan cache memory usage
	LabelForPreparedPlanCache int = -26
	// LabelForSession represents the label of a session.
	LabelForSession int = -27
	// LabelForMemDB represents the label of the MemDB
	LabelForMemDB int = -28
	// LabelForCursorFetch represents the label of the execution of cursor fetch
	LabelForCursorFetch int = -29
	// LabelForChunkDataInDiskByChunks represents the label of the chunk list in disk
	LabelForChunkDataInDiskByChunks int = -30
	// LabelForSortPartition represents the label of the sort partition
	LabelForSortPartition int = -31
	// LabelForHashTableInHashJoinV2 represents the label of the hash join v2's hash table
	LabelForHashTableInHashJoinV2 int = -32
)

// MetricsTypes is used to get label for metrics
// string[0] is LblModule, string[1] is heap-in-use type, string[2] is released type
var MetricsTypes = map[int][]string{
	LabelForGlobalAnalyzeMemory: {"analyze", "inuse", "released"},
}

const (
	memArbitratorStateSmallBudget   int32 = iota // using small budget
	memArbitratorStateIntoBigBudget              // initializing big budget from small budget
	memArbitratorStateBigBudget                  // using big budget
	memArbitratorStateDown                       // down
)

type memArbitrator struct {
	*MemArbitrator
	ctx    *ArbitrationContext
	killer *sqlkiller.SQLKiller
	budget struct {
		smallB *TrackedConcurrentBudget
		mu     struct {
			bigB      ConcurrentBudget // bigB.Used (aks growThreshold): threshold to pull from upstream (95% * bigB.Capacity)
			bigUsed   atomic.Int64     // bigUsed <= growThreshold <= bigB.Capacity
			smallUsed atomic.Int64
			_         cpuCacheLinePad
		}
		smallLimit int64
		useBig     struct {
			sync.Mutex
			atomic.Bool
		}
	}
	uid         uint64
	digestID    uint64 // identify the digest profile of root-pool / SQL
	reserveSize int64
	isInternal  bool
	state       atomic.Int32 // states: the current state of memArbitrator

	AwaitAlloc struct {
		TotalDur   atomic.Int64 // total time spent waiting for memory allocation in nanoseconds
		StartUtime int64        // start time of the last allocation attempt in nanoseconds.
		Size       int64        // size of the last allocation attempt in bytes. 0 means no allocation attempt is in progress.
	}
}

func (m *memArbitrator) bigBudget() *ConcurrentBudget {
	return &m.budget.mu.bigB
}

func (m *memArbitrator) smallBudget() *TrackedConcurrentBudget {
	return m.budget.smallB
}

func (m *memArbitrator) bigBudgetGrowThreshold() int64 {
	return m.bigBudget().Used.Load()
}

func (m *memArbitrator) bigBudgetCap() int64 {
	return m.bigBudget().approxCapacity()
}

func (m *memArbitrator) bigBudgetUsed() int64 {
	return m.budget.mu.bigUsed.Load()
}

func (m *memArbitrator) setBigBudgetGrowThreshold(x int64) {
	m.bigBudget().Used.Store(x)
}

func (m *memArbitrator) doSetBigBudgetCap(x int64) {
	m.bigBudget().Capacity = x
}

func (m *memArbitrator) addBigBudgetUsed(d int64) int64 {
	return m.budget.mu.bigUsed.Add(d)
}

func (m *memArbitrator) smallBudgetUsed() int64 {
	return m.budget.mu.smallUsed.Load()
}

func (m *memArbitrator) addSmallBudget(d int64) int64 {
	m.smallBudget().HeapInuse.Add(d)
	m.smallBudget().Used.Add(d)
	return m.budget.mu.smallUsed.Add(d)
}

func (m *memArbitrator) cleanSmallBudget() (res int64) {
	res = m.budget.mu.smallUsed.Swap(0)
	m.smallBudget().HeapInuse.Add(-res)
	m.smallBudget().Used.Add(-res)
	return res
}

func (m *memArbitrator) useBigBudget() bool {
	return m.budget.useBig.Load()
}

// MemArbitration returns the time cost of memory arbitration in nanoseconds
func (t *Tracker) MemArbitration() time.Duration {
	if t == nil {
		return 0
	}
	m := t.MemArbitrator
	if m == nil {
		return 0
	}
	return time.Duration(m.AwaitAlloc.TotalDur.Load())
}

// WaitArbitrate returns the start time and size of the last memory allocation attempt.
func (t *Tracker) WaitArbitrate() (ts time.Time, size int64) {
	if t == nil {
		return
	}
	m := t.MemArbitrator
	if m == nil {
		return
	}
	return time.Unix(0, m.AwaitAlloc.StartUtime), m.AwaitAlloc.Size
}

func (m *memArbitrator) growBigBudget() {
	duration := int64(0)
	{
		upper := m.bigBudget()
		upper.Lock()

		used, growThreshold, capacity := m.bigBudgetUsed(), m.bigBudgetGrowThreshold(), m.bigBudgetCap()
		if used > growThreshold {
			// expect next cap := used * 2.718
			extra := max(((used*2783)>>10)-capacity, upper.Pool.allocAlignSize)
			extra = min(extra, m.poolAllocStats.MaxPoolAllocUnit)
			extra = max(extra, used-capacity)
			m.AwaitAlloc.StartUtime = time.Now().UnixNano()
			m.AwaitAlloc.Size = extra
			if err := upper.Pool.allocate(extra); err == nil {
				capacity += extra
				m.doSetBigBudgetCap(capacity)
				m.setBigBudgetGrowThreshold(max(capacity*95/100, used))
			}
			duration = time.Now().UnixNano() - m.AwaitAlloc.StartUtime
			m.AwaitAlloc.StartUtime = 0
			m.AwaitAlloc.Size = 0
		}

		upper.Unlock()
	}

	if duration > 0 {
		m.AwaitAlloc.TotalDur.Add(duration)
		metrics.GlobalMemArbitrationDuration.Observe(time.Duration(duration).Seconds())
	}
}

func (m *memArbitrator) initBigBudget() {
	m.budget.useBig.Lock()
	defer m.budget.useBig.Unlock()

	if m.useBigBudget() {
		return
	}

	if smallUsed := m.smallBudgetUsed(); smallUsed > 0 {
		m.addBigBudgetUsed(smallUsed)
		defer m.cleanSmallBudget()
	}

	root, err := m.EmplaceRootPool(m.uid)
	if err != nil {
		panic(err)
	}

	if m.isInternal {
		globalArbitrator.metrics.pools.internalSession.Add(1)
	}

	if !root.Restart(m.ctx) || !m.state.CompareAndSwap(memArbitratorStateSmallBudget, memArbitratorStateIntoBigBudget) {
		panic("failed to init mem pool")
	}

	{
		globalArbitrator.metrics.pools.small.Add(-1)
		globalArbitrator.metrics.pools.intoBig.Add(1)
	}

	if intest.InTest {
		if mockDebugInject != nil {
			mockDebugInject()
		}
	}

	m.bigBudget().Pool = root.entry.pool

	if m.reserveSize > 0 {
		m.reserveBigBudget(m.reserveSize)
		metrics.GlobalMemArbitratorSubEvents.PoolInitReserve.Inc()
	} else if m.ctx.PrevMaxMem > 0 {
		metrics.GlobalMemArbitratorSubEvents.PoolInitHitDigest.Inc()
		m.reserveBigBudget(m.ctx.PrevMaxMem)
	} else if m.bigBudgetUsed() > m.poolAllocStats.SmallPoolLimit {
		if initCap := m.SuggestPoolInitCap(); initCap != 0 {
			m.reserveBigBudget(initCap)
			metrics.GlobalMemArbitratorSubEvents.PoolInitMediumQuota.Inc()
		}
	}

	if m.bigBudgetCap() == 0 {
		metrics.GlobalMemArbitratorSubEvents.PoolInitNone.Inc()
	}

	m.budget.useBig.Store(true)

	if intest.InTest {
		if mockDebugInject != nil {
			mockDebugInject()
		}
	}

	if m.state.CompareAndSwap(memArbitratorStateIntoBigBudget, memArbitratorStateBigBudget) {
		globalArbitrator.metrics.pools.intoBig.Add(-1)
		globalArbitrator.metrics.pools.big.Add(1)
	}
}

func (m *memArbitrator) reserveBigBudget(newCap int64) {
	duration := int64(0)
	{
		upper := m.bigBudget()
		upper.Lock()

		capacity := m.bigBudgetCap()
		extra := max(newCap*1053/1000, m.bigBudgetGrowThreshold(), capacity, m.bigBudgetUsed()) - capacity
		m.AwaitAlloc.StartUtime = time.Now().UnixNano()
		m.AwaitAlloc.Size = extra
		if err := upper.Pool.allocate(extra); err == nil {
			capacity += extra
			m.doSetBigBudgetCap(capacity)
			m.setBigBudgetGrowThreshold(capacity * 95 / 100)
		}
		duration = time.Now().UnixNano() - m.AwaitAlloc.StartUtime
		m.AwaitAlloc.StartUtime = 0
		m.AwaitAlloc.Size = 0

		upper.Unlock()
	}

	if duration > 0 {
		m.AwaitAlloc.TotalDur.Add(duration)
		metrics.GlobalMemArbitrationDuration.Observe(time.Duration(duration).Seconds())
	}
}

func (t *Tracker) resetMemArbitrator() {
	t.MemArbitrator = nil
}

// DetachMemArbitrator detaches the mem arbitrator from the tracker and cleans up related resources.
func (t *Tracker) DetachMemArbitrator() bool {
	m := t.MemArbitrator
	if m == nil {
		return false
	}

	if m.smallBudgetUsed() != 0 {
		m.cleanSmallBudget()
	}

	if m.state.Load() == memArbitratorStateDown {
		return false
	}

	switch m.state.Swap(memArbitratorStateDown) {
	case memArbitratorStateSmallBudget:
		globalArbitrator.metrics.pools.small.Add(-1)
	case memArbitratorStateIntoBigBudget:
		{
			m.budget.useBig.Lock() // wait for initBigBudget to finish

			globalArbitrator.metrics.pools.intoBig.Add(-1)

			m.budget.useBig.Unlock()
		}
	case memArbitratorStateBigBudget:
		globalArbitrator.metrics.pools.big.Add(-1)
	default:
		return false
	}

	if m.isInternal {
		globalArbitrator.metrics.pools.internal.Add(-1)
	}

	killed := false
	if m.killer != nil {
		killed = m.killer.Signal != 0
	}
	maxConsumed := t.maxConsumed.Load()

	if !killed {
		m.UpdateDigestProfileCache(m.digestID, maxConsumed, m.approxUnixTimeSec())
	}

	if m.useBigBudget() {
		m.bigBudget().Stop()
		m.ResetRootPoolByID(m.uid, maxConsumed, !killed)
	}
	return true
}

// InitMemArbitratorForTest is a simplified version of InitMemArbitrator for test usage.
func (t *Tracker) InitMemArbitratorForTest() bool {
	return t.InitMemArbitrator(GlobalMemArbitrator(), 0, nil, "", ArbitrationPriorityMedium, false, 0, false)
}

// InitMemArbitrator attaches (not thread-safe) to the mem arbitrator and initializes the context
// "m" is the mem-arbitrator.
// "memQuotaQuery" is the maximum memory quota for query.
// "killer" is the sql killer.
// "digestKey" is the digest key.
// "memPriority" is the memory priority for arbitration.
// "waitAverse" represents the wait averse property.
// "explicitReserveSize" is the explicit mem quota size to be reserved.
// "isInternal" indicates whether the tracker is for internal session.
func (t *Tracker) InitMemArbitrator(
	g *MemArbitrator,
	memQuotaQuery int64,
	killer *sqlkiller.SQLKiller,
	digestKey string,
	memPriority ArbitrationPriority,
	waitAverse bool,
	explicitReserveSize int64,
	isInternal bool,
) bool {
	if g == nil || t == nil || t.MemArbitrator != nil {
		return false
	}

	uid := t.SessionID.Load()
	digestID := HashStr(digestKey)
	prevMaxMem := int64(0)

	if explicitReserveSize == 0 && len(digestKey) > 0 {
		if maxMem, found := g.GetDigestProfileCache(digestID, g.approxUnixTimeSec()); found {
			prevMaxMem = maxMem
		}
	}

	var cancelChan <-chan struct{}
	if killer != nil {
		cancelChan = killer.GetKillEventChan()
	}
	ctx := NewArbitrationContext(
		cancelChan,
		prevMaxMem,
		memQuotaQuery,
		&trackerArbitrateHelper{
			tracker: t,
		},
		memPriority,
		waitAverse,
		true,
	)

	m := &memArbitrator{
		MemArbitrator: g,
		uid:           uid,
		killer:        killer,
		digestID:      digestID,
		reserveSize:   explicitReserveSize,
		ctx:           ctx,
		isInternal:    isInternal,
	}
	t.MemArbitrator = m

	globalArbitrator.metrics.pools.small.Add(1)
	if m.isInternal {
		globalArbitrator.metrics.pools.internal.Add(1)
	}

	if explicitReserveSize > 0 || prevMaxMem > g.poolAllocStats.SmallPoolLimit {
		m.initBigBudget()
	} else {
		m.budget.smallB = g.GetAwaitFreeBudgets(uid)
		m.budget.smallLimit = g.poolAllocStats.SmallPoolLimit
	}

	return true
}

type trackerArbitrateHelper struct {
	tracker *Tracker
	killed  atomic.Bool
}

func (h *trackerArbitrateHelper) Finish() {
	t := h.tracker
	t.DetachMemArbitrator()
	if t.MemArbitrator.isInternal {
		globalArbitrator.metrics.pools.internalSession.Add(-1)
	}
}

func (h *trackerArbitrateHelper) Stop(reason ArbitratorStopReason) bool {
	if h.killed.Load() || h.killed.Swap(true) {
		return false
	}
	for tracker := h.tracker; tracker != nil; tracker = tracker.getParent() {
		if tracker.IsRootTrackerOfSess && tracker.Killer != nil {
			tracker.Killer.SendKillSignalWithKillEventReason(sqlkiller.KilledByMemArbitrator, reason.String())
			break
		}
	}
	return true
}

func (h *trackerArbitrateHelper) HeapInuse() int64 {
	return h.tracker.BytesConsumed()
}
