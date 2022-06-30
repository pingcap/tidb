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
	"sort"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/metrics"
	atomicutil "go.uber.org/atomic"
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
	mu struct {
		sync.Mutex
		// The children memory trackers. If the Tracker is the Global Tracker, like executor.GlobalDiskUsageTracker,
		// we wouldn't maintain its children in order to avoiding mutex contention.
		children map[int][]*Tracker
	}
	actionMuForHardLimit actionMu
	actionMuForSoftLimit actionMu
	parMu                struct {
		sync.Mutex
		parent *Tracker // The parent memory tracker.
	}

	label         int   // Label of this "Tracker".
	bytesConsumed int64 // Consumed bytes.
	bytesLimit    atomic.Value
	maxConsumed   atomicutil.Int64 // max number of bytes consumed during execution.
	isGlobal      bool             // isGlobal indicates whether this tracker is global tracker
}

type actionMu struct {
	sync.Mutex
	actionOnExceed ActionOnExceed
}

// softScale means the scale of the soft limit to the hard limit.
const softScale = 0.8

// bytesLimits holds limit config atomically.
type bytesLimits struct {
	bytesHardLimit int64 // bytesHardLimit <= 0 means no limit, used for actionMuForHardLimit.
	bytesSoftLimit int64 // bytesSoftLimit <= 0 means no limit, used for actionMuForSoftLimit.
}

// InitTracker initializes a memory tracker.
//	1. "label" is the label used in the usage string.
//	2. "bytesLimit <= 0" means no limit.
// For the common tracker, isGlobal is default as false
func InitTracker(t *Tracker, label int, bytesLimit int64, action ActionOnExceed) {
	t.mu.children = nil
	t.actionMuForHardLimit.actionOnExceed = action
	t.actionMuForSoftLimit.actionOnExceed = nil
	t.parMu.parent = nil

	t.label = label
	t.bytesLimit.Store(&bytesLimits{
		bytesHardLimit: bytesLimit,
		bytesSoftLimit: int64(float64(bytesLimit) * softScale),
	})
	t.maxConsumed.Store(0)
	t.isGlobal = false
}

// NewTracker creates a memory tracker.
//	1. "label" is the label used in the usage string.
//	2. "bytesLimit <= 0" means no limit.
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
	return t.bytesLimit.Load().(*bytesLimits).bytesHardLimit == val
}

// SetBytesLimit sets the bytes limit for this tracker.
// "bytesHardLimit <= 0" means no limit.
func (t *Tracker) SetBytesLimit(bytesLimit int64) {
	t.bytesLimit.Store(&bytesLimits{
		bytesHardLimit: bytesLimit,
		bytesSoftLimit: int64(float64(bytesLimit) * softScale),
	})
}

// GetBytesLimit gets the bytes limit for this tracker.
// "bytesHardLimit <= 0" means no limit.
func (t *Tracker) GetBytesLimit() int64 {
	return t.bytesLimit.Load().(*bytesLimits).bytesHardLimit
}

// CheckExceed checks whether the consumed bytes is exceed for this tracker.
func (t *Tracker) CheckExceed() bool {
	bytesHardLimit := t.bytesLimit.Load().(*bytesLimits).bytesHardLimit
	return atomic.LoadInt64(&t.bytesConsumed) >= bytesHardLimit && bytesHardLimit > 0
}

// SetActionOnExceed sets the action when memory usage exceeds bytesHardLimit.
func (t *Tracker) SetActionOnExceed(a ActionOnExceed) {
	t.actionMuForHardLimit.Lock()
	t.actionMuForHardLimit.actionOnExceed = a
	t.actionMuForHardLimit.Unlock()
}

// FallbackOldAndSetNewAction sets the action when memory usage exceeds bytesHardLimit
// and set the original action as its fallback.
func (t *Tracker) FallbackOldAndSetNewAction(a ActionOnExceed) {
	t.actionMuForHardLimit.Lock()
	defer t.actionMuForHardLimit.Unlock()
	t.actionMuForHardLimit.actionOnExceed = reArrangeFallback(t.actionMuForHardLimit.actionOnExceed, a)
}

// FallbackOldAndSetNewActionForSoftLimit sets the action when memory usage exceeds bytesSoftLimit
// and set the original action as its fallback.
func (t *Tracker) FallbackOldAndSetNewActionForSoftLimit(a ActionOnExceed) {
	t.actionMuForSoftLimit.Lock()
	defer t.actionMuForSoftLimit.Unlock()
	t.actionMuForSoftLimit.actionOnExceed = reArrangeFallback(t.actionMuForSoftLimit.actionOnExceed, a)
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
		a.SetFallback(b)
	} else {
		a.SetFallback(reArrangeFallback(a.GetFallback(), b))
	}
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
	parent := t.getParent()
	if parent == nil {
		return
	}
	if parent.isGlobal {
		t.DetachFromGlobalTracker()
		return
	}
	parent.remove(t)
	t.mu.Lock()
	defer t.mu.Unlock()
	t.setParent(nil)
}

func (t *Tracker) remove(oldChild *Tracker) {
	found := false
	label := oldChild.label
	t.mu.Lock()
	if t.mu.children != nil {
		children := t.mu.children[label]
		for i, child := range children {
			if child == oldChild {
				children = append(children[:i], children[i+1:]...)
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
func (t *Tracker) Consume(bytes int64) {
	if bytes == 0 {
		return
	}
	var rootExceed, rootExceedForSoftLimit *Tracker
	for tracker := t; tracker != nil; tracker = tracker.getParent() {
		bytesConsumed := atomic.AddInt64(&tracker.bytesConsumed, bytes)
		limits := tracker.bytesLimit.Load().(*bytesLimits)
		if bytesConsumed >= limits.bytesHardLimit && limits.bytesHardLimit > 0 {
			rootExceed = tracker
		}
		if bytesConsumed >= limits.bytesSoftLimit && limits.bytesSoftLimit > 0 {
			rootExceedForSoftLimit = tracker
		}

		for {
			maxNow := tracker.maxConsumed.Load()
			consumed := atomic.LoadInt64(&tracker.bytesConsumed)
			if consumed > maxNow && !tracker.maxConsumed.CAS(maxNow, consumed) {
				continue
			}
			if label, ok := MetricsTypes[tracker.label]; ok {
				metrics.MemoryUsage.WithLabelValues(label).Set(float64(consumed))
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

	if bytes > 0 && rootExceedForSoftLimit != nil {
		tryAction(&rootExceedForSoftLimit.actionMuForSoftLimit, rootExceedForSoftLimit)
	}
	if bytes > 0 && rootExceed != nil {
		tryAction(&rootExceed.actionMuForHardLimit, rootExceed)
	}
}

// BufferedConsume is used to buffer memory usage and do late consume
func (t *Tracker) BufferedConsume(bufferedMemSize *int64, bytes int64) {
	*bufferedMemSize += bytes
	if *bufferedMemSize > int64(config.TrackMemWhenExceeds) {
		t.Consume(*bufferedMemSize)
		*bufferedMemSize = int64(0)
	}
}

// BytesConsumed returns the consumed memory usage value in bytes.
func (t *Tracker) BytesConsumed() int64 {
	return atomic.LoadInt64(&t.bytesConsumed)
}

// MaxConsumed returns max number of bytes consumed during execution.
func (t *Tracker) MaxConsumed() int64 {
	return t.maxConsumed.Load()
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
	sort.Ints(labels)
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
func (t *Tracker) FormatBytes(numBytes int64) string {
	return FormatBytes(numBytes)
}

// BytesToString converts the memory consumption to a readable string.
func BytesToString(numBytes int64) string {
	GB := float64(numBytes) / float64(byteSizeGB)
	if GB > 1 {
		return fmt.Sprintf("%v GB", GB)
	}

	MB := float64(numBytes) / float64(byteSizeMB)
	if MB > 1 {
		return fmt.Sprintf("%v MB", MB)
	}

	KB := float64(numBytes) / float64(byteSizeKB)
	if KB > 1 {
		return fmt.Sprintf("%v KB", KB)
	}

	return fmt.Sprintf("%v Bytes", numBytes)
}

const (
	byteSizeGB = int64(1 << 30)
	byteSizeMB = int64(1 << 20)
	byteSizeKB = int64(1 << 10)
	byteSizeBB = int64(1)
)

// FormatBytes uses to format bytes, this function will prune precision before format bytes.
func FormatBytes(numBytes int64) string {
	if numBytes <= byteSizeKB {
		return BytesToString(numBytes)
	}
	unit, unitStr := getByteUnit(numBytes)
	if unit == byteSizeBB {
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
	return byteSizeBB, "Bytes"
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
	t.Consume(-t.BytesConsumed())
	t.Consume(bytes)
}

func (t *Tracker) getParent() *Tracker {
	t.parMu.Lock()
	defer t.parMu.Unlock()
	return t.parMu.parent
}

func (t *Tracker) setParent(parent *Tracker) {
	t.parMu.Lock()
	defer t.parMu.Unlock()
	t.parMu.parent = parent
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
	// LabelForChunkListInDisk represents the label of the chunk list in disk
	LabelForChunkListInDisk int = -9
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
)

// MetricsTypes is used to get label for metrics
var MetricsTypes = map[int]string{
	LabelForGlobalAnalyzeMemory: "analyze",
}
