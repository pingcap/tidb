// Copyright 2015 PingCAP, Inc.
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

package autoid

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	minStep            = 30000
	maxStep            = 2000000
	defaultConsumeTime = 10 * time.Second
	minIncrement       = 1
	maxIncrement       = 65535
)

// Test needs to change it, so it's a variable.
var step = int64(30000)

var errInvalidTableID = terror.ClassAutoid.New(codeInvalidTableID, "invalid TableID")

// CustomAutoIncCacheOption is one kind of AllocOption to customize the allocator step length.
type CustomAutoIncCacheOption int64

// ApplyOn is implement the AllocOption interface.
func (step CustomAutoIncCacheOption) ApplyOn(alloc *allocator) {
	alloc.step = int64(step)
	alloc.customStep = true
}

// AllocOption is a interface to define allocator custom options coming in future.
type AllocOption interface {
	ApplyOn(*allocator)
}

// Allocator is an auto increment id generator.
// Just keep id unique actually.
type Allocator interface {
	// Alloc allocs N consecutive autoID for table with tableID, returning (min, max] of the allocated autoID batch.
	// It gets a batch of autoIDs at a time. So it does not need to access storage for each call.
	// The consecutive feature is used to insert multiple rows in a statement.
	// increment & offset is used to validate the start position (the allocator's base is not always the last allocated id).
	// The returned range is (min, max]:
	// case increment=1 & offset=1: you can derive the ids like min+1, min+2... max.
	// case increment=x & offset=y: you firstly need to seek to firstID by `SeekToFirstAutoIDXXX`, then derive the IDs like firstID, firstID + increment * 2... in the caller.
	Alloc(tableID int64, n uint64, increment, offset int64) (int64, int64, error)

	// Rebase rebases the autoID base for table with tableID and the new base value.
	// If allocIDs is true, it will allocate some IDs and save to the cache.
	// If allocIDs is false, it will not allocate IDs.
	Rebase(tableID, newBase int64, allocIDs bool) error
	// Base return the current base of Allocator.
	Base() int64
	// End is only used for test.
	End() int64
	// NextGlobalAutoID returns the next global autoID.
	NextGlobalAutoID(tableID int64) (int64, error)
}

type allocator struct {
	mu    sync.Mutex
	base  int64
	end   int64
	store kv.Storage
	// dbID is current database's ID.
	dbID          int64
	isUnsigned    bool
	lastAllocTime time.Time
	step          int64
	customStep    bool
}

// GetStep is only used by tests
func GetStep() int64 {
	return step
}

// SetStep is only used by tests
func SetStep(s int64) {
	step = s
}

// Base implements autoid.Allocator Base interface.
func (alloc *allocator) Base() int64 {
	return alloc.base
}

// End implements autoid.Allocator End interface.
func (alloc *allocator) End() int64 {
	return alloc.end
}

// NextGlobalAutoID implements autoid.Allocator NextGlobalAutoID interface.
func (alloc *allocator) NextGlobalAutoID(tableID int64) (int64, error) {
	var autoID int64
	startTime := time.Now()
	err := kv.RunInNewTxn(alloc.store, true, func(txn kv.Transaction) error {
		var err1 error
		m := meta.NewMeta(txn)
		autoID, err1 = m.GetAutoTableID(alloc.dbID, tableID)
		if err1 != nil {
			return errors.Trace(err1)
		}
		return nil
	})
	metrics.AutoIDHistogram.WithLabelValues(metrics.GlobalAutoID, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	if alloc.isUnsigned {
		return int64(uint64(autoID) + 1), err
	}
	return autoID + 1, err
}

func (alloc *allocator) rebase4Unsigned(tableID int64, requiredBase uint64, allocIDs bool) error {
	// Satisfied by alloc.base, nothing to do.
	if requiredBase <= uint64(alloc.base) {
		return nil
	}
	// Satisfied by alloc.end, need to update alloc.base.
	if requiredBase <= uint64(alloc.end) {
		alloc.base = int64(requiredBase)
		return nil
	}
	var newBase, newEnd uint64
	startTime := time.Now()
	err := kv.RunInNewTxn(alloc.store, true, func(txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		currentEnd, err1 := m.GetAutoTableID(alloc.dbID, tableID)
		if err1 != nil {
			return err1
		}
		uCurrentEnd := uint64(currentEnd)
		if allocIDs {
			newBase = mathutil.MaxUint64(uCurrentEnd, requiredBase)
			newEnd = mathutil.MinUint64(math.MaxUint64-uint64(alloc.step), newBase) + uint64(alloc.step)
		} else {
			if uCurrentEnd >= requiredBase {
				newBase = uCurrentEnd
				newEnd = uCurrentEnd
				// Required base satisfied, we don't need to update KV.
				return nil
			}
			// If we don't want to allocate IDs, for example when creating a table with a given base value,
			// We need to make sure when other TiDB server allocates ID for the first time, requiredBase + 1
			// will be allocated, so we need to increase the end to exactly the requiredBase.
			newBase = requiredBase
			newEnd = requiredBase
		}
		_, err1 = m.GenAutoTableID(alloc.dbID, tableID, int64(newEnd-uCurrentEnd))
		return err1
	})
	metrics.AutoIDHistogram.WithLabelValues(metrics.TableAutoIDRebase, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	if err != nil {
		return err
	}
	alloc.base, alloc.end = int64(newBase), int64(newEnd)
	return nil
}

func (alloc *allocator) rebase4Signed(tableID, requiredBase int64, allocIDs bool) error {
	// Satisfied by alloc.base, nothing to do.
	if requiredBase <= alloc.base {
		return nil
	}
	// Satisfied by alloc.end, need to update alloc.base.
	if requiredBase <= alloc.end {
		alloc.base = requiredBase
		return nil
	}
	var newBase, newEnd int64
	startTime := time.Now()
	err := kv.RunInNewTxn(alloc.store, true, func(txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		currentEnd, err1 := m.GetAutoTableID(alloc.dbID, tableID)
		if err1 != nil {
			return err1
		}
		if allocIDs {
			newBase = mathutil.MaxInt64(currentEnd, requiredBase)
			newEnd = mathutil.MinInt64(math.MaxInt64-alloc.step, newBase) + alloc.step
		} else {
			if currentEnd >= requiredBase {
				newBase = currentEnd
				newEnd = currentEnd
				// Required base satisfied, we don't need to update KV.
				return nil
			}
			// If we don't want to allocate IDs, for example when creating a table with a given base value,
			// We need to make sure when other TiDB server allocates ID for the first time, requiredBase + 1
			// will be allocated, so we need to increase the end to exactly the requiredBase.
			newBase = requiredBase
			newEnd = requiredBase
		}
		_, err1 = m.GenAutoTableID(alloc.dbID, tableID, newEnd-currentEnd)
		return err1
	})
	metrics.AutoIDHistogram.WithLabelValues(metrics.TableAutoIDRebase, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	if err != nil {
		return err
	}
	alloc.base, alloc.end = newBase, newEnd
	return nil
}

// Rebase implements autoid.Allocator Rebase interface.
// The requiredBase is the minimum base value after Rebase.
// The real base may be greater than the required base.
func (alloc *allocator) Rebase(tableID, requiredBase int64, allocIDs bool) error {
	if tableID == 0 {
		return errInvalidTableID.GenWithStack("Invalid tableID")
	}

	alloc.mu.Lock()
	defer alloc.mu.Unlock()

	if alloc.isUnsigned {
		return alloc.rebase4Unsigned(tableID, uint64(requiredBase), allocIDs)
	}
	return alloc.rebase4Signed(tableID, requiredBase, allocIDs)
}

// NextStep return new auto id step according to previous step and consuming time.
func NextStep(curStep int64, consumeDur time.Duration) int64 {
	failpoint.Inject("mockAutoIDCustomize", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(3)
		}
	})
	failpoint.Inject("mockAutoIDChange", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(step)
		}
	})

	consumeRate := defaultConsumeTime.Seconds() / consumeDur.Seconds()
	res := int64(float64(curStep) * consumeRate)
	if res < minStep {
		return minStep
	} else if res > maxStep {
		return maxStep
	}
	return res
}

// NewAllocator returns a new auto increment id generator on the store.
func NewAllocator(store kv.Storage, dbID int64, isUnsigned bool, opts ...AllocOption) Allocator {
	alloc := &allocator{
		store:         store,
		dbID:          dbID,
		isUnsigned:    isUnsigned,
		step:          step,
		lastAllocTime: time.Now(),
	}
	for _, fn := range opts {
		fn.ApplyOn(alloc)
	}
	return alloc
}

//codeInvalidTableID is the code of autoid error.
const codeInvalidTableID terror.ErrCode = 1

var localSchemaID = int64(math.MaxInt64)

// GenLocalSchemaID generates a local schema ID.
func GenLocalSchemaID() int64 {
	return atomic.AddInt64(&localSchemaID, -1)
}

// Alloc implements autoid.Allocator Alloc interface.
// For autoIncrement allocator, the increment and offset should always be positive in [1, 65535].
// Attention:
// When increment and offset is not the default value(1), the return range (min, max] need to
// calculate the correct start position rather than simply the add 1 to min. Then you can derive
// the successive autoID by adding increment * cnt to firstID for (n-1) times.
//
// Example:
// (6, 13] is returned, increment = 4, offset = 1, n = 2.
// 6 is the last allocated value for other autoID or handle, maybe with different increment and step,
// but actually we don't care about it, all we need is to calculate the new autoID corresponding to the
// increment and offset at this time now. To simplify the rule is like (ID - offset) % increment = 0,
// so the first autoID should be 9, then add increment to it to get 13.
func (alloc *allocator) Alloc(tableID int64, n uint64, increment, offset int64) (int64, int64, error) {
	if tableID == 0 {
		return 0, 0, errInvalidTableID.GenWithStackByArgs("Invalid tableID")
	}
	if n == 0 {
		return 0, 0, nil
	}
	var skipValid bool
	failpoint.Inject("unValidIncrementAndOffset", func(val failpoint.Value) {
		if val.(bool) {
			skipValid = true
		}
	})
	if !skipValid {
		if !validIncrementAndOffset(increment, offset) {
			return 0, 0, errInvalidIncrementAndOffset.GenWithStackByArgs(increment, offset)
		}
	}
	alloc.mu.Lock()
	defer alloc.mu.Unlock()
	if alloc.isUnsigned {
		return alloc.alloc4Unsigned(tableID, n, increment, offset)
	}
	return alloc.alloc4Signed(tableID, n, increment, offset)
}

func validIncrementAndOffset(increment, offset int64) bool {
	return (increment >= minIncrement && increment <= maxIncrement) && (offset >= minIncrement && offset <= maxIncrement)
}

// CalcNeededBatchSize is used to calculate batch size for autoID allocation.
// It firstly seeks to the first valid position based on increment and offset,
// then plus the length remained, which could be (n-1) * increment.
func CalcNeededBatchSize(base, n, increment, offset int64, isUnsigned bool) int64 {
	if increment == 1 {
		return n
	}
	if isUnsigned {
		// SeekToFirstAutoIDUnSigned seeks to the next unsigned valid position.
		nr := SeekToFirstAutoIDUnSigned(uint64(base), uint64(increment), uint64(offset))
		// Calculate the total batch size needed.
		nr += (uint64(n) - 1) * uint64(increment)
		return int64(nr - uint64(base))
	}
	nr := SeekToFirstAutoIDSigned(base, increment, offset)
	// Calculate the total batch size needed.
	nr += (n - 1) * increment
	return nr - base
}

// SeekToFirstAutoIDSigned seeks to the next valid signed position.
func SeekToFirstAutoIDSigned(base, increment, offset int64) int64 {
	nr := (base + increment - offset) / increment
	nr = nr*increment + offset
	return nr
}

// SeekToFirstAutoIDUnSigned seeks to the next valid unsigned position.
func SeekToFirstAutoIDUnSigned(base, increment, offset uint64) uint64 {
	nr := (base + increment - offset) / increment
	nr = nr*increment + offset
	return nr
}

func (alloc *allocator) alloc4Signed(tableID int64, n uint64, increment, offset int64) (int64, int64, error) {
	// Check offset rebase if necessary.
	if offset-1 > alloc.base {
		if err := alloc.rebase4Signed(tableID, offset-1, true); err != nil {
			return 0, 0, err
		}
	}
	// CalcNeededBatchSize calculates the total batch size needed.
	n1 := CalcNeededBatchSize(alloc.base, int64(n), increment, offset, alloc.isUnsigned)

	// Condition alloc.base+N1 > alloc.end will overflow when alloc.base + N1 > MaxInt64. So need this.
	if math.MaxInt64-alloc.base <= n1 {
		return 0, 0, ErrAutoincReadFailed
	}
	// The local rest is not enough for allocN, skip it.
	if alloc.base+n1 > alloc.end {
		var newBase, newEnd int64
		startTime := time.Now()
		nextStep := alloc.step
		if !alloc.customStep {
			// Although it may skip a segment here, we still think it is consumed.
			consumeDur := startTime.Sub(alloc.lastAllocTime)
			nextStep = NextStep(alloc.step, consumeDur)
		}
		err := kv.RunInNewTxn(alloc.store, true, func(txn kv.Transaction) error {
			m := meta.NewMeta(txn)
			var err1 error
			newBase, err1 = m.GetAutoTableID(alloc.dbID, tableID)
			if err1 != nil {
				return err1
			}
			// CalcNeededBatchSize calculates the total batch size needed on global base.
			n1 = CalcNeededBatchSize(newBase, int64(n), increment, offset, alloc.isUnsigned)
			// Although the step is customized by user, we still need to make sure nextStep is big enough for insert batch.
			if nextStep < n1 {
				nextStep = n1
			}
			tmpStep := mathutil.MinInt64(math.MaxInt64-newBase, nextStep)
			// The global rest is not enough for alloc.
			if tmpStep < n1 {
				return ErrAutoincReadFailed
			}
			newEnd, err1 = m.GenAutoTableID(alloc.dbID, tableID, tmpStep)
			return err1
		})
		metrics.AutoIDHistogram.WithLabelValues(metrics.TableAutoIDAlloc, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
		if err != nil {
			return 0, 0, err
		}
		// Store the step for non-customized-step allocator to calculate next dynamic step.
		if !alloc.customStep {
			alloc.step = nextStep
		}
		alloc.lastAllocTime = time.Now()
		if newBase == math.MaxInt64 {
			return 0, 0, ErrAutoincReadFailed
		}
		alloc.base, alloc.end = newBase, newEnd
	}
	logutil.Logger(context.TODO()).Debug("alloc N signed ID",
		zap.Uint64("from ID", uint64(alloc.base)),
		zap.Uint64("to ID", uint64(alloc.base+n1)),
		zap.Int64("table ID", tableID),
		zap.Int64("database ID", alloc.dbID))
	min := alloc.base
	alloc.base += n1
	return min, alloc.base, nil
}

func (alloc *allocator) alloc4Unsigned(tableID int64, n uint64, increment, offset int64) (int64, int64, error) {
	// Check offset rebase if necessary.
	if uint64(offset-1) > uint64(alloc.base) {
		if err := alloc.rebase4Unsigned(tableID, uint64(offset-1), true); err != nil {
			return 0, 0, err
		}
	}
	// CalcNeededBatchSize calculates the total batch size needed.
	n1 := CalcNeededBatchSize(alloc.base, int64(n), increment, offset, alloc.isUnsigned)

	// Condition alloc.base+n1 > alloc.end will overflow when alloc.base + n1 > MaxInt64. So need this.
	if math.MaxUint64-uint64(alloc.base) <= uint64(n1) {
		return 0, 0, ErrAutoincReadFailed
	}
	// The local rest is not enough for alloc, skip it.
	if uint64(alloc.base)+uint64(n1) > uint64(alloc.end) {
		var newBase, newEnd int64
		startTime := time.Now()
		nextStep := alloc.step
		if !alloc.customStep {
			// Although it may skip a segment here, we still treat it as consumed.
			consumeDur := startTime.Sub(alloc.lastAllocTime)
			nextStep = NextStep(alloc.step, consumeDur)
		}
		err := kv.RunInNewTxn(alloc.store, true, func(txn kv.Transaction) error {
			m := meta.NewMeta(txn)
			var err1 error
			newBase, err1 = m.GetAutoTableID(alloc.dbID, tableID)
			if err1 != nil {
				return err1
			}
			// CalcNeededBatchSize calculates the total batch size needed on new base.
			n1 = CalcNeededBatchSize(newBase, int64(n), increment, offset, alloc.isUnsigned)
			// Although the step is customized by user, we still need to make sure nextStep is big enough for insert batch.
			if nextStep < n1 {
				nextStep = n1
			}
			tmpStep := int64(mathutil.MinUint64(math.MaxUint64-uint64(newBase), uint64(nextStep)))
			// The global rest is not enough for alloc.
			if tmpStep < n1 {
				return ErrAutoincReadFailed
			}
			newEnd, err1 = m.GenAutoTableID(alloc.dbID, tableID, tmpStep)
			return err1
		})
		metrics.AutoIDHistogram.WithLabelValues(metrics.TableAutoIDAlloc, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
		if err != nil {
			return 0, 0, err
		}
		// Store the step for non-customized-step allocator to calculate next dynamic step.
		if !alloc.customStep {
			alloc.step = nextStep
		}
		alloc.lastAllocTime = time.Now()
		if uint64(newBase) == math.MaxUint64 {
			return 0, 0, ErrAutoincReadFailed
		}
		alloc.base, alloc.end = newBase, newEnd
	}
	logutil.Logger(context.TODO()).Debug("alloc unsigned ID",
		zap.Uint64(" from ID", uint64(alloc.base)),
		zap.Uint64("to ID", uint64(alloc.base+n1)),
		zap.Int64("table ID", tableID),
		zap.Int64("database ID", alloc.dbID))
	min := alloc.base
	// Use uint64 n directly.
	alloc.base = int64(uint64(alloc.base) + uint64(n1))
	return min, alloc.base, nil
}

// TestModifyBaseAndEndInjection exported for testing modifying the base and end.
func TestModifyBaseAndEndInjection(alloc Allocator, base, end int64) {
	alloc.(*allocator).mu.Lock()
	alloc.(*allocator).base = base
	alloc.(*allocator).end = end
	alloc.(*allocator).mu.Unlock()
}
