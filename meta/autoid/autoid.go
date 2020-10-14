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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// Attention:
// For reading cluster TiDB memory tables, the system schema/table should be same.
// Once the system schema/table id been allocated, it can't be changed any more.
// Change the system schema/table id may have the compatibility problem.
const (
	// SystemSchemaIDFlag is the system schema/table id flag, uses the highest bit position as system schema ID flag, it's exports for test.
	SystemSchemaIDFlag = 1 << 62
	// InformationSchemaDBID is the information_schema schema id, it's exports for test.
	InformationSchemaDBID int64 = SystemSchemaIDFlag | 1
	// PerformanceSchemaDBID is the performance_schema schema id, it's exports for test.
	PerformanceSchemaDBID int64 = SystemSchemaIDFlag | 10000
	// MetricSchemaDBID is the metrics_schema schema id, it's exported for test.
	MetricSchemaDBID int64 = SystemSchemaIDFlag | 20000
)

const (
	minStep            = 30000
	maxStep            = 2000000
	defaultConsumeTime = 10 * time.Second
	minIncrement       = 1
	maxIncrement       = 65535
)

// RowIDBitLength is the bit number of a row id in TiDB.
const RowIDBitLength = 64

// DefaultAutoRandomBits is the default value of auto sharding.
const DefaultAutoRandomBits = 5

// MaxAutoRandomBits is the max value of auto sharding.
const MaxAutoRandomBits = 15

// Test needs to change it, so it's a variable.
var step = int64(30000)

// AllocatorType is the type of allocator for generating auto-id. Different type of allocators use different key-value pairs.
type AllocatorType = uint8

const (
	// RowIDAllocType indicates the allocator is used to allocate row id.
	RowIDAllocType AllocatorType = iota
	// AutoIncrementType indicates the allocator is used to allocate auto increment value.
	AutoIncrementType
	// AutoRandomType indicates the allocator is used to allocate auto-shard id.
	AutoRandomType
	// SequenceType indicates the allocator is used to allocate sequence value.
	SequenceType
)

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
	// Alloc allocates N consecutive autoIDs.
	// It gets a batch of autoIDs at a time. So it does not need to access storage for each call.
	// The consecutive feature is used to insert multiple rows in a statement.
	Alloc(tableID int64, n uint64, increment, offset int64) (IDIterator, error)

	// AllocSeqCache allocs sequence batch value cached in table level（rather than in alloc), the returned range covering
	// the size of sequence cache with it's increment. The returned round indicates the sequence cycle times if it is with
	// cycle option.
	AllocSeqCache(sequenceID int64) (min int64, max int64, round int64, err error)

	// Rebase rebases the autoID base for table with tableID and the new base value.
	// If allocIDs is true, it will allocate some IDs and save to the cache.
	// If allocIDs is false, it will not allocate IDs.
	Rebase(tableID, newBase int64, allocIDs bool) error

	// RebaseSeq rebases the sequence value in number axis with tableID and the new base value.
	RebaseSeq(table, newBase int64) (int64, bool, error)

	// Base return the current base of Allocator.
	Base() int64
	// End is only used for test.
	End() int64
	// NextGlobalAutoID returns the next global autoID.
	NextGlobalAutoID(tableID int64) (int64, error)
	GetType() AllocatorType
}

// IDIterator represents a list of values allocated by Alloc.alloc().
type IDIterator struct {
	restCount uint64
	current   int64
	increment int64
}

// Next returns the next allocated value.
func (iter *IDIterator) Next() (bool, int64) {
	if iter.restCount == 0 {
		return false, 0
	}
	ret := iter.current
	iter.current = int64(uint64(iter.current) + uint64(iter.increment))
	iter.restCount = iter.restCount - 1
	return true, ret
}

// Current returns the current allocated value. Note: if there is no values, 0 is returned.
func (iter *IDIterator) Current() int64 {
	return iter.current
}

// Last returns the last allocated value. Note: if there is no values, 0 is returned.
func (iter *IDIterator) Last() int64 {
	return iter.current + int64(iter.restCount-1)*iter.increment
}

// Skip skip one value in the IDIterator, and return the IDIterator itself(for chain call).
func (iter *IDIterator) Skip() *IDIterator {
	if iter.restCount == 0 {
		return iter
	}
	iter.current = iter.current + iter.increment
	iter.restCount = iter.restCount - 1
	return iter
}

// Count returns the remaining number of values in IDIterator.
func (iter *IDIterator) Count() uint64 {
	return iter.restCount
}

// Allocators represents a set of `Allocator`s.
type Allocators []Allocator

// NewAllocators packs multiple `Allocator`s into Allocators.
func NewAllocators(allocators ...Allocator) Allocators {
	return allocators
}

// Get returns the Allocator according to the AllocatorType.
func (all Allocators) Get(allocType AllocatorType) Allocator {
	for _, a := range all {
		if a.GetType() == allocType {
			return a
		}
	}
	return nil
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
	allocType     AllocatorType
	sequence      *model.SequenceInfo
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
		autoID, err1 = getAutoIDByAllocType(m, alloc.dbID, tableID, alloc.allocType)
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

// rebase4Sequence won't alloc batch immediately, cause it won't cache value in allocator.
func (alloc *allocator) rebase4Sequence(tableID, requiredBase int64) (int64, bool, error) {
	startTime := time.Now()
	alreadySatisfied := false
	err := kv.RunInNewTxn(alloc.store, true, func(txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		currentEnd, err := getAutoIDByAllocType(m, alloc.dbID, tableID, alloc.allocType)
		if err != nil {
			return err
		}
		if alloc.sequence.Increment > 0 {
			if currentEnd >= requiredBase {
				// Required base satisfied, we don't need to update KV.
				alreadySatisfied = true
				return nil
			}
		} else {
			if currentEnd <= requiredBase {
				// Required base satisfied, we don't need to update KV.
				alreadySatisfied = true
				return nil
			}
		}

		// If we don't want to allocate IDs, for example when creating a table with a given base value,
		// We need to make sure when other TiDB server allocates ID for the first time, requiredBase + 1
		// will be allocated, so we need to increase the end to exactly the requiredBase.
		_, err = generateAutoIDByAllocType(m, alloc.dbID, tableID, requiredBase-currentEnd, alloc.allocType)
		return err
	})
	// TODO: sequence metrics
	metrics.AutoIDHistogram.WithLabelValues(metrics.TableAutoIDRebase, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	if err != nil {
		return 0, false, err
	}
	if alreadySatisfied {
		return 0, true, nil
	}
	return requiredBase, false, err
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
	return alloc.rebase(tableID, requiredBase, allocIDs)
}

func (alloc *allocator) rebase(tableID, requiredBase int64, allocIDs bool) error {
	// Satisfied by alloc.base, nothing to do.
	if alloc.cmp(requiredBase, alloc.base) <= 0 {
		return nil
	}
	// Satisfied by alloc.end, need to update alloc.base.
	if alloc.cmp(requiredBase, alloc.end) <= 0 {
		alloc.base = requiredBase
		return nil
	}

	var newBase, newEnd int64
	startTime := time.Now()
	err := kv.RunInNewTxn(alloc.store, true, func(txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		currentEnd, err1 := getAutoIDByAllocType(m, alloc.dbID, tableID, alloc.allocType)
		if err1 != nil {
			return err1
		}
		if allocIDs {
			newBase = alloc.max(currentEnd, requiredBase)
			newEnd, _ = alloc.plus(newBase, alloc.step)
		} else {
			if alloc.cmp(currentEnd, requiredBase) >= 0 {
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
		_, err1 = generateAutoIDByAllocType(m, alloc.dbID, tableID, newEnd-currentEnd, alloc.allocType)
		return err1
	})
	metrics.AutoIDHistogram.WithLabelValues(metrics.TableAutoIDRebase, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	if err != nil {
		return err
	}
	alloc.base, alloc.end = newBase, newEnd
	return nil
}

func (alloc *allocator) cmp(a int64, b int64) int {
	if a == b {
		return 0
	}
	if alloc.isUnsigned {
		if uint64(a) < uint64(b) {
			return -1
		}
		return 1
	}
	if a < b {
		return -1
	}
	return 1
}

func (alloc *allocator) max(a, b int64) int64 {
	if alloc.cmp(a, b) < 0 {
		return b
	}
	return a
}

func (alloc *allocator) plus(a, b int64) (result int64, overflow bool) {
	if alloc.isUnsigned {
		isOverflow := math.MaxUint64-uint64(a) < uint64(b)
		if isOverflow {
			return -1 /* int64(math.MaxUint64) */, true
		}
		return a + b, false
	}
	isOverflow := math.MaxInt64-a < b
	if isOverflow {
		return math.MaxInt64, true
	}
	return a + b, false
}

func (alloc *allocator) multiply(a, b int64) (result int64, overflow bool) {
	ua, ub := uint64(a), uint64(b)
	if ua <= 1 || ub <= 1 {
		return a * b, false
	}
	d := ua * ub
	return int64(d), d/ub != ua
}

// Rebase implements autoid.Allocator RebaseSeq interface.
// The return value is quite same as expression function, bool means whether it should be NULL,
// here it will be used in setval expression function (true meaning the set value has been satisfied, return NULL).
// case1:When requiredBase is satisfied with current value, it will return (0, true, nil),
// case2:When requiredBase is successfully set in, it will return (requiredBase, false, nil).
// If some error occurs in the process, return it immediately.
func (alloc *allocator) RebaseSeq(tableID, requiredBase int64) (int64, bool, error) {
	if tableID == 0 {
		return 0, false, errInvalidTableID.GenWithStack("Invalid tableID")
	}

	alloc.mu.Lock()
	defer alloc.mu.Unlock()
	return alloc.rebase4Sequence(tableID, requiredBase)
}

func (alloc *allocator) GetType() AllocatorType {
	return alloc.allocType
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
func NewAllocator(store kv.Storage, dbID int64, isUnsigned bool, allocType AllocatorType, opts ...AllocOption) Allocator {
	alloc := &allocator{
		store:         store,
		dbID:          dbID,
		isUnsigned:    isUnsigned,
		step:          step,
		lastAllocTime: time.Now(),
		allocType:     allocType,
	}
	for _, fn := range opts {
		fn.ApplyOn(alloc)
	}
	return alloc
}

// NewSequenceAllocator returns a new sequence value generator on the store.
func NewSequenceAllocator(store kv.Storage, dbID int64, info *model.SequenceInfo) Allocator {
	return &allocator{
		store: store,
		dbID:  dbID,
		// Sequence allocator is always signed.
		isUnsigned:    false,
		lastAllocTime: time.Now(),
		allocType:     SequenceType,
		sequence:      info,
	}
}

// NewAllocatorsFromTblInfo creates an array of allocators of different types with the information of model.TableInfo.
func NewAllocatorsFromTblInfo(store kv.Storage, schemaID int64, tblInfo *model.TableInfo) Allocators {
	var allocs []Allocator
	dbID := tblInfo.GetDBID(schemaID)
	hasRowID := !tblInfo.PKIsHandle && !tblInfo.IsCommonHandle
	hasAutoIncID := tblInfo.GetAutoIncrementColInfo() != nil
	if hasRowID || hasAutoIncID {
		if tblInfo.AutoIdCache > 0 {
			allocs = append(allocs, NewAllocator(store, dbID, tblInfo.IsAutoIncColUnsigned(), RowIDAllocType, CustomAutoIncCacheOption(tblInfo.AutoIdCache)))
		} else {
			allocs = append(allocs, NewAllocator(store, dbID, tblInfo.IsAutoIncColUnsigned(), RowIDAllocType))
		}
	}
	if tblInfo.ContainsAutoRandomBits() {
		allocs = append(allocs, NewAllocator(store, dbID, tblInfo.IsAutoRandomBitColUnsigned(), AutoRandomType))
	}
	if tblInfo.IsSequence() {
		allocs = append(allocs, NewSequenceAllocator(store, dbID, tblInfo.Sequence))
	}
	return NewAllocators(allocs...)
}

// Alloc implements autoid.Allocator Alloc interface.
// For autoIncrement allocator, the increment and offset should always be positive in [1, 65535].
// The values(ID) returned by the IDIterator satisfy the equation `(ID - offset) % increment = 0`.
func (alloc *allocator) Alloc(tableID int64, n uint64, increment, offset int64) (IDIterator, error) {
	if tableID == 0 {
		return IDIterator{}, errInvalidTableID.GenWithStackByArgs("Invalid tableID")
	}
	if n == 0 {
		return IDIterator{}, nil
	}
	switch alloc.allocType {
	case AutoIncrementType, RowIDAllocType, AutoRandomType:
		if !validIncrementAndOffset(increment, offset) {
			return IDIterator{}, errInvalidIncrementAndOffset.GenWithStackByArgs(increment, offset)
		}
	}
	alloc.mu.Lock()
	defer alloc.mu.Unlock()
	return alloc.alloc(tableID, n, increment, offset)
}

func (alloc *allocator) alloc(tableID int64, n uint64, increment, offset int64) (IDIterator, error) {
	// Check offset rebase if necessary.
	if alloc.cmp(offset-1, alloc.base) > 0 {
		if err := alloc.rebase(tableID, offset-1, true); err != nil {
			return IDIterator{}, err
		}
	}
	// calcNeededBatchSize calculates the total batch size needed.
	firstID, batchSize, expectedEnd, isOverflow := alloc.calcNeededBatchSize(alloc.base, int64(n), increment, offset)
	if isOverflow {
		return IDIterator{}, ErrAutoincReadFailed
	}
	// The local rest is not enough for alloc, skip it.
	if alloc.cmp(expectedEnd, alloc.end) > 0 {
		var newBase, newEnd int64
		startTime := time.Now()
		alloc.adjustStepByTime(startTime)
		err := kv.RunInNewTxn(alloc.store, true, func(txn kv.Transaction) error {
			m := meta.NewMeta(txn)
			var err1 error
			newBase, err1 = getAutoIDByAllocType(m, alloc.dbID, tableID, alloc.allocType)
			if err1 != nil {
				return err1
			}
			// recalculate the actual batch size.
			firstID, batchSize, _, isOverflow = alloc.calcNeededBatchSize(newBase, int64(n), increment, offset)
			if isOverflow {
				return ErrAutoincReadFailed
			}
			actualStep := alloc.calcActualStepAndAdjust(newBase, batchSize)
			newEnd, err1 = generateAutoIDByAllocType(m, alloc.dbID, tableID, actualStep, alloc.allocType)
			return err1
		})
		metrics.AutoIDHistogram.WithLabelValues(metrics.TableAutoIDAlloc, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
		if err != nil {
			return IDIterator{}, err
		}
		alloc.lastAllocTime = time.Now()
		alloc.base, alloc.end = newBase, newEnd
	}

	logutil.Logger(context.TODO()).Debug("alloc ID",
		zap.Uint64("from ID", uint64(alloc.base)),
		zap.Uint64("to ID", uint64(alloc.base+batchSize)),
		zap.Int64("table ID", tableID),
		zap.Int64("database ID", alloc.dbID))
	iter := IDIterator{
		restCount: n,
		current:   firstID,
		increment: increment,
	}
	alloc.base, _ = alloc.plus(alloc.base, batchSize)
	return iter, nil
}

func (alloc *allocator) AllocSeqCache(tableID int64) (int64, int64, int64, error) {
	if tableID == 0 {
		return 0, 0, 0, errInvalidTableID.GenWithStackByArgs("Invalid tableID")
	}
	alloc.mu.Lock()
	defer alloc.mu.Unlock()
	return alloc.alloc4Sequence(tableID)
}

func validIncrementAndOffset(increment, offset int64) bool {
	failpoint.Inject("skipIncrementOffsetValidation", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(true)
		}
	})
	return (increment >= minIncrement && increment <= maxIncrement) && (offset >= minIncrement && offset <= maxIncrement)
}

// calcNeededBatchSize is used to calculate batch size for autoID allocation.
// It firstly seeks to the first valid position based on increment and offset,
// then plus the length remained, which could be (n-1) * increment.
func (alloc *allocator) calcNeededBatchSize(base, n, increment, offset int64) (firstID, batchSize, expectedEnd int64, isOverflow bool) {
	if increment == 1 {
		expectedEnd, overflow := alloc.plus(base, n)
		if overflow {
			return 0, n, 0, true
		}
		firstID, overflow := alloc.seekToFirstAutoID(base, increment, offset)
		if overflow {
			return 0, n, 0, true
		}
		return firstID, n, expectedEnd, false
	}
	diff, overflow := alloc.multiply(n-1, increment)
	if overflow {
		return 0, 0, 0, true
	}
	firstID, overflow = alloc.seekToFirstAutoID(base, increment, offset)
	if overflow {
		return 0, 0, 0, true
	}
	expectedEnd, overflow = alloc.plus(firstID, diff)
	if overflow {
		return 0, 0, 0, true
	}
	return firstID, expectedEnd - base, expectedEnd, false
}

func (alloc *allocator) adjustStepByTime(startTime time.Time) {
	if alloc.customStep {
		return
	}
	consumeDur := startTime.Sub(alloc.lastAllocTime)
	alloc.step = NextStep(alloc.step, consumeDur)
}

// calcActualStep assumes `base + batchSize` doesn't overflow.
func (alloc *allocator) calcActualStepAndAdjust(base, batchSize int64) (actualStep int64) {
	actualStep = alloc.step
	if _, isOverflow := alloc.plus(base, alloc.step); isOverflow {
		// base < batchSize < alloc.step
		actualStep = batchSize
	}
	if alloc.cmp(actualStep, batchSize) < 0 {
		actualStep = batchSize
	}
	if !alloc.customStep {
		alloc.step = actualStep
	}
	return actualStep
}

// CalcSequenceBatchSize calculate the next sequence batch size.
func CalcSequenceBatchSize(base, size, increment, offset, MIN, MAX int64) (int64, error) {
	// The sequence is positive growth.
	if increment > 0 {
		if increment == 1 {
			// Sequence is already allocated to the end.
			if base >= MAX {
				return 0, ErrAutoincReadFailed
			}
			// The rest of sequence < cache size, return the rest.
			if MAX-base < size {
				return MAX - base, nil
			}
			// The rest of sequence is adequate.
			return size, nil
		}
		nr, ok := SeekToFirstSequenceValue(base, increment, offset, MIN, MAX)
		if !ok {
			return 0, ErrAutoincReadFailed
		}
		// The rest of sequence < cache size, return the rest.
		if MAX-nr < (size-1)*increment {
			return MAX - base, nil
		}
		return (nr - base) + (size-1)*increment, nil
	}
	// The sequence is negative growth.
	if increment == -1 {
		if base <= MIN {
			return 0, ErrAutoincReadFailed
		}
		if base-MIN < size {
			return base - MIN, nil
		}
		return size, nil
	}
	nr, ok := SeekToFirstSequenceValue(base, increment, offset, MIN, MAX)
	if !ok {
		return 0, ErrAutoincReadFailed
	}
	// The rest of sequence < cache size, return the rest.
	if nr-MIN < (size-1)*(-increment) {
		return base - MIN, nil
	}
	return (base - nr) + (size-1)*(-increment), nil
}

// SeekToFirstSequenceValue seeks to the next valid value (must be in range of [MIN, MAX]),
// the bool indicates whether the first value is got.
// The seeking formula is describe as below:
//  nr  := (base + increment - offset) / increment
// first := nr*increment + offset
// Because formula computation will overflow Int64, so we transfer it to uint64 for distance computation.
func SeekToFirstSequenceValue(base, increment, offset, MIN, MAX int64) (int64, bool) {
	if increment > 0 {
		// Sequence is already allocated to the end.
		if base >= MAX {
			return 0, false
		}
		uMax := EncodeIntToCmpUint(MAX)
		uBase := EncodeIntToCmpUint(base)
		uOffset := EncodeIntToCmpUint(offset)
		uIncrement := uint64(increment)
		if uMax-uBase < uIncrement {
			// Enum the possible first value.
			for i := uBase + 1; i <= uMax; i++ {
				if (i-uOffset)%uIncrement == 0 {
					return DecodeCmpUintToInt(i), true
				}
			}
			return 0, false
		}
		nr := (uBase + uIncrement - uOffset) / uIncrement
		nr = nr*uIncrement + uOffset
		first := DecodeCmpUintToInt(nr)
		return first, true
	}
	// Sequence is already allocated to the end.
	if base <= MIN {
		return 0, false
	}
	uMin := EncodeIntToCmpUint(MIN)
	uBase := EncodeIntToCmpUint(base)
	uOffset := EncodeIntToCmpUint(offset)
	uIncrement := uint64(-increment)
	if uBase-uMin < uIncrement {
		// Enum the possible first value.
		for i := uBase - 1; i >= uMin; i-- {
			if (uOffset-i)%uIncrement == 0 {
				return DecodeCmpUintToInt(i), true
			}
		}
		return 0, false
	}
	nr := (uOffset - uBase + uIncrement) / uIncrement
	nr = uOffset - nr*uIncrement
	first := DecodeCmpUintToInt(nr)
	return first, true
}

func (alloc *allocator) seekToFirstAutoID(base, increment, offset int64) (firstID int64, isOverflow bool) {
	if alloc.isUnsigned {
		if increment == 1 && offset == 1 {
			firstID := uint64(base) + 1
			return int64(firstID), uint64(base) >= firstID
		}
		uBase, uInc, uOff := uint64(base), uint64(increment), uint64(offset)
		nr := (uBase + uInc - uOff) / uInc
		nr = nr*uInc + uOff
		return int64(nr), uBase >= nr
	}
	if increment == 1 && offset == 1 {
		firstID := base + 1
		return firstID, base >= firstID
	}
	nr := (base + increment - offset) / increment
	nr = nr*increment + offset
	return nr, base >= nr
}

// alloc4Sequence is used to alloc value for sequence, there are several aspects different from autoid logic.
// 1: sequence allocation don't need check rebase.
// 2: sequence allocation don't need auto step.
// 3: sequence allocation may have negative growth.
// 4: sequence allocation batch length can be dissatisfied.
// 5: sequence batch allocation will be consumed immediately.
func (alloc *allocator) alloc4Sequence(tableID int64) (min int64, max int64, round int64, err error) {
	increment := alloc.sequence.Increment
	offset := alloc.sequence.Start
	minValue := alloc.sequence.MinValue
	maxValue := alloc.sequence.MaxValue
	cacheSize := alloc.sequence.CacheValue
	if !alloc.sequence.Cache {
		cacheSize = 1
	}

	var newBase, newEnd int64
	startTime := time.Now()
	err = kv.RunInNewTxn(alloc.store, true, func(txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		var (
			err1    error
			seqStep int64
		)
		// Get the real offset if the sequence is in cycle.
		// round is used to count cycle times in sequence with cycle option.
		if alloc.sequence.Cycle {
			// GetSequenceCycle is used to get the flag `round`, which indicates whether the sequence is already in cycle.
			round, err1 = m.GetSequenceCycle(alloc.dbID, tableID)
			if err1 != nil {
				return err1
			}
			if round > 0 {
				if increment > 0 {
					offset = alloc.sequence.MinValue
				} else {
					offset = alloc.sequence.MaxValue
				}
			}
		}

		// Get the global new base.
		newBase, err1 = getAutoIDByAllocType(m, alloc.dbID, tableID, alloc.allocType)
		if err1 != nil {
			return err1
		}

		// CalcNeededBatchSize calculates the total batch size needed.
		seqStep, err1 = CalcSequenceBatchSize(newBase, cacheSize, increment, offset, minValue, maxValue)

		if err1 != nil && err1 == ErrAutoincReadFailed {
			if !alloc.sequence.Cycle {
				return err1
			}
			// Reset the sequence base and offset.
			if alloc.sequence.Increment > 0 {
				newBase = alloc.sequence.MinValue - 1
				offset = alloc.sequence.MinValue
			} else {
				newBase = alloc.sequence.MaxValue + 1
				offset = alloc.sequence.MaxValue
			}
			err1 = m.SetSequenceValue(alloc.dbID, tableID, newBase)
			if err1 != nil {
				return err1
			}

			// Reset sequence round state value.
			round++
			// SetSequenceCycle is used to store the flag `round` which indicates whether the sequence is already in cycle.
			// round > 0 means the sequence is already in cycle, so the offset should be minvalue / maxvalue rather than sequence.start.
			// TiDB is a stateless node, it should know whether the sequence is already in cycle when restart.
			err1 = m.SetSequenceCycle(alloc.dbID, tableID, round)
			if err1 != nil {
				return err1
			}

			// Recompute the sequence next batch size.
			seqStep, err1 = CalcSequenceBatchSize(newBase, cacheSize, increment, offset, minValue, maxValue)
			if err1 != nil {
				return err1
			}
		}
		var delta int64
		if alloc.sequence.Increment > 0 {
			delta = seqStep
		} else {
			delta = -seqStep
		}
		newEnd, err1 = generateAutoIDByAllocType(m, alloc.dbID, tableID, delta, alloc.allocType)
		return err1
	})

	// TODO: sequence metrics
	metrics.AutoIDHistogram.WithLabelValues(metrics.TableAutoIDAlloc, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	if err != nil {
		return 0, 0, 0, err
	}
	logutil.Logger(context.TODO()).Debug("alloc sequence value",
		zap.Uint64(" from value", uint64(newBase)),
		zap.Uint64("to value", uint64(newEnd)),
		zap.Int64("table ID", tableID),
		zap.Int64("database ID", alloc.dbID))
	return newBase, newEnd, round, nil
}

func getAutoIDByAllocType(m *meta.Meta, dbID, tableID int64, allocType AllocatorType) (int64, error) {
	switch allocType {
	// Currently, row id allocator and auto-increment value allocator shares the same key-value pair.
	case RowIDAllocType, AutoIncrementType:
		return m.GetAutoTableID(dbID, tableID)
	case AutoRandomType:
		return m.GetAutoRandomID(dbID, tableID)
	case SequenceType:
		return m.GetSequenceValue(dbID, tableID)
	default:
		return 0, ErrInvalidAllocatorType.GenWithStackByArgs()
	}
}

func generateAutoIDByAllocType(m *meta.Meta, dbID, tableID, step int64, allocType AllocatorType) (int64, error) {
	switch allocType {
	case RowIDAllocType, AutoIncrementType:
		return m.GenAutoTableID(dbID, tableID, step)
	case AutoRandomType:
		return m.GenAutoRandomID(dbID, tableID, step)
	case SequenceType:
		return m.GenSequenceValue(dbID, tableID, step)
	default:
		return 0, ErrInvalidAllocatorType.GenWithStackByArgs()
	}
}

const signMask uint64 = 0x8000000000000000

// EncodeIntToCmpUint make int v to comparable uint type
func EncodeIntToCmpUint(v int64) uint64 {
	return uint64(v) ^ signMask
}

// DecodeCmpUintToInt decodes the u that encoded by EncodeIntToCmpUint
func DecodeCmpUintToInt(u uint64) int64 {
	return int64(u ^ signMask)
}

// TestModifyBaseAndEndInjection exported for testing modifying the base and end.
func TestModifyBaseAndEndInjection(alloc Allocator, base, end int64) {
	alloc.(*allocator).mu.Lock()
	alloc.(*allocator).base = base
	alloc.(*allocator).end = end
	alloc.(*allocator).mu.Unlock()
}

// AutoRandomIDLayout is used to calculate the bits length of different section in auto_random id.
// The primary key with auto_random can only be `bigint` column, the total layout length of auto random is 64 bits.
// These are two type of layout:
// 1. Signed bigint:
//   | [sign_bit] | [shard_bits] | [incremental_bits] |
//   sign_bit(1 fixed) + shard_bits(15 max) + incremental_bits(the rest) = total_layout_bits(64 fixed)
// 2. Unsigned bigint:
//   | [shard_bits] | [incremental_bits] |
//   shard_bits(15 max) + incremental_bits(the rest) = total_layout_bits(64 fixed)
// Please always use NewAutoRandomIDLayout() to instantiate.
type AutoRandomIDLayout struct {
	FieldType *types.FieldType
	ShardBits uint64
	// Derived fields.
	TypeBitsLength  uint64
	IncrementalBits uint64
	HasSignBit      bool
}

// NewAutoRandomIDLayout create an instance of AutoRandomIDLayout.
func NewAutoRandomIDLayout(fieldType *types.FieldType, shardBits uint64) *AutoRandomIDLayout {
	typeBitsLength := uint64(mysql.DefaultLengthOfMysqlTypes[mysql.TypeLonglong] * 8)
	incrementalBits := typeBitsLength - shardBits
	hasSignBit := !mysql.HasUnsignedFlag(fieldType.Flag)
	if hasSignBit {
		incrementalBits -= 1
	}
	return &AutoRandomIDLayout{
		FieldType:       fieldType,
		ShardBits:       shardBits,
		TypeBitsLength:  typeBitsLength,
		IncrementalBits: incrementalBits,
		HasSignBit:      hasSignBit,
	}
}

// IncrementalBitsCapacity returns the max capacity of incremental section of the current layout.
func (l *AutoRandomIDLayout) IncrementalBitsCapacity() uint64 {
	return uint64(math.Pow(2, float64(l.IncrementalBits))) - 1
}

// IncrementalMask returns 00..0[11..1], where [xxx] is the incremental section of the current layout.
func (l *AutoRandomIDLayout) IncrementalMask() int64 {
	return (1 << l.IncrementalBits) - 1
}
