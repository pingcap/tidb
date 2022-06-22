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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package autoid

import (
	"bytes"
	"context"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
	tikvutil "github.com/tikv/client-go/v2/util"
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
type AllocatorType uint8

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

func (a AllocatorType) String() string {
	switch a {
	case RowIDAllocType:
		return "_tidb_rowid"
	case AutoIncrementType:
		return "auto_increment"
	case AutoRandomType:
		return "auto_random"
	case SequenceType:
		return "sequence"
	}
	return "unknown"
}

// CustomAutoIncCacheOption is one kind of AllocOption to customize the allocator step length.
type CustomAutoIncCacheOption int64

// ApplyOn implements the AllocOption interface.
func (step CustomAutoIncCacheOption) ApplyOn(alloc *allocator) {
	if step == 0 {
		return
	}
	alloc.step = int64(step)
	alloc.customStep = true
}

// AllocOptionTableInfoVersion is used to pass the TableInfo.Version to the allocator.
type AllocOptionTableInfoVersion uint16

// ApplyOn implements the AllocOption interface.
func (v AllocOptionTableInfoVersion) ApplyOn(alloc *allocator) {
	alloc.tbVersion = uint16(v)
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
	Alloc(ctx context.Context, n uint64, increment, offset int64) (int64, int64, error)

	// AllocSeqCache allocs sequence batch value cached in table level（rather than in alloc), the returned range covering
	// the size of sequence cache with it's increment. The returned round indicates the sequence cycle times if it is with
	// cycle option.
	AllocSeqCache() (min int64, max int64, round int64, err error)

	// Rebase rebases the autoID base for table with tableID and the new base value.
	// If allocIDs is true, it will allocate some IDs and save to the cache.
	// If allocIDs is false, it will not allocate IDs.
	Rebase(ctx context.Context, newBase int64, allocIDs bool) error

	// ForceRebase set the next global auto ID to newBase.
	ForceRebase(newBase int64) error

	// RebaseSeq rebases the sequence value in number axis with tableID and the new base value.
	RebaseSeq(newBase int64) (int64, bool, error)

	// Base return the current base of Allocator.
	Base() int64
	// End is only used for test.
	End() int64
	// NextGlobalAutoID returns the next global autoID.
	NextGlobalAutoID() (int64, error)
	GetType() AllocatorType
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

// Filter filters all the allocators that match pred.
func (all Allocators) Filter(pred func(Allocator) bool) Allocators {
	var ret Allocators
	for _, a := range all {
		if pred(a) {
			ret = append(ret, a)
		}
	}
	return ret
}

type allocator struct {
	mu    sync.Mutex
	base  int64
	end   int64
	store kv.Storage
	// dbID is current database's ID.
	dbID          int64
	tbID          int64
	tbVersion     uint16
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
func (alloc *allocator) NextGlobalAutoID() (int64, error) {
	var autoID int64
	startTime := time.Now()
	err := kv.RunInNewTxn(context.Background(), alloc.store, true, func(ctx context.Context, txn kv.Transaction) error {
		var err1 error
		autoID, err1 = alloc.getIDAccessor(txn).Get()
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

func (alloc *allocator) rebase4Unsigned(ctx context.Context, requiredBase uint64, allocIDs bool) error {
	// Satisfied by alloc.base, nothing to do.
	if requiredBase <= uint64(alloc.base) {
		return nil
	}
	// Satisfied by alloc.end, need to update alloc.base.
	if requiredBase <= uint64(alloc.end) {
		alloc.base = int64(requiredBase)
		return nil
	}

	ctx, allocatorStats, commitDetail := getAllocatorStatsFromCtx(ctx)
	if allocatorStats != nil {
		allocatorStats.rebaseCount++
		defer func() {
			if commitDetail != nil {
				allocatorStats.mergeCommitDetail(*commitDetail)
			}
		}()
	}
	var newBase, newEnd uint64
	startTime := time.Now()
	err := kv.RunInNewTxn(ctx, alloc.store, true, func(ctx context.Context, txn kv.Transaction) error {
		if allocatorStats != nil {
			txn.SetOption(kv.CollectRuntimeStats, allocatorStats.SnapshotRuntimeStats)
		}
		idAcc := alloc.getIDAccessor(txn)
		currentEnd, err1 := idAcc.Get()
		if err1 != nil {
			return err1
		}
		uCurrentEnd := uint64(currentEnd)
		if allocIDs {
			newBase = mathutil.Max(uCurrentEnd, requiredBase)
			newEnd = mathutil.Min(math.MaxUint64-uint64(alloc.step), newBase) + uint64(alloc.step)
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
		_, err1 = idAcc.Inc(int64(newEnd - uCurrentEnd))
		return err1
	})
	metrics.AutoIDHistogram.WithLabelValues(metrics.TableAutoIDRebase, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	if err != nil {
		return err
	}
	alloc.base, alloc.end = int64(newBase), int64(newEnd)
	return nil
}

func (alloc *allocator) rebase4Signed(ctx context.Context, requiredBase int64, allocIDs bool) error {
	// Satisfied by alloc.base, nothing to do.
	if requiredBase <= alloc.base {
		return nil
	}
	// Satisfied by alloc.end, need to update alloc.base.
	if requiredBase <= alloc.end {
		alloc.base = requiredBase
		return nil
	}

	ctx, allocatorStats, commitDetail := getAllocatorStatsFromCtx(ctx)
	if allocatorStats != nil {
		allocatorStats.rebaseCount++
		defer func() {
			if commitDetail != nil {
				allocatorStats.mergeCommitDetail(*commitDetail)
			}
		}()
	}
	var newBase, newEnd int64
	startTime := time.Now()
	err := kv.RunInNewTxn(ctx, alloc.store, true, func(ctx context.Context, txn kv.Transaction) error {
		if allocatorStats != nil {
			txn.SetOption(kv.CollectRuntimeStats, allocatorStats.SnapshotRuntimeStats)
		}
		idAcc := alloc.getIDAccessor(txn)
		currentEnd, err1 := idAcc.Get()
		if err1 != nil {
			return err1
		}
		if allocIDs {
			newBase = mathutil.Max(currentEnd, requiredBase)
			newEnd = mathutil.Min(math.MaxInt64-alloc.step, newBase) + alloc.step
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
		_, err1 = idAcc.Inc(newEnd - currentEnd)
		return err1
	})
	metrics.AutoIDHistogram.WithLabelValues(metrics.TableAutoIDRebase, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	if err != nil {
		return err
	}
	alloc.base, alloc.end = newBase, newEnd
	return nil
}

// rebase4Sequence won't alloc batch immediately, cause it won't cache value in allocator.
func (alloc *allocator) rebase4Sequence(requiredBase int64) (int64, bool, error) {
	startTime := time.Now()
	alreadySatisfied := false
	err := kv.RunInNewTxn(context.Background(), alloc.store, true, func(ctx context.Context, txn kv.Transaction) error {
		acc := meta.NewMeta(txn).GetAutoIDAccessors(alloc.dbID, alloc.tbID)
		currentEnd, err := acc.SequenceValue().Get()
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
		_, err = acc.SequenceValue().Inc(requiredBase - currentEnd)
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
func (alloc *allocator) Rebase(ctx context.Context, requiredBase int64, allocIDs bool) error {
	alloc.mu.Lock()
	defer alloc.mu.Unlock()
	if alloc.isUnsigned {
		return alloc.rebase4Unsigned(ctx, uint64(requiredBase), allocIDs)
	}
	return alloc.rebase4Signed(ctx, requiredBase, allocIDs)
}

// ForceRebase implements autoid.Allocator ForceRebase interface.
func (alloc *allocator) ForceRebase(requiredBase int64) error {
	if requiredBase == -1 {
		return ErrAutoincReadFailed.GenWithStack("Cannot force rebase the next global ID to '0'")
	}
	alloc.mu.Lock()
	defer alloc.mu.Unlock()
	startTime := time.Now()
	err := kv.RunInNewTxn(context.Background(), alloc.store, true, func(ctx context.Context, txn kv.Transaction) error {
		idAcc := alloc.getIDAccessor(txn)
		currentEnd, err1 := idAcc.Get()
		if err1 != nil {
			return err1
		}
		var step int64
		if !alloc.isUnsigned {
			step = requiredBase - currentEnd
		} else {
			uRequiredBase, uCurrentEnd := uint64(requiredBase), uint64(currentEnd)
			step = int64(uRequiredBase - uCurrentEnd)
		}
		_, err1 = idAcc.Inc(step)
		return err1
	})
	metrics.AutoIDHistogram.WithLabelValues(metrics.TableAutoIDRebase, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	if err != nil {
		return err
	}
	alloc.base, alloc.end = requiredBase, requiredBase
	return nil
}

// Rebase implements autoid.Allocator RebaseSeq interface.
// The return value is quite same as expression function, bool means whether it should be NULL,
// here it will be used in setval expression function (true meaning the set value has been satisfied, return NULL).
// case1:When requiredBase is satisfied with current value, it will return (0, true, nil),
// case2:When requiredBase is successfully set in, it will return (requiredBase, false, nil).
// If some error occurs in the process, return it immediately.
func (alloc *allocator) RebaseSeq(requiredBase int64) (int64, bool, error) {
	alloc.mu.Lock()
	defer alloc.mu.Unlock()
	return alloc.rebase4Sequence(requiredBase)
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
func NewAllocator(store kv.Storage, dbID, tbID int64, isUnsigned bool,
	allocType AllocatorType, opts ...AllocOption) Allocator {
	alloc := &allocator{
		store:         store,
		dbID:          dbID,
		tbID:          tbID,
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
func NewSequenceAllocator(store kv.Storage, dbID, tbID int64, info *model.SequenceInfo) Allocator {
	return &allocator{
		store: store,
		dbID:  dbID,
		tbID:  tbID,
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
	idCacheOpt := CustomAutoIncCacheOption(tblInfo.AutoIdCache)
	tblVer := AllocOptionTableInfoVersion(tblInfo.Version)

	hasRowID := !tblInfo.PKIsHandle && !tblInfo.IsCommonHandle
	hasAutoIncID := tblInfo.GetAutoIncrementColInfo() != nil
	if hasRowID || hasAutoIncID {
		alloc := NewAllocator(store, dbID, tblInfo.ID, tblInfo.IsAutoIncColUnsigned(), RowIDAllocType, idCacheOpt, tblVer)
		allocs = append(allocs, alloc)
	}
	hasAutoRandID := tblInfo.ContainsAutoRandomBits()
	if hasAutoRandID {
		alloc := NewAllocator(store, dbID, tblInfo.ID, tblInfo.IsAutoRandomBitColUnsigned(), AutoRandomType, idCacheOpt, tblVer)
		allocs = append(allocs, alloc)
	}
	if tblInfo.IsSequence() {
		allocs = append(allocs, NewSequenceAllocator(store, dbID, tblInfo.ID, tblInfo.Sequence))
	}
	return NewAllocators(allocs...)
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
func (alloc *allocator) Alloc(ctx context.Context, n uint64, increment, offset int64) (int64, int64, error) {
	if alloc.tbID == 0 {
		return 0, 0, errInvalidTableID.GenWithStackByArgs("Invalid tableID")
	}
	if n == 0 {
		return 0, 0, nil
	}
	if alloc.allocType == AutoIncrementType || alloc.allocType == RowIDAllocType {
		if !validIncrementAndOffset(increment, offset) {
			return 0, 0, errInvalidIncrementAndOffset.GenWithStackByArgs(increment, offset)
		}
	}
	alloc.mu.Lock()
	defer alloc.mu.Unlock()
	if alloc.isUnsigned {
		return alloc.alloc4Unsigned(ctx, n, increment, offset)
	}
	return alloc.alloc4Signed(ctx, n, increment, offset)
}

func (alloc *allocator) AllocSeqCache() (int64, int64, int64, error) {
	alloc.mu.Lock()
	defer alloc.mu.Unlock()
	return alloc.alloc4Sequence()
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

func (alloc *allocator) alloc4Signed(ctx context.Context, n uint64, increment, offset int64) (int64, int64, error) {
	// Check offset rebase if necessary.
	if offset-1 > alloc.base {
		if err := alloc.rebase4Signed(ctx, offset-1, true); err != nil {
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

		ctx, allocatorStats, commitDetail := getAllocatorStatsFromCtx(ctx)
		if allocatorStats != nil {
			allocatorStats.allocCount++
			defer func() {
				if commitDetail != nil {
					allocatorStats.mergeCommitDetail(*commitDetail)
				}
			}()
		}

		err := kv.RunInNewTxn(ctx, alloc.store, true, func(ctx context.Context, txn kv.Transaction) error {
			if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
				span1 := span.Tracer().StartSpan("alloc.alloc4Signed", opentracing.ChildOf(span.Context()))
				defer span1.Finish()
				opentracing.ContextWithSpan(ctx, span1)
			}
			if allocatorStats != nil {
				txn.SetOption(kv.CollectRuntimeStats, allocatorStats.SnapshotRuntimeStats)
			}

			idAcc := alloc.getIDAccessor(txn)
			var err1 error
			newBase, err1 = idAcc.Get()
			if err1 != nil {
				return err1
			}
			// CalcNeededBatchSize calculates the total batch size needed on global base.
			n1 = CalcNeededBatchSize(newBase, int64(n), increment, offset, alloc.isUnsigned)
			// Although the step is customized by user, we still need to make sure nextStep is big enough for insert batch.
			if nextStep < n1 {
				nextStep = n1
			}
			tmpStep := mathutil.Min(math.MaxInt64-newBase, nextStep)
			// The global rest is not enough for alloc.
			if tmpStep < n1 {
				return ErrAutoincReadFailed
			}
			newEnd, err1 = idAcc.Inc(tmpStep)
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
		zap.Int64("table ID", alloc.tbID),
		zap.Int64("database ID", alloc.dbID))
	min := alloc.base
	alloc.base += n1
	return min, alloc.base, nil
}

func (alloc *allocator) alloc4Unsigned(ctx context.Context, n uint64, increment, offset int64) (int64, int64, error) {
	// Check offset rebase if necessary.
	if uint64(offset-1) > uint64(alloc.base) {
		if err := alloc.rebase4Unsigned(ctx, uint64(offset-1), true); err != nil {
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

		ctx, allocatorStats, commitDetail := getAllocatorStatsFromCtx(ctx)
		if allocatorStats != nil {
			allocatorStats.allocCount++
			defer func() {
				if commitDetail != nil {
					allocatorStats.mergeCommitDetail(*commitDetail)
				}
			}()
		}

		err := kv.RunInNewTxn(ctx, alloc.store, true, func(ctx context.Context, txn kv.Transaction) error {
			if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
				span1 := span.Tracer().StartSpan("alloc.alloc4Unsigned", opentracing.ChildOf(span.Context()))
				defer span1.Finish()
				opentracing.ContextWithSpan(ctx, span1)
			}
			if allocatorStats != nil {
				txn.SetOption(kv.CollectRuntimeStats, allocatorStats.SnapshotRuntimeStats)
			}

			idAcc := alloc.getIDAccessor(txn)
			var err1 error
			newBase, err1 = idAcc.Get()
			if err1 != nil {
				return err1
			}
			// CalcNeededBatchSize calculates the total batch size needed on new base.
			n1 = CalcNeededBatchSize(newBase, int64(n), increment, offset, alloc.isUnsigned)
			// Although the step is customized by user, we still need to make sure nextStep is big enough for insert batch.
			if nextStep < n1 {
				nextStep = n1
			}
			tmpStep := int64(mathutil.Min(math.MaxUint64-uint64(newBase), uint64(nextStep)))
			// The global rest is not enough for alloc.
			if tmpStep < n1 {
				return ErrAutoincReadFailed
			}
			newEnd, err1 = idAcc.Inc(tmpStep)
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
		zap.Int64("table ID", alloc.tbID),
		zap.Int64("database ID", alloc.dbID))
	min := alloc.base
	// Use uint64 n directly.
	alloc.base = int64(uint64(alloc.base) + uint64(n1))
	return min, alloc.base, nil
}

func getAllocatorStatsFromCtx(ctx context.Context) (context.Context, *AllocatorRuntimeStats, **tikvutil.CommitDetails) {
	var allocatorStats *AllocatorRuntimeStats
	var commitDetail *tikvutil.CommitDetails
	ctxValue := ctx.Value(AllocatorRuntimeStatsCtxKey)
	if ctxValue != nil {
		allocatorStats = ctxValue.(*AllocatorRuntimeStats)
		ctx = context.WithValue(ctx, tikvutil.CommitDetailCtxKey, &commitDetail)
	}
	return ctx, allocatorStats, &commitDetail
}

// alloc4Sequence is used to alloc value for sequence, there are several aspects different from autoid logic.
// 1: sequence allocation don't need check rebase.
// 2: sequence allocation don't need auto step.
// 3: sequence allocation may have negative growth.
// 4: sequence allocation batch length can be dissatisfied.
// 5: sequence batch allocation will be consumed immediately.
func (alloc *allocator) alloc4Sequence() (min int64, max int64, round int64, err error) {
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
	err = kv.RunInNewTxn(context.Background(), alloc.store, true, func(ctx context.Context, txn kv.Transaction) error {
		acc := meta.NewMeta(txn).GetAutoIDAccessors(alloc.dbID, alloc.tbID)
		var (
			err1    error
			seqStep int64
		)
		// Get the real offset if the sequence is in cycle.
		// round is used to count cycle times in sequence with cycle option.
		if alloc.sequence.Cycle {
			// GetSequenceCycle is used to get the flag `round`, which indicates whether the sequence is already in cycle.
			round, err1 = acc.SequenceCycle().Get()
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
		newBase, err1 = acc.SequenceValue().Get()
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
			err1 = acc.SequenceValue().Put(newBase)
			if err1 != nil {
				return err1
			}

			// Reset sequence round state value.
			round++
			// SetSequenceCycle is used to store the flag `round` which indicates whether the sequence is already in cycle.
			// round > 0 means the sequence is already in cycle, so the offset should be minvalue / maxvalue rather than sequence.start.
			// TiDB is a stateless node, it should know whether the sequence is already in cycle when restart.
			err1 = acc.SequenceCycle().Put(round)
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
		newEnd, err1 = acc.SequenceValue().Inc(delta)
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
		zap.Int64("table ID", alloc.tbID),
		zap.Int64("database ID", alloc.dbID))
	return newBase, newEnd, round, nil
}

func (alloc *allocator) getIDAccessor(txn kv.Transaction) meta.AutoIDAccessor {
	acc := meta.NewMeta(txn).GetAutoIDAccessors(alloc.dbID, alloc.tbID)
	switch alloc.allocType {
	case RowIDAllocType:
		return acc.RowID()
	case AutoIncrementType:
		return acc.IncrementID(alloc.tbVersion)
	case AutoRandomType:
		return acc.RandomID()
	case SequenceType:
		return acc.SequenceValue()
	}
	return nil
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

// ShardIDLayout is used to calculate the bits length of different segments in auto id.
// Generally, an auto id is consist of 3 segments: sign bit, shard bits and incremental bits.
// Take ``a BIGINT AUTO_INCREMENT PRIMARY KEY`` as an example, assume that the `shard_row_id_bits` = 5,
// the layout is like
//  | [sign_bit] (1 bit) | [shard_bits] (5 bits) | [incremental_bits] (64-1-5=58 bits) |
// Please always use NewShardIDLayout() to instantiate.
type ShardIDLayout struct {
	FieldType *types.FieldType
	ShardBits uint64
	// Derived fields.
	TypeBitsLength  uint64
	IncrementalBits uint64
	HasSignBit      bool
}

// NewShardIDLayout create an instance of ShardIDLayout.
func NewShardIDLayout(fieldType *types.FieldType, shardBits uint64) *ShardIDLayout {
	typeBitsLength := uint64(mysql.DefaultLengthOfMysqlTypes[mysql.TypeLonglong] * 8)
	incrementalBits := typeBitsLength - shardBits
	hasSignBit := !mysql.HasUnsignedFlag(fieldType.GetFlag())
	if hasSignBit {
		incrementalBits -= 1
	}
	return &ShardIDLayout{
		FieldType:       fieldType,
		ShardBits:       shardBits,
		TypeBitsLength:  typeBitsLength,
		IncrementalBits: incrementalBits,
		HasSignBit:      hasSignBit,
	}
}

// IncrementalBitsCapacity returns the max capacity of incremental section of the current layout.
func (l *ShardIDLayout) IncrementalBitsCapacity() uint64 {
	return uint64(math.Pow(2, float64(l.IncrementalBits))) - 1
}

// IncrementalMask returns 00..0[11..1], where [xxx] is the incremental section of the current layout.
func (l *ShardIDLayout) IncrementalMask() int64 {
	return (1 << l.IncrementalBits) - 1
}

type allocatorRuntimeStatsCtxKeyType struct{}

// AllocatorRuntimeStatsCtxKey is the context key of allocator runtime stats.
var AllocatorRuntimeStatsCtxKey = allocatorRuntimeStatsCtxKeyType{}

// AllocatorRuntimeStats is the execution stats of auto id allocator.
type AllocatorRuntimeStats struct {
	*txnsnapshot.SnapshotRuntimeStats
	*execdetails.RuntimeStatsWithCommit
	allocCount  int
	rebaseCount int
}

// NewAllocatorRuntimeStats return a new AllocatorRuntimeStats.
func NewAllocatorRuntimeStats() *AllocatorRuntimeStats {
	return &AllocatorRuntimeStats{
		SnapshotRuntimeStats: &txnsnapshot.SnapshotRuntimeStats{},
	}
}

func (e *AllocatorRuntimeStats) mergeCommitDetail(detail *tikvutil.CommitDetails) {
	if detail == nil {
		return
	}
	if e.RuntimeStatsWithCommit == nil {
		e.RuntimeStatsWithCommit = &execdetails.RuntimeStatsWithCommit{}
	}
	e.RuntimeStatsWithCommit.MergeCommitDetails(detail)
}

// String implements the RuntimeStats interface.
func (e *AllocatorRuntimeStats) String() string {
	if e.allocCount == 0 && e.rebaseCount == 0 {
		return ""
	}
	var buf bytes.Buffer
	buf.WriteString("auto_id_allocator: {")
	initialSize := buf.Len()
	if e.allocCount > 0 {
		buf.WriteString("alloc_cnt: ")
		buf.WriteString(strconv.FormatInt(int64(e.allocCount), 10))
	}
	if e.rebaseCount > 0 {
		if buf.Len() > initialSize {
			buf.WriteString(", ")
		}
		buf.WriteString("rebase_cnt: ")
		buf.WriteString(strconv.FormatInt(int64(e.rebaseCount), 10))
	}
	if e.SnapshotRuntimeStats != nil {
		stats := e.SnapshotRuntimeStats.String()
		if stats != "" {
			if buf.Len() > initialSize {
				buf.WriteString(", ")
			}
			buf.WriteString(e.SnapshotRuntimeStats.String())
		}
	}
	if e.RuntimeStatsWithCommit != nil {
		stats := e.RuntimeStatsWithCommit.String()
		if stats != "" {
			if buf.Len() > initialSize {
				buf.WriteString(", ")
			}
			buf.WriteString(stats)
		}
	}
	buf.WriteString("}")
	return buf.String()
}

// Clone implements the RuntimeStats interface.
func (e *AllocatorRuntimeStats) Clone() *AllocatorRuntimeStats {
	newRs := &AllocatorRuntimeStats{
		allocCount:  e.allocCount,
		rebaseCount: e.rebaseCount,
	}
	if e.SnapshotRuntimeStats != nil {
		snapshotStats := e.SnapshotRuntimeStats.Clone()
		newRs.SnapshotRuntimeStats = snapshotStats
	}
	if e.RuntimeStatsWithCommit != nil {
		newRs.RuntimeStatsWithCommit = e.RuntimeStatsWithCommit.Clone().(*execdetails.RuntimeStatsWithCommit)
	}
	return newRs
}

// Merge implements the RuntimeStats interface.
func (e *AllocatorRuntimeStats) Merge(other *AllocatorRuntimeStats) {
	if other == nil {
		return
	}
	if other.SnapshotRuntimeStats != nil {
		if e.SnapshotRuntimeStats == nil {
			e.SnapshotRuntimeStats = other.SnapshotRuntimeStats.Clone()
		} else {
			e.SnapshotRuntimeStats.Merge(other.SnapshotRuntimeStats)
		}
	}
	if other.RuntimeStatsWithCommit != nil {
		if e.RuntimeStatsWithCommit == nil {
			e.RuntimeStatsWithCommit = other.RuntimeStatsWithCommit.Clone().(*execdetails.RuntimeStatsWithCommit)
		} else {
			e.RuntimeStatsWithCommit.Merge(other.RuntimeStatsWithCommit)
		}
	}
}
