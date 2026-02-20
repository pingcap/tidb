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

package executor

import (
	"context"
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

func setDatumAutoIDAndCast(ctx sessionctx.Context, d *types.Datum, id int64, col *table.Column) error {
	d.SetAutoID(id, col.GetFlag())
	var err error
	*d, err = table.CastValue(ctx, *d, col.ToInfo(), false, false)
	if err == nil && d.GetInt64() < id {
		// Auto ID is out of range.
		sc := ctx.GetSessionVars().StmtCtx
		insertPlan, ok := sc.GetPlan().(*physicalop.Insert)
		if ok && sc.TypeFlags().TruncateAsWarning() && len(insertPlan.OnDuplicate) > 0 {
			// Fix issue #38950: AUTO_INCREMENT is incompatible with mysql
			// An auto id out of range error occurs in `insert ignore into ... on duplicate ...`.
			// We should allow the SQL to be executed successfully.
			return nil
		}
		// The truncated ID is possible to duplicate with an existing ID.
		// To prevent updating unrelated rows in the REPLACE statement, it is better to throw an error.
		return autoid.ErrAutoincReadFailed
	}
	return err
}

// lazyAdjustAutoIncrementDatum is quite similar to adjustAutoIncrementDatum
// except it will cache auto increment datum previously for lazy batch allocation of autoID.
func (e *InsertValues) lazyAdjustAutoIncrementDatum(ctx context.Context, rows [][]types.Datum) (
	[][]types.Datum, error,
) {
	// Not in lazyFillAutoID mode means no need to fill.
	if !e.lazyFillAutoID {
		return rows, nil
	}
	col, idx, found := findAutoIncrementColumn(e.Table)
	if !found {
		return rows, nil
	}
	sessVars := e.Ctx().GetSessionVars()
	retryInfo := sessVars.RetryInfo
	rowCount := len(rows)
	for processedIdx := 0; processedIdx < rowCount; processedIdx++ {
		autoDatum := rows[processedIdx][idx]

		var err error
		var recordID int64
		if !autoDatum.IsNull() {
			recordID, err = getAutoRecordID(autoDatum, &col.FieldType, true)
			if err != nil {
				return nil, err
			}
		}
		// Use the value if it's not null and not 0.
		if recordID != 0 {
			alloc := e.Table.Allocators(e.Ctx().GetTableCtx()).Get(autoid.AutoIncrementType)
			err = alloc.Rebase(ctx, recordID, true)
			if err != nil {
				return nil, err
			}
			e.Ctx().GetSessionVars().StmtCtx.InsertID = uint64(recordID)
			retryInfo.AddAutoIncrementID(recordID)
			continue
		}

		// Change NULL to auto id.
		// Change value 0 to auto id, if NoAutoValueOnZero SQL mode is not set.
		if autoDatum.IsNull() || e.Ctx().GetSessionVars().SQLMode&mysql.ModeNoAutoValueOnZero == 0 {
			// Consume the auto IDs in RetryInfo first.
			for retryInfo.Retrying && processedIdx < rowCount {
				nextID, ok := retryInfo.GetCurrAutoIncrementID()
				if !ok {
					break
				}
				err = setDatumAutoIDAndCast(e.Ctx(), &rows[processedIdx][idx], nextID, col)
				if err != nil {
					return nil, err
				}
				processedIdx++
				if processedIdx == rowCount {
					return rows, nil
				}
			}
			// Find consecutive num.
			start := processedIdx
			cnt := 1
			for processedIdx+1 < rowCount && e.isAutoNull(ctx, rows[processedIdx+1][idx], col) {
				processedIdx++
				cnt++
			}
			// AllocBatchAutoIncrementValue allocates batch N consecutive autoIDs.
			// The max value can be derived from adding the increment value to min for cnt-1 times.
			minv, increment, err := table.AllocBatchAutoIncrementValue(ctx, e.Table, e.Ctx(), cnt)
			if e.handleErr(col, &autoDatum, cnt, err) != nil {
				return nil, err
			}
			// It's compatible with mysql setting the first allocated autoID to lastInsertID.
			// Cause autoID may be specified by user, judge only the first row is not suitable.
			if e.lastInsertID == 0 {
				e.lastInsertID = uint64(minv)
			}
			// Assign autoIDs to rows.
			for j := range cnt {
				offset := j + start
				id := int64(uint64(minv) + uint64(j)*uint64(increment))
				err = setDatumAutoIDAndCast(e.Ctx(), &rows[offset][idx], id, col)
				if err != nil {
					return nil, err
				}
				retryInfo.AddAutoIncrementID(id)
			}
			continue
		}

		err = setDatumAutoIDAndCast(e.Ctx(), &rows[processedIdx][idx], recordID, col)
		if err != nil {
			return nil, err
		}
		retryInfo.AddAutoIncrementID(recordID)
	}
	return rows, nil
}

func (e *InsertValues) adjustAutoIncrementDatum(
	ctx context.Context, d types.Datum, hasValue bool, c *table.Column,
) (types.Datum, error) {
	sessVars := e.Ctx().GetSessionVars()
	retryInfo := sessVars.RetryInfo
	if retryInfo.Retrying {
		id, ok := retryInfo.GetCurrAutoIncrementID()
		if ok {
			err := setDatumAutoIDAndCast(e.Ctx(), &d, id, c)
			if err != nil {
				return types.Datum{}, err
			}
			return d, nil
		}
	}

	var err error
	var recordID int64
	if !hasValue {
		d.SetNull()
	}
	if !d.IsNull() {
		recordID, err = getAutoRecordID(d, &c.FieldType, true)
		if err != nil {
			return types.Datum{}, err
		}
	}
	// Use the value if it's not null and not 0.
	if recordID != 0 {
		err = e.Table.Allocators(e.Ctx().GetTableCtx()).Get(autoid.AutoIncrementType).Rebase(ctx, recordID, true)
		if err != nil {
			return types.Datum{}, err
		}
		e.Ctx().GetSessionVars().StmtCtx.InsertID = uint64(recordID)
		retryInfo.AddAutoIncrementID(recordID)
		return d, nil
	}

	// Change NULL to auto id.
	// Change value 0 to auto id, if NoAutoValueOnZero SQL mode is not set.
	if d.IsNull() || e.Ctx().GetSessionVars().SQLMode&mysql.ModeNoAutoValueOnZero == 0 {
		recordID, err = table.AllocAutoIncrementValue(ctx, e.Table, e.Ctx())
		if e.handleErr(c, &d, 0, err) != nil {
			return types.Datum{}, err
		}
		// It's compatible with mysql setting the first allocated autoID to lastInsertID.
		// Cause autoID may be specified by user, judge only the first row is not suitable.
		if e.lastInsertID == 0 {
			e.lastInsertID = uint64(recordID)
		}
	}

	err = setDatumAutoIDAndCast(e.Ctx(), &d, recordID, c)
	if err != nil {
		return types.Datum{}, err
	}
	retryInfo.AddAutoIncrementID(recordID)
	return d, nil
}

func getAutoRecordID(d types.Datum, target *types.FieldType, isInsert bool) (int64, error) {
	var recordID int64
	switch target.GetType() {
	case mysql.TypeFloat, mysql.TypeDouble:
		f := d.GetFloat64()
		if isInsert {
			recordID = int64(math.Round(f))
		} else {
			recordID = int64(f)
		}
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		recordID = d.GetInt64()
	default:
		return 0, errors.Errorf("unexpected field type [%v]", target.GetType())
	}

	return recordID, nil
}

func (e *InsertValues) adjustAutoRandomDatum(
	ctx context.Context, d types.Datum, hasValue bool, c *table.Column,
) (types.Datum, error) {
	retryInfo := e.Ctx().GetSessionVars().RetryInfo
	if retryInfo.Retrying {
		autoRandomID, ok := retryInfo.GetCurrAutoRandomID()
		if ok {
			err := setDatumAutoIDAndCast(e.Ctx(), &d, autoRandomID, c)
			if err != nil {
				return types.Datum{}, err
			}
			return d, nil
		}
	}

	var err error
	var recordID int64
	if !hasValue {
		d.SetNull()
	}
	if !d.IsNull() {
		recordID, err = getAutoRecordID(d, &c.FieldType, true)
		if err != nil {
			return types.Datum{}, err
		}
	}
	// Use the value if it's not null and not 0.
	if recordID != 0 {
		if !e.Ctx().GetSessionVars().AllowAutoRandExplicitInsert {
			return types.Datum{}, dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomExplicitInsertDisabledErrMsg)
		}
		err = e.rebaseAutoRandomID(ctx, recordID, &c.FieldType)
		if err != nil {
			return types.Datum{}, err
		}
		e.Ctx().GetSessionVars().StmtCtx.InsertID = uint64(recordID)
		err = setDatumAutoIDAndCast(e.Ctx(), &d, recordID, c)
		if err != nil {
			return types.Datum{}, err
		}
		retryInfo.AddAutoRandomID(recordID)
		return d, nil
	}

	// Change NULL to auto id.
	// Change value 0 to auto id, if NoAutoValueOnZero SQL mode is not set.
	if d.IsNull() || e.Ctx().GetSessionVars().SQLMode&mysql.ModeNoAutoValueOnZero == 0 {
		recordID, err = e.allocAutoRandomID(ctx, &c.FieldType)
		if err != nil {
			return types.Datum{}, err
		}
		// It's compatible with mysql setting the first allocated autoID to lastInsertID.
		// Cause autoID may be specified by user, judge only the first row is not suitable.
		if e.lastInsertID == 0 {
			e.lastInsertID = uint64(recordID)
		}
	}

	err = setDatumAutoIDAndCast(e.Ctx(), &d, recordID, c)
	if err != nil {
		return types.Datum{}, err
	}
	retryInfo.AddAutoRandomID(recordID)
	return d, nil
}

// allocAutoRandomID allocates a random id for primary key column. It assumes tableInfo.AutoRandomBits > 0.
func (e *InsertValues) allocAutoRandomID(ctx context.Context, fieldType *types.FieldType) (int64, error) {
	alloc := e.Table.Allocators(e.Ctx().GetTableCtx()).Get(autoid.AutoRandomType)
	tableInfo := e.Table.Meta()
	increment := e.Ctx().GetSessionVars().AutoIncrementIncrement
	offset := e.Ctx().GetSessionVars().AutoIncrementOffset
	_, autoRandomID, err := alloc.Alloc(ctx, 1, int64(increment), int64(offset))
	if err != nil {
		return 0, err
	}
	shardFmt := autoid.NewShardIDFormat(fieldType, tableInfo.AutoRandomBits, tableInfo.AutoRandomRangeBits)
	if shardFmt.IncrementalMask()&autoRandomID != autoRandomID {
		return 0, autoid.ErrAutoRandReadFailed
	}
	_, err = e.Ctx().Txn(true)
	if err != nil {
		return 0, err
	}
	currentShard := e.Ctx().GetSessionVars().GetRowIDShardGenerator().GetCurrentShard(1)
	return shardFmt.Compose(currentShard, autoRandomID), nil
}

func (e *InsertValues) rebaseAutoRandomID(ctx context.Context, recordID int64, fieldType *types.FieldType) error {
	if recordID < 0 {
		return nil
	}
	alloc := e.Table.Allocators(e.Ctx().GetTableCtx()).Get(autoid.AutoRandomType)
	tableInfo := e.Table.Meta()

	shardFmt := autoid.NewShardIDFormat(fieldType, tableInfo.AutoRandomBits, tableInfo.AutoRandomRangeBits)
	autoRandomID := shardFmt.IncrementalMask() & recordID

	return alloc.Rebase(ctx, autoRandomID, true)
}

func (e *InsertValues) adjustImplicitRowID(
	ctx context.Context, d types.Datum, hasValue bool, c *table.Column,
) (types.Datum, error) {
	var err error
	var recordID int64
	if !hasValue {
		d.SetNull()
	}
	if !d.IsNull() {
		recordID = d.GetInt64()
	}
	// Use the value if it's not null and not 0.
	if recordID != 0 {
		if !e.Ctx().GetSessionVars().AllowWriteRowID {
			return types.Datum{}, errors.Errorf("insert, update and replace statements for _tidb_rowid are not supported")
		}
		err = e.rebaseImplicitRowID(ctx, recordID)
		if err != nil {
			return types.Datum{}, err
		}
		d.SetInt64(recordID)
		return d, nil
	}
	// Change NULL to auto id.
	// Change value 0 to auto id, if NoAutoValueOnZero SQL mode is not set.
	if d.IsNull() || e.Ctx().GetSessionVars().SQLMode&mysql.ModeNoAutoValueOnZero == 0 {
		_, err := e.Ctx().Txn(true)
		if err != nil {
			return types.Datum{}, errors.Trace(err)
		}
		intHandle, err := tables.AllocHandle(ctx, e.Ctx().GetTableCtx(), e.Table)
		if err != nil {
			return types.Datum{}, err
		}
		recordID = intHandle.IntValue()
	}
	err = setDatumAutoIDAndCast(e.Ctx(), &d, recordID, c)
	if err != nil {
		return types.Datum{}, err
	}
	return d, nil
}

func (e *InsertValues) rebaseImplicitRowID(ctx context.Context, recordID int64) error {
	if recordID < 0 {
		return nil
	}
	alloc := e.Table.Allocators(e.Ctx().GetTableCtx()).Get(autoid.RowIDAllocType)
	tableInfo := e.Table.Meta()

	shardFmt := autoid.NewShardIDFormat(
		types.NewFieldType(mysql.TypeLonglong),
		tableInfo.ShardRowIDBits,
		autoid.RowIDBitLength,
	)
	newTiDBRowIDBase := shardFmt.IncrementalMask() & recordID

	return alloc.Rebase(ctx, newTiDBRowIDBase, true)
}
