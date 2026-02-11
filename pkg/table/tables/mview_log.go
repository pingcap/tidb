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

package tables

import (
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
)

// MLogDMLType indicates which SQL statement is mutating the base table, used to decide the
// `_MLOG$_DML_TYPE` value written into the materialized view log (MLog) table.
//
// Note: `INSERT ... ON DUPLICATE KEY UPDATE` is built as an Insert statement, but its internal
// update path calls `table.Table.UpdateRecord` (and/or `RemoveRecord` + `AddRecord` with
// `table.IsUpdate`). The wrapper handles it specially and emits `U` instead of `I`.
type MLogDMLType string

const (
	// MLogDMLTypeInsert writes DML type "I".
	MLogDMLTypeInsert MLogDMLType = "I"
	// MLogDMLTypeUpdate writes DML type "U".
	MLogDMLTypeUpdate MLogDMLType = "U"
	// MLogDMLTypeDelete writes DML type "D".
	MLogDMLTypeDelete MLogDMLType = "D"
	// MLogDMLTypeReplace writes DML type "R".
	MLogDMLTypeReplace MLogDMLType = "R"
	// MLogDMLTypeLoadData writes DML type "L".
	MLogDMLTypeLoadData MLogDMLType = "L"
)

const (
	mlogOldRowMarker int64 = -1
	mlogNewRowMarker int64 = 1
)

func validateMLogMetaColumn(mlogMeta *model.TableInfo) error {
	cols := mlogMeta.Columns
	expectedLen := len(mlogMeta.MaterializedViewLog.Columns) + 2
	if len(cols) != expectedLen {
		return errors.Errorf("invalid mlog meta columns, expect %d, got %d", expectedLen, len(cols))
	}

	if cols[len(cols)-2].Name.L != strings.ToLower(model.MaterializedViewLogDMLTypeColumnName) ||
		cols[len(cols)-1].Name.L != strings.ToLower(model.MaterializedViewLogOldNewColumnName) {
		return errors.Errorf(
			"invalid mlog meta columns, expect %s,%s, got %s,%s",
			model.MaterializedViewLogDMLTypeColumnName,
			model.MaterializedViewLogOldNewColumnName,
			cols[len(cols)-2].Name.O,
			cols[len(cols)-1].Name.O,
		)
	}
	return nil
}

// WrapTableWithMaterializedViewLog wraps a base table with its materialized view log (MLog) table.
//
// The returned object still implements `table.Table` and should be used in DML executors so that
// base table mutations will synchronously append corresponding rows into the MLog table.
func WrapTableWithMaterializedViewLog(
	base table.Table,
	mlog table.Table,
	tp MLogDMLType,
) (table.Table, error) {
	if base == nil || mlog == nil {
		return nil, errors.New("wrap table with mlog: base or mlog is nil")
	}
	baseMeta := base.Meta()
	mlogMeta := mlog.Meta()
	if baseMeta == nil || mlogMeta == nil {
		return nil, errors.New("wrap table with mlog: base or mlog meta is nil")
	}
	if baseMeta.MaterializedViewBase == nil || baseMeta.MaterializedViewBase.MLogID == 0 {
		return nil, errors.New("wrap table with mlog: base table has no mlog info")
	}
	if baseMeta.MaterializedViewBase.MLogID != mlogMeta.ID {
		return nil, errors.Errorf(
			"wrap table with mlog: mlog id mismatch, base mlog id=%d, mlog table id=%d",
			baseMeta.MaterializedViewBase.MLogID,
			mlogMeta.ID,
		)
	}
	if mlogMeta.MaterializedViewLog == nil {
		return nil, errors.New("wrap table with mlog: mlog table has no MaterializedViewLog info")
	}
	if mlogMeta.MaterializedViewLog.BaseTableID != baseMeta.ID {
		return nil, errors.Errorf(
			"wrap table with mlog: base table id mismatch, mlog base id=%d, base id=%d",
			mlogMeta.MaterializedViewLog.BaseTableID, baseMeta.ID,
		)
	}

	baseCols := baseMeta.Columns
	baseColOffsetByName := make(map[string]int, len(baseCols))
	for _, c := range baseCols {
		baseColOffsetByName[c.Name.L] = c.Offset
	}

	// Validate once at wrap-time to avoid silently writing wrong values if metadata is unexpected.
	if err := validateMLogMetaColumn(mlogMeta); err != nil {
		return nil, errors.Wrap(err, "wrap table with mlog")
	}

	trackedOffsets := make([]int, 0, len(mlogMeta.MaterializedViewLog.Columns))
	for _, c := range mlogMeta.MaterializedViewLog.Columns {
		offset, ok := baseColOffsetByName[c.L]
		if !ok {
			return nil, errors.Errorf(
				"wrap table with mlog: base column %s not found",
				c.O,
			)
		}
		trackedOffsets = append(trackedOffsets, offset)
	}

	return &mlogTable{
		Table:              base,
		mlog:               mlog,
		tp:                 tp,
		trackedBaseOffsets: trackedOffsets,
	}, nil
}

// mlogTable is a `table.Table` wrapper which writes corresponding rows into MLog table on DML.
//
// It intentionally keeps a minimal surface area: only DML methods are overridden; other methods
// are promoted from the embedded base `table.Table`.
type mlogTable struct {
	table.Table

	mlog table.Table
	tp   MLogDMLType

	// Base table column offsets recorded in mlog (user-specified columns).
	trackedBaseOffsets []int
}

// AddRecord implements table.Table.
//
// For handle-changed updates (e.g. UPDATE that modifies PK, or IODKU that changes PK), the
// executor calls RemoveRecord(old) + AddRecord(new, IsUpdate). In this path we intentionally
// do NOT check shouldLogUpdate — we always log — because we don't have `touched` information
// and it is safer to over-log than to miss a change (conservative strategy).
func (t *mlogTable) AddRecord(
	ctx table.MutateContext,
	txn kv.Transaction,
	r []types.Datum,
	opts ...table.AddRecordOption,
) (recordID kv.Handle, err error) {
	recordID, err = t.Table.AddRecord(ctx, txn, r, opts...)
	if err != nil {
		return nil, err
	}

	opt := table.NewAddRecordOpt(opts...)
	dmlType := t.addRecordDMLType(opt)
	mlogOpts := []table.AddRecordOption{
		table.DupKeyCheckSkip,
		opt.PessimisticLazyDupKeyCheck(),
	}
	if goCtx := opt.Ctx(); goCtx != nil {
		mlogOpts = append(mlogOpts, table.WithCtx(goCtx))
	}
	if err := t.writeMLogRow(ctx, txn, r, dmlType, mlogNewRowMarker, mlogOpts...); err != nil {
		return nil, err
	}
	return recordID, nil
}

// UpdateRecord implements table.Table.
func (t *mlogTable) UpdateRecord(
	ctx table.MutateContext,
	txn kv.Transaction,
	h kv.Handle,
	currData []types.Datum,
	newData []types.Datum,
	touched []bool,
	opts ...table.UpdateRecordOption,
) error {
	if err := t.Table.UpdateRecord(ctx, txn, h, currData, newData, touched, opts...); err != nil {
		return err
	}
	if !t.shouldLogUpdate(touched) {
		return nil
	}

	updateOpt := table.NewUpdateRecordOpt(opts...)
	mlogOpts := []table.AddRecordOption{
		table.DupKeyCheckSkip,
		updateOpt.PessimisticLazyDupKeyCheck(),
	}
	if goCtx := updateOpt.Ctx(); goCtx != nil {
		mlogOpts = append(mlogOpts, table.WithCtx(goCtx))
	}
	dmlType := t.updateRecordDMLType()
	if err := t.writeMLogRow(ctx, txn, currData, dmlType, mlogOldRowMarker, mlogOpts...); err != nil {
		return err
	}
	return t.writeMLogRow(ctx, txn, newData, dmlType, mlogNewRowMarker, mlogOpts...)
}

// RemoveRecord implements table.Table.
//
// For handle-changed updates (e.g. UPDATE that modifies PK, or IODKU that changes PK), the
// executor calls RemoveRecord(old) + AddRecord(new, IsUpdate). Like AddRecord, we do NOT
// check shouldLogUpdate here — we always log (conservative strategy; see AddRecord comment).
func (t *mlogTable) RemoveRecord(
	ctx table.MutateContext,
	txn kv.Transaction,
	h kv.Handle,
	r []types.Datum,
	opts ...table.RemoveRecordOption,
) error {
	if err := t.Table.RemoveRecord(ctx, txn, h, r, opts...); err != nil {
		return err
	}

	dmlType := t.removeRecordDMLType()
	return t.writeMLogRow(ctx, txn, r, dmlType, mlogOldRowMarker, table.DupKeyCheckSkip)
}

func (t *mlogTable) shouldLogUpdate(touched []bool) bool {
	// shouldLogUpdate is only called from UpdateRecord — the handle-unchanged update path.
	// The handle-changed path (RemoveRecord + AddRecord) always logs unconditionally; see
	// the comments on AddRecord and RemoveRecord.
	//
	// Note: `touched` is built from executor's column mapping. It can be shorter than the
	// column offsets recorded in `trackedBaseOffsets` (e.g. column pruning or DDL intermediate
	// states). In that case we cannot reliably decide whether tracked columns changed. To avoid
	// missing logs, fall back to logging conservatively.
	for _, offset := range t.trackedBaseOffsets {
		if offset < 0 || offset >= len(touched) {
			return true
		}
		if touched[offset] {
			return true
		}
	}
	return false
}

func (t *mlogTable) addRecordDMLType(opt *table.AddRecordOpt) MLogDMLType {
	if t.tp == MLogDMLTypeInsert && opt.IsUpdate() {
		return MLogDMLTypeUpdate
	}
	return t.tp
}

func (t *mlogTable) updateRecordDMLType() MLogDMLType {
	// UpdateRecord called in INSERT statement means "ON DUPLICATE KEY UPDATE".
	if t.tp == MLogDMLTypeInsert {
		return MLogDMLTypeUpdate
	}
	// For normal UPDATE, use "U". For other statements, fallback to statement kind.
	if t.tp == MLogDMLTypeUpdate {
		return MLogDMLTypeUpdate
	}
	return t.tp
}

func (t *mlogTable) removeRecordDMLType() MLogDMLType {
	// RemoveRecord called in INSERT statement is expected to be the "handle changed" update path,
	// for example `INSERT ... ON DUPLICATE KEY UPDATE pk = ...`.
	if t.tp == MLogDMLTypeInsert {
		return MLogDMLTypeUpdate
	}
	// Delete/Replace/LoadData/Update follow the statement kind.
	return t.tp
}

func (t *mlogTable) writeMLogRow(
	ctx table.MutateContext,
	txn kv.Transaction,
	baseRow []types.Datum,
	dmlType MLogDMLType,
	oldNew int64,
	opts ...table.AddRecordOption,
) error {
	mlogRow := make([]types.Datum, 0, len(t.trackedBaseOffsets)+2)
	for _, offset := range t.trackedBaseOffsets {
		if offset < 0 || offset >= len(baseRow) {
			return errors.Errorf(
				"write mlog row: base row too short, need offset=%d, row len=%d",
				offset,
				len(baseRow),
			)
		}
		var d types.Datum
		baseRow[offset].Copy(&d)
		mlogRow = append(mlogRow, d)
	}
	mlogRow = append(mlogRow, types.NewStringDatum(string(dmlType)), types.NewIntDatum(oldNew))

	_, err := t.mlog.AddRecord(ctx, txn, mlogRow, opts...)
	return err
}
