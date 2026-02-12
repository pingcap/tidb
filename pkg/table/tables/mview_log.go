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

// MLogSourceStmt indicates which statement kind writes the mlog.
//
// It intentionally keeps the source statement type (INSERT/UPDATE/DELETE/REPLACE/LOAD DATA)
// separate from the `_MLOG$_DML_TYPE` column value (I/U/D).
type MLogSourceStmt int

const (
	// MLogSourceInsert marks rows produced by INSERT statements
	MLogSourceInsert MLogSourceStmt = iota
	// MLogSourceUpdate marks rows produced by UPDATE statements.
	MLogSourceUpdate
	// MLogSourceDelete marks rows produced by DELETE statements.
	MLogSourceDelete
	// MLogSourceReplace marks rows produced by REPLACE statements.
	MLogSourceReplace
	// MLogSourceLoadData marks rows produced by LOAD DATA statements.
	MLogSourceLoadData
)

// MLogDMLType indicates the logical row change kind written to `_MLOG$_DML_TYPE`.
type MLogDMLType string

const (
	// MLogDMLTypeInsert represents a logical INSERT DML operation.
	MLogDMLTypeInsert MLogDMLType = "I"
	// MLogDMLTypeUpdate represents a logical UPDATE DML operation.
	MLogDMLTypeUpdate MLogDMLType = "U"
	// MLogDMLTypeDelete represents a logical DELETE DML operation.
	MLogDMLTypeDelete MLogDMLType = "D"
)

const (
	mlogOldRowMarker int64 = -1
	mlogNewRowMarker int64 = 1
)

func validateMLogMetaColumn(mlogMeta *model.TableInfo) error {
	cols := mlogMeta.Columns
	trackedCols := mlogMeta.MaterializedViewLog.Columns
	expectedLen := len(trackedCols) + 2
	if len(cols) != expectedLen {
		return errors.Errorf("invalid mlog meta columns, expect %d, got %d", expectedLen, len(cols))
	}
	for i, trackedCol := range trackedCols {
		if cols[i].Name.L != trackedCol.L {
			return errors.Errorf(
				"invalid mlog tracked columns order at position %d, expect %s, got %s",
				i,
				trackedCol.O,
				cols[i].Name.O,
			)
		}
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
	sourceStmt MLogSourceStmt,
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
			return nil, errors.Errorf("wrap table with mlog: base column %s not found", c.O)
		}
		trackedOffsets = append(trackedOffsets, offset)
	}

	return &mlogTable{
		Table:              base,
		mlog:               mlog,
		sourceStmt:         sourceStmt,
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
	// sourceStmt records the original statement kind at wrap time.
	// It is mapped to row-level `_MLOG$_DML_TYPE` (I/U/D) per operation.
	sourceStmt MLogSourceStmt

	// Base table column offsets recorded in mlog (user-specified columns).
	trackedBaseOffsets []int
	// removedConflict marks that RemoveRecord happened under INSERT/REPLACE/LOAD DATA source statement.
	// The next AddRecord should be classified as logical update (`U`) rather than insert (`I`).
	removedConflict bool
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
	opt := table.NewAddRecordOpt(opts...)

	// Consume the one-shot marker up front to avoid leaking it on AddRecord failure.
	hadRemovedConflict := t.removedConflict
	t.removedConflict = false

	recordID, err = t.Table.AddRecord(ctx, txn, r, opts...)
	if err != nil {
		return nil, err
	}

	dmlType := t.addRecordDMLType(opt, hadRemovedConflict)
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
// Like AddRecord, it always logs unconditionally — see AddRecord comment for
// the conservative-strategy rationale.
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

	if t.sourceStmt == MLogSourceInsert ||
		t.sourceStmt == MLogSourceReplace ||
		t.sourceStmt == MLogSourceLoadData {
		t.removedConflict = true
	}

	dmlType := t.removeRecordDMLType()
	return t.writeMLogRow(ctx, txn, r, dmlType, mlogOldRowMarker, table.DupKeyCheckSkip)
}

func (t *mlogTable) shouldLogUpdate(touched []bool) bool {
	// shouldLogUpdate is only called from UpdateRecord — the handle-unchanged update path.
	// The handle-changed path (RemoveRecord + AddRecord) always logs unconditionally.
	for _, offset := range t.trackedBaseOffsets {
		if offset < 0 || offset >= len(touched) {
			return true
		}
		if touched[offset] {
			return true
		}
	}

	// No tracked column was touched, no mlog row is needed.
	return false
}

func (t *mlogTable) addRecordDMLType(opt *table.AddRecordOpt, hadRemovedConflict bool) MLogDMLType {
	switch t.sourceStmt {
	case MLogSourceUpdate:
		return MLogDMLTypeUpdate
	case MLogSourceInsert, MLogSourceReplace, MLogSourceLoadData:
		// opt.IsUpdate() is true when AddRecord is the "insert half" of a logical update.
		// For INSERT statements, this happens in IODKU handle-changed path
		//
		// hadRemovedConflict indicates a preceding RemoveRecord was seen for the same row
		// in this wrapper, then consumed by this AddRecord.
		if opt.IsUpdate() || hadRemovedConflict {
			return MLogDMLTypeUpdate
		}
		return MLogDMLTypeInsert
	default:
		return MLogDMLTypeInsert
	}
}

func (t *mlogTable) updateRecordDMLType() MLogDMLType {
	// In all known UpdateRecord paths, `_MLOG$_DML_TYPE` should be U.
	return MLogDMLTypeUpdate
}

func (t *mlogTable) removeRecordDMLType() MLogDMLType {
	switch t.sourceStmt {
	case MLogSourceDelete:
		return MLogDMLTypeDelete
	case MLogSourceInsert, MLogSourceUpdate, MLogSourceReplace, MLogSourceLoadData:
		// For non-DELETE source statements, RemoveRecord is the old-row side of a logical update,
		// so `_MLOG$_DML_TYPE` should be U:
		//   - MLogSourceInsert: IODKU handle-changed path removes old row before adding new row.
		//   - MLogSourceUpdate: UPDATE handle-changed path removes old row.
		//   - MLogSourceReplace: REPLACE removes conflicting rows before insert.
		//   - MLogSourceLoadData: LOAD DATA ... REPLACE removes conflicting rows before insert.
		return MLogDMLTypeUpdate
	default:
		return MLogDMLTypeUpdate
	}
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
				"write mlog row: column at offset %d is missing from the base row (len %d)",
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
