// Copyright 2022 PingCAP, Inc.
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

package core

import (
	"context"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/ranger"
)

type ForeignKeyTrigger struct {
	Tp                    FKTriggerType
	OnModifyReferredTable *OnModifyReferredTableFKInfo
	OnModifyChildTable    *OnModifyChildTableFKInfo
}

type OnModifyReferredTableFKInfo struct {
	ReferredFK *model.ReferredFKInfo
	ChildTable table.Table
	FK         *model.FKInfo
}

// OnModifyChildTableFKInfo contains foreign key information when Insert or Update child table.
type OnModifyChildTableFKInfo struct {
	DBName     string
	TblName    string
	ReferTable table.Table
	FK         *model.FKInfo
}

type FKTriggerType int8

const (
	FKTriggerOnDelete                   FKTriggerType = 1
	FKTriggerOnUpdate                   FKTriggerType = 2
	FKTriggerOnInsertOrUpdateChildTable FKTriggerType = 3
)

func buildOnDeleteForeignKeyTrigger(ctx sessionctx.Context, is infoschema.InfoSchema, tblID2table map[int64]table.Table) map[int64][]*ForeignKeyTrigger {
	if !ctx.GetSessionVars().ForeignKeyChecks {
		return nil
	}
	fkTriggers := make(map[int64][]*ForeignKeyTrigger)
	for tid, tbl := range tblID2table {
		tblInfo := tbl.Meta()
		for _, referredFK := range tblInfo.ReferredForeignKeys {
			fkTrigger := buildForeignKeyTriggerForReferredFK(is, referredFK, FKTriggerOnDelete)
			fkTriggers[tid] = append(fkTriggers[tid], fkTrigger)
		}
	}
	return fkTriggers
}

func buildOnUpdateForeignKeyTrigger(ctx sessionctx.Context, is infoschema.InfoSchema, tblID2table map[int64]table.Table, tblID2UpdateColumns map[int64]map[string]*model.ColumnInfo, tblID2Schema map[int64]string) map[int64][]*ForeignKeyTrigger {
	if !ctx.GetSessionVars().ForeignKeyChecks {
		return nil
	}
	fkTriggers := make(map[int64][]*ForeignKeyTrigger)
	for tid, tbl := range tblID2table {
		updateCols := tblID2UpdateColumns[tid]
		if len(updateCols) == 0 {
			continue
		}
		for _, referredFK := range tbl.Meta().ReferredForeignKeys {
			exist := false
			for _, referredCol := range referredFK.Cols {
				_, exist = updateCols[referredCol.L]
				if exist {
					break
				}
			}
			if !exist {
				continue
			}
			fkTrigger := buildForeignKeyTriggerForReferredFK(is, referredFK, FKTriggerOnUpdate)
			fkTriggers[tid] = append(fkTriggers[tid], fkTrigger)
		}
		triggers := buildOnModifyChildForeignKeyTrigger(ctx, is, tblID2Schema[tid], tbl.Meta())
		fkTriggers[tid] = append(fkTriggers[tid], triggers...)
	}
	return fkTriggers
}

func buildOnModifyChildForeignKeyTrigger(ctx sessionctx.Context, is infoschema.InfoSchema, dbName string, tblInfo *model.TableInfo) []*ForeignKeyTrigger {
	if !ctx.GetSessionVars().ForeignKeyChecks {
		return nil
	}
	fkTriggers := make([]*ForeignKeyTrigger, 0, len(tblInfo.ForeignKeys))
	for _, fk := range tblInfo.ForeignKeys {
		referTable, err := is.TableByName(fk.RefSchema, fk.RefTable)
		if err != nil {
			// todo: append warning?
			continue
		}
		fkTriggers = append(fkTriggers, &ForeignKeyTrigger{
			Tp: FKTriggerOnInsertOrUpdateChildTable,
			OnModifyChildTable: &OnModifyChildTableFKInfo{
				FK:         fk,
				DBName:     dbName,
				TblName:    tblInfo.Name.L,
				ReferTable: referTable,
			}})
	}
	return fkTriggers
}

func buildForeignKeyTriggerForReferredFK(is infoschema.InfoSchema, referredFK *model.ReferredFKInfo, tp FKTriggerType) *ForeignKeyTrigger {
	childTable, err := is.TableByName(referredFK.ChildSchema, referredFK.ChildTable)
	if err != nil {
		// todo: append warning?
		return nil
	}
	fk := model.FindFKInfoByName(childTable.Meta().ForeignKeys, referredFK.ChildFKName.L)
	if fk == nil || fk.Version == 0 {
		// todo: append warning?
		return nil
	}
	return &ForeignKeyTrigger{
		Tp: tp,
		OnModifyReferredTable: &OnModifyReferredTableFKInfo{
			ReferredFK: referredFK,
			ChildTable: childTable,
			FK:         fk,
		},
	}
}

func (b *PlanBuilder) BuildOnUpdateFKTriggerPlan(ctx context.Context, onModifyReferredTable *OnModifyReferredTableFKInfo) (FKTriggerPlan, error) {
	fk, referredFK, childTable := onModifyReferredTable.FK, onModifyReferredTable.ReferredFK, onModifyReferredTable.ChildTable
	switch ast.ReferOptionType(fk.OnUpdate) {
	case ast.ReferOptionCascade:
		return b.buildUpdateForeignKeyCascade(ctx, referredFK.ChildSchema, childTable, fk)
	case ast.ReferOptionSetNull:
		return b.buildUpdateForeignKeySetNull(ctx, referredFK.ChildSchema, childTable, fk)
	case ast.ReferOptionRestrict, ast.ReferOptionNoOption, ast.ReferOptionNoAction, ast.ReferOptionSetDefault:
		failedErr := ErrRowIsReferenced2.GenWithStackByArgs(fk.String(referredFK.ChildSchema.L, referredFK.ChildTable.L))
		return buildFKCheckPlan(b.ctx, childTable, fk, fk.Cols, fk.RefCols, false, failedErr)
	}
	return nil, nil
}

func (b *PlanBuilder) BuildOnDeleteFKTriggerPlan(ctx context.Context, onModifyReferredTable *OnModifyReferredTableFKInfo) (FKTriggerPlan, error) {
	fk, referredFK, childTable := onModifyReferredTable.FK, onModifyReferredTable.ReferredFK, onModifyReferredTable.ChildTable
	switch ast.ReferOptionType(fk.OnDelete) {
	case ast.ReferOptionCascade:
		return b.buildForeignKeyCascadeDelete(ctx, referredFK)
	case ast.ReferOptionSetNull:
		return b.buildUpdateForeignKeySetNull(ctx, referredFK.ChildSchema, childTable, fk)
	case ast.ReferOptionRestrict, ast.ReferOptionNoOption, ast.ReferOptionNoAction, ast.ReferOptionSetDefault:
		failedErr := ErrRowIsReferenced2.GenWithStackByArgs(fk.String(referredFK.ChildSchema.L, referredFK.ChildTable.L))
		return buildFKCheckPlan(b.ctx, childTable, fk, fk.Cols, fk.RefCols, false, failedErr)
	}
	return nil, nil
}

func (b *PlanBuilder) BuildOnInsertFKTriggerPlan(info *OnModifyChildTableFKInfo) (FKTriggerPlan, error) {
	fk := info.FK
	failedErr := ErrNoReferencedRow2.FastGenByArgs(fk.String(info.DBName, info.TblName))
	return buildFKCheckPlan(b.ctx, info.ReferTable, fk, fk.RefCols, fk.Cols, true, failedErr)
}

func (b *PlanBuilder) buildForeignKeyCascadeDelete(ctx context.Context, referredFK *model.ReferredFKInfo) (FKTriggerPlan, error) {
	b.pushSelectOffset(0)
	b.pushTableHints(nil, 0)
	defer func() {
		b.popSelectOffset()
		// table hints are only visible in the current DELETE statement.
		b.popTableHints()
	}()

	b.inDeleteStmt = true
	b.isForUpdateRead = true

	tn := &ast.TableName{
		Schema: referredFK.ChildSchema,
		Name:   referredFK.ChildTable,
	}
	datasource, err := b.buildDataSource(ctx, tn, &model.CIStr{})
	if err != nil {
		return nil, err
	}
	var ds *DataSource
	var logicalUnionScan *LogicalUnionScan
	switch v := datasource.(type) {
	case *DataSource:
		ds = v
	case *LogicalUnionScan:
		ds = v.children[0].(*DataSource)
		logicalUnionScan = v
	default:
		return nil, errors.Errorf("unknown datasource plan: %#v", datasource)
	}

	fk := model.FindFKInfoByName(ds.tableInfo.ForeignKeys, referredFK.ChildFKName.L)
	if fk == nil || fk.Version == 0 {
		return nil, errors.Errorf("should never happen")
	}

	tableReader, err := b.buildTableReaderForFK(ds, fk.Cols)
	if err != nil {
		return nil, err
	}
	if logicalUnionScan != nil {
		physicalUnionScan := PhysicalUnionScan{
			Conditions: logicalUnionScan.conditions,
			HandleCols: logicalUnionScan.handleCols,
		}.Init(logicalUnionScan.ctx, tableReader.statsInfo(), logicalUnionScan.blockOffset, nil)
		physicalUnionScan.SetChildren(tableReader)
		tableReader = physicalUnionScan
	}
	del := Delete{
		SelectPlan: tableReader,
	}.Init(b.ctx)
	del.names = ds.names

	tblID2Handle := make(map[int64][]HandleCols)
	tblID2Table := make(map[int64]table.Table)
	tid := ds.tableInfo.ID
	tblID2Handle[tid] = []HandleCols{ds.handleCols}
	tblID2Table[tid] = ds.table
	tblID2Handle, err = resolveIndicesForTblID2Handle(tblID2Handle, tableReader.Schema())
	if err != nil {
		return nil, err
	}
	del.TblColPosInfos, err = buildColumns2Handle(del.names, tblID2Handle, tblID2Table, false)
	if err != nil {
		return nil, err
	}
	del.FKTriggers = buildOnDeleteForeignKeyTrigger(b.ctx, b.is, tblID2Table)
	return &FKOnDeleteCascadePlan{
		Delete:            del,
		baseFKTriggerPlan: baseFKTriggerPlan{fk, fk.RefCols},
	}, nil
}

func (b *PlanBuilder) buildUpdateForeignKeyCascade(ctx context.Context, dbName model.CIStr, tbl table.Table, fk *model.FKInfo) (FKTriggerPlan, error) {
	triggerPlan, err := b.buildUpdateForeignKeySetNull(ctx, dbName, tbl, fk)
	if err != nil {
		return nil, err
	}
	return &FKOnUpdateCascadePlan{
		baseFKTriggerPlan: triggerPlan.baseFKTriggerPlan,
		Update:            triggerPlan.Update,
	}, nil
}

func (b *PlanBuilder) buildUpdateForeignKeySetNull(ctx context.Context, dbName model.CIStr, tbl table.Table, fk *model.FKInfo) (*FKUpdateSetNullPlan, error) {
	b.pushSelectOffset(0)
	b.pushTableHints(nil, 0)
	defer func() {
		b.popSelectOffset()
		// table hints are only visible in the current UPDATE statement.
		b.popTableHints()
	}()
	b.inUpdateStmt = true
	b.isForUpdateRead = true

	tn := &ast.TableName{
		Schema: dbName,
		Name:   tbl.Meta().Name,
	}
	datasource, err := b.buildDataSource(ctx, tn, &model.CIStr{})
	if err != nil {
		return nil, err
	}
	var ds *DataSource
	var logicalUnionScan *LogicalUnionScan
	switch v := datasource.(type) {
	case *DataSource:
		ds = v
	case *LogicalUnionScan:
		ds = v.children[0].(*DataSource)
		logicalUnionScan = v
	default:
		return nil, errors.Errorf("unknown datasource plan: %#v", datasource)
	}

	tableReader, err := b.buildTableReaderForFK(ds, fk.Cols)
	if err != nil {
		return nil, err
	}
	if logicalUnionScan != nil {
		physicalUnionScan := PhysicalUnionScan{
			Conditions: logicalUnionScan.conditions,
			HandleCols: logicalUnionScan.handleCols,
		}.Init(logicalUnionScan.ctx, tableReader.statsInfo(), logicalUnionScan.blockOffset, nil)
		physicalUnionScan.SetChildren(tableReader)
		tableReader = physicalUnionScan
	}

	schema := tableReader.Schema()
	orderedList := make([]*expression.Assignment, 0, len(fk.Cols))
	for _, col := range fk.Cols {
		idx := expression.FindFieldNameIdxByColName(ds.names, col.L)
		if idx < 0 {
			return nil, ErrUnknownColumn
		}
		value := expression.NewNull()
		value.RetType.SetType(mysql.TypeNull)
		assignment := &expression.Assignment{
			Col:     schema.Columns[idx],
			ColName: col,
			Expr:    value,
			LazyErr: nil,
		}
		orderedList = append(orderedList, assignment)
	}

	update := Update{
		OrderedList:               orderedList,
		AllAssignmentsAreConstant: true,
		VirtualAssignmentsOffset:  len(orderedList),
	}.Init(b.ctx)
	update.SelectPlan = tableReader
	update.names = ds.names
	err = update.ResolveIndices()
	if err != nil {
		return nil, err
	}

	tblID2Handle := make(map[int64][]HandleCols)
	tblID2Table := make(map[int64]table.Table)
	tid := ds.tableInfo.ID
	tblID2Handle[tid] = []HandleCols{ds.handleCols}
	tblID2Table[tid] = tbl

	tblID2Handle, err = resolveIndicesForTblID2Handle(tblID2Handle, tableReader.Schema())
	if err != nil {
		return nil, err
	}
	update.TblColPosInfos, err = buildColumns2Handle(update.names, tblID2Handle, tblID2Table, false)
	if err != nil {
		return nil, err
	}
	update.tblID2Table = tblID2Table
	tblID2UpdateColumns := buildTbl2UpdateColumns(update)
	tblID2Schema, err := buildTblID2Schema(b.is, tblID2Table)
	if err != nil {
		return nil, err
	}
	update.FKTriggers = buildOnUpdateForeignKeyTrigger(b.ctx, b.is, tblID2Table, tblID2UpdateColumns, tblID2Schema)
	return &FKUpdateSetNullPlan{
		Update:            update,
		baseFKTriggerPlan: baseFKTriggerPlan{fk, fk.RefCols},
	}, nil
}

func buildFKCheckPlan(ctx sessionctx.Context, tbl table.Table, fk *model.FKInfo, cols, rowCols []model.CIStr, checkExist bool, failedErr error) (*FKCheckPlan, error) {
	tblInfo := tbl.Meta()
	if tblInfo.PKIsHandle && len(cols) == 1 {
		refColInfo := model.FindColumnInfo(tblInfo.Columns, cols[0].L)
		if refColInfo != nil && mysql.HasPriKeyFlag(refColInfo.GetFlag()) {
			refCol := table.FindCol(tbl.Cols(), refColInfo.Name.O)
			return FKCheckPlan{
				baseFKTriggerPlan: baseFKTriggerPlan{fk, rowCols},
				Tbl:               tbl,
				IdxIsPrimaryKey:   true,
				IdxIsExclusive:    true,
				HandleCols:        []*table.Column{refCol},
				CheckExist:        checkExist,
				FailedErr:         failedErr,
			}.Init(ctx), nil
		}
	}

	referTbIdxInfo := model.FindIndexByColumns(tblInfo.Indices, cols...)
	if referTbIdxInfo == nil {
		return nil, failedErr
	}
	var tblIdx table.Index
	for _, idx := range tbl.Indices() {
		if idx.Meta().ID == referTbIdxInfo.ID {
			tblIdx = idx
		}
	}
	if tblIdx == nil {
		return nil, failedErr
	}

	var handleCols []*table.Column
	if referTbIdxInfo.Primary && tblInfo.IsCommonHandle {
		cols := tbl.Cols()
		for _, idxCol := range referTbIdxInfo.Columns {
			handleCols = append(handleCols, cols[idxCol.Offset])
		}
	}

	return FKCheckPlan{
		baseFKTriggerPlan: baseFKTriggerPlan{fk, rowCols},
		Tbl:               tbl,
		Idx:               tblIdx,
		IdxIsExclusive:    len(cols) == len(referTbIdxInfo.Columns),
		IdxIsPrimaryKey:   referTbIdxInfo.Primary && tblInfo.IsCommonHandle,
		CheckExist:        checkExist,
		FailedErr:         failedErr,
	}.Init(ctx), nil
}

func (b *PlanBuilder) buildTableReaderForFK(ds *DataSource, cols []model.CIStr) (PhysicalPlan, error) {
	tblInfo := ds.tableInfo
	if tblInfo.PKIsHandle && len(cols) == 1 {
		colInfo := model.FindColumnInfo(tblInfo.Columns, cols[0].L)
		if colInfo != nil && mysql.HasPriKeyFlag(colInfo.GetFlag()) {
			return b.buildPhysicalTableReader(ds)
		}
	}
	idx := model.FindIndexByColumns(tblInfo.Indices, cols...)
	if idx == nil {
		return nil, errors.Errorf("foreign key doesn't have related index to use, should never happen")
	}
	if ds.tableInfo.IsCommonHandle && idx.Primary {
		return b.buildPhysicalTableReader(ds)
	}

	return b.buildPhysicalIndexLookUpReader(ds.DBName, ds.table, idx, ds.schema, ds.Columns)
}

// FKTriggerPlan is the foreign key trigger plan
type FKTriggerPlan interface {
	Plan

	GetCols() []model.CIStr

	SetRangeForSelectPlan([][]types.Datum) error
}

type FKOnDeleteCascadePlan struct {
	baseFKTriggerPlan

	*Delete
}

type FKUpdateSetNullPlan struct {
	baseFKTriggerPlan

	*Update
}

type FKOnUpdateCascadePlan struct {
	baseFKTriggerPlan

	*Update
}

type FKCheckPlan struct {
	baseSchemaProducer
	baseFKTriggerPlan

	DBName     model.CIStr
	Tbl        table.Table
	Idx        table.Index
	Cols       []model.CIStr
	HandleCols []*table.Column

	IdxIsPrimaryKey bool
	IdxIsExclusive  bool

	CheckExist bool
	FailedErr  error

	ToBeCheckedHandleKeys []kv.Handle
	ToBeCheckedUniqueKeys []kv.Key
	ToBeCheckedIndexKeys  []kv.Key
}

type baseFKTriggerPlan struct {
	fk   *model.FKInfo
	cols []model.CIStr
}

func (p *baseFKTriggerPlan) GetCols() []model.CIStr {
	return p.cols
}

func (p *baseFKTriggerPlan) setRangeForSelectPlan(selectPlan PhysicalPlan, fkValues [][]types.Datum) error {
	if us, ok := selectPlan.(*PhysicalUnionScan); ok {
		return p.setRangeForSelectPlan(us.children[0], fkValues)
	}
	ranges := make([]*ranger.Range, 0, len(fkValues))
	for _, vals := range fkValues {
		ranges = append(ranges, &ranger.Range{
			LowVal:      vals,
			HighVal:     vals,
			LowExclude:  false,
			HighExclude: false,
		})
	}

	switch v := selectPlan.(type) {
	case *PhysicalIndexLookUpReader:
		is := v.IndexPlans[0].(*PhysicalIndexScan)
		is.Ranges = ranges
	case *PhysicalTableReader:
		reader := v.tablePlan.(*PhysicalTableScan)
		reader.Ranges = ranges
	default:
		return errors.Errorf("unknown")
	}
	return nil
}

func (p *FKOnDeleteCascadePlan) SetRangeForSelectPlan(fkValues [][]types.Datum) error {
	return p.setRangeForSelectPlan(p.SelectPlan, fkValues)
}

func (p *FKUpdateSetNullPlan) SetRangeForSelectPlan(fkValues [][]types.Datum) error {
	return p.setRangeForSelectPlan(p.SelectPlan, fkValues)
}

func (p *FKOnUpdateCascadePlan) SetRangeForSelectPlan(fkValues [][]types.Datum) error {
	return p.setRangeForSelectPlan(p.SelectPlan, fkValues)
}

func (p *FKOnUpdateCascadePlan) SetUpdatedValues(fkValues []types.Datum) error {
	for i, assgisn := range p.Update.OrderedList {
		assgisn.Expr = &expression.Constant{Value: fkValues[i], RetType: assgisn.Col.RetType}
	}
	return nil
}

func (p *FKCheckPlan) SetRangeForSelectPlan(fkValues [][]types.Datum) error {
	for _, vals := range fkValues {
		err := p.addRowNeedToCheck(vals)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *FKCheckPlan) addRowNeedToCheck(vals []types.Datum) error {
	sc := p.ctx.GetSessionVars().StmtCtx
	if p.IdxIsPrimaryKey {
		handleKey, err := p.buildHandleFromFKValues(sc, vals)
		if err != nil {
			return err
		}
		if p.IdxIsExclusive {
			p.ToBeCheckedHandleKeys = append(p.ToBeCheckedHandleKeys, handleKey)
		} else {
			key := tablecodec.EncodeRecordKey(p.Tbl.RecordPrefix(), handleKey)
			p.ToBeCheckedIndexKeys = append(p.ToBeCheckedIndexKeys, key)
		}
		return nil
	}
	key, distinct, err := p.Idx.GenIndexKey(sc, vals, nil, nil)
	if err != nil {
		return err
	}
	if distinct && p.IdxIsExclusive {
		p.ToBeCheckedUniqueKeys = append(p.ToBeCheckedUniqueKeys, key)
	} else {
		p.ToBeCheckedIndexKeys = append(p.ToBeCheckedIndexKeys, key)
	}
	return nil
}
