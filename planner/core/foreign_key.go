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
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/statistics"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/ranger"
)

// FKCheck indicates the foreign key constraint checker.
type FKCheck struct {
	FK         *model.FKInfo
	ReferredFK *model.ReferredFKInfo
	Tbl        table.Table
	Idx        table.Index
	Cols       []model.CIStr

	IdxIsPrimaryKey bool
	IdxIsExclusive  bool

	CheckExist bool
	FailedErr  error
}

// FKCascade indicates the foreign key constraint cascade behaviour.
type FKCascade struct {
	OnDelete *FKCascadeInfo
	OnUpdate *FKCascadeInfo
}

// FKCascadeInfo contains the foreign key constraint information.
type FKCascadeInfo struct {
	ReferredFK *model.ReferredFKInfo
	ChildTable table.Table
	FK         *model.FKInfo
}

func (p *Insert) buildOnInsertFKChecks(ctx sessionctx.Context, is infoschema.InfoSchema, dbName string) ([]*FKCheck, error) {
	if !ctx.GetSessionVars().ForeignKeyChecks {
		return nil, nil
	}
	tblInfo := p.Table.Meta()
	fkChecks := make([]*FKCheck, 0, len(tblInfo.ForeignKeys))
	updateCols := p.buildOnDuplicateUpdateColumns()
	if len(updateCols) > 0 {
		referredFKChecks, err := buildOnUpdateReferredFKChecks(is, dbName, tblInfo, updateCols)
		if err != nil {
			return nil, err
		}
		if len(referredFKChecks) > 0 {
			fkChecks = append(fkChecks, referredFKChecks...)
		}
	}
	for _, fk := range tblInfo.ForeignKeys {
		if fk.Version < 1 {
			continue
		}
		failedErr := ErrNoReferencedRow2.FastGenByArgs(fk.String(dbName, tblInfo.Name.L))
		fkCheck, err := buildFKCheckOnModifyChildTable(is, fk, failedErr)
		if err != nil {
			return nil, err
		}
		if fkCheck != nil {
			fkChecks = append(fkChecks, fkCheck)
		}
	}
	return fkChecks, nil
}

func (p *Insert) buildOnDuplicateUpdateColumns() map[string]struct{} {
	m := make(map[string]struct{})
	for _, assign := range p.OnDuplicate {
		m[assign.ColName.L] = struct{}{}
	}
	return m
}

func (updt *Update) buildOnUpdateFKChecks(ctx sessionctx.Context, is infoschema.InfoSchema, tblID2table map[int64]table.Table) error {
	if !ctx.GetSessionVars().ForeignKeyChecks {
		return nil
	}
	tblID2UpdateColumns := updt.buildTbl2UpdateColumns()
	fkChecks := make(map[int64][]*FKCheck)
	for tid, tbl := range tblID2table {
		tblInfo := tbl.Meta()
		dbInfo, exist := is.SchemaByTable(tblInfo)
		if !exist {
			// Normally, it should never happen. Just check here to avoid panic here.
			return infoschema.ErrDatabaseNotExists
		}
		updateCols := tblID2UpdateColumns[tid]
		if len(updateCols) == 0 {
			continue
		}
		referredFKChecks, err := buildOnUpdateReferredFKChecks(is, dbInfo.Name.L, tblInfo, updateCols)
		if err != nil {
			return err
		}
		if len(referredFKChecks) > 0 {
			fkChecks[tid] = append(fkChecks[tid], referredFKChecks...)
		}
		childFKChecks, err := buildOnUpdateChildFKChecks(is, dbInfo.Name.L, tblInfo, updateCols)
		if err != nil {
			return err
		}
		if len(childFKChecks) > 0 {
			fkChecks[tid] = append(fkChecks[tid], childFKChecks...)
		}
	}
	updt.FKChecks = fkChecks
	return nil
}

func buildOnUpdateReferredFKChecks(is infoschema.InfoSchema, dbName string, tblInfo *model.TableInfo, updateCols map[string]struct{}) ([]*FKCheck, error) {
	referredFKs := is.GetTableReferredForeignKeys(dbName, tblInfo.Name.L)
	fkChecks := make([]*FKCheck, 0, len(referredFKs))
	for _, referredFK := range referredFKs {
		if !isMapContainAnyCols(updateCols, referredFK.Cols...) {
			continue
		}
		fkCheck, err := buildFKCheckOnModifyReferTable(is, referredFK)
		if err != nil {
			return nil, err
		}
		if fkCheck != nil {
			fkChecks = append(fkChecks, fkCheck)
		}
	}
	return fkChecks, nil
}

func buildOnUpdateChildFKChecks(is infoschema.InfoSchema, dbName string, tblInfo *model.TableInfo, updateCols map[string]struct{}) ([]*FKCheck, error) {
	fkChecks := make([]*FKCheck, 0, len(tblInfo.ForeignKeys))
	for _, fk := range tblInfo.ForeignKeys {
		if fk.Version < 1 {
			continue
		}
		if !isMapContainAnyCols(updateCols, fk.Cols...) {
			continue
		}
		failedErr := ErrNoReferencedRow2.FastGenByArgs(fk.String(dbName, tblInfo.Name.L))
		fkCheck, err := buildFKCheckOnModifyChildTable(is, fk, failedErr)
		if err != nil {
			return nil, err
		}
		if fkCheck != nil {
			fkChecks = append(fkChecks, fkCheck)
		}
	}
	return fkChecks, nil
}

func (updt *Update) buildTbl2UpdateColumns() map[int64]map[string]struct{} {
	colsInfo := GetUpdateColumnsInfo(updt.tblID2Table, updt.TblColPosInfos, len(updt.SelectPlan.Schema().Columns))
	tblID2UpdateColumns := make(map[int64]map[string]struct{})
	for _, assign := range updt.OrderedList {
		col := colsInfo[assign.Col.Index]
		for _, content := range updt.TblColPosInfos {
			if assign.Col.Index >= content.Start && assign.Col.Index < content.End {
				if _, ok := tblID2UpdateColumns[content.TblID]; !ok {
					tblID2UpdateColumns[content.TblID] = make(map[string]struct{})
				}
				tblID2UpdateColumns[content.TblID][col.Name.L] = struct{}{}
				break
			}
		}
	}
	for tid, tbl := range updt.tblID2Table {
		updateCols := tblID2UpdateColumns[tid]
		if len(updateCols) == 0 {
			continue
		}
		for _, col := range tbl.WritableCols() {
			if !col.IsGenerated() || !col.GeneratedStored {
				continue
			}
			for depCol := range col.Dependences {
				if _, ok := updateCols[depCol]; ok {
					tblID2UpdateColumns[tid][col.Name.L] = struct{}{}
				}
			}
		}
	}
	return tblID2UpdateColumns
}

func (del *Delete) buildOnDeleteFKTriggers(ctx sessionctx.Context, is infoschema.InfoSchema, tblID2table map[int64]table.Table) error {
	if !ctx.GetSessionVars().ForeignKeyChecks {
		return nil
	}
	fkCascades := make(map[int64][]*FKCascade)
	for tid, tbl := range tblID2table {
		tblInfo := tbl.Meta()
		dbInfo, exist := is.SchemaByTable(tblInfo)
		if !exist {
			return infoschema.ErrDatabaseNotExists
		}
		referredFKs := is.GetTableReferredForeignKeys(dbInfo.Name.L, tblInfo.Name.L)
		for _, referredFK := range referredFKs {
			fkCascade, err := buildOnDeleteFKTrigger(is, referredFK)
			if err != nil {
				return err
			}
			if fkCascade != nil {
				fkCascades[tid] = append(fkCascades[tid], fkCascade)
			}
		}
	}
	del.FKCascades = fkCascades
	return nil
}

func buildOnDeleteFKTrigger(is infoschema.InfoSchema, referredFK *model.ReferredFKInfo) (*FKCascade, error) {
	childTable, err := is.TableByName(referredFK.ChildSchema, referredFK.ChildTable)
	if err != nil {
		return nil, nil
	}
	fk := model.FindFKInfoByName(childTable.Meta().ForeignKeys, referredFK.ChildFKName.L)
	if fk == nil || fk.Version < 1 {
		return nil, nil
	}
	switch model.ReferOptionType(fk.OnDelete) {
	case model.ReferOptionCascade:
		return &FKCascade{
			OnDelete: &FKCascadeInfo{
				ReferredFK: referredFK,
				ChildTable: childTable,
				FK:         fk,
			}}, nil
	}
	return nil, nil
}

func isMapContainAnyCols(colsMap map[string]struct{}, cols ...model.CIStr) bool {
	for _, col := range cols {
		_, exist := colsMap[col.L]
		if exist {
			return true
		}
	}
	return false
}

func buildFKCheckOnModifyChildTable(is infoschema.InfoSchema, fk *model.FKInfo, failedErr error) (*FKCheck, error) {
	referTable, err := is.TableByName(fk.RefSchema, fk.RefTable)
	if err != nil {
		return nil, nil
	}
	fkCheck, err := buildFKCheck(referTable, fk.RefCols, failedErr)
	if err != nil {
		return nil, err
	}
	fkCheck.CheckExist = true
	fkCheck.FK = fk
	return fkCheck, nil
}

func buildFKCheckOnModifyReferTable(is infoschema.InfoSchema, referredFK *model.ReferredFKInfo) (*FKCheck, error) {
	childTable, err := is.TableByName(referredFK.ChildSchema, referredFK.ChildTable)
	if err != nil {
		return nil, nil
	}
	fk := model.FindFKInfoByName(childTable.Meta().ForeignKeys, referredFK.ChildFKName.L)
	if fk == nil || fk.Version < 1 {
		return nil, nil
	}
	failedErr := ErrRowIsReferenced2.GenWithStackByArgs(fk.String(referredFK.ChildSchema.L, referredFK.ChildTable.L))
	fkCheck, err := buildFKCheck(childTable, fk.Cols, failedErr)
	if err != nil {
		return nil, err
	}
	fkCheck.CheckExist = false
	fkCheck.ReferredFK = referredFK
	return fkCheck, nil
}

func buildFKCheck(tbl table.Table, cols []model.CIStr, failedErr error) (*FKCheck, error) {
	tblInfo := tbl.Meta()
	if tblInfo.PKIsHandle && len(cols) == 1 {
		refColInfo := model.FindColumnInfo(tblInfo.Columns, cols[0].L)
		if refColInfo != nil && mysql.HasPriKeyFlag(refColInfo.GetFlag()) {
			return &FKCheck{
				Tbl:             tbl,
				IdxIsPrimaryKey: true,
				IdxIsExclusive:  true,
				FailedErr:       failedErr,
			}, nil
		}
	}

	referTbIdxInfo := model.FindIndexByColumns(tblInfo, cols...)
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

	return &FKCheck{
		Tbl:             tbl,
		Idx:             tblIdx,
		IdxIsExclusive:  len(cols) == len(referTbIdxInfo.Columns),
		IdxIsPrimaryKey: referTbIdxInfo.Primary && tblInfo.IsCommonHandle,
		FailedErr:       failedErr,
	}, nil
}

// FKCascadePlan is the foreign key cascade plan
type FKCascadePlan interface {
	Plan

	SetRangeForSelectPlan([][]types.Datum) error
}

// FKOnDeleteCascadePlan is the foreign key cascade delete plan.
type FKOnDeleteCascadePlan struct {
	baseFKCascadePlan

	*Delete
}

type baseFKCascadePlan struct{}

func (p *baseFKCascadePlan) setRangeForSelectPlan(selectPlan PhysicalPlan, fkValues [][]types.Datum) error {
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
		return errors.Errorf("unknown plan %v#", v)
	}
	return nil
}

// SetRangeForSelectPlan implements the FKCascadePlan interface.
func (p *FKOnDeleteCascadePlan) SetRangeForSelectPlan(fkValues [][]types.Datum) error {
	return p.setRangeForSelectPlan(p.SelectPlan, fkValues)
}

// BuildOnDeleteFKCascadePlan builds on delete cascade plan.
func (b *PlanBuilder) BuildOnDeleteFKCascadePlan(ctx context.Context, onModifyReferredTable *FKCascadeInfo) (FKCascadePlan, error) {
	fk, referredFK, childTable := onModifyReferredTable.FK, onModifyReferredTable.ReferredFK, onModifyReferredTable.ChildTable
	switch model.ReferOptionType(fk.OnDelete) {
	case model.ReferOptionCascade:
		return b.buildForeignKeyCascadeDelete(ctx, referredFK.ChildSchema, childTable, fk)
	}
	return nil, nil
}

func (b *PlanBuilder) buildForeignKeyCascadeDelete(ctx context.Context, dbName model.CIStr, tbl table.Table, fk *model.FKInfo) (FKCascadePlan, error) {
	b.pushSelectOffset(0)
	b.pushTableHints(nil, 0)
	defer func() {
		b.popSelectOffset()
		// table hints are only visible in the current DELETE statement.
		b.popTableHints()
	}()

	b.inDeleteStmt = true
	b.isForUpdateRead = true

	ds, tableReader, err := b.buildTableReaderForFK(ctx, dbName, tbl.Meta().Name, fk.Cols)
	if err != nil {
		return nil, err
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
	err = del.buildOnDeleteFKTriggers(b.ctx, b.is, tblID2Table)
	return &FKOnDeleteCascadePlan{
		Delete: del,
	}, err
}

func (b *PlanBuilder) buildTableReaderForFK(ctx context.Context, dbName, tblName model.CIStr, cols []model.CIStr) (*DataSource, PhysicalPlan, error) {
	tn := &ast.TableName{
		Schema: dbName,
		Name:   tblName,
	}
	datasource, err := b.buildDataSource(ctx, tn, &model.CIStr{})
	if err != nil {
		return nil, nil, err
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
		return nil, nil, errors.Errorf("unknown datasource plan: %#v", datasource)
	}

	tableReader, err := b.buildPhysicalTableReaderForFK(ds, cols)
	if err != nil {
		return nil, nil, err
	}
	if logicalUnionScan == nil {
		return ds, tableReader, nil
	}
	physicalUnionScan := PhysicalUnionScan{
		Conditions: logicalUnionScan.conditions,
		HandleCols: logicalUnionScan.handleCols,
	}.Init(logicalUnionScan.ctx, tableReader.statsInfo(), logicalUnionScan.blockOffset, nil)
	physicalUnionScan.SetChildren(tableReader)
	return ds, physicalUnionScan, nil
}

func (b *PlanBuilder) buildPhysicalTableReaderForFK(ds *DataSource, cols []model.CIStr) (PhysicalPlan, error) {
	tblInfo := ds.tableInfo
	if tblInfo.PKIsHandle && len(cols) == 1 {
		colInfo := model.FindColumnInfo(tblInfo.Columns, cols[0].L)
		if colInfo != nil && mysql.HasPriKeyFlag(colInfo.GetFlag()) {
			return b.buildPhysicalTableReader(ds)
		}
	}
	idx := model.FindIndexByColumns(tblInfo, cols...)
	if idx == nil {
		return nil, errors.Errorf("foreign key doesn't have related index to use, should never happen")
	}
	if ds.tableInfo.IsCommonHandle && idx.Primary {
		return b.buildPhysicalTableReader(ds)
	}

	return b.buildPhysicalIndexLookUpReader(ds.DBName, ds.table, idx, ds.schema, ds.Columns)
}

func (b *PlanBuilder) buildPhysicalTableReader(ds *DataSource) (PhysicalPlan, error) {
	tblInfo := ds.tableInfo
	ts := PhysicalTableScan{
		Table:           tblInfo,
		Columns:         ds.Columns,
		DBName:          ds.DBName,
		TableAsName:     &tblInfo.Name,
		physicalTableID: ds.physicalTableID,
		isPartition:     ds.isPartition,
		tblCols:         ds.TblCols,
		tblColHists:     &(statistics.PseudoTable(tblInfo)).HistColl,
	}.Init(b.ctx, b.getSelectOffset())
	ts.SetSchema(ds.schema)
	ts.stats = &property.StatsInfo{}
	var extraCol *expression.Column
	for _, col := range ds.schema.Columns {
		if col.ID == model.ExtraHandleID {
			extraCol = col
		}
	}
	_, commonCols, _ := tryGetCommonHandleCols(ds.table, ds.schema)
	cop := &copTask{
		tablePlan:         ts,
		indexPlanFinished: true,
		tblColHists:       ts.tblColHists,
		extraHandleCol:    extraCol,
		commonHandleCols:  commonCols,
	}
	rootT := cop.convertToRootTask(b.ctx)
	if err := rootT.p.ResolveIndices(); err != nil {
		return nil, err
	}
	return rootT.p, nil
}
