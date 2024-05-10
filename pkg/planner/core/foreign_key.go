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
	"fmt"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
)

// FKCheck indicates the foreign key constraint checker.
type FKCheck struct {
	basePhysicalPlan
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
	basePhysicalPlan
	Tp         FKCascadeType
	ReferredFK *model.ReferredFKInfo
	ChildTable table.Table
	FK         *model.FKInfo
	FKCols     []*model.ColumnInfo
	FKIdx      *model.IndexInfo
	// CascadePlans contains the child cascade plan.
	// CascadePlans will be filled during execution, so only `explain analyze` statement result contains the cascade plan,
	// `explain` statement result doesn't contain the cascade plan.
	CascadePlans []base.Plan
}

// FKCascadeType indicates in which (delete/update) statements.
type FKCascadeType int8

const (
	// FKCascadeOnDelete indicates in delete statement.
	FKCascadeOnDelete FKCascadeType = 1
	// FKCascadeOnUpdate indicates in update statement.
	FKCascadeOnUpdate FKCascadeType = 2

	emptyFkCheckSize   = int64(unsafe.Sizeof(FKCheck{}))
	emptyFkCascadeSize = int64(unsafe.Sizeof(FKCascade{}))
)

// AccessObject implements dataAccesser interface.
func (f *FKCheck) AccessObject() base.AccessObject {
	if f.Idx == nil {
		return OtherAccessObject(fmt.Sprintf("table:%s", f.Tbl.Meta().Name))
	}
	return OtherAccessObject(fmt.Sprintf("table:%s, index:%s", f.Tbl.Meta().Name, f.Idx.Meta().Name))
}

// OperatorInfo implements dataAccesser interface.
func (f *FKCheck) OperatorInfo(bool) string {
	if f.FK != nil {
		return fmt.Sprintf("foreign_key:%s, check_exist", f.FK.Name)
	}
	if f.ReferredFK != nil {
		return fmt.Sprintf("foreign_key:%s, check_not_exist", f.ReferredFK.ChildFKName)
	}
	return ""
}

// ExplainInfo implement Plan interface.
func (f *FKCheck) ExplainInfo() string {
	return f.AccessObject().String() + ", " + f.OperatorInfo(false)
}

// MemoryUsage return the memory usage of FKCheck
func (f *FKCheck) MemoryUsage() (sum int64) {
	if f == nil {
		return
	}

	sum = emptyFkCheckSize
	for _, cis := range f.Cols {
		sum += cis.MemoryUsage()
	}
	return
}

// AccessObject implements dataAccesser interface.
func (f *FKCascade) AccessObject() base.AccessObject {
	if f.FKIdx == nil {
		return OtherAccessObject(fmt.Sprintf("table:%s", f.ChildTable.Meta().Name))
	}
	return OtherAccessObject(fmt.Sprintf("table:%s, index:%s", f.ChildTable.Meta().Name, f.FKIdx.Name))
}

// OperatorInfo implements dataAccesser interface.
func (f *FKCascade) OperatorInfo(bool) string {
	switch f.Tp {
	case FKCascadeOnDelete:
		return fmt.Sprintf("foreign_key:%s, on_delete:%s", f.FK.Name, model.ReferOptionType(f.FK.OnDelete).String())
	case FKCascadeOnUpdate:
		return fmt.Sprintf("foreign_key:%s, on_update:%s", f.FK.Name, model.ReferOptionType(f.FK.OnUpdate).String())
	}
	return ""
}

// ExplainInfo implement Plan interface.
func (f *FKCascade) ExplainInfo() string {
	return f.AccessObject().String() + ", " + f.OperatorInfo(false)
}

// MemoryUsage return the memory usage of FKCascade
func (f *FKCascade) MemoryUsage() (sum int64) {
	if f == nil {
		return
	}
	sum = emptyFkCascadeSize
	return
}

func (p *Insert) buildOnInsertFKTriggers(ctx base.PlanContext, is infoschema.InfoSchema, dbName string) error {
	if !ctx.GetSessionVars().ForeignKeyChecks {
		return nil
	}
	tblInfo := p.Table.Meta()
	fkChecks := make([]*FKCheck, 0, len(tblInfo.ForeignKeys))
	fkCascades := make([]*FKCascade, 0, len(tblInfo.ForeignKeys))
	updateCols := p.buildOnDuplicateUpdateColumns()
	if len(updateCols) > 0 {
		referredFKChecks, referredFKCascades, err := buildOnUpdateReferredFKTriggers(ctx, is, dbName, tblInfo, updateCols)
		if err != nil {
			return err
		}
		if len(referredFKChecks) > 0 {
			fkChecks = append(fkChecks, referredFKChecks...)
		}
		if len(referredFKCascades) > 0 {
			fkCascades = append(fkCascades, referredFKCascades...)
		}
	} else if p.IsReplace {
		referredFKChecks, referredFKCascades, err := p.buildOnReplaceReferredFKTriggers(ctx, is, dbName, tblInfo)
		if err != nil {
			return err
		}
		if len(referredFKChecks) > 0 {
			fkChecks = append(fkChecks, referredFKChecks...)
		}
		if len(referredFKCascades) > 0 {
			fkCascades = append(fkCascades, referredFKCascades...)
		}
	}
	for _, fk := range tblInfo.ForeignKeys {
		if fk.Version < 1 {
			continue
		}
		failedErr := plannererrors.ErrNoReferencedRow2.FastGenByArgs(fk.String(dbName, tblInfo.Name.L))
		fkCheck, err := buildFKCheckOnModifyChildTable(ctx, is, fk, failedErr)
		if err != nil {
			return err
		}
		if fkCheck != nil {
			fkChecks = append(fkChecks, fkCheck)
		}
	}
	p.FKChecks = fkChecks
	p.FKCascades = fkCascades
	return nil
}

func (p *Insert) buildOnDuplicateUpdateColumns() map[string]struct{} {
	m := make(map[string]struct{})
	for _, assign := range p.OnDuplicate {
		m[assign.ColName.L] = struct{}{}
	}
	return m
}

func (*Insert) buildOnReplaceReferredFKTriggers(ctx base.PlanContext, is infoschema.InfoSchema, dbName string, tblInfo *model.TableInfo) ([]*FKCheck, []*FKCascade, error) {
	referredFKs := is.GetTableReferredForeignKeys(dbName, tblInfo.Name.L)
	fkChecks := make([]*FKCheck, 0, len(referredFKs))
	fkCascades := make([]*FKCascade, 0, len(referredFKs))
	for _, referredFK := range referredFKs {
		fkCheck, fkCascade, err := buildOnDeleteOrUpdateFKTrigger(ctx, is, referredFK, FKCascadeOnDelete)
		if err != nil {
			return nil, nil, err
		}
		if fkCheck != nil {
			fkChecks = append(fkChecks, fkCheck)
		}
		if fkCascade != nil {
			fkCascades = append(fkCascades, fkCascade)
		}
	}
	return fkChecks, fkCascades, nil
}

func (updt *Update) buildOnUpdateFKTriggers(ctx base.PlanContext, is infoschema.InfoSchema, tblID2table map[int64]table.Table) error {
	if !ctx.GetSessionVars().ForeignKeyChecks {
		return nil
	}
	tblID2UpdateColumns := updt.buildTbl2UpdateColumns()
	fkChecks := make(map[int64][]*FKCheck)
	fkCascades := make(map[int64][]*FKCascade)
	for tid, tbl := range tblID2table {
		tblInfo := tbl.Meta()
		dbInfo, exist := infoschema.SchemaByTable(is, tblInfo)
		if !exist {
			// Normally, it should never happen. Just check here to avoid panic here.
			return infoschema.ErrDatabaseNotExists
		}
		updateCols := tblID2UpdateColumns[tid]
		if len(updateCols) == 0 {
			continue
		}
		referredFKChecks, referredFKCascades, err := buildOnUpdateReferredFKTriggers(ctx, is, dbInfo.Name.L, tblInfo, updateCols)
		if err != nil {
			return err
		}
		if len(referredFKChecks) > 0 {
			fkChecks[tid] = append(fkChecks[tid], referredFKChecks...)
		}
		if len(referredFKCascades) > 0 {
			fkCascades[tid] = append(fkCascades[tid], referredFKCascades...)
		}
		childFKChecks, err := buildOnUpdateChildFKChecks(ctx, is, dbInfo.Name.L, tblInfo, updateCols)
		if err != nil {
			return err
		}
		if len(childFKChecks) > 0 {
			fkChecks[tid] = append(fkChecks[tid], childFKChecks...)
		}
	}
	updt.FKChecks = fkChecks
	updt.FKCascades = fkCascades
	return nil
}

func (del *Delete) buildOnDeleteFKTriggers(ctx base.PlanContext, is infoschema.InfoSchema, tblID2table map[int64]table.Table) error {
	if !ctx.GetSessionVars().ForeignKeyChecks {
		return nil
	}
	fkChecks := make(map[int64][]*FKCheck)
	fkCascades := make(map[int64][]*FKCascade)
	for tid, tbl := range tblID2table {
		tblInfo := tbl.Meta()
		dbInfo, exist := infoschema.SchemaByTable(is, tblInfo)
		if !exist {
			return infoschema.ErrDatabaseNotExists
		}
		referredFKs := is.GetTableReferredForeignKeys(dbInfo.Name.L, tblInfo.Name.L)
		for _, referredFK := range referredFKs {
			fkCheck, fkCascade, err := buildOnDeleteOrUpdateFKTrigger(ctx, is, referredFK, FKCascadeOnDelete)
			if err != nil {
				return err
			}
			if fkCheck != nil {
				fkChecks[tid] = append(fkChecks[tid], fkCheck)
			}
			if fkCascade != nil {
				fkCascades[tid] = append(fkCascades[tid], fkCascade)
			}
		}
	}
	del.FKChecks = fkChecks
	del.FKCascades = fkCascades
	return nil
}

func buildOnUpdateReferredFKTriggers(ctx base.PlanContext, is infoschema.InfoSchema, dbName string, tblInfo *model.TableInfo, updateCols map[string]struct{}) ([]*FKCheck, []*FKCascade, error) {
	referredFKs := is.GetTableReferredForeignKeys(dbName, tblInfo.Name.L)
	fkChecks := make([]*FKCheck, 0, len(referredFKs))
	fkCascades := make([]*FKCascade, 0, len(referredFKs))
	for _, referredFK := range referredFKs {
		if !isMapContainAnyCols(updateCols, referredFK.Cols...) {
			continue
		}
		fkCheck, fkCascade, err := buildOnDeleteOrUpdateFKTrigger(ctx, is, referredFK, FKCascadeOnUpdate)
		if err != nil {
			return nil, nil, err
		}
		if fkCheck != nil {
			fkChecks = append(fkChecks, fkCheck)
		}
		if fkCascade != nil {
			fkCascades = append(fkCascades, fkCascade)
		}
	}
	return fkChecks, fkCascades, nil
}

func buildOnUpdateChildFKChecks(ctx base.PlanContext, is infoschema.InfoSchema, dbName string, tblInfo *model.TableInfo, updateCols map[string]struct{}) ([]*FKCheck, error) {
	fkChecks := make([]*FKCheck, 0, len(tblInfo.ForeignKeys))
	for _, fk := range tblInfo.ForeignKeys {
		if fk.Version < 1 {
			continue
		}
		if !isMapContainAnyCols(updateCols, fk.Cols...) {
			continue
		}
		failedErr := plannererrors.ErrNoReferencedRow2.FastGenByArgs(fk.String(dbName, tblInfo.Name.L))
		fkCheck, err := buildFKCheckOnModifyChildTable(ctx, is, fk, failedErr)
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

func buildOnDeleteOrUpdateFKTrigger(ctx base.PlanContext, is infoschema.InfoSchema, referredFK *model.ReferredFKInfo, tp FKCascadeType) (*FKCheck, *FKCascade, error) {
	childTable, err := is.TableByName(referredFK.ChildSchema, referredFK.ChildTable)
	if err != nil {
		return nil, nil, nil
	}
	fk := model.FindFKInfoByName(childTable.Meta().ForeignKeys, referredFK.ChildFKName.L)
	if fk == nil || fk.Version < 1 {
		return nil, nil, nil
	}
	var fkReferOption model.ReferOptionType
	if fk.State != model.StatePublic {
		fkReferOption = model.ReferOptionRestrict
	} else {
		switch tp {
		case FKCascadeOnDelete:
			fkReferOption = model.ReferOptionType(fk.OnDelete)
		case FKCascadeOnUpdate:
			fkReferOption = model.ReferOptionType(fk.OnUpdate)
		}
	}
	switch fkReferOption {
	case model.ReferOptionCascade, model.ReferOptionSetNull:
		fkCascade, err := buildFKCascade(ctx, tp, referredFK, childTable, fk)
		return nil, fkCascade, err
	default:
		fkCheck, err := buildFKCheckForReferredFK(ctx, childTable, fk, referredFK)
		return fkCheck, nil, err
	}
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

func buildFKCheckOnModifyChildTable(ctx base.PlanContext, is infoschema.InfoSchema, fk *model.FKInfo, failedErr error) (*FKCheck, error) {
	referTable, err := is.TableByName(fk.RefSchema, fk.RefTable)
	if err != nil {
		return nil, nil
	}
	fkCheck, err := buildFKCheck(ctx, referTable, fk.RefCols, failedErr)
	if err != nil {
		return nil, err
	}
	fkCheck.CheckExist = true
	fkCheck.FK = fk
	return fkCheck, nil
}

func buildFKCheckForReferredFK(ctx base.PlanContext, childTable table.Table, fk *model.FKInfo, referredFK *model.ReferredFKInfo) (*FKCheck, error) {
	failedErr := plannererrors.ErrRowIsReferenced2.GenWithStackByArgs(fk.String(referredFK.ChildSchema.L, referredFK.ChildTable.L))
	fkCheck, err := buildFKCheck(ctx, childTable, fk.Cols, failedErr)
	if err != nil {
		return nil, err
	}
	fkCheck.CheckExist = false
	fkCheck.ReferredFK = referredFK
	return fkCheck, nil
}

func buildFKCheck(ctx base.PlanContext, tbl table.Table, cols []model.CIStr, failedErr error) (*FKCheck, error) {
	tblInfo := tbl.Meta()
	if tblInfo.PKIsHandle && len(cols) == 1 {
		refColInfo := model.FindColumnInfo(tblInfo.Columns, cols[0].L)
		if refColInfo != nil && mysql.HasPriKeyFlag(refColInfo.GetFlag()) {
			return FKCheck{
				Tbl:             tbl,
				IdxIsPrimaryKey: true,
				IdxIsExclusive:  true,
				FailedErr:       failedErr,
			}.Init(ctx), nil
		}
	}

	referTbIdxInfo := model.FindIndexByColumns(tblInfo, tblInfo.Indices, cols...)
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

	return FKCheck{
		Tbl:             tbl,
		Idx:             tblIdx,
		IdxIsExclusive:  len(cols) == len(referTbIdxInfo.Columns),
		IdxIsPrimaryKey: referTbIdxInfo.Primary && tblInfo.IsCommonHandle,
		FailedErr:       failedErr,
	}.Init(ctx), nil
}

func buildFKCascade(ctx base.PlanContext, tp FKCascadeType, referredFK *model.ReferredFKInfo, childTable table.Table, fk *model.FKInfo) (*FKCascade, error) {
	cols := make([]*model.ColumnInfo, len(fk.Cols))
	childTableColumns := childTable.Meta().Columns
	for i, c := range fk.Cols {
		col := model.FindColumnInfo(childTableColumns, c.L)
		if col == nil {
			return nil, errors.Errorf("foreign key column %s is not found in table %s", c.L, childTable.Meta().Name)
		}
		cols[i] = col
	}
	fkCascade := FKCascade{
		Tp:         tp,
		ReferredFK: referredFK,
		ChildTable: childTable,
		FK:         fk,
		FKCols:     cols,
	}.Init(ctx)
	if childTable.Meta().PKIsHandle && len(cols) == 1 {
		refColInfo := model.FindColumnInfo(childTableColumns, cols[0].Name.L)
		if refColInfo != nil && mysql.HasPriKeyFlag(refColInfo.GetFlag()) {
			return fkCascade, nil
		}
	}
	indexForFK := model.FindIndexByColumns(childTable.Meta(), childTable.Meta().Indices, fk.Cols...)
	if indexForFK == nil {
		return nil, errors.Errorf("Missing index for '%s' foreign key columns in the table '%s'", fk.Name, childTable.Meta().Name)
	}
	fkCascade.FKIdx = indexForFK
	return fkCascade, nil
}
