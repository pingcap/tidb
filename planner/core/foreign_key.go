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
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
)

// FKCheck indicates the foreign key constraint checker.
type FKCheck struct {
	FK         *model.FKInfo
	ReferredFK *model.ReferredFKInfo
	Tbl        table.Table
	Idx        table.Index
	Cols       []model.CIStr
	HandleCols []*table.Column

	IdxIsPrimaryKey bool
	IdxIsExclusive  bool

	CheckExist bool
	FailedErr  error
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
			refCol := table.FindCol(tbl.Cols(), refColInfo.Name.O)
			return &FKCheck{
				Tbl:             tbl,
				IdxIsPrimaryKey: true,
				IdxIsExclusive:  true,
				HandleCols:      []*table.Column{refCol},
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

	var handleCols []*table.Column
	if referTbIdxInfo.Primary && tblInfo.IsCommonHandle {
		cols := tbl.Cols()
		for _, idxCol := range referTbIdxInfo.Columns {
			handleCols = append(handleCols, cols[idxCol.Offset])
		}
	}

	return &FKCheck{
		Tbl:             tbl,
		Idx:             tblIdx,
		IdxIsExclusive:  len(cols) == len(referTbIdxInfo.Columns),
		IdxIsPrimaryKey: referTbIdxInfo.Primary && tblInfo.IsCommonHandle,
		FailedErr:       failedErr,
	}, nil
}
