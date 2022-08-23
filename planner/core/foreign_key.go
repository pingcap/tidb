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

func buildOnModifyChildForeignKeyChecks(ctx sessionctx.Context, is infoschema.InfoSchema, dbName string, tblInfo *model.TableInfo) ([]*FKCheck, error) {
	if !ctx.GetSessionVars().ForeignKeyChecks {
		return nil, nil
	}
	fkChecks := make([]*FKCheck, 0, len(tblInfo.ForeignKeys))
	for _, fk := range tblInfo.ForeignKeys {
		if fk.Version < 1 {
			continue
		}
		referTable, err := is.TableByName(fk.RefSchema, fk.RefTable)
		if err != nil {
			continue
		}
		failedErr := ErrNoReferencedRow2.FastGenByArgs(fk.String(dbName, tblInfo.Name.L))
		fkCheck, err := buildFKCheck(referTable, fk, fk.RefCols, true, failedErr)
		if err != nil {
			return nil, err
		}

		fkChecks = append(fkChecks, fkCheck)
	}
	return fkChecks, nil
}

func (updt *Update) buildOnUpdateFKChecks(ctx sessionctx.Context, is infoschema.InfoSchema, tblID2table map[int64]table.Table) (map[int64][]*FKCheck, error) {
	if !ctx.GetSessionVars().ForeignKeyChecks {
		return nil, nil
	}
	tblID2UpdateColumns := updt.buildTbl2UpdateColumns()
	fkChecks := make(map[int64][]*FKCheck)
	for tid, tbl := range tblID2table {
		tblInfo := tbl.Meta()
		dbInfo, exist := is.SchemaByTable(tblInfo)
		if !exist {
			return nil, infoschema.ErrDatabaseNotExists
		}
		updateCols := tblID2UpdateColumns[tid]
		if len(updateCols) == 0 {
			continue
		}
		for _, fk := range tblInfo.ForeignKeys {
			if fk.Version < 1 {
				continue
			}
			exist := false
			for _, referredCol := range fk.Cols {
				_, exist = updateCols[referredCol.L]
				if exist {
					break
				}
			}
			if !exist {
				continue
			}
			referTable, err := is.TableByName(fk.RefSchema, fk.RefTable)
			if err != nil {
				continue
			}
			failedErr := ErrNoReferencedRow2.FastGenByArgs(fk.String(dbInfo.Name.L, tblInfo.Name.L))
			fkCheck, err := buildFKCheck(referTable, fk, fk.RefCols, true, failedErr)
			if err != nil {
				return nil, err
			}
			fkChecks[tid] = append(fkChecks[tid], fkCheck)
		}
	}
	return fkChecks, nil
}

func (updt *Update) buildTbl2UpdateColumns() map[int64]map[string]*model.ColumnInfo {
	colsInfo := make([]*model.ColumnInfo, len(updt.SelectPlan.Schema().Columns))
	for _, content := range updt.TblColPosInfos {
		tbl := updt.tblID2Table[content.TblID]
		for i, c := range tbl.WritableCols() {
			colsInfo[content.Start+i] = c.ColumnInfo
		}
	}
	tblID2UpdateColumns := make(map[int64]map[string]*model.ColumnInfo)
	for tid := range updt.tblID2Table {
		tblID2UpdateColumns[tid] = make(map[string]*model.ColumnInfo)
	}
	for _, assign := range updt.OrderedList {
		col := colsInfo[assign.Col.Index]
		for _, content := range updt.TblColPosInfos {
			if assign.Col.Index >= content.Start && assign.Col.Index < content.End {
				tblID2UpdateColumns[content.TblID][col.Name.L] = col
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
					tblID2UpdateColumns[tid][col.Name.L] = col.ColumnInfo
				}
			}
		}
	}
	return tblID2UpdateColumns
}

func buildFKCheck(tbl table.Table, fk *model.FKInfo, cols []model.CIStr, checkExist bool, failedErr error) (*FKCheck, error) {
	tblInfo := tbl.Meta()
	if tblInfo.PKIsHandle && len(cols) == 1 {
		refColInfo := model.FindColumnInfo(tblInfo.Columns, cols[0].L)
		if refColInfo != nil && mysql.HasPriKeyFlag(refColInfo.GetFlag()) {
			refCol := table.FindCol(tbl.Cols(), refColInfo.Name.O)
			return &FKCheck{
				FK:              fk,
				Tbl:             tbl,
				IdxIsPrimaryKey: true,
				IdxIsExclusive:  true,
				HandleCols:      []*table.Column{refCol},
				CheckExist:      checkExist,
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
		FK:              fk,
		Tbl:             tbl,
		Idx:             tblIdx,
		IdxIsExclusive:  len(cols) == len(referTbIdxInfo.Columns),
		IdxIsPrimaryKey: referTbIdxInfo.Primary && tblInfo.IsCommonHandle,
		CheckExist:      checkExist,
		FailedErr:       failedErr,
	}, nil
}

type FKCheck struct {
	FK         *model.FKInfo
	Tbl        table.Table
	Idx        table.Index
	Cols       []model.CIStr
	HandleCols []*table.Column

	IdxIsPrimaryKey bool
	IdxIsExclusive  bool

	CheckExist bool
	FailedErr  error
}
