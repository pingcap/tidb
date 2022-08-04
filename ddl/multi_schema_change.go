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

package ddl

import (
	"github.com/pingcap/errors"
	ddlutil "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/dbterror"
)

func (d *ddl) MultiSchemaChange(ctx sessionctx.Context, ti ast.Ident) error {
	schema, t, err := d.getSchemaAndTableByIdent(ctx, ti)
	if err != nil {
		return errors.Trace(err)
	}
	tzName, tzOffset := ddlutil.GetTimeZone(ctx)
	job := &model.Job{
		SchemaID:        schema.ID,
		TableID:         t.Meta().ID,
		SchemaName:      schema.Name.L,
		TableName:       t.Meta().Name.L,
		Type:            model.ActionMultiSchemaChange,
		BinlogInfo:      &model.HistoryInfo{},
		Args:            nil,
		MultiSchemaInfo: ctx.GetSessionVars().StmtCtx.MultiSchemaInfo,
		ReorgMeta: &model.DDLReorgMeta{
			SQLMode:       ctx.GetSessionVars().SQLMode,
			Warnings:      make(map[errors.ErrorID]*terror.Error),
			WarningsCount: make(map[errors.ErrorID]int64),
			Location:      &model.TimeZoneLocation{Name: tzName, Offset: tzOffset},
		},
	}
	err = checkMultiSchemaInfo(ctx.GetSessionVars().StmtCtx.MultiSchemaInfo, t)
	if err != nil {
		return errors.Trace(err)
	}
	ctx.GetSessionVars().StmtCtx.MultiSchemaInfo = nil
	err = d.DoDDLJob(ctx, job)
	return d.callHookOnChanged(job, err)
}

func checkMultiSchemaInfo(info *model.MultiSchemaInfo, t table.Table) error {
	err := checkOperateSameColAndIdx(info)
	if err != nil {
		return err
	}

	err = checkVisibleColumnCnt(t, len(info.AddColumns), len(info.DropColumns))
	if err != nil {
		return err
	}

	return checkAddColumnTooManyColumns(len(t.Cols()) + len(info.AddColumns) - len(info.DropColumns))
}

func checkOperateSameColAndIdx(info *model.MultiSchemaInfo) error {
	modifyCols := make(map[string]struct{})
	modifyIdx := make(map[string]struct{})

	checkColumns := func(colNames []model.CIStr, addToModifyCols bool) error {
		for _, colName := range colNames {
			name := colName.L
			if _, ok := modifyCols[name]; ok {
				return dbterror.ErrOperateSameColumn.GenWithStackByArgs(name)
			}
			if addToModifyCols {
				modifyCols[name] = struct{}{}
			}
		}
		return nil
	}

	checkIndexes := func(idxNames []model.CIStr, addToModifyIdx bool) error {
		for _, idxName := range idxNames {
			name := idxName.L
			if _, ok := modifyIdx[name]; ok {
				return dbterror.ErrOperateSameIndex.GenWithStackByArgs(name)
			}
			if addToModifyIdx {
				modifyIdx[name] = struct{}{}
			}
		}
		return nil
	}

	if err := checkColumns(info.AddColumns, true); err != nil {
		return err
	}
	if err := checkColumns(info.DropColumns, true); err != nil {
		return err
	}
	if err := checkColumns(info.RelativeColumns, false); err != nil {
		return err
	}
	if err := checkColumns(info.ModifyColumns, true); err != nil {
		return err
	}

	if err := checkIndexes(info.AddIndexes, true); err != nil {
		return err
	}
	if err := checkIndexes(info.DropIndexes, true); err != nil {
		return err
	}
	return checkIndexes(info.AlterIndexes, true)
}
