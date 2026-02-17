// Copyright 2016 PingCAP, Inc.
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

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package ddl

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

func (e *executor) CreateTable(ctx sessionctx.Context, s *ast.CreateTableStmt) (err error) {
	ident := ast.Ident{Schema: s.Table.Schema, Name: s.Table.Name}
	is := e.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}

	var (
		referTbl     table.Table
		involvingRef []model.InvolvingSchemaInfo
	)
	if s.ReferTable != nil {
		referIdent := ast.Ident{Schema: s.ReferTable.Schema, Name: s.ReferTable.Name}
		_, ok := is.SchemaByName(referIdent.Schema)
		if !ok {
			return infoschema.ErrTableNotExists.GenWithStackByArgs(referIdent.Schema, referIdent.Name)
		}
		referTbl, err = is.TableByName(e.ctx, referIdent.Schema, referIdent.Name)
		if err != nil {
			return infoschema.ErrTableNotExists.GenWithStackByArgs(referIdent.Schema, referIdent.Name)
		}
		involvingRef = append(involvingRef, model.InvolvingSchemaInfo{
			Database: s.ReferTable.Schema.L,
			Table:    s.ReferTable.Name.L,
			Mode:     model.SharedInvolving,
		})
	}

	// build tableInfo
	metaBuildCtx := NewMetaBuildContextWithSctx(ctx)
	var tbInfo *model.TableInfo
	if s.ReferTable != nil {
		tbInfo, err = BuildTableInfoWithLike(ident, referTbl.Meta(), s)
	} else {
		tbInfo, err = BuildTableInfoWithStmt(metaBuildCtx, s, schema.Charset, schema.Collate, schema.PlacementPolicyRef)
	}
	if err != nil {
		return errors.Trace(err)
	}

	if err = rewritePartitionQueryString(ctx, s.Partition, tbInfo); err != nil {
		return err
	}

	if err = checkTableInfoValidWithStmt(metaBuildCtx, tbInfo, s); err != nil {
		return err
	}
	if err = checkTableForeignKeysValid(ctx, is, schema.Name.L, tbInfo); err != nil {
		return err
	}

	onExist := OnExistError
	if s.IfNotExists {
		onExist = OnExistIgnore
	}

	return e.CreateTableWithInfo(ctx, schema.Name, tbInfo, involvingRef, WithOnExist(onExist))
}

// createTableWithInfoJob returns the table creation job.
// WARNING: it may return a nil job, which means you don't need to submit any DDL job.
func (e *executor) createTableWithInfoJob(
	ctx sessionctx.Context,
	dbName ast.CIStr,
	tbInfo *model.TableInfo,
	involvingRef []model.InvolvingSchemaInfo,
	cfg CreateTableConfig,
) (jobW *JobWrapper, err error) {
	is := e.infoCache.GetLatest()
	schema, ok := is.SchemaByName(dbName)
	if !ok {
		return nil, infoschema.ErrDatabaseNotExists.GenWithStackByArgs(dbName)
	}

	if err = handleTablePlacement(ctx, tbInfo); err != nil {
		return nil, errors.Trace(err)
	}

	var oldViewTblID int64
	if oldTable, err := is.TableByName(e.ctx, schema.Name, tbInfo.Name); err == nil {
		err = infoschema.ErrTableExists.GenWithStackByArgs(ast.Ident{Schema: schema.Name, Name: tbInfo.Name})
		switch cfg.OnExist {
		case OnExistIgnore:
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			// if target TableMode is ModeRestore, we check if the existing mode is consistent the new one
			if tbInfo.Mode == model.TableModeRestore {
				oldTableMode := oldTable.Meta().Mode
				if oldTableMode != model.TableModeRestore {
					return nil, infoschema.ErrInvalidTableModeSet.GenWithStackByArgs(oldTableMode, tbInfo.Mode, tbInfo.Name)
				}
			}
			// Currently, target TableMode will NEVER be ModeImport because ImportInto does not use this function
			return nil, nil
		case OnExistReplace:
			// only CREATE OR REPLACE VIEW is supported at the moment.
			if tbInfo.View != nil {
				if oldTable.Meta().IsView() {
					oldViewTblID = oldTable.Meta().ID
					break
				}
				// The object to replace isn't a view.
				return nil, dbterror.ErrWrongObject.GenWithStackByArgs(dbName, tbInfo.Name, "VIEW")
			}
			return nil, err
		default:
			return nil, err
		}
	}

	if err := checkTableInfoValidExtra(ctx.GetSessionVars().StmtCtx.ErrCtx(), ctx.GetStore(), dbName, tbInfo); err != nil {
		return nil, err
	}

	var actionType model.ActionType
	switch {
	case tbInfo.View != nil:
		actionType = model.ActionCreateView
	case tbInfo.Sequence != nil:
		actionType = model.ActionCreateSequence
	default:
		actionType = model.ActionCreateTable
	}

	var involvingSchemas []model.InvolvingSchemaInfo
	sharedInvolvingFromTableInfo := getSharedInvolvingSchemaInfo(tbInfo)

	if sum := len(involvingRef) + len(sharedInvolvingFromTableInfo); sum > 0 {
		involvingSchemas = make([]model.InvolvingSchemaInfo, 0, sum+1)
		involvingSchemas = append(involvingSchemas, model.InvolvingSchemaInfo{
			Database: schema.Name.L,
			Table:    tbInfo.Name.L,
		})
		involvingSchemas = append(involvingSchemas, involvingRef...)
		involvingSchemas = append(involvingSchemas, sharedInvolvingFromTableInfo...)
	}

	job := &model.Job{
		Version:             model.GetJobVerInUse(),
		SchemaID:            schema.ID,
		SchemaName:          schema.Name.L,
		TableName:           tbInfo.Name.L,
		Type:                actionType,
		BinlogInfo:          &model.HistoryInfo{},
		CDCWriteSource:      ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: involvingSchemas,
		SQLMode:             ctx.GetSessionVars().SQLMode,
		SessionVars:         make(map[string]string),
	}
	job.AddSystemVars(vardef.TiDBScatterRegion, getScatterScopeFromSessionctx(ctx))
	args := &model.CreateTableArgs{
		TableInfo:      tbInfo,
		OnExistReplace: cfg.OnExist == OnExistReplace,
		OldViewTblID:   oldViewTblID,
		FKCheck:        ctx.GetSessionVars().ForeignKeyChecks,
	}
	return NewJobWrapperWithArgs(job, args, cfg.IDAllocated), nil
}

func getSharedInvolvingSchemaInfo(info *model.TableInfo) []model.InvolvingSchemaInfo {
	ret := make([]model.InvolvingSchemaInfo, 0, len(info.ForeignKeys)+1)
	for _, fk := range info.ForeignKeys {
		ret = append(ret, model.InvolvingSchemaInfo{
			Database: fk.RefSchema.L,
			Table:    fk.RefTable.L,
			Mode:     model.SharedInvolving,
		})
	}
	if ref := info.PlacementPolicyRef; ref != nil {
		ret = append(ret, model.InvolvingSchemaInfo{
			Policy: ref.Name.L,
			Mode:   model.SharedInvolving,
		})
	}
	return ret
}

func (e *executor) CreateTableWithInfo(
	ctx sessionctx.Context,
	dbName ast.CIStr,
	tbInfo *model.TableInfo,
	involvingRef []model.InvolvingSchemaInfo,
	cs ...CreateTableOption,
) (err error) {
	c := GetCreateTableConfig(cs)

	jobW, err := e.createTableWithInfoJob(ctx, dbName, tbInfo, involvingRef, c)
	if err != nil {
		return err
	}
	if jobW == nil {
		return nil
	}

	err = e.DoDDLJobWrapper(ctx, jobW)
	if err != nil {
		// table exists, but if_not_exists flags is true, so we ignore this error.
		if c.OnExist == OnExistIgnore && infoschema.ErrTableExists.Equal(err) {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			err = nil
		}
	} else {
		var scatterScope string
		if val, ok := jobW.GetSystemVars(vardef.TiDBScatterRegion); ok {
			scatterScope = val
		}

		preSplitAndScatterTable(ctx, e.store, tbInfo, scatterScope)
		if e.startMode == BR {
			if err := handleAutoIncID(e.getAutoIDRequirement(), jobW.Job, tbInfo); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return errors.Trace(err)
}

func (e *executor) BatchCreateTableWithInfo(ctx sessionctx.Context,
	dbName ast.CIStr,
	infos []*model.TableInfo,
	cs ...CreateTableOption,
) error {
	failpoint.Inject("RestoreBatchCreateTableEntryTooLarge", func(val failpoint.Value) {
		injectBatchSize := val.(int)
		if len(infos) > injectBatchSize {
			failpoint.Return(kv.ErrEntryTooLarge)
		}
	})
	c := GetCreateTableConfig(cs)

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
		SessionVars:    make(map[string]string),
	}
	job.AddSystemVars(vardef.TiDBScatterRegion, getScatterScopeFromSessionctx(ctx))

	var err error

	// check if there are any duplicated table names
	duplication := make(map[string]struct{})
	// TODO filter those duplicated info out.
	for _, info := range infos {
		if _, ok := duplication[info.Name.L]; ok {
			err = infoschema.ErrTableExists.FastGenByArgs("can not batch create tables with same name")
			if c.OnExist == OnExistIgnore && infoschema.ErrTableExists.Equal(err) {
				ctx.GetSessionVars().StmtCtx.AppendNote(err)
				err = nil
			}
		}
		if err != nil {
			return errors.Trace(err)
		}

		duplication[info.Name.L] = struct{}{}
	}

	args := &model.BatchCreateTableArgs{
		Tables: make([]*model.CreateTableArgs, 0, len(infos)),
	}
	for _, info := range infos {
		jobItem, err := e.createTableWithInfoJob(ctx, dbName, info, nil, c)
		if err != nil {
			return errors.Trace(err)
		}
		if jobItem == nil {
			continue
		}

		// if jobW.Type == model.ActionCreateTables, it is initialized
		// if not, initialize jobW by job.XXXX
		if job.Type != model.ActionCreateTables {
			job.Type = model.ActionCreateTables
			job.SchemaID = jobItem.SchemaID
			job.SchemaName = jobItem.SchemaName
		}

		// append table job args
		args.Tables = append(args.Tables, jobItem.JobArgs.(*model.CreateTableArgs))
		job.InvolvingSchemaInfo = append(job.InvolvingSchemaInfo, model.InvolvingSchemaInfo{
			Database: dbName.L,
			Table:    info.Name.L,
		})
		if sharedInv := getSharedInvolvingSchemaInfo(info); len(sharedInv) > 0 {
			job.InvolvingSchemaInfo = append(job.InvolvingSchemaInfo, sharedInv...)
		}
	}
	if len(args.Tables) == 0 {
		return nil
	}

	jobW := NewJobWrapperWithArgs(job, args, c.IDAllocated)
	err = e.DoDDLJobWrapper(ctx, jobW)
	if err != nil {
		// table exists, but if_not_exists flags is true, so we ignore this error.
		if c.OnExist == OnExistIgnore && infoschema.ErrTableExists.Equal(err) {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			err = nil
		}
		return errors.Trace(err)
	}
	var scatterScope string
	if val, ok := jobW.GetSystemVars(vardef.TiDBScatterRegion); ok {
		scatterScope = val
	}
	for _, tblArgs := range args.Tables {
		preSplitAndScatterTable(ctx, e.store, tblArgs.TableInfo, scatterScope)
		if e.startMode == BR {
			if err := handleAutoIncID(e.getAutoIDRequirement(), jobW.Job, tblArgs.TableInfo); err != nil {
				return errors.Trace(err)
			}
		}
	}

	return nil
}

func (e *executor) CreatePlacementPolicyWithInfo(ctx sessionctx.Context, policy *model.PolicyInfo, onExist OnExist) error {
	if checkIgnorePlacementDDL(ctx) {
		return nil
	}

	policyName := policy.Name
	if policyName.L == defaultPlacementPolicyName {
		return errors.Trace(infoschema.ErrReservedSyntax.GenWithStackByArgs(policyName))
	}

	// Check policy existence.
	_, ok := e.infoCache.GetLatest().PolicyByName(policyName)
	if ok {
		err := infoschema.ErrPlacementPolicyExists.GenWithStackByArgs(policyName)
		switch onExist {
		case OnExistIgnore:
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		case OnExistError:
			return err
		}
	}

	if err := checkPolicyValidation(policy.PlacementSettings); err != nil {
		return err
	}

	policyID, err := e.genPlacementPolicyID()
	if err != nil {
		return err
	}
	policy.ID = policyID

	job := &model.Job{
		Version:    model.GetJobVerInUse(),
		SchemaName: policy.Name.L,
		Type:       model.ActionCreatePlacementPolicy,
		BinlogInfo: &model.HistoryInfo{},
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{
			Policy: policy.Name.L,
		}},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	args := &model.PlacementPolicyArgs{
		Policy:         policy,
		ReplaceOnExist: onExist == OnExistReplace,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

// preSplitAndScatter performs pre-split and scatter of the table's regions.
// If `pi` is not nil, will only split region for `pi`, this is used when add partition.
func preSplitAndScatter(ctx sessionctx.Context, store kv.Storage, tbInfo *model.TableInfo, parts []model.PartitionDefinition, scatterScope string) {
	failpoint.InjectCall("preSplitAndScatter", scatterScope)
	if tbInfo.TempTableType != model.TempTableNone {
		return
	}
	sp, ok := store.(kv.SplittableStore)
	if !ok || atomic.LoadUint32(&EnableSplitTableRegion) == 0 {
		return
	}
	var preSplit func()
	if len(parts) > 0 {
		preSplit = func() { splitPartitionTableRegion(ctx, sp, tbInfo, parts, scatterScope) }
	} else {
		preSplit = func() { splitTableRegion(ctx, sp, tbInfo, scatterScope) }
	}
	if scatterScope != vardef.ScatterOff {
		preSplit()
	} else {
		go preSplit()
	}
}

func preSplitAndScatterTable(ctx sessionctx.Context, store kv.Storage, tbInfo *model.TableInfo, scatterScope string) {
	var partitions []model.PartitionDefinition
	if pi := tbInfo.GetPartitionInfo(); pi != nil {
		partitions = pi.Definitions
	}
	preSplitAndScatter(ctx, store, tbInfo, partitions, scatterScope)
}

func (e *executor) FlashbackCluster(ctx sessionctx.Context, flashbackTS uint64) error {
	logutil.DDLLogger().Info("get flashback cluster job", zap.Stringer("flashbackTS", oracle.GetTimeFromTS(flashbackTS)))
	nowTS, err := ctx.GetStore().GetOracle().GetTimestamp(e.ctx, &oracle.Option{})
	if err != nil {
		return errors.Trace(err)
	}
	gap := time.Until(oracle.GetTimeFromTS(nowTS)).Abs()
	if gap > 1*time.Second {
		ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("Gap between local time and PD TSO is %s, please check PD/system time", gap))
	}
	job := &model.Job{
		Version:    model.GetJobVerInUse(),
		Type:       model.ActionFlashbackCluster,
		BinlogInfo: &model.HistoryInfo{},
		// The value for global variables is meaningless, it will cover during flashback cluster.
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		// FLASHBACK CLUSTER affects all schemas and tables.
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{
			Database: model.InvolvingAll,
			Table:    model.InvolvingAll,
		}},
		SQLMode: ctx.GetSessionVars().SQLMode,
	}

	args := &model.FlashbackClusterArgs{
		FlashbackTS:       flashbackTS,
		PDScheduleValue:   map[string]any{},
		EnableGC:          true,
		EnableAutoAnalyze: true,
		EnableTTLJob:      true,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

func (e *executor) RecoverTable(ctx sessionctx.Context, recoverTableInfo *model.RecoverTableInfo) (err error) {
	is := e.infoCache.GetLatest()
	schemaID, tbInfo := recoverTableInfo.SchemaID, recoverTableInfo.TableInfo
	// Check schema exist.
	schema, ok := is.SchemaByID(schemaID)
	if !ok {
		return errors.Trace(infoschema.ErrDatabaseNotExists.GenWithStackByArgs(
			fmt.Sprintf("(Schema ID %d)", schemaID),
		))
	}
	// Check not exist table with same name.
	if ok := is.TableExists(schema.Name, tbInfo.Name); ok {
		return infoschema.ErrTableExists.GenWithStackByArgs(tbInfo.Name)
	}

	// for "flashback table xxx to yyy"
	// Note: this case only allow change table name, schema remains the same.
	var involvedSchemas []model.InvolvingSchemaInfo
	if recoverTableInfo.OldTableName != tbInfo.Name.L {
		involvedSchemas = []model.InvolvingSchemaInfo{
			{Database: schema.Name.L, Table: recoverTableInfo.OldTableName},
			{Database: schema.Name.L, Table: tbInfo.Name.L},
		}
	}

	tbInfo.State = model.StateNone
	job := &model.Job{
		Version:             model.GetJobVerInUse(),
		SchemaID:            schemaID,
		TableID:             tbInfo.ID,
		SchemaName:          schema.Name.L,
		TableName:           tbInfo.Name.L,
		Type:                model.ActionRecoverTable,
		BinlogInfo:          &model.HistoryInfo{},
		InvolvingSchemaInfo: involvedSchemas,
		CDCWriteSource:      ctx.GetSessionVars().CDCWriteSource,
		SQLMode:             ctx.GetSessionVars().SQLMode,
	}

	args := &model.RecoverArgs{
		RecoverInfo: &model.RecoverSchemaInfo{
			RecoverTableInfos: []*model.RecoverTableInfo{recoverTableInfo},
		},
		CheckFlag: recoverCheckFlagNone}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

func (e *executor) CreateView(ctx sessionctx.Context, s *ast.CreateViewStmt) (err error) {
	viewInfo, err := BuildViewInfo(s)
	if err != nil {
		return err
	}

	cols := make([]*table.Column, len(s.Cols))
	for i, v := range s.Cols {
		cols[i] = table.ToColumn(&model.ColumnInfo{
			Name:   v,
			ID:     int64(i),
			Offset: i,
			State:  model.StatePublic,
		})
	}

	tblCharset := ""
	tblCollate := ""
	if v, ok := ctx.GetSessionVars().GetSystemVar(vardef.CharacterSetConnection); ok {
		tblCharset = v
	}
	if v, ok := ctx.GetSessionVars().GetSystemVar(vardef.CollationConnection); ok {
		tblCollate = v
	}

	tbInfo, err := BuildTableInfo(NewMetaBuildContextWithSctx(ctx), s.ViewName.Name, cols, nil, tblCharset, tblCollate)
	if err != nil {
		return err
	}
	tbInfo.View = viewInfo

	onExist := OnExistError
	if s.OrReplace {
		onExist = OnExistReplace
	}

	return e.CreateTableWithInfo(ctx, s.ViewName.Schema, tbInfo, nil, WithOnExist(onExist))
}
