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
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/ddl/resourcegroup"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/privilege"
	rg "github.com/pingcap/tidb/pkg/resourcegroup"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"go.uber.org/zap"
)

// AddResourceGroup implements the DDL interface, creates a resource group.
func (e *executor) AddResourceGroup(ctx sessionctx.Context, stmt *ast.CreateResourceGroupStmt) (err error) {
	groupName := stmt.ResourceGroupName
	groupInfo := &model.ResourceGroupInfo{Name: groupName, ResourceGroupSettings: model.NewResourceGroupSettings()}
	groupInfo, err = buildResourceGroup(groupInfo, stmt.ResourceGroupOptionList)
	if err != nil {
		return err
	}

	if _, ok := e.infoCache.GetLatest().ResourceGroupByName(groupName); ok {
		if stmt.IfNotExists {
			err = infoschema.ErrResourceGroupExists.FastGenByArgs(groupName)
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return infoschema.ErrResourceGroupExists.GenWithStackByArgs(groupName)
	}

	if err := checkResourceGroupValidation(groupInfo); err != nil {
		return err
	}

	logutil.DDLLogger().Debug("create resource group", zap.String("name", groupName.O), zap.Stringer("resource group settings", groupInfo.ResourceGroupSettings))

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaName:     groupName.L,
		Type:           model.ActionCreateResourceGroup,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{
			ResourceGroup: groupInfo.Name.L,
		}},
		SQLMode: ctx.GetSessionVars().SQLMode,
	}
	args := &model.ResourceGroupArgs{RGInfo: groupInfo}
	err = e.doDDLJob2(ctx, job, args)
	return err
}

// DropResourceGroup implements the DDL interface.
func (e *executor) DropResourceGroup(ctx sessionctx.Context, stmt *ast.DropResourceGroupStmt) (err error) {
	groupName := stmt.ResourceGroupName
	if groupName.L == rg.DefaultResourceGroupName {
		return resourcegroup.ErrDroppingInternalResourceGroup
	}
	is := e.infoCache.GetLatest()
	// Check group existence.
	group, ok := is.ResourceGroupByName(groupName)
	if !ok {
		err = infoschema.ErrResourceGroupNotExists.GenWithStackByArgs(groupName)
		if stmt.IfExists {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return err
	}

	// check to see if some user has dependency on the group
	checker := privilege.GetPrivilegeManager(ctx)
	if checker == nil {
		return errors.New("miss privilege checker")
	}
	user, matched := checker.MatchUserResourceGroupName(ctx.GetRestrictedSQLExecutor(), groupName.L)
	if matched {
		err = errors.Errorf("user [%s] depends on the resource group to drop", user)
		return err
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       group.ID,
		SchemaName:     group.Name.L,
		Type:           model.ActionDropResourceGroup,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{
			ResourceGroup: groupName.L,
		}},
		SQLMode: ctx.GetSessionVars().SQLMode,
	}
	args := &model.ResourceGroupArgs{RGInfo: &model.ResourceGroupInfo{Name: groupName}}
	err = e.doDDLJob2(ctx, job, args)
	return err
}

// AlterResourceGroup implements the DDL interface.
func (e *executor) AlterResourceGroup(ctx sessionctx.Context, stmt *ast.AlterResourceGroupStmt) (err error) {
	groupName := stmt.ResourceGroupName
	is := e.infoCache.GetLatest()
	// Check group existence.
	group, ok := is.ResourceGroupByName(groupName)
	if !ok {
		err := infoschema.ErrResourceGroupNotExists.GenWithStackByArgs(groupName)
		if stmt.IfExists {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return err
	}
	newGroupInfo, err := buildResourceGroup(group, stmt.ResourceGroupOptionList)
	if err != nil {
		return errors.Trace(err)
	}

	if err := checkResourceGroupValidation(newGroupInfo); err != nil {
		return err
	}

	logutil.DDLLogger().Debug("alter resource group", zap.String("name", groupName.L), zap.Stringer("new resource group settings", newGroupInfo.ResourceGroupSettings))

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       newGroupInfo.ID,
		SchemaName:     newGroupInfo.Name.L,
		Type:           model.ActionAlterResourceGroup,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{
			ResourceGroup: newGroupInfo.Name.L,
		}},
		SQLMode: ctx.GetSessionVars().SQLMode,
	}
	args := &model.ResourceGroupArgs{RGInfo: newGroupInfo}
	err = e.doDDLJob2(ctx, job, args)
	return err
}

func (e *executor) CreatePlacementPolicy(ctx sessionctx.Context, stmt *ast.CreatePlacementPolicyStmt) (err error) {
	if checkIgnorePlacementDDL(ctx) {
		return nil
	}

	if stmt.OrReplace && stmt.IfNotExists {
		return dbterror.ErrWrongUsage.GenWithStackByArgs("OR REPLACE", "IF NOT EXISTS")
	}

	policyInfo, err := buildPolicyInfo(stmt.PolicyName, stmt.PlacementOptions)
	if err != nil {
		return errors.Trace(err)
	}

	var onExists OnExist
	switch {
	case stmt.IfNotExists:
		onExists = OnExistIgnore
	case stmt.OrReplace:
		onExists = OnExistReplace
	default:
		onExists = OnExistError
	}

	return e.CreatePlacementPolicyWithInfo(ctx, policyInfo, onExists)
}

func (e *executor) DropPlacementPolicy(ctx sessionctx.Context, stmt *ast.DropPlacementPolicyStmt) (err error) {
	if checkIgnorePlacementDDL(ctx) {
		return nil
	}
	policyName := stmt.PolicyName
	is := e.infoCache.GetLatest()
	// Check policy existence.
	policy, ok := is.PolicyByName(policyName)
	if !ok {
		err = infoschema.ErrPlacementPolicyNotExists.GenWithStackByArgs(policyName)
		if stmt.IfExists {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return err
	}

	if err = CheckPlacementPolicyNotInUseFromInfoSchema(is, policy); err != nil {
		return err
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       policy.ID,
		SchemaName:     policy.Name.L,
		Type:           model.ActionDropPlacementPolicy,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{
			Policy: policyName.L,
		}},
		SQLMode: ctx.GetSessionVars().SQLMode,
	}

	args := &model.PlacementPolicyArgs{
		PolicyName: policyName,
		PolicyID:   policy.ID,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

func (e *executor) AlterPlacementPolicy(ctx sessionctx.Context, stmt *ast.AlterPlacementPolicyStmt) (err error) {
	if checkIgnorePlacementDDL(ctx) {
		return nil
	}
	policyName := stmt.PolicyName
	is := e.infoCache.GetLatest()
	// Check policy existence.
	policy, ok := is.PolicyByName(policyName)
	if !ok {
		return infoschema.ErrPlacementPolicyNotExists.GenWithStackByArgs(policyName)
	}

	newPolicyInfo, err := buildPolicyInfo(policy.Name, stmt.PlacementOptions)
	if err != nil {
		return errors.Trace(err)
	}

	err = checkPolicyValidation(newPolicyInfo.PlacementSettings)
	if err != nil {
		return err
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       policy.ID,
		SchemaName:     policy.Name.L,
		Type:           model.ActionAlterPlacementPolicy,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{
			Policy: newPolicyInfo.Name.L,
		}},
		SQLMode: ctx.GetSessionVars().SQLMode,
	}
	args := &model.PlacementPolicyArgs{
		Policy:   newPolicyInfo,
		PolicyID: policy.ID,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

func (e *executor) AlterTableCache(sctx sessionctx.Context, ti ast.Ident) (err error) {
	schema, t, err := e.getSchemaAndTableByIdent(ti)
	if err != nil {
		return err
	}
	// if a table is already in cache state, return directly
	if t.Meta().TableCacheStatusType == model.TableCacheStatusEnable {
		return nil
	}

	// forbidden cache table in system database.
	if metadef.IsMemOrSysDB(schema.Name.L) {
		return errors.Trace(dbterror.ErrUnsupportedAlterCacheForSysTable)
	} else if t.Meta().TempTableType != model.TempTableNone {
		return dbterror.ErrOptOnTemporaryTable.GenWithStackByArgs("alter temporary table cache")
	}

	if t.Meta().Partition != nil {
		return dbterror.ErrOptOnCacheTable.GenWithStackByArgs("partition mode")
	}

	succ, err := checkCacheTableSize(e.store, t.Meta().ID)
	if err != nil {
		return errors.Trace(err)
	}
	if !succ {
		return dbterror.ErrOptOnCacheTable.GenWithStackByArgs("table too large")
	}

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	ddlQuery, _ := sctx.Value(sessionctx.QueryString).(string)
	// Initialize the cached table meta lock info in `mysql.table_cache_meta`.
	// The operation shouldn't fail in most cases, and if it does, return the error directly.
	// This DML and the following DDL is not atomic, that's not a problem.
	_, _, err = sctx.GetRestrictedSQLExecutor().ExecRestrictedSQL(ctx, nil,
		"replace into mysql.table_cache_meta values (%?, 'NONE', 0, 0)", t.Meta().ID)
	if err != nil {
		return errors.Trace(err)
	}

	sctx.SetValue(sessionctx.QueryString, ddlQuery)

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		SchemaName:     schema.Name.L,
		TableName:      t.Meta().Name.L,
		TableID:        t.Meta().ID,
		Type:           model.ActionAlterCacheTable,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: sctx.GetSessionVars().CDCWriteSource,
		SQLMode:        sctx.GetSessionVars().SQLMode,
	}

	return e.doDDLJob2(sctx, job, &model.EmptyArgs{})
}

func checkCacheTableSize(store kv.Storage, tableID int64) (bool, error) {
	const cacheTableSizeLimit = 64 * (1 << 20) // 64M
	succ := true
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnCacheTable)
	err := kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
		txn.SetOption(kv.RequestSourceType, kv.InternalTxnCacheTable)
		prefix := tablecodec.GenTablePrefix(tableID)
		it, err := txn.Iter(prefix, prefix.PrefixNext())
		if err != nil {
			return errors.Trace(err)
		}
		defer it.Close()

		totalSize := 0
		for it.Valid() && it.Key().HasPrefix(prefix) {
			key := it.Key()
			value := it.Value()
			totalSize += len(key)
			totalSize += len(value)

			if totalSize > cacheTableSizeLimit {
				succ = false
				break
			}

			err = it.Next()
			if err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	})
	return succ, err
}

func (e *executor) AlterTableNoCache(ctx sessionctx.Context, ti ast.Ident) (err error) {
	schema, t, err := e.getSchemaAndTableByIdent(ti)
	if err != nil {
		return err
	}
	// if a table is not in cache state, return directly
	if t.Meta().TableCacheStatusType == model.TableCacheStatusDisable {
		return nil
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		SchemaName:     schema.Name.L,
		TableName:      t.Meta().Name.L,
		TableID:        t.Meta().ID,
		Type:           model.ActionAlterNoCacheTable,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	return e.doDDLJob2(ctx, job, &model.EmptyArgs{})
}

func (e *executor) CreateCheckConstraint(ctx sessionctx.Context, ti ast.Ident, constrName ast.CIStr, constr *ast.Constraint) error {
	schema, t, err := e.getSchemaAndTableByIdent(ti)
	if err != nil {
		return errors.Trace(err)
	}
	if constraintInfo := t.Meta().FindConstraintInfoByName(constrName.L); constraintInfo != nil {
		return infoschema.ErrCheckConstraintDupName.GenWithStackByArgs(constrName.L)
	}

	// allocate the temporary constraint name for dependency-check-error-output below.
	constrNames := map[string]bool{}
	for _, constr := range t.Meta().Constraints {
		constrNames[constr.Name.L] = true
	}
	setEmptyCheckConstraintName(t.Meta().Name.L, constrNames, []*ast.Constraint{constr})

	// existedColsMap can be used to check the existence of depended.
	existedColsMap := make(map[string]struct{})
	cols := t.Cols()
	for _, v := range cols {
		existedColsMap[v.Name.L] = struct{}{}
	}
	// check expression if supported
	if ok, err := table.IsSupportedExpr(constr); !ok {
		return err
	}

	dependedColsMap := findDependentColsInExpr(constr.Expr)
	dependedCols := make([]ast.CIStr, 0, len(dependedColsMap))
	for k := range dependedColsMap {
		if _, ok := existedColsMap[k]; !ok {
			// The table constraint depended on a non-existed column.
			return dbterror.ErrBadField.GenWithStackByArgs(k, "check constraint "+constr.Name+" expression")
		}
		dependedCols = append(dependedCols, ast.NewCIStr(k))
	}

	// build constraint meta info.
	tblInfo := t.Meta()

	// check auto-increment column
	if table.ContainsAutoIncrementCol(dependedCols, tblInfo) {
		return dbterror.ErrCheckConstraintRefersAutoIncrementColumn.GenWithStackByArgs(constr.Name)
	}
	// check foreign key
	if err := table.HasForeignKeyRefAction(tblInfo.ForeignKeys, nil, constr, dependedCols); err != nil {
		return err
	}
	constraintInfo, err := buildConstraintInfo(tblInfo, dependedCols, constr, model.StateNone)
	if err != nil {
		return errors.Trace(err)
	}
	// check if the expression is bool type
	if err := table.IfCheckConstraintExprBoolType(ctx.GetExprCtx(), constraintInfo, tblInfo); err != nil {
		return err
	}
	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        tblInfo.ID,
		SchemaName:     schema.Name.L,
		TableName:      tblInfo.Name.L,
		Type:           model.ActionAddCheckConstraint,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		Priority:       ctx.GetSessionVars().DDLReorgPriority,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	args := &model.AddCheckConstraintArgs{
		Constraint: constraintInfo,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

func (e *executor) DropCheckConstraint(ctx sessionctx.Context, ti ast.Ident, constrName ast.CIStr) error {
	is := e.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return errors.Trace(infoschema.ErrDatabaseNotExists)
	}
	t, err := is.TableByName(context.Background(), ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ti.Schema, ti.Name))
	}
	tblInfo := t.Meta()

	constraintInfo := tblInfo.FindConstraintInfoByName(constrName.L)
	if constraintInfo == nil {
		return dbterror.ErrConstraintNotFound.GenWithStackByArgs(constrName)
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        tblInfo.ID,
		SchemaName:     schema.Name.L,
		TableName:      tblInfo.Name.L,
		Type:           model.ActionDropCheckConstraint,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	args := &model.CheckConstraintArgs{
		ConstraintName: constrName,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

func (e *executor) AlterCheckConstraint(ctx sessionctx.Context, ti ast.Ident, constrName ast.CIStr, enforced bool) error {
	is := e.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return errors.Trace(infoschema.ErrDatabaseNotExists)
	}
	t, err := is.TableByName(context.Background(), ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ti.Schema, ti.Name))
	}
	tblInfo := t.Meta()

	constraintInfo := tblInfo.FindConstraintInfoByName(constrName.L)
	if constraintInfo == nil {
		return dbterror.ErrConstraintNotFound.GenWithStackByArgs(constrName)
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        tblInfo.ID,
		SchemaName:     schema.Name.L,
		TableName:      tblInfo.Name.L,
		Type:           model.ActionAlterCheckConstraint,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	args := &model.CheckConstraintArgs{
		ConstraintName: constrName,
		Enforced:       enforced,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}
