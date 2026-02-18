// Copyright 2015 PingCAP, Inc.
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
	"slices"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/intest"
)

func (*PlanBuilder) buildLoadStats(ld *ast.LoadStatsStmt) base.Plan {
	p := &LoadStats{Path: ld.Path}
	return p
}

func (b *PlanBuilder) buildRefreshStats(rs *ast.RefreshStatsStmt) (base.Plan, error) {
	if err := fillDefaultDBForRefreshStats(b.ctx, rs); err != nil {
		return nil, err
	}
	rs.Dedup()
	b.requireSelectOrRestoreAdminPrivForRefreshStats(rs)
	p := &Simple{
		Statement:    rs,
		IsFromRemote: false,
		ResolveCtx:   b.resolveCtx,
	}
	return p, nil
}

func fillDefaultDBForRefreshStats(ctx base.PlanContext, rs *ast.RefreshStatsStmt) error {
	if len(rs.RefreshObjects) == 0 {
		return nil
	}

	currentDB := ctx.GetSessionVars().CurrentDB
	for _, obj := range rs.RefreshObjects {
		if obj.RefreshObjectScope != ast.RefreshObjectScopeTable {
			continue
		}
		if obj.DBName.L != "" {
			continue
		}
		if currentDB == "" {
			return plannererrors.ErrNoDB
		}
		obj.DBName = ast.NewCIStr(currentDB)
	}
	return nil
}

func (b *PlanBuilder) requireSelectOrRestoreAdminPrivForRefreshStats(rs *ast.RefreshStatsStmt) {
	if len(rs.RefreshObjects) == 0 {
		intest.Assert(len(rs.RefreshObjects) > 0, "RefreshStatsStmt.RefreshObjects should not be empty")
		return
	}

	checker := privilege.GetPrivilegeManager(b.ctx)
	if checker != nil {
		activeRoles := b.ctx.GetSessionVars().ActiveRoles
		if checker.RequestDynamicVerification(activeRoles, "RESTORE_ADMIN", false) {
			return
		}
	}

	user := b.ctx.GetSessionVars().User
	for _, obj := range rs.RefreshObjects {
		switch obj.RefreshObjectScope {
		case ast.RefreshObjectScopeGlobal:
			var err error
			if user != nil {
				err = plannererrors.ErrPrivilegeCheckFail.GenWithStackByArgs("SELECT")
			} else {
				err = plannererrors.ErrPrivilegeCheckFail
			}
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SelectPriv, "", "", "", err)
			return
		case ast.RefreshObjectScopeDatabase:
			var err error
			if user != nil {
				err = plannererrors.ErrDBaccessDenied.GenWithStackByArgs(user.AuthUsername, user.AuthHostname, obj.DBName.O)
			}
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SelectPriv, obj.DBName.L, "", "", err)
		case ast.RefreshObjectScopeTable:
			dbName := obj.DBName.L
			var err error
			if user != nil {
				err = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("SELECT", user.AuthUsername, user.AuthHostname, obj.TableName.O)
			}
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SelectPriv, dbName, obj.TableName.L, "", err)
		}
	}
}

// buildLockStats requires INSERT and SELECT privilege for the tables same as buildAnalyze.
func (b *PlanBuilder) buildLockStats(ld *ast.LockStatsStmt) base.Plan {
	p := &LockStats{
		Tables: ld.Tables,
	}

	b.requireInsertAndSelectPriv(ld.Tables)

	return p
}

// buildUnlockStats requires INSERT and SELECT privilege for the tables same as buildAnalyze.
func (b *PlanBuilder) buildUnlockStats(ld *ast.UnlockStatsStmt) base.Plan {
	p := &UnlockStats{
		Tables: ld.Tables,
	}
	b.requireInsertAndSelectPriv(ld.Tables)

	return p
}

// requireInsertAndSelectPriv requires INSERT and SELECT privilege for the tables.
func (b *PlanBuilder) requireInsertAndSelectPriv(tables []*ast.TableName) {
	for _, tbl := range tables {
		user := b.ctx.GetSessionVars().User
		var insertErr, selectErr error
		if user != nil {
			insertErr = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("INSERT", user.AuthUsername, user.AuthHostname, tbl.Name.O)
			selectErr = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("SELECT", user.AuthUsername, user.AuthHostname, tbl.Name.O)
		}
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.InsertPriv, tbl.Schema.L, tbl.Name.L, "", insertErr)
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SelectPriv, tbl.Schema.L, tbl.Name.L, "", selectErr)
	}
}

var ruleList = []string{"leader-scatter", "peer-scatter", "learner-scatter"}
var engineList = []string{"tikv", "tiflash"}

func (b *PlanBuilder) buildDistributeTable(node *ast.DistributeTableStmt) (base.Plan, error) {
	tnW := b.resolveCtx.GetTableName(node.Table)
	tblInfo := tnW.TableInfo
	if !slices.Contains(ruleList, node.Rule) {
		return nil, plannererrors.ErrWrongArguments.GenWithStackByArgs("rule must be leader-scatter, peer-scatter or learner-scatter")
	}
	if !slices.Contains(engineList, node.Engine) {
		return nil, plannererrors.ErrWrongArguments.GenWithStackByArgs("engine must be tikv or tiflash")
	}

	if node.Engine == "tiflash" && node.Rule != "learner-scatter" {
		return nil, plannererrors.ErrWrongArguments.GenWithStackByArgs("the rule of tiflash must be learner-scatter")
	}
	plan := &DistributeTable{
		TableInfo:      tblInfo,
		PartitionNames: node.PartitionNames,
		Engine:         node.Engine,
		Rule:           node.Rule,
		Timeout:        node.Timeout,
	}
	plan.SetSchemaAndNames(buildDistributeTableSchema())
	return plan, nil
}

func (b *PlanBuilder) buildSplitRegion(node *ast.SplitRegionStmt) (base.Plan, error) {
	tnW := b.resolveCtx.GetTableName(node.Table)
	if tnW.TableInfo.TempTableType != model.TempTableNone {
		return nil, plannererrors.ErrOptOnTemporaryTable.GenWithStackByArgs("split table")
	}
	if node.SplitSyntaxOpt != nil && node.SplitSyntaxOpt.HasPartition && tnW.TableInfo.Partition == nil {
		return nil, plannererrors.ErrPartitionClauseOnNonpartitioned
	}
	if len(node.IndexName.L) != 0 {
		return b.buildSplitIndexRegion(node)
	}
	return b.buildSplitTableRegion(node)
}

func (b *PlanBuilder) buildSplitIndexRegion(node *ast.SplitRegionStmt) (base.Plan, error) {
	tnW := b.resolveCtx.GetTableName(node.Table)
	tblInfo := tnW.TableInfo
	if node.IndexName.L == strings.ToLower(mysql.PrimaryKeyName) &&
		(tblInfo.IsCommonHandle || tblInfo.PKIsHandle) {
		return nil, plannererrors.ErrKeyDoesNotExist.FastGen("unable to split clustered index, please split table instead.")
	}

	indexInfo := tblInfo.FindIndexByName(node.IndexName.L)
	if indexInfo == nil {
		return nil, plannererrors.ErrKeyDoesNotExist.GenWithStackByArgs(node.IndexName, tblInfo.Name)
	}
	mockTablePlan := logicalop.LogicalTableDual{}.Init(b.ctx, b.getSelectOffset())
	schema, names, err := expression.TableInfo2SchemaAndNames(b.ctx.GetExprCtx(), node.Table.Schema, tblInfo)
	if err != nil {
		return nil, err
	}
	mockTablePlan.SetSchema(schema)
	mockTablePlan.SetOutputNames(names)

	p := &SplitRegion{
		TableInfo:      tblInfo,
		PartitionNames: node.PartitionNames,
		IndexInfo:      indexInfo,
	}
	p.SetOutputNames(names)
	p.SetSchemaAndNames(buildSplitRegionsSchema())
	// Split index regions by user specified value lists.
	if len(node.SplitOpt.ValueLists) > 0 {
		indexValues := make([][]types.Datum, 0, len(node.SplitOpt.ValueLists))
		for i, valuesItem := range node.SplitOpt.ValueLists {
			if len(valuesItem) > len(indexInfo.Columns) {
				return nil, plannererrors.ErrWrongValueCountOnRow.GenWithStackByArgs(i + 1)
			}
			values, err := b.convertValue2ColumnType(valuesItem, mockTablePlan, indexInfo, tblInfo)
			if err != nil {
				return nil, err
			}
			indexValues = append(indexValues, values)
		}
		p.ValueLists = indexValues
		return p, nil
	}

	// Split index regions by lower, upper value.
	checkLowerUpperValue := func(valuesItem []ast.ExprNode, name string) ([]types.Datum, error) {
		if len(valuesItem) == 0 {
			return nil, errors.Errorf("Split index `%v` region %s value count should more than 0", indexInfo.Name, name)
		}
		if len(valuesItem) > len(indexInfo.Columns) {
			return nil, errors.Errorf("Split index `%v` region column count doesn't match value count at %v", indexInfo.Name, name)
		}
		return b.convertValue2ColumnType(valuesItem, mockTablePlan, indexInfo, tblInfo)
	}
	lowerValues, err := checkLowerUpperValue(node.SplitOpt.Lower, "lower")
	if err != nil {
		return nil, err
	}
	upperValues, err := checkLowerUpperValue(node.SplitOpt.Upper, "upper")
	if err != nil {
		return nil, err
	}
	p.Lower = lowerValues
	p.Upper = upperValues

	maxSplitRegionNum := int64(config.GetGlobalConfig().SplitRegionMaxNum)
	if node.SplitOpt.Num > maxSplitRegionNum {
		return nil, errors.Errorf("Split index region num exceeded the limit %v", maxSplitRegionNum)
	} else if node.SplitOpt.Num < 1 {
		return nil, errors.Errorf("Split index region num should more than 0")
	}
	p.Num = int(node.SplitOpt.Num)
	return p, nil
}

func (b *PlanBuilder) convertValue2ColumnType(valuesItem []ast.ExprNode, mockTablePlan base.LogicalPlan, indexInfo *model.IndexInfo, tblInfo *model.TableInfo) ([]types.Datum, error) {
	values := make([]types.Datum, 0, len(valuesItem))
	for j, valueItem := range valuesItem {
		colOffset := indexInfo.Columns[j].Offset
		value, err := b.convertValue(valueItem, mockTablePlan, tblInfo.Columns[colOffset])
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

func (b *PlanBuilder) convertValue(valueItem ast.ExprNode, mockTablePlan base.LogicalPlan, col *model.ColumnInfo) (d types.Datum, err error) {
	var expr expression.Expression
	switch x := valueItem.(type) {
	case *driver.ValueExpr:
		expr = &expression.Constant{
			Value:   x.Datum,
			RetType: &x.Type,
		}
	default:
		expr, _, err = b.rewrite(context.TODO(), valueItem, mockTablePlan, nil, true)
		if err != nil {
			return d, err
		}
	}
	constant, ok := expr.(*expression.Constant)
	if !ok {
		return d, errors.New("Expect constant values")
	}
	value, err := constant.Eval(b.ctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
	if err != nil {
		return d, err
	}
	d, err = value.ConvertTo(b.ctx.GetSessionVars().StmtCtx.TypeCtx(), &col.FieldType)
	if err != nil {
		if !types.ErrTruncated.Equal(err) && !types.ErrTruncatedWrongVal.Equal(err) && !types.ErrBadNumber.Equal(err) {
			return d, err
		}
		valStr, err1 := value.ToString()
		if err1 != nil {
			return d, err
		}
		return d, types.ErrTruncated.GenWithStack("Incorrect value: '%-.128s' for column '%.192s'", valStr, col.Name.O)
	}
	return d, nil
}

func (b *PlanBuilder) buildSplitTableRegion(node *ast.SplitRegionStmt) (base.Plan, error) {
	tnW := b.resolveCtx.GetTableName(node.Table)
	tblInfo := tnW.TableInfo
	handleColInfos := buildHandleColumnInfos(tblInfo)
	mockTablePlan := logicalop.LogicalTableDual{}.Init(b.ctx, b.getSelectOffset())
	schema, names, err := expression.TableInfo2SchemaAndNames(b.ctx.GetExprCtx(), node.Table.Schema, tblInfo)
	if err != nil {
		return nil, err
	}
	mockTablePlan.SetSchema(schema)
	mockTablePlan.SetOutputNames(names)

	p := &SplitRegion{
		TableInfo:      tblInfo,
		PartitionNames: node.PartitionNames,
	}
	p.SetSchemaAndNames(buildSplitRegionsSchema())
	if len(node.SplitOpt.ValueLists) > 0 {
		values := make([][]types.Datum, 0, len(node.SplitOpt.ValueLists))
		for i, valuesItem := range node.SplitOpt.ValueLists {
			data, err := convertValueListToData(valuesItem, handleColInfos, i, b, mockTablePlan)
			if err != nil {
				return nil, err
			}
			values = append(values, data)
		}
		p.ValueLists = values
		return p, nil
	}

	p.Lower, err = convertValueListToData(node.SplitOpt.Lower, handleColInfos, lowerBound, b, mockTablePlan)
	if err != nil {
		return nil, err
	}
	p.Upper, err = convertValueListToData(node.SplitOpt.Upper, handleColInfos, upperBound, b, mockTablePlan)
	if err != nil {
		return nil, err
	}

	maxSplitRegionNum := int64(config.GetGlobalConfig().SplitRegionMaxNum)
	if node.SplitOpt.Num > maxSplitRegionNum {
		return nil, errors.Errorf("Split table region num exceeded the limit %v", maxSplitRegionNum)
	} else if node.SplitOpt.Num < 1 {
		return nil, errors.Errorf("Split table region num should more than 0")
	}
	p.Num = int(node.SplitOpt.Num)
	return p, nil
}

func buildHandleColumnInfos(tblInfo *model.TableInfo) []*model.ColumnInfo {
	switch {
	case tblInfo.PKIsHandle:
		if col := tblInfo.GetPkColInfo(); col != nil {
			return []*model.ColumnInfo{col}
		}
	case tblInfo.IsCommonHandle:
		pkIdx := tables.FindPrimaryIndex(tblInfo)
		pkCols := make([]*model.ColumnInfo, 0, len(pkIdx.Columns))
		cols := tblInfo.Columns
		for _, idxCol := range pkIdx.Columns {
			pkCols = append(pkCols, cols[idxCol.Offset])
		}
		return pkCols
	default:
		return []*model.ColumnInfo{model.NewExtraHandleColInfo()}
	}
	return nil
}

const (
	lowerBound int = -1
	upperBound int = -2
)

func convertValueListToData(valueList []ast.ExprNode, handleColInfos []*model.ColumnInfo, rowIdx int,
	b *PlanBuilder, mockTablePlan *logicalop.LogicalTableDual) ([]types.Datum, error) {
	if len(valueList) != len(handleColInfos) {
		var err error
		switch rowIdx {
		case lowerBound:
			err = errors.Errorf("Split table region lower value count should be %d", len(handleColInfos))
		case upperBound:
			err = errors.Errorf("Split table region upper value count should be %d", len(handleColInfos))
		default:
			err = plannererrors.ErrWrongValueCountOnRow.GenWithStackByArgs(rowIdx)
		}
		return nil, err
	}
	data := make([]types.Datum, 0, len(handleColInfos))
	for i, v := range valueList {
		convertedDatum, err := b.convertValue(v, mockTablePlan, handleColInfos[i])
		if err != nil {
			return nil, err
		}
		if convertedDatum.IsNull() {
			return nil, plannererrors.ErrBadNull.GenWithStackByArgs(handleColInfos[i].Name.O)
		}
		data = append(data, convertedDatum)
	}
	return data, nil
}

type userVariableChecker struct {
	hasUserVariables bool
}

func (e *userVariableChecker) Enter(in ast.Node) (ast.Node, bool) {
	if _, ok := in.(*ast.VariableExpr); ok {
		e.hasUserVariables = true
		return in, true
	}
	return in, false
}

func (*userVariableChecker) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

// Check for UserVariables
func checkForUserVariables(in ast.Node) error {
	v := &userVariableChecker{hasUserVariables: false}
	_, ok := in.Accept(v)
	if !ok || v.hasUserVariables {
		return dbterror.ErrViewSelectVariable
	}
	return nil
}
