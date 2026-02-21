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
	"net/url"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/s3like"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	semv1 "github.com/pingcap/tidb/pkg/util/sem"
	sem "github.com/pingcap/tidb/pkg/util/sem/compat"
)

func (b *PlanBuilder) buildLoadData(ctx context.Context, ld *ast.LoadDataStmt) (base.Plan, error) {
	mockTablePlan := logicalop.LogicalTableDual{}.Init(b.ctx, b.getSelectOffset())
	var (
		err     error
		options = make([]*LoadDataOpt, 0, len(ld.Options))
	)
	for _, opt := range ld.Options {
		loadDataOpt := LoadDataOpt{Name: opt.Name}
		if opt.Value != nil {
			loadDataOpt.Value, _, err = b.rewrite(ctx, opt.Value, mockTablePlan, nil, true)
			if err != nil {
				return nil, err
			}
		}
		options = append(options, &loadDataOpt)
	}
	tnW := b.resolveCtx.GetTableName(ld.Table)
	p := LoadData{
		FileLocRef:         ld.FileLocRef,
		OnDuplicate:        ld.OnDuplicate,
		Path:               ld.Path,
		Format:             ld.Format,
		Table:              tnW,
		Charset:            ld.Charset,
		Columns:            ld.Columns,
		FieldsInfo:         ld.FieldsInfo,
		LinesInfo:          ld.LinesInfo,
		IgnoreLines:        ld.IgnoreLines,
		ColumnAssignments:  ld.ColumnAssignments,
		ColumnsAndUserVars: ld.ColumnsAndUserVars,
		Options:            options,
	}.Init(b.ctx)
	user := b.ctx.GetSessionVars().User
	var insertErr, deleteErr error
	if user != nil {
		insertErr = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("INSERT", user.AuthUsername, user.AuthHostname, p.Table.Name.O)
		deleteErr = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("DELETE", user.AuthUsername, user.AuthHostname, p.Table.Name.O)
	}
	b.visitInfo = appendVisitInfo(b.visitInfo, mysql.InsertPriv, p.Table.Schema.L, p.Table.Name.L, "", insertErr)
	if p.OnDuplicate == ast.OnDuplicateKeyHandlingReplace {
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DeletePriv, p.Table.Schema.L, p.Table.Name.L, "", deleteErr)
	}
	tableInfo := p.Table.TableInfo
	tableInPlan, ok := b.is.TableByID(ctx, tableInfo.ID)
	if !ok {
		db := b.ctx.GetSessionVars().CurrentDB
		return nil, infoschema.ErrTableNotExists.FastGenByArgs(db, tableInfo.Name.O)
	}
	schema, names, err := expression.TableInfo2SchemaAndNames(b.ctx.GetExprCtx(), ast.NewCIStr(""), tableInfo)
	if err != nil {
		return nil, err
	}
	mockTablePlan.SetSchema(schema)
	mockTablePlan.SetOutputNames(names)

	p.GenCols, err = b.resolveGeneratedColumns(ctx, tableInPlan.Cols(), nil, mockTablePlan)
	return p, err
}

var (
	importIntoSchemaNames = []string{
		"Job_ID", "Group_Key", "Data_Source", "Target_Table", "Table_ID",
		"Phase", "Status", "Source_File_Size", "Imported_Rows",
		"Result_Message", "Create_Time", "Start_Time", "End_Time", "Created_By", "Last_Update_Time",
		"Cur_Step", "Cur_Step_Processed_Size", "Cur_Step_Total_Size", "Cur_Step_Progress_Pct", "Cur_Step_Speed", "Cur_Step_ETA",
	}
	// ImportIntoSchemaFTypes store the field types of the show import jobs schema.
	ImportIntoSchemaFTypes = []byte{
		mysql.TypeLonglong, mysql.TypeString, mysql.TypeString, mysql.TypeString, mysql.TypeLonglong,
		mysql.TypeString, mysql.TypeString, mysql.TypeString, mysql.TypeLonglong,
		mysql.TypeString, mysql.TypeTimestamp, mysql.TypeTimestamp, mysql.TypeTimestamp, mysql.TypeString, mysql.TypeTimestamp,
		mysql.TypeString, mysql.TypeString, mysql.TypeString, mysql.TypeString, mysql.TypeString, mysql.TypeString,
	}

	// ImportIntoFieldMap store the mapping from field names to their indices.
	// As there are many test cases that use the index to check the result from
	// `SHOW IMPORT JOBS`, this structure is used to avoid hardcoding these indexs,
	// so adding new fields does not require modifying all the tests.
	ImportIntoFieldMap = make(map[string]int)

	showImportGroupsNames = []string{"Group_Key", "Total_Jobs",
		"Pending", "Running", "Completed", "Failed", "Cancelled",
		"First_Job_Create_Time", "Last_Job_Update_Time"}
	showImportGroupsFTypes = []byte{mysql.TypeString, mysql.TypeLonglong,
		mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeLonglong,
		mysql.TypeTimestamp, mysql.TypeTimestamp}

	// ImportIntoDataSource used inplannererrors.ErrLoadDataInvalidURI.
	ImportIntoDataSource = "data source"
)

func init() {
	for idx, name := range importIntoSchemaNames {
		normalized := strings.ReplaceAll(name, "_", "")
		ImportIntoFieldMap[normalized] = idx
	}
}

var (
	distributionJobsSchemaNames = []string{"Job_ID", "Database", "Table", "Partition_List", "Engine", "Rule", "Status",
		"Timeout", "Create_Time", "Start_Time", "Finish_Time"}
	distributionJobsSchedulerFTypes = []byte{mysql.TypeLonglong, mysql.TypeString, mysql.TypeString, mysql.TypeString,
		mysql.TypeString, mysql.TypeString, mysql.TypeString, mysql.TypeString,
		mysql.TypeTimestamp, mysql.TypeTimestamp, mysql.TypeTimestamp}
)

// importIntoCollAssignmentChecker implements ast.Visitor interface.
// It is used to check the column assignment expressions in IMPORT INTO statement.
// Currently, the import into column assignment only supports some simple expressions.
type importIntoCollAssignmentChecker struct {
	idx        int
	err        error
	neededVars map[string]int
}

func newImportIntoCollAssignmentChecker() *importIntoCollAssignmentChecker {
	return &importIntoCollAssignmentChecker{neededVars: make(map[string]int)}
}

// checkImportIntoColAssignments checks the column assignment expressions in IMPORT INTO statement.
func checkImportIntoColAssignments(assignments []*ast.Assignment) (map[string]int, error) {
	checker := newImportIntoCollAssignmentChecker()
	for i, assign := range assignments {
		checker.idx = i
		assign.Expr.Accept(checker)
		if checker.err != nil {
			break
		}
	}
	return checker.neededVars, checker.err
}

// Enter implements ast.Visitor interface.
func (*importIntoCollAssignmentChecker) Enter(node ast.Node) (ast.Node, bool) {
	return node, false
}

// Leave implements ast.Visitor interface.
func (v *importIntoCollAssignmentChecker) Leave(node ast.Node) (ast.Node, bool) {
	switch n := node.(type) {
	case *ast.ColumnNameExpr:
		v.err = errors.Errorf("COLUMN reference is not supported in IMPORT INTO column assignment, index %d", v.idx)
		return n, false
	case *ast.SubqueryExpr:
		v.err = errors.Errorf("subquery is not supported in IMPORT INTO column assignment, index %d", v.idx)
		return n, false
	case *ast.VariableExpr:
		if n.IsSystem {
			v.err = errors.Errorf("system variable is not supported in IMPORT INTO column assignment, index %d", v.idx)
			return n, false
		}
		if n.Value != nil {
			v.err = errors.Errorf("setting a variable in IMPORT INTO column assignment is not supported, index %d", v.idx)
			return n, false
		}
		v.neededVars[strings.ToLower(n.Name)] = v.idx
	case *ast.DefaultExpr:
		v.err = errors.Errorf("FUNCTION default is not supported in IMPORT INTO column assignment, index %d", v.idx)
		return n, false
	case *ast.WindowFuncExpr:
		v.err = errors.Errorf("window FUNCTION %s is not supported in IMPORT INTO column assignment, index %d", n.Name, v.idx)
		return n, false
	case *ast.AggregateFuncExpr:
		v.err = errors.Errorf("aggregate FUNCTION %s is not supported in IMPORT INTO column assignment, index %d", n.F, v.idx)
		return n, false
	case *ast.FuncCallExpr:
		fnName := n.FnName.L
		switch fnName {
		case ast.Grouping:
			v.err = errors.Errorf("FUNCTION %s is not supported in IMPORT INTO column assignment, index %d", n.FnName.O, v.idx)
			return n, false
		case ast.GetVar:
			if len(n.Args) > 0 {
				val, ok := n.Args[0].(*driver.ValueExpr)
				if !ok || val.Kind() != types.KindString {
					v.err = errors.Errorf("the argument of getvar should be a constant string in IMPORT INTO column assignment, index %d", v.idx)
					return n, false
				}
				v.neededVars[strings.ToLower(val.GetString())] = v.idx
			}
		default:
			if !expression.IsFunctionSupported(fnName) {
				v.err = errors.Errorf("FUNCTION %s is not supported in IMPORT INTO column assignment, index %d", n.FnName.O, v.idx)
				return n, false
			}
		}
	case *ast.ValuesExpr:
		v.err = errors.Errorf("VALUES is not supported in IMPORT INTO column assignment, index %d", v.idx)
		return n, false
	}
	return node, v.err == nil
}

func (b *PlanBuilder) buildImportInto(ctx context.Context, ld *ast.ImportIntoStmt) (base.Plan, error) {
	mockTablePlan := logicalop.LogicalTableDual{}.Init(b.ctx, b.getSelectOffset())
	var (
		err              error
		options          = make([]*LoadDataOpt, 0, len(ld.Options))
		importFromServer bool
	)

	if ld.Select == nil {
		u, err := url.Parse(ld.Path)
		if err != nil {
			return nil, exeerrors.ErrLoadDataInvalidURI.FastGenByArgs(ImportIntoDataSource, err.Error())
		}
		importFromServer = objstore.IsLocal(u)
		// for SEM v2, they are checked by configured rules.
		if semv1.IsEnabled() {
			if importFromServer {
				return nil, plannererrors.ErrNotSupportedWithSem.GenWithStackByArgs("IMPORT INTO from server disk")
			}
			if kerneltype.IsNextGen() && objstore.IsS3Like(u) {
				if err := checkNextGenS3PathWithSem(u); err != nil {
					return nil, err
				}
			}
		}
		// a nextgen cluster might be shared by multiple tenants, and they might
		// share the same AWS role to access import-into source data bucket, this
		// external ID can be used to restrict the access only to the current tenant.
		// when SEM enabled, we need set it.
		if kerneltype.IsNextGen() && sem.IsEnabled() && objstore.IsS3Like(u) {
			values := u.Query()
			values.Set(s3like.S3ExternalID, config.GetGlobalKeyspaceName())
			u.RawQuery = values.Encode()
			ld.Path = u.String()
		}
	}

	for _, opt := range ld.Options {
		loadDataOpt := LoadDataOpt{Name: opt.Name}
		if opt.Value != nil {
			loadDataOpt.Value, _, err = b.rewrite(ctx, opt.Value, mockTablePlan, nil, true)
			if err != nil {
				return nil, err
			}
		}
		options = append(options, &loadDataOpt)
	}

	neededVars, err := checkImportIntoColAssignments(ld.ColumnAssignments)
	if err != nil {
		return nil, err
	}

	for _, v := range ld.ColumnsAndUserVars {
		userVar := v.UserVar
		if userVar == nil {
			continue
		}
		delete(neededVars, strings.ToLower(userVar.Name))
	}
	if len(neededVars) > 0 {
		valuesStr := make([]string, 0, len(neededVars))
		for _, v := range neededVars {
			valuesStr = append(valuesStr, strconv.Itoa(v))
		}
		return nil, errors.Errorf(
			"column assignment cannot use variables set outside IMPORT INTO statement, index %s",
			strings.Join(valuesStr, ","),
		)
	}

	tnW := b.resolveCtx.GetTableName(ld.Table)
	if tnW.TableInfo.TempTableType != model.TempTableNone {
		return nil, errors.Errorf("IMPORT INTO does not support temporary table")
	} else if tnW.TableInfo.TableCacheStatusType != model.TableCacheStatusDisable {
		return nil, errors.Errorf("IMPORT INTO does not support cached table")
	}
	p := ImportInto{
		Path:               ld.Path,
		Format:             ld.Format,
		Table:              tnW,
		ColumnAssignments:  ld.ColumnAssignments,
		ColumnsAndUserVars: ld.ColumnsAndUserVars,
		Options:            options,
		Stmt:               ld.Text(),
	}.Init(b.ctx)
	user := b.ctx.GetSessionVars().User
	// IMPORT INTO need full DML privilege of the target table
	// to support add-index by SQL, we need ALTER privilege
	var selectErr, updateErr, insertErr, deleteErr, alterErr error
	if user != nil {
		selectErr = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("SELECT", user.AuthUsername, user.AuthHostname, p.Table.Name.O)
		updateErr = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("UPDATE", user.AuthUsername, user.AuthHostname, p.Table.Name.O)
		insertErr = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("INSERT", user.AuthUsername, user.AuthHostname, p.Table.Name.O)
		deleteErr = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("DELETE", user.AuthUsername, user.AuthHostname, p.Table.Name.O)
		alterErr = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("ALTER", user.AuthUsername, user.AuthHostname, p.Table.Name.O)
	}
	b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SelectPriv, p.Table.Schema.L, p.Table.Name.L, "", selectErr)
	b.visitInfo = appendVisitInfo(b.visitInfo, mysql.UpdatePriv, p.Table.Schema.L, p.Table.Name.L, "", updateErr)
	b.visitInfo = appendVisitInfo(b.visitInfo, mysql.InsertPriv, p.Table.Schema.L, p.Table.Name.L, "", insertErr)
	b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DeletePriv, p.Table.Schema.L, p.Table.Name.L, "", deleteErr)
	b.visitInfo = appendVisitInfo(b.visitInfo, mysql.AlterPriv, p.Table.Schema.L, p.Table.Name.L, "", alterErr)
	if importFromServer {
		b.visitInfo = appendVisitInfo(b.visitInfo, mysql.FilePriv, "", "", "", plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("FILE"))
	}
	tableInfo := p.Table.TableInfo
	// we use the latest IS to support IMPORT INTO dst FROM SELECT * FROM src AS OF TIMESTAMP '2020-01-01 00:00:00'
	// Note: we need to get p.Table when preprocessing, at that time, IS of session
	// transaction is used, if the session ctx is already in snapshot read using tidb_snapshot, we might
	// not get the schema or get a stale schema of the target table, so we don't
	// support set 'tidb_snapshot' first and then import into the target table.
	//
	// tidb_read_staleness can be used to do stale read too, it's allowed as long as
	// TableInfo.ID matches with the latest schema.
	latestIS := b.ctx.GetLatestInfoSchema().(infoschema.InfoSchema)
	tableInPlan, ok := latestIS.TableByID(ctx, tableInfo.ID)
	if !ok {
		// adaptor.handleNoDelayExecutor has a similar check, but we want to give
		// a more specific error message here.
		if b.ctx.GetSessionVars().SnapshotTS != 0 {
			return nil, errors.New("can not execute IMPORT statement when 'tidb_snapshot' is set")
		}
		db := b.ctx.GetSessionVars().CurrentDB
		return nil, infoschema.ErrTableNotExists.FastGenByArgs(db, tableInfo.Name.O)
	}
	schema, names, err := expression.TableInfo2SchemaAndNames(b.ctx.GetExprCtx(), ast.NewCIStr(""), tableInfo)
	if err != nil {
		return nil, err
	}
	mockTablePlan.SetSchema(schema)
	mockTablePlan.SetOutputNames(names)

	p.GenCols, err = b.resolveGeneratedColumns(ctx, tableInPlan.Cols(), nil, mockTablePlan)
	if err != nil {
		return nil, err
	}

	if ld.Select != nil {
		// privilege of tables in select will be checked here
		nodeW := resolve.NewNodeWWithCtx(ld.Select, b.resolveCtx)
		selectPlan, err2 := b.Build(ctx, nodeW)
		if err2 != nil {
			return nil, err2
		}
		// it's allowed to use IMPORT INTO t FROM SELECT * FROM t
		// as we pre-check that the target table must be empty.
		if (len(ld.ColumnsAndUserVars) > 0 && len(selectPlan.Schema().Columns) != len(ld.ColumnsAndUserVars)) ||
			(len(ld.ColumnsAndUserVars) == 0 && len(selectPlan.Schema().Columns) != len(tableInPlan.VisibleCols())) {
			return nil, plannererrors.ErrWrongValueCountOnRow.GenWithStackByArgs(1)
		}
		p.SelectPlan, _, err2 = DoOptimize(ctx, b.ctx, b.optFlag, selectPlan.(base.LogicalPlan))
		if err2 != nil {
			return nil, err2
		}
	} else {
		outputSchema, outputFields := convert2OutputSchemasAndNames(importIntoSchemaNames, ImportIntoSchemaFTypes, []uint{})
		p.SetSchemaAndNames(outputSchema, outputFields)
	}
	return p, nil
}
