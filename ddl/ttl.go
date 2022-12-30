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
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/dbterror"
)

func onTTLInfoRemove(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	tblInfo.TTLInfo = nil
	ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func onTTLInfoChange(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	// at least one for them is not nil
	var ttlInfo *model.TTLInfo
	var ttlInfoEnable *bool

	if err := job.DecodeArgs(&ttlInfo, &ttlInfoEnable); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	if ttlInfo != nil {
		// if the TTL_ENABLE is not set explicitly, use the original value
		if ttlInfoEnable == nil && tblInfo.TTLInfo != nil {
			ttlInfo.Enable = tblInfo.TTLInfo.Enable
		}
		tblInfo.TTLInfo = ttlInfo
	}
	if ttlInfoEnable != nil {
		if tblInfo.TTLInfo == nil {
			return ver, errors.Trace(dbterror.ErrSetTTLEnableForNonTTLTable)
		}

		tblInfo.TTLInfo.Enable = *ttlInfoEnable
	}

	ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func checkTTLInfoValid(ctx sessionctx.Context, schema model.CIStr, tblInfo *model.TableInfo) error {
	if err := checkTTLIntervalExpr(ctx, tblInfo.TTLInfo); err != nil {
		return err
	}

	if err := checkTTLTableSuitable(ctx, schema, tblInfo); err != nil {
		return err
	}

	return checkTTLInfoColumnType(tblInfo)
}

func checkTTLIntervalExpr(ctx sessionctx.Context, ttlInfo *model.TTLInfo) error {
	// FIXME: use a better way to validate the interval expression in ttl
	var nowAddIntervalExpr ast.ExprNode

	unit := ast.TimeUnitType(ttlInfo.IntervalTimeUnit)
	expr := fmt.Sprintf("select NOW() + INTERVAL %s %s", ttlInfo.IntervalExprStr, unit.String())
	stmts, _, err := parser.New().ParseSQL(expr)
	if err != nil {
		// FIXME: the error information can be wrong, as it could indicate an unknown position to user.
		return errors.Trace(err)
	}
	nowAddIntervalExpr = stmts[0].(*ast.SelectStmt).Fields.Fields[0].Expr
	_, err = expression.EvalAstExpr(ctx, nowAddIntervalExpr)
	return err
}

func checkTTLInfoColumnType(tblInfo *model.TableInfo) error {
	colInfo := findColumnByName(tblInfo.TTLInfo.ColumnName.L, tblInfo)
	if colInfo == nil {
		return dbterror.ErrBadField.GenWithStackByArgs(tblInfo.TTLInfo.ColumnName.O, "TTL config")
	}
	if !types.IsTypeTime(colInfo.FieldType.GetType()) {
		return dbterror.ErrUnsupportedColumnInTTLConfig.GenWithStackByArgs(tblInfo.TTLInfo.ColumnName.O)
	}

	return nil
}

// checkTTLTableSuitable returns whether this table is suitable to be a TTL table
// A temporary table or a parent table referenced by a foreign key cannot be TTL table
func checkTTLTableSuitable(ctx sessionctx.Context, schema model.CIStr, tblInfo *model.TableInfo) error {
	if tblInfo.TempTableType != model.TempTableNone {
		return dbterror.ErrTempTableNotAllowedWithTTL
	}

	// checks even when the foreign key check is not enabled, to keep safe
	is := sessiontxn.GetTxnManager(ctx).GetTxnInfoSchema()
	if referredFK := checkTableHasForeignKeyReferred(is, schema.L, tblInfo.Name.L, nil, true); referredFK != nil {
		return dbterror.ErrUnsupportedTTLReferencedByFK
	}

	return nil
}

func checkDropColumnWithTTLConfig(tblInfo *model.TableInfo, colName string) error {
	if tblInfo.TTLInfo != nil {
		if tblInfo.TTLInfo.ColumnName.L == colName {
			return dbterror.ErrTTLColumnCannotDrop.GenWithStackByArgs(colName)
		}
	}

	return nil
}

// getTTLInfoInOptions returns the aggregated ttlInfo, the ttlEnable, or an error.
// if TTL or TTL_ENABLE is not set in the config, the corresponding return value will be nil.
// if both of them are set, the `ttlInfo.Enable` will be equal with `ttlEnable`.
func getTTLInfoInOptions(options []*ast.TableOption) (ttlInfo *model.TTLInfo, ttlEnable *bool, err error) {
	for _, op := range options {
		switch op.Tp {
		case ast.TableOptionTTL:
			var sb strings.Builder
			restoreFlags := format.RestoreStringSingleQuotes | format.RestoreNameBackQuotes
			restoreCtx := format.NewRestoreCtx(restoreFlags, &sb)
			err := op.Value.Restore(restoreCtx)
			if err != nil {
				return nil, nil, err
			}

			intervalExpr := sb.String()
			ttlInfo = &model.TTLInfo{
				ColumnName:       op.ColumnName.Name,
				IntervalExprStr:  intervalExpr,
				IntervalTimeUnit: int(op.TimeUnitValue.Unit),
				Enable:           true,
			}
		case ast.TableOptionTTLEnable:
			ttlEnable = &op.BoolValue
		}
	}

	if ttlInfo != nil && ttlEnable != nil {
		ttlInfo.Enable = *ttlEnable
	}
	return ttlInfo, ttlEnable, nil
}
