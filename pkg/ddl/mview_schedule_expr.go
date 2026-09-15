// Copyright 2026 PingCAP, Inc.
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
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/generatedexpr"
	"go.uber.org/zap"
)

// BuildAndValidateMViewScheduleExpr restores an AST expression into canonical SQL and
// validates that its expression type is DATETIME/TIMESTAMP.
func BuildAndValidateMViewScheduleExpr(sctx sessionctx.Context, expr ast.ExprNode, clause string) (string, error) {
	exprSQL, err := restoreNodeToCanonicalSQL(expr)
	if err != nil {
		return "", err
	}

	builtExpr, err := expression.BuildSimpleExpr(sctx.GetExprCtx(), expr)
	if err != nil {
		return "", errors.Trace(err)
	}

	ft := builtExpr.GetType(sctx.GetExprCtx().GetEvalCtx())
	if ft == nil {
		return "", errors.Errorf("failed to infer expression type for %s", clause)
	}

	tp := ft.GetType()
	if tp != mysql.TypeDatetime && tp != mysql.TypeTimestamp {
		return "", dbterror.ErrGeneralUnsupportedDDL.GenWithStack(
			fmt.Sprintf("%s expression must return DATETIME/TIMESTAMP, but got %s", clause, types.TypeStr(tp)),
		)
	}
	return exprSQL, nil
}

func deriveCreateMaterializedScheduleNextUnixSeconds(
	ctx context.Context,
	ddlSess *sess.Session,
	schemaName string,
	tableName string,
	startExpr string,
	nextExpr string,
	scheduleTimeZone *time.Location,
	logNullUpdate func(schemaName string, tableName string, nullExprClause string, startExpr string, nextExpr string),
) (nextUnixSeconds *int64, shouldUpdate bool, err error) {
	// shouldUpdate reports whether the persisted NEXT_* value should be overwritten.
	startExpr = strings.TrimSpace(startExpr)
	nextExpr = strings.TrimSpace(nextExpr)
	if startExpr == "" && nextExpr == "" {
		return nil, true, nil
	}

	nowTime, err := loadCreateMaterializedViewScheduleNow(ctx, ddlSess)
	if err != nil {
		return nil, false, errors.Trace(err)
	}

	// START WITH takes precedence unless it is near now and NEXT is present.
	if startExpr != "" {
		startAt, err := evalCreateMaterializedViewScheduleExprToDatetime(ddlSess, startExpr)
		if err != nil {
			return nil, false, errors.Trace(err)
		}
		if startAt == nil {
			logNullUpdate(schemaName, tableName, "START WITH", startExpr, nextExpr)
			return nil, true, nil
		}
		if nextExpr == "" {
			nextUnixSeconds, err := expression.MaterializedScheduleTimeToUnixSeconds(startAt, scheduleTimeZone)
			return nextUnixSeconds, true, errors.Trace(err)
		}

		goNow, err := nowTime.GoTime(scheduleTimeZone)
		if err != nil {
			return nil, false, errors.Trace(err)
		}
		nearNowThreshold := types.NewTime(types.FromGoTime(goNow.Add(10*time.Second)), nowTime.Type(), nowTime.Fsp())
		if startAt.Compare(nearNowThreshold) < 0 {
			nextAt, err := evalCreateMaterializedViewScheduleExprToDatetime(ddlSess, nextExpr)
			if err != nil {
				return nil, false, errors.Trace(err)
			}
			if nextAt == nil {
				logNullUpdate(schemaName, tableName, "NEXT", startExpr, nextExpr)
				return nil, true, nil
			}
			nextUnixSeconds, err := expression.MaterializedScheduleTimeToUnixSeconds(nextAt, scheduleTimeZone)
			return nextUnixSeconds, true, errors.Trace(err)
		}
		nextUnixSeconds, err := expression.MaterializedScheduleTimeToUnixSeconds(startAt, scheduleTimeZone)
		return nextUnixSeconds, true, errors.Trace(err)
	}

	if nextExpr != "" {
		nextAt, err := evalCreateMaterializedViewScheduleExprToDatetime(ddlSess, nextExpr)
		if err != nil {
			return nil, false, errors.Trace(err)
		}
		if nextAt == nil {
			logNullUpdate(schemaName, tableName, "NEXT", startExpr, nextExpr)
			return nil, true, nil
		}
		nextUnixSeconds, err := expression.MaterializedScheduleTimeToUnixSeconds(nextAt, scheduleTimeZone)
		return nextUnixSeconds, true, errors.Trace(err)
	}
	return
}

func logCreateMaterializedViewNextUnixSecondsUpdateNull(
	mviewSchemaName string,
	mvTableName string,
	nullExprClause string,
	startExpr string,
	nextExpr string,
) {
	if strings.TrimSpace(nextExpr) != "" {
		logutil.DDLLogger().Error(
			"create materialized view: automatic refresh schedule disabled because schedule expression evaluated to NULL, updating NEXT_REFRESH_UNIX_SECONDS to NULL",
			zap.String("schemaName", mviewSchemaName),
			zap.String("tableName", mvTableName),
			zap.String("nullExprClause", nullExprClause),
			zap.String("refreshStartWith", startExpr),
			zap.String("refreshNext", nextExpr),
		)
		return
	}
	logutil.DDLLogger().Warn(
		"create materialized view: schedule expression evaluated to NULL, updating NEXT_REFRESH_UNIX_SECONDS to NULL",
		zap.String("schemaName", mviewSchemaName),
		zap.String("tableName", mvTableName),
		zap.String("nullExprClause", nullExprClause),
		zap.String("refreshStartWith", startExpr),
		zap.String("refreshNext", nextExpr),
	)
}

func logCreateMaterializedViewLogNextUnixSecondsUpdateNull(
	mlogSchemaName string,
	mlogTableName string,
	nullExprClause string,
	startExpr string,
	nextExpr string,
) {
	if strings.TrimSpace(nextExpr) != "" {
		logutil.DDLLogger().Error(
			"create materialized view log: automatic purge schedule disabled because schedule expression evaluated to NULL, updating NEXT_PURGE_UNIX_SECONDS to NULL",
			zap.String("schemaName", mlogSchemaName),
			zap.String("tableName", mlogTableName),
			zap.String("nullExprClause", nullExprClause),
			zap.String("purgeStartWith", startExpr),
			zap.String("purgeNext", nextExpr),
		)
		return
	}
	logutil.DDLLogger().Warn(
		"create materialized view log: purge schedule expression evaluated to NULL, updating NEXT_PURGE_UNIX_SECONDS to NULL",
		zap.String("schemaName", mlogSchemaName),
		zap.String("tableName", mlogTableName),
		zap.String("nullExprClause", nullExprClause),
		zap.String("purgeStartWith", startExpr),
		zap.String("purgeNext", nextExpr),
	)
}

func setCreateMaterializedViewScheduleEvalSession(
	sctx sessionctx.Context,
	sqlMode mysql.SQLMode,
	scheduleTimeZone *time.Location,
) func() {
	sessVars := sctx.GetSessionVars() //nolint:forbidigo
	originalSQLMode := sessVars.SQLMode
	originalTypeFlags := sessVars.StmtCtx.TypeFlags()
	originalErrLevels := sessVars.StmtCtx.ErrLevels()

	var originalTZ *time.Location
	if sessVars.TimeZone != nil {
		tz := *sessVars.TimeZone
		originalTZ = &tz
	}
	originalStmtTZ := sessVars.StmtCtx.TimeZone()

	sessVars.SQLMode = sqlMode
	sessVars.StmtCtx.SetTypeFlags(expression.MaterializedScheduleTypeFlagsWithSQLMode(sqlMode))
	sessVars.StmtCtx.SetErrLevels(expression.MaterializedScheduleErrLevelsWithSQLMode(sqlMode))
	sessVars.TimeZone = scheduleTimeZone
	sessVars.StmtCtx.SetTimeZone(scheduleTimeZone)

	return func() {
		sessVars.SQLMode = originalSQLMode
		sessVars.StmtCtx.SetErrLevels(originalErrLevels)
		sessVars.StmtCtx.SetTypeFlags(originalTypeFlags)
		sessVars.TimeZone = originalTZ
		if originalStmtTZ != nil {
			sessVars.StmtCtx.SetTimeZone(originalStmtTZ)
			return
		}
		sessVars.StmtCtx.SetTimeZone(sessVars.Location())
	}
}

func loadCreateMaterializedViewScheduleNow(ctx context.Context, ddlSess *sess.Session) (types.Time, error) {
	rows, err := ddlSess.Execute(ctx, "SELECT NOW(6)", "mview-refresh-info-next-time-now")
	if err != nil {
		return types.ZeroTime, errors.Trace(err)
	}
	if len(rows) != 1 || rows[0].IsNull(0) {
		return types.ZeroTime, dbterror.ErrInvalidDDLJob.GenWithStackByArgs("create materialized view: failed to evaluate refresh schedule expression")
	}
	return rows[0].GetTime(0), nil
}

func evalCreateMaterializedViewScheduleExprToDatetime(ddlSess *sess.Session, exprSQL string) (*types.Time, error) {
	exprNode, err := generatedexpr.ParseExpression(exprSQL)
	if err != nil {
		return nil, errors.Trace(err)
	}
	builtExpr, err := expression.BuildSimpleExpr(ddlSess.Session().GetExprCtx(), exprNode)
	if err != nil {
		return nil, errors.Trace(err)
	}

	evalCtx := ddlSess.Session().GetExprCtx().GetEvalCtx()
	v, err := builtExpr.Eval(evalCtx, chunk.Row{})
	if err != nil {
		return nil, errors.Trace(err)
	}
	if v.IsNull() {
		return nil, nil
	}

	targetTp := types.NewFieldType(mysql.TypeDatetime)
	targetTp.SetDecimal(types.MaxFsp)
	datetimeV, err := v.ConvertTo(evalCtx.TypeCtx(), targetTp)
	if err != nil {
		return nil, errors.Trace(err)
	}
	t := datetimeV.GetMysqlTime()
	return &t, nil
}

func deriveCreateMaterializedViewNextUnixSeconds(
	ctx context.Context,
	ddlSess *sess.Session,
	mviewSchemaName string,
	mvTableName string,
	mviewInfo *model.MaterializedViewInfo,
) (*int64, bool, error) {
	if mviewInfo == nil {
		return nil, false, nil
	}
	tz, err := mviewInfo.RefreshScheduleTimeZone.GetLocation()
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	return deriveCreateMaterializedScheduleNextUnixSeconds(ctx, ddlSess, mviewSchemaName, mvTableName, mviewInfo.RefreshStartWith, mviewInfo.RefreshNext, tz, logCreateMaterializedViewNextUnixSecondsUpdateNull)
}

func deriveCreateMaterializedViewLogNextUnixSeconds(
	ctx context.Context,
	ddlSess *sess.Session,
	mlogSchemaName string,
	mlogTableName string,
	mlogInfo *model.MaterializedViewLogInfo,
) (*int64, bool, error) {
	if mlogInfo == nil {
		return nil, false, nil
	}
	tz, err := mlogInfo.PurgeScheduleTimeZone.GetLocation()
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	return deriveCreateMaterializedScheduleNextUnixSeconds(ctx, ddlSess, mlogSchemaName, mlogTableName, mlogInfo.PurgeStartWith, mlogInfo.PurgeNext, tz, logCreateMaterializedViewLogNextUnixSecondsUpdateNull)
}
