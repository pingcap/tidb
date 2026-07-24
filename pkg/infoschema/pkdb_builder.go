// Copyright 2026 PingCAP, Inc.

package infoschema

import (
	"context"
	"strings"
	"sync/atomic"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	parserast "github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	ptypes "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

// BootstrapFinishGlobalVars will be set after bootstrapSessionImpl finish
// setting global variables.
var BootstrapFinishGlobalVars atomic.Bool

// ReloadLoadableFunctions reconciles the process-local loadable UDF registry
// with mysql.func. It returns true only after mysql.func has been successfully
// read and the registry has been reconciled.
func ReloadLoadableFunctions(factory func() (pools.Resource, error)) (bool, error) {
	// The sys session factory is optional. Leave loadable functions unchanged in such cases.
	if factory == nil {
		return false, nil
	}

	res, err := factory()
	if err != nil {
		return false, errors.Trace(err)
	}
	defer res.Close()
	sctx, ok := res.(sessionctx.Context)
	if !ok {
		return false, errors.Errorf("unexpected sys session type %T", res)
	}
	sessIS := sctx.GetDomainInfoSchema()
	if sessIS == nil {
		// During bootstrap/domain initialization the session may not have any usable infoschema yet.
		// Skip loadable function loading in such cases; it will be retried on later infoschema reloads.
		return false, nil
	}
	if ext, ok := sessIS.(*SessionExtendedInfoSchema); ok && ext.InfoSchema == nil {
		// During bootstrap/domain initialization the session may not have any usable infoschema yet.
		// Skip loadable function loading in such cases; it will be retried on later infoschema reloads.
		return false, nil
	}
	if _, err := sessIS.TableInfoByName(pmodel.NewCIStr(mysql.SystemDB), pmodel.NewCIStr("func")); err != nil {
		// During first bootstrap mysql.func may not have been created yet.
		// Skip loadable function loading in such cases; it will be retried on later infoschema reloads.
		return false, nil
	}

	exec := sctx.GetSQLExecutor()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnProcedure)
	rs, err := exec.ExecuteInternal(ctx, "SELECT name, ret, dl FROM mysql.func WHERE type = 'function'")
	if err != nil {
		logutil.BgLogger().Error("failed to execute query for loadable functions", zap.Error(err))
		return false, nil
	}
	if rs == nil {
		return true, nil
	}
	chunkRows, err := sqlexec.DrainRecordSet(ctx, rs, 8)
	closeErr := rs.Close()
	if err != nil {
		logutil.BgLogger().Error("failed to drain record set for loadable functions", zap.Error(err))
		return false, nil
	}
	if closeErr != nil {
		logutil.BgLogger().Error("failed to close record set for loadable functions", zap.Error(closeErr))
		return false, nil
	}

	dbFuncs := make(map[string]struct{}, len(chunkRows))
	for _, row := range chunkRows {
		name := row.GetString(0)
		lowerName := strings.ToLower(name)
		dbFuncs[lowerName] = struct{}{}
		if expression.HasLoadableFunction(lowerName) {
			continue
		}

		ret := row.GetInt64(1)
		dl := row.GetString(2)
		funcDef, err := expression.LoadUDF(dl, lowerName, expression.CastUDFArgTypeIntToEvalType(int(ret)))
		if err != nil {
			logutil.BgLogger().Error("failed to load UDF expression",
				zap.String("soName", dl),
				zap.String("name", lowerName),
				zap.Error(err))
			continue
		}

		exist, err := expression.CreateLoadableFunction(funcDef)
		if err != nil || exist {
			logutil.BgLogger().Error(
				"failed to create loadable function",
				zap.String("name", lowerName),
				zap.Bool("exist", exist),
				zap.Error(err))
			funcDef.Drop()
			continue
		}
	}

	for _, name := range expression.LoadableFunctionNames() {
		if _, ok := dbFuncs[name]; ok {
			continue
		}
		if def, ok := expression.RemoveLoadableFunction(name); ok {
			if def != nil {
				def.Drop()
			}
		}
	}
	return true, nil
}

func (b *Builder) reloadRoutines() error {
	if b.infoSchema.routineMap == nil {
		b.infoSchema.routineMap = make(map[string]map[string]*model.ProcedureInfo)
	}

	// The sys session factory is optional (for example, in some loading paths).
	// Leave the routine cache empty in such cases.
	if b.factory == nil {
		return nil
	}

	res, err := b.factory()
	if err != nil {
		return errors.Trace(err)
	}
	defer res.Close()
	sctx, ok := res.(sessionctx.Context)
	if !ok {
		return errors.Errorf("unexpected sys session type %T", res)
	}
	if !BootstrapFinishGlobalVars.Load() {
		// During bootstrap/domain initialization the session may not have any usable infoschema yet.
		// Skip routine loading in such cases; it will be loaded on later infoschema reloads.
		return nil
	}
	sessIS := sctx.GetInfoSchema()
	if sessIS == nil {
		return nil
	}
	if ext, ok := sessIS.(*SessionExtendedInfoSchema); ok && ext.InfoSchema == nil {
		return nil
	}

	exec := sctx.GetSQLExecutor()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnProcedure)
	sql, args := BuildRoutineMetadataSQL(RoutineMetadataFilter{})
	rs, err := exec.ExecuteInternal(ctx, sql, args...)
	if err != nil {
		b.infoSchema.routineMap = make(map[string]map[string]*model.ProcedureInfo)
		return nil
	}
	if rs == nil {
		return nil
	}
	chunkRows, err := sqlexec.DrainRecordSet(ctx, rs, 1024)
	closeErr := rs.Close()
	if err != nil || closeErr != nil {
		b.infoSchema.routineMap = make(map[string]map[string]*model.ProcedureInfo)
		return nil
	}

	newRoutineMap := make(map[string]map[string]*model.ProcedureInfo)
	for _, row := range chunkRows {
		if row.Len() != 18 {
			continue
		}
		metadata, err := DecodeRoutineMetadataRow(row)
		if err != nil {
			return err
		}

		procInfo := metadata.ProcedureInfo
		procInfo.Created = metadata.Created.String()
		procInfo.LastAltered = metadata.LastAltered.String()

		if procInfo.Type == "FUNCTION" {
			procInfo.RetType = getStoredFuncRetType(procInfo.DefinitionUTF8, procInfo.SQLMode)
		}
		procInfo.State = model.StatePublic

		routines, ok := newRoutineMap[procInfo.Schema.L]
		if !ok {
			routines = make(map[string]*model.ProcedureInfo)
			newRoutineMap[procInfo.Schema.L] = routines
		}
		routines[routineKey(procInfo.Type, procInfo.Name.L)] = procInfo
	}

	b.infoSchema.routineMap = newRoutineMap
	return nil
}

func getStoredFuncRetType(defUTF8, sqlModeStr string) *ptypes.FieldType {
	retType, _, _, err := ParseStoredFunctionDefinition(defUTF8, sqlModeStr)
	if err != nil {
		logutil.BgLogger().Error("failed to parse stored function definition",
			zap.String("definition_utf8", defUTF8),
			zap.String("sql_mode", sqlModeStr),
			zap.Error(err))
		return nil
	}
	return retType
}

// ParseStoredFunctionDefinition parses a stored function definition fragment from mysql.routines.
// The input definition must be in the form "RETURNS <type> <body>" without the leading CREATE FUNCTION header.
func ParseStoredFunctionDefinition(defUTF8, sqlModeStr string) (*ptypes.FieldType, string, string, error) {
	sqlMode, err := mysql.GetSQLMode(sqlModeStr)
	if err != nil {
		return nil, "", "", errors.Trace(err)
	}

	p := parser.New()
	p.SetSQLMode(sqlMode)
	stmt, err := p.ParseOneStmt("create function p() "+defUTF8, "", "")
	if err != nil {
		return nil, "", "", errors.Trace(err)
	}
	createFn, ok := stmt.(*parserast.CreateProcedureInfo)
	if !ok {
		return nil, "", "", errors.Errorf("unexpected statement type %T", stmt)
	}
	if createFn.FunctionInfo.RetType == nil {
		return nil, "", "", errors.New("stored function return type is nil")
	}
	if createFn.ProcedureBody == nil {
		return nil, "", "", errors.New("stored function body is nil")
	}
	bodyText := createFn.ProcedureBody.Text()
	rest, ok := strings.CutPrefix(defUTF8, "RETURNS ")
	if !ok {
		return nil, "", "", errors.New("stored function definition does not start with RETURNS")
	}
	suffix := " " + bodyText
	if !strings.HasSuffix(rest, suffix) {
		return nil, "", "", errors.New("stored function definition does not end with procedure body")
	}
	retTypeText := strings.TrimSpace(strings.TrimSuffix(rest, suffix))
	if retTypeText == "" {
		return nil, "", "", errors.New("stored function return type text is empty")
	}
	return createFn.FunctionInfo.RetType, retTypeText, bodyText, nil
}
