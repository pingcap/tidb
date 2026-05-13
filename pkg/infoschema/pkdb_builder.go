// Copyright 2026 PingCAP, Inc.

package infoschema

import (
	"context"
	"strings"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	parserast "github.com/pingcap/tidb/pkg/parser/ast"
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
