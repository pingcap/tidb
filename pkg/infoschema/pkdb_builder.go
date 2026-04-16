// Copyright 2026 PingCAP, Inc.

package infoschema

import (
	"context"
	"fmt"

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
	sqlMode, err := mysql.GetSQLMode(sqlModeStr)
	if err != nil {
		logutil.BgLogger().Error("failed to parse SQL mode from string",
			zap.String("sql_mode", sqlModeStr),
			zap.Error(err))
		return nil
	}

	p := parser.New()
	p.SetSQLMode(sqlMode)
	createFnSQL := "create function p() " + defUTF8
	stmt, err := p.ParseOneStmt(createFnSQL, "", "")
	if err != nil {
		logutil.BgLogger().Error("failed to parse create function statement",
			zap.Error(err))
		return nil
	}
	createFn, ok := stmt.(*parserast.CreateProcedureInfo)
	if !ok || createFn.FunctionInfo.RetType == nil {
		logutil.BgLogger().Error("failed to parse create function ret type",
			zap.String("statementType", fmt.Sprintf("%T", stmt)),
			zap.Bool("retTypeIsNil", ok && createFn.FunctionInfo.RetType == nil))
		return nil
	}
	return createFn.FunctionInfo.RetType
}
