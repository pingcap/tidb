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

package core

// Will cause the import cycle, so implement this file in the server package. This file will be removed in the future.

import (
	"context"

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/sqlexec"
)

//func parseRawArgs(sctx sessionctx.Context, rawArgs []string) ([]types.Datum, error) {
//	// TODO
//	return nil, nil
//}

func getStmtIDByRawSQL(sctx sessionctx.Context, rawSQL string) (stmtID uint32, existed bool) {
	// TODO
	return 0, false
}

type prepExecHandler interface {
	// PrepareStmt executes prepare statement in binary protocol.
	PrepareStmt(sql string) (stmtID uint32, paramCount int, fields []*ast.ResultField, err error)
	// ExecutePreparedStmt executes a prepared statement.
	ExecutePreparedStmt(ctx context.Context, stmtID uint32, param []types.Datum) (sqlexec.RecordSet, error)
}

func getPlanFromCache(ctx context.Context, sctx sessionctx.Context, handler prepExecHandler, originalSQL string) (rs sqlexec.RecordSet, existed bool, err error) {
	if !sctx.GetSessionVars().EnableGeneralPlanCache {
		return nil, false, nil
	}

	rawSQL, params, ok := FastLexer(originalSQL)
	if !ok {
		return nil, false, nil
	}

	stmtID, existed := getStmtIDByRawSQL(sctx, rawSQL)
	if !existed {
		stmtID, _, _, err = handler.PrepareStmt(rawSQL)
		if err != nil {
			return nil, false, err
		}
	}

	//params, err := parseRawArgs(sctx, rawArgs)
	//if err != nil {
	//	return nil, false, err
	//}

	rs, err = handler.ExecutePreparedStmt(ctx, stmtID, params)
	if err != nil {
		return nil, false, err
	}

	return rs, true, nil
}
