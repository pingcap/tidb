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

package server

import (
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
)

func getStmtIDByRawSQL(tc *TiDBContext, rawSQL string) (stmtID uint32, existed bool) {
	return tc.GetStmtID(rawSQL)
}

// GetStatement4GeneralSQL ...
func GetStatement4GeneralSQL(tc *TiDBContext, sctx sessionctx.Context, originalSQL string) (stmt PreparedStatement, args []types.Datum, existed bool, err error) {
	if !sctx.GetSessionVars().EnableGeneralPlanCache {
		return nil, nil, false, nil
	}

	rawSQL, params, ok := plannercore.FastLexer(originalSQL)
	if !ok {
		return nil, nil, false, nil
	}

	stmtID, existed := getStmtIDByRawSQL(tc, rawSQL)
	if !existed {
		// Use TiDBContext.Prepare or session.PrepareStmt
		stmt, _, _, err := tc.Prepare(rawSQL)
		if err != nil {
			return nil, nil, false, err
		}
		// stmtID, _, _, err = handler.PrepareStmt(rawSQL)
		stmtID = uint32(stmt.ID())
		tc.SetStmtID(rawSQL, stmtID)
	}

	stmt = tc.GetStatement(int(stmtID))
	if stmt == nil {
		return nil, nil, false, err
	}
	return stmt, params, true, err
}
