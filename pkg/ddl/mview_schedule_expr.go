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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

// buildAndValidateMViewScheduleExpr restores an AST expression into canonical SQL and
// validates that its expression type is DATETIME/TIMESTAMP.
func buildAndValidateMViewScheduleExpr(sctx sessionctx.Context, expr ast.ExprNode, clause string) (string, error) {
	exprSQL, err := restoreExprToCanonicalSQL(expr)
	if err != nil {
		return "", err
	}

	internalCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	_, resultFields, err := sctx.GetRestrictedSQLExecutor().ExecRestrictedSQL(
		internalCtx,
		nil,
		"SELECT ("+exprSQL+") LIMIT 0",
	)
	if err != nil {
		return "", errors.Trace(err)
	}
	if len(resultFields) != 1 || resultFields[0] == nil || resultFields[0].Column == nil {
		return "", errors.Errorf("failed to infer expression type for %s", clause)
	}

	tp := resultFields[0].Column.GetType()
	if tp != mysql.TypeDatetime && tp != mysql.TypeTimestamp {
		return "", dbterror.ErrGeneralUnsupportedDDL.GenWithStack(
			fmt.Sprintf("%s expression must return DATETIME/TIMESTAMP, but got %s", clause, types.TypeStr(tp)),
		)
	}
	return exprSQL, nil
}
