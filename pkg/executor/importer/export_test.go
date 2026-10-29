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

package importer

import (
	"context"

	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/sessionctx"
)

// PlanImportQueryForTest plans the SELECT of the IMPORT INTO statement sql in
// sctx like the statement does, so that CaptureQuerySource can capture it.
func PlanImportQueryForTest(sctx sessionctx.Context, sql string) error {
	stmt, node, err := ParseImportQuery(sctx, sql)
	if err != nil {
		return err
	}
	// Each statement starts with no related tables.
	clear(sctx.GetSessionVars().StmtCtx.RelatedTableIDs)
	// Preprocess the IMPORT INTO statement as it is, then plan its SELECT.
	stmtW := resolve.NewNodeW(stmt)
	ret := &plannercore.PreprocessorReturn{}
	if err := plannercore.Preprocess(context.Background(), sctx, stmtW,
		plannercore.WithPreprocessorReturn(ret), plannercore.InitTxnContextProvider); err != nil {
		return err
	}
	_, err = plannercore.BuildLogicalPlanForTest(context.Background(), sctx,
		resolve.NewNodeWWithCtx(node, stmtW.GetResolveContext()), ret.InfoSchema)
	return err
}
