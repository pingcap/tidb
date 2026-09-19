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

package executor

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/infoschema"
	infoschemactx "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/plannersession"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/util/hint"
)

// The planner uses the submitted source schema for this query attempt.
type importQuerySession struct {
	sessionctx.Context
	*plannersession.PlanCtxExtended
	schema infoschema.InfoSchema
}

func (c *importQuerySession) GetLatestInfoSchema() infoschemactx.MetaOnlyInfoSchema {
	return c.schema
}

func (c *importQuerySession) GetLatestISWithoutSessExt() infoschemactx.MetaOnlyInfoSchema {
	return c.schema
}

func (c *importQuerySession) GetInfoSchema() infoschemactx.MetaOnlyInfoSchema {
	return c.schema
}

func (c *importQuerySession) GetPlanCtx() base.PlanContext { return c }

// newImportQuerySession prepares and wraps the caller-owned session for one query attempt.
func newImportQuerySession(
	ctx context.Context, sctx sessionctx.Context,
	q *importer.QueryPlan, sql string, memoryLimit int64,
) (*importQuerySession, ast.StmtNode, error) {
	if sctx.GetStore().GetKeyspace() != q.Keyspace {
		return nil, nil, errors.New("import query runtime keyspace mismatch")
	}
	vars := sctx.GetSessionVars()
	if err := q.InitSessionVars(vars, memoryLimit); err != nil {
		return nil, nil, err
	}
	is, err := q.BuildInfoSchema(sctx.GetLatestInfoSchema().(infoschema.InfoSchema))
	if err != nil {
		return nil, nil, err
	}
	node, err := parseImportQuery(sctx, sql)
	if err != nil {
		return nil, nil, err
	}
	ast.Walk(node, importQueryMemoryHint(memoryLimit))
	if err := ResetContextOfStmt(sctx, node); err != nil {
		return nil, nil, err
	}
	if err := sessiontxn.GetTxnManager(sctx).EnterNewTxn(
		ctx, &sessiontxn.EnterNewTxnRequest{Type: sessiontxn.EnterNewTxnBeforeStmt},
	); err != nil {
		return nil, nil, err
	}
	vars.SnapshotTS = q.ReadTS
	vars.SnapshotInfoschema = is
	vars.StmtCtx.InitFromPBFlagAndTz(q.PushDownFlags, vars.Location())
	// Publish the snapshot before compilation; cross-keyspace GC reporting reads
	// ProcessInfo, and the lazy transaction may not yet have a start TS.
	if pi, ok := sctx.(processinfoSetter); ok {
		pi.SetProcessInfo(node.Text(), time.Now(), mysql.ComQuery, 0)
	}
	querySession := &importQuerySession{Context: sctx, schema: is}
	querySession.PlanCtxExtended = plannersession.NewPlanCtxExtended(querySession)
	return querySession, node, nil
}

// importQueryMemoryHint keeps the worker resource budget while preserving query hints.
type importQueryMemoryHint int64

func (limit importQueryMemoryHint) Enter(node ast.Node) bool {
	if h, ok := node.(*ast.TableOptimizerHint); ok {
		switch h.HintName.L {
		case hint.HintMemoryQuota:
			h.HintData = int64(limit)
		case "set_var":
			setting := h.HintData.(ast.HintSetVar)
			if strings.EqualFold(setting.VarName, vardef.TiDBMemQuotaQuery) {
				setting.Value = strconv.FormatInt(int64(limit), 10)
				h.HintData = setting
			}
		}
	}
	return false
}

func (importQueryMemoryHint) Leave(ast.Node) bool { return true }
