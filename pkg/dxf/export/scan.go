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

package export

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/distsql"
	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression/exprstatic"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/resourcegroup"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/ppcpuusage"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/pingcap/tidb/pkg/util/tiflash"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"github.com/pingcap/tipb/go-tipb"
	tikvstore "github.com/tikv/client-go/v2/kv"
	kvutil "github.com/tikv/client-go/v2/util"
)

// exportColumns returns the stored columns to export, in schema order.
// Virtual generated columns are rejected at submit time.
func exportColumns(tblInfo *model.TableInfo) ([]*model.ColumnInfo, []*types.FieldType) {
	colInfos := make([]*model.ColumnInfo, 0, len(tblInfo.Columns))
	fieldTps := make([]*types.FieldType, 0, len(tblInfo.Columns))
	for _, col := range tblInfo.Columns {
		if col.Hidden || col.State != model.StatePublic {
			continue
		}
		colInfos = append(colInfos, col)
		fieldTps = append(fieldTps, &col.FieldType)
	}
	return colInfos, fieldTps
}

func newExportExprCtx() *exprstatic.ExprContext {
	evalCtx := exprstatic.NewEvalContext(
		exprstatic.WithSQLMode(mysql.ModeNone),
		exprstatic.WithTypeFlags(types.DefaultStmtFlags),
		exprstatic.WithErrLevelMap(stmtctx.DefaultStmtErrLevels),
	)
	return exprstatic.NewExprContext(exprstatic.WithEvalCtx(evalCtx))
}

// newExportDistSQLCtx builds a session-independent DistSQLContext, modeled on
// the reorg one used by add-index backfill.
func newExportDistSQLCtx(kvClient kv.Client) *distsqlctx.DistSQLContext {
	warnHandler := contextutil.NewStaticWarnHandler(0)
	var sqlKiller sqlkiller.SQLKiller
	var execDetails execdetails.SyncExecDetails
	var cpuUsages ppcpuusage.SQLCPUUsages
	return &distsqlctx.DistSQLContext{
		WarnHandler:                          warnHandler,
		Client:                               kvClient,
		EnableChunkRPC:                       true,
		EnabledRateLimitAction:               vardef.DefTiDBEnableRateLimitAction,
		KVVars:                               tikvstore.NewVariables(&sqlKiller.Signal),
		SessionMemTracker:                    memory.NewTracker(memory.LabelForSession, -1),
		Location:                             time.UTC,
		SQLKiller:                            &sqlKiller,
		CPUUsage:                             &cpuUsages,
		ErrCtx:                               errctx.NewContextWithLevels(stmtctx.DefaultStmtErrLevels, warnHandler),
		TiFlashReplicaRead:                   tiflash.GetTiFlashReplicaReadByStr(vardef.DefTiFlashReplicaRead),
		TiFlashMaxThreads:                    vardef.DefTiFlashMaxThreads,
		TiFlashMaxBytesBeforeExternalJoin:    vardef.DefTiFlashMaxBytesBeforeExternalJoin,
		TiFlashMaxBytesBeforeExternalGroupBy: vardef.DefTiFlashMaxBytesBeforeExternalGroupBy,
		TiFlashMaxBytesBeforeExternalSort:    vardef.DefTiFlashMaxBytesBeforeExternalSort,
		TiFlashMaxQueryMemoryPerNode:         vardef.DefTiFlashMemQuotaQueryPerNode,
		TiFlashQuerySpillRatio:               vardef.DefTiFlashQuerySpillRatio,
		TiFlashHashJoinVersion:               vardef.DefTiFlashHashJoinVersion,
		ResourceGroupName:                    resourcegroup.DefaultResourceGroupName,
		ExecDetails:                          &execDetails,
	}
}

func buildExportDAG(exprCtx *exprstatic.ExprContext, distCtx *distsqlctx.DistSQLContext,
	tblInfo *model.TableInfo, physicalID int64, colInfos []*model.ColumnInfo) (*tipb.DAGRequest, error) {
	dagReq := &tipb.DAGRequest{}
	dagReq.TimeZoneName, dagReq.TimeZoneOffset = timeutil.Zone(exprCtx.GetEvalCtx().Location())
	for i := range colInfos {
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(i))
	}
	tblScan := tables.BuildTableScanFromInfos(tblInfo, colInfos, false)
	tblScan.TableId = physicalID
	if err := tables.SetPBColumnsDefaultValue(exprCtx, tblScan.Columns, colInfos); err != nil {
		return nil, err
	}
	dagReq.Executors = []*tipb.Executor{{Tp: tipb.ExecType_TypeTableScan, TblScan: tblScan}}
	distsql.SetEncodeType(distCtx, dagReq)
	return dagReq, nil
}

// buildScan starts a cop table scan over [start, end) at snapshotTS, returning
// rows in handle order.
func buildScan(ctx context.Context, exprCtx *exprstatic.ExprContext, distCtx *distsqlctx.DistSQLContext,
	tblInfo *model.TableInfo, physicalID int64, colInfos []*model.ColumnInfo, fieldTps []*types.FieldType,
	snapshotTS uint64, start, end kv.Key) (distsql.SelectResult, error) {
	dagPB, err := buildExportDAG(exprCtx, distCtx, tblInfo, physicalID, colInfos)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var builder distsql.RequestBuilder
	kvReq, err := builder.
		SetDAGRequest(dagPB).
		SetStartTS(snapshotTS).
		SetKeyRanges([]kv.KeyRange{{StartKey: start, EndKey: end}}).
		SetKeepOrder(true).
		SetFromSessionVars(distCtx).
		SetConcurrency(1).
		Build()
	if err != nil {
		return nil, errors.Trace(err)
	}
	kvReq.RequestSource.RequestSourceInternal = true
	kvReq.RequestSource.RequestSourceType = kvutil.ExplicitTypeDumpling
	kvReq.RequestSource.ExplicitRequestSourceType = kvutil.ExplicitTypeDumpling
	return distsql.Select(ctx, distCtx, kvReq, fieldTps)
}
