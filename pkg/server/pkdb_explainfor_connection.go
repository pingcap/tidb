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

package server

import (
	"context"
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/distsql"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/globalconn"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"github.com/pingcap/tipb/go-tipb"
)

// GetProcessInfoForExplain implements util.ExplainProcessInfoFetcher.
func (s *Server) GetProcessInfoForExplain(ctx context.Context, execCtx util.ExplainProcessInfoContext, id uint64) (*util.ProcessInfo, error) {
	// Hit local sessions first to avoid cross-node requests on the common path.
	if pi, ok := s.GetProcessInfo(id); ok {
		return pi, nil
	}

	gcid, isTruncated, err := globalconn.ParseConnID(id)
	if err != nil {
		return nil, errors.Annotate(err, "parse connection id failed")
	}
	if isTruncated {
		return nil, errors.New("received a 32bits truncated ConnectionID, expect 64bits")
	}
	if gcid.ServerID == s.ServerID() {
		return nil, nil
	}

	return getRemoteProcessInfoForExplain(ctx, execCtx, gcid.ServerID, id)
}

func getRemoteProcessInfoForExplain(ctx context.Context, execCtx util.ExplainProcessInfoContext, serverID, connectionID uint64) (*util.ProcessInfo, error) {
	explainExec := &tipb.Executor{
		Tp: tipb.ExecType_TypeExplainForConnection,
		ExplainForConnection: &tipb.ExplainForConnection{
			ConnectionId: connectionID,
		},
	}

	dagReq := &tipb.DAGRequest{}
	dagReq.TimeZoneName, dagReq.TimeZoneOffset = timeutil.Zone(execCtx.GetDistSQLCtx().Location)
	dagReq.Executors = []*tipb.Executor{explainExec}
	dagReq.OutputOffsets = []uint32{0, 1, 2}
	distsql.SetEncodeType(execCtx.GetDistSQLCtx(), dagReq)

	var builder distsql.RequestBuilder
	kvReq, err := builder.
		SetDAGRequest(dagReq).
		SetFromSessionVars(execCtx.GetDistSQLCtx()).
		SetFromInfoSchema(execCtx.GetInfoSchema()).
		SetStoreType(kv.TiDB).
		SetTiDBServerID(serverID).
		SetStartTS(math.MaxUint64). // To make check visibility success.
		Build()
	if err != nil {
		return nil, err
	}

	fieldTypes := []*types.FieldType{
		types.NewFieldType(mysql.TypeVarchar),
		types.NewFieldType(mysql.TypeVarchar),
		types.NewFieldType(mysql.TypeString),
	}
	result, err := distsql.Select(ctx, execCtx.GetDistSQLCtx(), kvReq, fieldTypes)
	if err != nil {
		return nil, errors.Annotatef(err, "send explain for connection request to tidb server %d failed", serverID)
	}
	defer func() {
		_ = result.Close()
	}()

	chk := chunk.New(fieldTypes, 1, 1)
	if err := result.Next(ctx, chk); err != nil {
		return nil, errors.Annotatef(err, "read explain for connection result from tidb server %d failed", serverID)
	}
	if chk.NumRows() == 0 {
		return nil, nil
	}

	row := chk.GetRow(0)
	pi := &util.ProcessInfo{ID: connectionID}
	if !row.IsNull(0) {
		pi.User = row.GetString(0)
	}
	if !row.IsNull(1) {
		pi.Host = row.GetString(1)
	}
	if !row.IsNull(2) {
		pi.BriefBinaryPlan = row.GetString(2)
	}
	return pi, nil
}
