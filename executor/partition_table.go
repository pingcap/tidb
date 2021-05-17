// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tipb/go-tipb"
)

func updateExecutorTableID(ctx context.Context, exec *tipb.Executor, partitionID int64, recursive bool) error {
	var child *tipb.Executor
	switch exec.Tp {
	case tipb.ExecType_TypeTableScan:
		exec.TblScan.TableId = partitionID
		// For test coverage.
		if tmp := ctx.Value("nextPartitionUpdateDAGReq"); tmp != nil {
			m := tmp.(map[int64]struct{})
			m[partitionID] = struct{}{}
		}
	case tipb.ExecType_TypeIndexScan:
		exec.IdxScan.TableId = partitionID
	case tipb.ExecType_TypeSelection:
		child = exec.Selection.Child
	case tipb.ExecType_TypeAggregation, tipb.ExecType_TypeStreamAgg:
		child = exec.Aggregation.Child
	case tipb.ExecType_TypeTopN:
		child = exec.TopN.Child
	case tipb.ExecType_TypeLimit:
		child = exec.Limit.Child
	case tipb.ExecType_TypeExchangeSender:
		child = exec.ExchangeSender.Child
	case tipb.ExecType_TypeExchangeReceiver:
		child = nil
	case tipb.ExecType_TypeJoin:
		child = exec.Join.Children[1-exec.Join.InnerIdx]
	case tipb.ExecType_TypeProjection:
		child = exec.Projection.Child
	default:
		return errors.Trace(fmt.Errorf("unknown new tipb protocol %d", exec.Tp))
	}
	if child != nil && recursive {
		return updateExecutorTableID(ctx, child, partitionID, recursive)
	}
	return nil
}
