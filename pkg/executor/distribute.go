// Copyright 2025 PingCAP, Inc.
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

	pdhttp "github.com/tikv/pd/client/http"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/chunk"

)

// DistributeTableExec represents a distribute table  executor.
type DistributeTableExec struct {
	exec.BaseExecutor

	tableInfo      *model.TableInfo
	partitionNames []ast.CIStr
	rule           ast.CIStr
	engine         ast.CIStr
	jobID          uint64

	keyranges      []pdhttp.KeyRange
}

// Open implements the Executor Open interface.
func (e *DistributeTableExec) Open(context.Context) error {
	e.keyranges,err:=e.getKeyrange()
	return err
}

// Next implements the Executor Next interface.
func (e *DistributeTableExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()

	jobID, err := e.distributeTable(ctx)
	if err != nil {
		return err
	}
	chk.AppendInt64(0, jobID)
	return nil
}

func (e *DistributeTableExec) distributeTable(ctx context.Context) (uint64,error) {
	return 0
}

func (e *DistributeTableExec) getKeyrange()([]*pdhttp.KeyRange,error){
	tb, err := e.getTable()
	if err != nil {
		return err
	}
	physicalIDs := []int64{}
	if pi := tb.Meta().GetPartitionInfo(); pi != nil {
		for _, name := range e.Table.PartitionNames {
			pid, err := tables.FindPartitionByName(tb.Meta(), name.L)
			if err != nil {
				return err
			}
			physicalIDs = append(physicalIDs, pid)
		}
		if len(physicalIDs) == 0 {
			for _, p := range pi.Definitions {
				physicalIDs = append(physicalIDs, p.ID)
			}
		}
	} else {
		if len(e.Table.PartitionNames) != 0 {
			return plannererrors.ErrPartitionClauseOnNonpartitioned
		}
		physicalIDs = append(physicalIDs, tb.Meta().ID)
	}
	ranges:=make([]*pdhttp.KeyRange, len(physicalIDs))
	for _,id := range physicalIDs {
		startKey, endKey := tablecodec.GetTableHandleKeyRange(pid)
		ranges = append(ranges, &pdhttp.NewKeyRange(startKey,endKey))
	}
	return ranges, nil
}
