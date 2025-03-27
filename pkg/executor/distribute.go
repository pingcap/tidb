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
	"strings"

	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/chunk"
	pdhttp "github.com/tikv/pd/client/http"
)

var schedulerName = "balance-range-scheduler"

// DistributeTableExec represents a distribute table  executor.
type DistributeTableExec struct {
	exec.BaseExecutor

	tableInfo      *model.TableInfo
	is             infoschema.InfoSchema
	partitionNames []ast.CIStr
	rule           ast.CIStr
	engine         ast.CIStr
	jobID          uint64

	done      bool
	keyRanges []*pdhttp.KeyRange
}

// Open implements the Executor Open interface.
func (e *DistributeTableExec) Open(context.Context) error {
	ranges, err := e.getKeyRanges()
	if err != nil {
		return err
	}
	e.keyRanges = ranges
	return nil
}

// Next implements the Executor Next interface.
func (e *DistributeTableExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.done {
		return nil
	}
	e.done = true
	err := e.distributeTable(ctx)
	if err != nil {
		return err
	}
	jobs, err := infosync.GetSchedulerConfig(ctx, schedulerName)
	if err != nil {
		return err
	}
	alias := e.getAlias()
	for _, job := range jobs {
		if job["alias"] == alias && job["engine"] == e.engine.String() && job["rule"] == e.rule.String() {
			chk.AppendUint64(0, job["job-id"].(uint64))
		}
	}
	return nil
}

func (e *DistributeTableExec) distributeTable(ctx context.Context) error {
	input := make(map[string]any)
	input["alias"] = e.getAlias()
	input["engine"] = e.engine.String()
	input["rule"] = e.rule.String()
	input["start-key"] = e.keyRanges[0].StartKey
	input["end-key"] = e.keyRanges[0].EndKey
	return infosync.CreateSchedulerConfigWithInput(ctx, schedulerName, input)
}

func (e *DistributeTableExec) getAlias() string {
	partitionStr := ""
	if len(e.partitionNames) != 0 {
		partitionStr = "partition("
		for idx, partition := range e.partitionNames {
			partitionStr += partition.String()
			if idx != len(e.partitionNames)-1 {
				partitionStr += ","
			}
		}
		partitionStr += ")"
	}
	dbName := getSchemaName(e.is, e.tableInfo.DBID)
	return strings.Join([]string{dbName, e.tableInfo.Name.String(), partitionStr}, ".")
}

func (e *DistributeTableExec) getKeyRanges() ([]*pdhttp.KeyRange, error) {
	physicalIDs := make([]int64, 0)
	pi := e.tableInfo.GetPartitionInfo()
	if pi == nil {
		physicalIDs = append(physicalIDs, e.tableInfo.ID)
	} else {
		for _, name := range e.partitionNames {
			pid, err := tables.FindPartitionByName(e.tableInfo, name.L)
			if err != nil {
				return nil, err
			}
			physicalIDs = append(physicalIDs, pid)
		}
	}

	ranges := make([]*pdhttp.KeyRange, 0, len(physicalIDs))
	for _, id := range physicalIDs {
		startKey, endKey := tablecodec.GetTableHandleKeyRange(id)
		r := pdhttp.NewKeyRange(startKey, endKey)
		ranges = append(ranges, r)
	}
	return ranges, nil
}
