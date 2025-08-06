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
	"cmp"
	"context"
	"slices"
	"strings"

	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/tikv/pd/client/errs"
	pdhttp "github.com/tikv/pd/client/http"
)

var schedulerName = "balance-range-scheduler"

// DistributeTableExec represents a distribute table  executor.
type DistributeTableExec struct {
	exec.BaseExecutor

	tableInfo      *model.TableInfo
	is             infoschema.InfoSchema
	partitionNames []ast.CIStr
	rule           string
	engine         string
	timeout        string

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
	slices.SortFunc(e.partitionNames, func(i, j ast.CIStr) int {
		return cmp.Compare(i.L, j.L)
	})
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
	config, err := infosync.GetSchedulerConfig(ctx, schedulerName)
	configs, ok := config.([]any)
	if !ok {
		return errs.ErrClientProtoUnmarshal.FastGenByArgs(config)
	}
	jobs := make([]map[string]any, 0, len(configs))
	for _, cfg := range configs {
		job, ok := cfg.(map[string]any)
		if !ok {
			return errs.ErrClientProtoUnmarshal.FastGenByArgs(cfg)
		}
		jobs = append(jobs, job)
	}
	if err != nil {
		return err
	}
	alias := e.getAlias()
	jobID := float64(-1)
	for _, job := range jobs {
		// PD will ensure all the alias of uncompleted job are different.
		// PD return err if the some job alredy exist in the scheduler.
		if job["alias"] == alias && job["engine"] == e.engine && job["rule"] == e.rule && job["status"] != "finished" {
			id := job["job-id"].(float64)
			if id > jobID {
				jobID = id
			}
		}
	}
	if jobID != -1 {
		chk.AppendUint64(0, uint64(jobID))
	}
	return nil
}

func (e *DistributeTableExec) distributeTable(ctx context.Context) error {
	input := make(map[string]any)
	input["alias"] = e.getAlias()
	input["engine"] = e.engine
	input["rule"] = e.rule
	if len(e.timeout) > 0 {
		input["timeout"] = e.timeout
	}
	startKeys := make([]string, 0, len(e.keyRanges))
	endKeys := make([]string, 0, len(e.keyRanges))
	for _, r := range e.keyRanges {
		startKey, endKey := r.EscapeAsUTF8Str()
		startKeys = append(startKeys, startKey)
		endKeys = append(endKeys, endKey)
	}
	input["start-key"] = strings.Join(startKeys, ",")
	input["end-key"] = strings.Join(endKeys, ",")
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
		if len(physicalIDs) == 0 {
			for _, p := range pi.Definitions {
				physicalIDs = append(physicalIDs, p.ID)
			}
		}
	}
	slices.Sort(physicalIDs)

	ranges := make([]*pdhttp.KeyRange, 0, len(physicalIDs))
	for i, pid := range physicalIDs {
		if i == 0 || physicalIDs[i] != physicalIDs[i-1]+1 {
			startKey := codec.EncodeBytes([]byte{}, tablecodec.GenTablePrefix(pid))
			endKey := codec.EncodeBytes([]byte{}, tablecodec.GenTablePrefix(pid+1))
			r := pdhttp.NewKeyRange(startKey, endKey)
			ranges = append(ranges, r)
		} else {
			ranges[len(ranges)-1].EndKey = codec.EncodeBytes([]byte{}, tablecodec.GenTablePrefix(pid+1))
		}
	}
	return ranges, nil
}

// CancelDistributionJobExec represents a cancel distribution job executor.
type CancelDistributionJobExec struct {
	exec.BaseExecutor
	jobID uint64
	done  bool
}

var (
	_ exec.Executor = (*CancelDistributionJobExec)(nil)
)

// Next implements the Executor Next interface.
func (e *CancelDistributionJobExec) Next(ctx context.Context, _ *chunk.Chunk) error {
	if e.done {
		return nil
	}
	e.done = true
	return infosync.CancelSchedulerJob(ctx, schedulerName, e.jobID)
}
