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
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	pdhttp "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
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
	var jobID float64 = -1
	for i := range 3 {
		found, id := e.getSchedulerJob(ctx)
		if found {
			jobID = id
			break
		}
		if i < 2 {
			timer := time.NewTimer(500 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
				timer.Stop()
			}
		}
	}
	if jobID != -1 {
		chk.AppendUint64(0, uint64(jobID))
	}
	return nil
}

// getSchedulerJob retrieves the job ID of the scheduler for the current table.
// if returns -1 means the scheduler job is not found.
func (e *DistributeTableExec) getSchedulerJob(ctx context.Context) (bool, float64) {
	// this request maybe sent to the scheduler microservice but this service maybe not watch the config update.
	// So it can't get the latest config immediately. We need to retry a few times to get the job id.
	config, err := infosync.GetSchedulerConfig(ctx, schedulerName)
	if err != nil {
		logutil.Logger(ctx).Info("get scheduler config failed", zap.Error(err))
		return false, -1
	}
	configs, ok := config.([]any)
	if !ok {
		logutil.Logger(ctx).Info("get empty scheduler config")
		return false, -1
	}
	jobs := make([]map[string]any, 0, len(configs))
	for _, cfg := range configs {
		job, ok := cfg.(map[string]any)
		if !ok {
			logutil.Logger(ctx).Info("get invalid scheduler config", zap.Any("config", cfg))
			return false, -1
		}
		jobs = append(jobs, job)
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
	return jobID > -1, jobID
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
	alias := strings.Join([]string{dbName, e.tableInfo.Name.String(), partitionStr}, ".")
	if keyspaceID, ok := distributionKeyspaceID(e.Ctx().GetStore()); ok {
		alias = strconv.FormatUint(uint64(keyspaceID), 10) + ":" + alias
	}
	return alias
}

// distributionKeyspaceID returns the keyspace ID of the store, or false if the
// store isn't bound to a keyspace.
// All keyspaces share the same PD balance-range scheduler, and PD rejects a new
// job whose alias equals an uncompleted one, so in keyspace mode the job alias is
// prefixed with the keyspace ID ({keyspace_id}:{db_name}.{table_name}.{partition_name})
// to keep jobs of different keyspaces apart. The prefix is joined by ':' rather
// than '.' so that the alias still has three '.'-separated parts, which older TiDB
// versions require when they parse the jobs in SHOW DISTRIBUTION JOBS.
func distributionKeyspaceID(store kv.Storage) (uint32, bool) {
	if store.GetKeyspace() == "" {
		return 0, false
	}
	return uint32(store.GetCodec().GetKeyspaceID()), true
}

// trimDistributionAliasKeyspace strips the keyspace ID prefix from the job alias
// and reports whether the job belongs to the given keyspace. An alias without a
// numeric "{keyspace_id}:" prefix was written by an older TiDB or outside keyspace
// mode, so it's returned as is and treated as visible for compatibility.
func trimDistributionAliasKeyspace(alias string, keyspaceID uint32) (string, bool) {
	idStr, rest, found := strings.Cut(alias, ":")
	if !found {
		return alias, true
	}
	id, err := strconv.ParseUint(idStr, 10, 32)
	if err != nil {
		return alias, true
	}
	return rest, uint32(id) == keyspaceID
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

	storeCodec := e.Ctx().GetStore().GetCodec()
	ranges := make([]*pdhttp.KeyRange, 0, len(physicalIDs))
	for i, pid := range physicalIDs {
		endKey := storeCodec.EncodeRegionKey(tablecodec.GenTablePrefix(pid + 1))
		if i == 0 || physicalIDs[i] != physicalIDs[i-1]+1 {
			startKey := storeCodec.EncodeRegionKey(tablecodec.GenTablePrefix(pid))
			ranges = append(ranges, pdhttp.NewKeyRange(startKey, endKey))
		} else {
			ranges[len(ranges)-1].EndKey = endKey
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
