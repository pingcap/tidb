// Copyright 2018 PingCAP, Inc.
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

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/distsql"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

var _ exec.Executor = &ChecksumTableExec{}

// ChecksumTableExec represents ChecksumTable executor.
type ChecksumTableExec struct {
	exec.BaseExecutor

	tables map[int64]*checksumContext // tableID -> checksumContext
	done   bool
}

// Open implements the Executor Open interface.
func (e *ChecksumTableExec) Open(ctx context.Context) error {
	if err := e.BaseExecutor.Open(ctx); err != nil {
		return err
	}

	concurrency, err := getChecksumTableConcurrency(e.Ctx())
	if err != nil {
		return err
	}

	tasks, err := e.buildTasks()
	if err != nil {
		return err
	}

	taskCh := make(chan *checksumTask, len(tasks))
	resultCh := make(chan *checksumResult, len(tasks))
	for i := 0; i < concurrency; i++ {
		go e.checksumWorker(taskCh, resultCh)
	}

	for _, task := range tasks {
		taskCh <- task
	}
	close(taskCh)

	for i := 0; i < len(tasks); i++ {
		result := <-resultCh
		if result.err != nil {
			err = result.err
			logutil.Logger(ctx).Error("checksum failed", zap.Error(err))
			continue
		}
		logutil.Logger(ctx).Info(
			"got one checksum result",
			zap.Int64("tableID", result.tableID),
			zap.Int64("physicalTableID", result.physicalTableID),
			zap.Int64("indexID", result.indexID),
			zap.Uint64("checksum", result.response.Checksum),
			zap.Uint64("totalKvs", result.response.TotalKvs),
			zap.Uint64("totalBytes", result.response.TotalBytes),
		)
		e.handleResult(result)
	}
	if err != nil {
		return err
	}

	return nil
}

// Next implements the Executor Next interface.
func (e *ChecksumTableExec) Next(_ context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.done {
		return nil
	}
	for _, t := range e.tables {
		req.AppendString(0, t.dbInfo.Name.O)
		req.AppendString(1, t.tableInfo.Name.O)
		req.AppendUint64(2, t.response.Checksum)
		req.AppendUint64(3, t.response.TotalKvs)
		req.AppendUint64(4, t.response.TotalBytes)
	}
	e.done = true
	return nil
}

func (e *ChecksumTableExec) buildTasks() ([]*checksumTask, error) {
	allTasks := make([][]*checksumTask, 0, len(e.tables))
	taskCnt := 0
	for _, t := range e.tables {
		tasks, err := t.buildTasks(e.Ctx())
		if err != nil {
			return nil, err
		}
		allTasks = append(allTasks, tasks)
		taskCnt += len(tasks)
	}
	ret := make([]*checksumTask, 0, taskCnt)
	for _, task := range allTasks {
		ret = append(ret, task...)
	}
	return ret, nil
}

func (e *ChecksumTableExec) handleResult(result *checksumResult) {
	table := e.tables[result.tableID]
	table.handleResponse(result.response)
}

func (e *ChecksumTableExec) checksumWorker(taskCh <-chan *checksumTask, resultCh chan<- *checksumResult) {
	for task := range taskCh {
		result := &checksumResult{
			tableID:         task.tableID,
			physicalTableID: task.physicalTableID,
			indexID:         task.indexID,
		}
		result.response, result.err = e.handleChecksumRequest(task.request)
		resultCh <- result
	}
}

func (e *ChecksumTableExec) handleChecksumRequest(req *kv.Request) (resp *tipb.ChecksumResponse, err error) {
	if err = e.Ctx().GetSessionVars().SQLKiller.HandleSignal(); err != nil {
		return nil, err
	}
	ctx := distsql.WithSQLKvExecCounterInterceptor(context.TODO(), e.Ctx().GetSessionVars().StmtCtx.KvExecCounter)
	res, err := distsql.Checksum(ctx, e.Ctx().GetClient(), req, e.Ctx().GetSessionVars().KVVars)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err1 := res.Close(); err1 != nil {
			err = err1
		}
		failpoint.Inject("afterHandleChecksumRequest", nil)
	}()

	resp = &tipb.ChecksumResponse{}

	for {
		data, err := res.NextRaw(ctx)
		if err != nil {
			return nil, err
		}
		if data == nil {
			break
		}
		checksum := &tipb.ChecksumResponse{}
		if err = checksum.Unmarshal(data); err != nil {
			return nil, err
		}
		updateChecksumResponse(resp, checksum)
		if err = e.Ctx().GetSessionVars().SQLKiller.HandleSignal(); err != nil {
			return nil, err
		}
	}

	return resp, nil
}

type checksumTask struct {
	tableID         int64
	physicalTableID int64
	indexID         int64
	request         *kv.Request
}

type checksumResult struct {
	err             error
	tableID         int64
	physicalTableID int64
	indexID         int64
	response        *tipb.ChecksumResponse
}

type checksumContext struct {
	dbInfo    *model.DBInfo
	tableInfo *model.TableInfo
	startTs   uint64
	response  *tipb.ChecksumResponse
}

func newChecksumContext(db *model.DBInfo, table *model.TableInfo, startTs uint64) *checksumContext {
	return &checksumContext{
		dbInfo:    db,
		tableInfo: table,
		startTs:   startTs,
		response:  &tipb.ChecksumResponse{},
	}
}

func (c *checksumContext) buildTasks(ctx sessionctx.Context) ([]*checksumTask, error) {
	var partDefs []model.PartitionDefinition
	if part := c.tableInfo.Partition; part != nil {
		partDefs = part.Definitions
	}

	reqs := make([]*checksumTask, 0, (len(c.tableInfo.Indices)+1)*(len(partDefs)+1))
	if err := c.appendRequest4PhysicalTable(ctx, c.tableInfo.ID, c.tableInfo.ID, &reqs); err != nil {
		return nil, err
	}

	for _, partDef := range partDefs {
		if err := c.appendRequest4PhysicalTable(ctx, c.tableInfo.ID, partDef.ID, &reqs); err != nil {
			return nil, err
		}
	}

	return reqs, nil
}

func (c *checksumContext) appendRequest4PhysicalTable(
	ctx sessionctx.Context,
	tableID int64,
	physicalTableID int64,
	reqs *[]*checksumTask,
) error {
	req, err := c.buildTableRequest(ctx, physicalTableID)
	if err != nil {
		return err
	}

	*reqs = append(*reqs, &checksumTask{
		tableID:         tableID,
		physicalTableID: physicalTableID,
		indexID:         -1,
		request:         req,
	})
	for _, indexInfo := range c.tableInfo.Indices {
		if indexInfo.State != model.StatePublic {
			continue
		}
		req, err = c.buildIndexRequest(ctx, physicalTableID, indexInfo)
		if err != nil {
			return err
		}
		*reqs = append(*reqs, &checksumTask{
			tableID:         tableID,
			physicalTableID: physicalTableID,
			indexID:         indexInfo.ID,
			request:         req,
		})
	}

	return nil
}

func (c *checksumContext) buildTableRequest(ctx sessionctx.Context, physicalTableID int64) (*kv.Request, error) {
	checksum := &tipb.ChecksumRequest{
		ScanOn:    tipb.ChecksumScanOn_Table,
		Algorithm: tipb.ChecksumAlgorithm_Crc64_Xor,
	}

	var ranges []*ranger.Range
	if c.tableInfo.IsCommonHandle {
		ranges = ranger.FullNotNullRange()
	} else {
		ranges = ranger.FullIntRange(false)
	}

	var builder distsql.RequestBuilder
	builder.SetResourceGroupTagger(ctx.GetSessionVars().StmtCtx.GetResourceGroupTagger())
	return builder.SetHandleRanges(ctx.GetDistSQLCtx(), physicalTableID, c.tableInfo.IsCommonHandle, ranges).
		SetChecksumRequest(checksum).
		SetStartTS(c.startTs).
		SetConcurrency(ctx.GetSessionVars().DistSQLScanConcurrency()).
		SetResourceGroupName(ctx.GetSessionVars().StmtCtx.ResourceGroupName).
		SetExplicitRequestSourceType(ctx.GetSessionVars().ExplicitRequestSourceType).
		Build()
}

func (c *checksumContext) buildIndexRequest(ctx sessionctx.Context, physicalTableID int64, indexInfo *model.IndexInfo) (*kv.Request, error) {
	checksum := &tipb.ChecksumRequest{
		ScanOn:    tipb.ChecksumScanOn_Index,
		Algorithm: tipb.ChecksumAlgorithm_Crc64_Xor,
	}

	ranges := ranger.FullRange()

	var builder distsql.RequestBuilder
	builder.SetResourceGroupTagger(ctx.GetSessionVars().StmtCtx.GetResourceGroupTagger())
	return builder.SetIndexRanges(ctx.GetDistSQLCtx(), physicalTableID, indexInfo.ID, ranges).
		SetChecksumRequest(checksum).
		SetStartTS(c.startTs).
		SetConcurrency(ctx.GetSessionVars().DistSQLScanConcurrency()).
		SetResourceGroupName(ctx.GetSessionVars().StmtCtx.ResourceGroupName).
		SetExplicitRequestSourceType(ctx.GetSessionVars().ExplicitRequestSourceType).
		Build()
}

func (c *checksumContext) handleResponse(update *tipb.ChecksumResponse) {
	updateChecksumResponse(c.response, update)
}

func getChecksumTableConcurrency(ctx sessionctx.Context) (int, error) {
	sessionVars := ctx.GetSessionVars()
	concurrency, err := sessionVars.GetSessionOrGlobalSystemVar(context.Background(), variable.TiDBChecksumTableConcurrency)
	if err != nil {
		return 0, err
	}
	c, err := strconv.ParseInt(concurrency, 10, 64)
	return int(c), err
}

func updateChecksumResponse(resp, update *tipb.ChecksumResponse) {
	resp.Checksum ^= update.Checksum
	resp.TotalKvs += update.TotalKvs
	resp.TotalBytes += update.TotalBytes
}
