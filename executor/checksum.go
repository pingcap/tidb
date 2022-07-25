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

	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

var _ Executor = &ChecksumTableExec{}

// ChecksumTableExec represents ChecksumTable executor.
type ChecksumTableExec struct {
	baseExecutor

	tables map[int64]*checksumContext
	done   bool
}

// Open implements the Executor Open interface.
func (e *ChecksumTableExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}

	concurrency, err := getChecksumTableConcurrency(e.ctx)
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
		if result.Error != nil {
			err = result.Error
			logutil.Logger(ctx).Error("checksum failed", zap.Error(err))
			continue
		}
		e.handleResult(result)
	}
	if err != nil {
		return err
	}

	return nil
}

// Next implements the Executor Next interface.
func (e *ChecksumTableExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.done {
		return nil
	}
	for _, t := range e.tables {
		req.AppendString(0, t.DBInfo.Name.O)
		req.AppendString(1, t.TableInfo.Name.O)
		req.AppendUint64(2, t.Response.Checksum)
		req.AppendUint64(3, t.Response.TotalKvs)
		req.AppendUint64(4, t.Response.TotalBytes)
	}
	e.done = true
	return nil
}

func (e *ChecksumTableExec) buildTasks() ([]*checksumTask, error) {
	var tasks []*checksumTask
	for id, t := range e.tables {
		reqs, err := t.BuildRequests(e.ctx)
		if err != nil {
			return nil, err
		}
		for _, req := range reqs {
			tasks = append(tasks, &checksumTask{id, req})
		}
	}
	return tasks, nil
}

func (e *ChecksumTableExec) handleResult(result *checksumResult) {
	table := e.tables[result.TableID]
	table.HandleResponse(result.Response)
}

func (e *ChecksumTableExec) checksumWorker(taskCh <-chan *checksumTask, resultCh chan<- *checksumResult) {
	for task := range taskCh {
		result := &checksumResult{TableID: task.TableID}
		result.Response, result.Error = e.handleChecksumRequest(task.Request)
		resultCh <- result
	}
}

func (e *ChecksumTableExec) handleChecksumRequest(req *kv.Request) (resp *tipb.ChecksumResponse, err error) {
	ctx := distsql.WithSQLKvExecCounterInterceptor(context.TODO(), e.ctx.GetSessionVars().StmtCtx)
	res, err := distsql.Checksum(ctx, e.ctx.GetClient(), req, e.ctx.GetSessionVars().KVVars)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err1 := res.Close(); err1 != nil {
			err = err1
		}
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
	}

	return resp, nil
}

type checksumTask struct {
	TableID int64
	Request *kv.Request
}

type checksumResult struct {
	Error    error
	TableID  int64
	Response *tipb.ChecksumResponse
}

type checksumContext struct {
	DBInfo    *model.DBInfo
	TableInfo *model.TableInfo
	StartTs   uint64
	Response  *tipb.ChecksumResponse
}

func newChecksumContext(db *model.DBInfo, table *model.TableInfo, startTs uint64) *checksumContext {
	return &checksumContext{
		DBInfo:    db,
		TableInfo: table,
		StartTs:   startTs,
		Response:  &tipb.ChecksumResponse{},
	}
}

func (c *checksumContext) BuildRequests(ctx sessionctx.Context) ([]*kv.Request, error) {
	var partDefs []model.PartitionDefinition
	if part := c.TableInfo.Partition; part != nil {
		partDefs = part.Definitions
	}

	reqs := make([]*kv.Request, 0, (len(c.TableInfo.Indices)+1)*(len(partDefs)+1))
	if err := c.appendRequest(ctx, c.TableInfo.ID, &reqs); err != nil {
		return nil, err
	}

	for _, partDef := range partDefs {
		if err := c.appendRequest(ctx, partDef.ID, &reqs); err != nil {
			return nil, err
		}
	}

	return reqs, nil
}

func (c *checksumContext) appendRequest(ctx sessionctx.Context, tableID int64, reqs *[]*kv.Request) error {
	req, err := c.buildTableRequest(ctx, tableID)
	if err != nil {
		return err
	}

	*reqs = append(*reqs, req)
	for _, indexInfo := range c.TableInfo.Indices {
		if indexInfo.State != model.StatePublic {
			continue
		}
		req, err = c.buildIndexRequest(ctx, tableID, indexInfo)
		if err != nil {
			return err
		}
		*reqs = append(*reqs, req)
	}

	return nil
}

func (c *checksumContext) buildTableRequest(ctx sessionctx.Context, tableID int64) (*kv.Request, error) {
	checksum := &tipb.ChecksumRequest{
		ScanOn:    tipb.ChecksumScanOn_Table,
		Algorithm: tipb.ChecksumAlgorithm_Crc64_Xor,
	}

	var ranges []*ranger.Range
	if c.TableInfo.IsCommonHandle {
		ranges = ranger.FullNotNullRange()
	} else {
		ranges = ranger.FullIntRange(false)
	}

	var builder distsql.RequestBuilder
	builder.SetResourceGroupTagger(ctx.GetSessionVars().StmtCtx.GetResourceGroupTagger())
	return builder.SetHandleRanges(ctx.GetSessionVars().StmtCtx, tableID, c.TableInfo.IsCommonHandle, ranges, nil).
		SetChecksumRequest(checksum).
		SetStartTS(c.StartTs).
		SetConcurrency(ctx.GetSessionVars().DistSQLScanConcurrency()).
		Build()
}

func (c *checksumContext) buildIndexRequest(ctx sessionctx.Context, tableID int64, indexInfo *model.IndexInfo) (*kv.Request, error) {
	checksum := &tipb.ChecksumRequest{
		ScanOn:    tipb.ChecksumScanOn_Index,
		Algorithm: tipb.ChecksumAlgorithm_Crc64_Xor,
	}

	ranges := ranger.FullRange()

	var builder distsql.RequestBuilder
	builder.SetResourceGroupTagger(ctx.GetSessionVars().StmtCtx.GetResourceGroupTagger())
	return builder.SetIndexRanges(ctx.GetSessionVars().StmtCtx, tableID, indexInfo.ID, ranges).
		SetChecksumRequest(checksum).
		SetStartTS(c.StartTs).
		SetConcurrency(ctx.GetSessionVars().DistSQLScanConcurrency()).
		Build()
}

func (c *checksumContext) HandleResponse(update *tipb.ChecksumResponse) {
	updateChecksumResponse(c.Response, update)
}

func getChecksumTableConcurrency(ctx sessionctx.Context) (int, error) {
	sessionVars := ctx.GetSessionVars()
	concurrency, err := variable.GetSessionOrGlobalSystemVar(sessionVars, variable.TiDBChecksumTableConcurrency)
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
