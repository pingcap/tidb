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
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"strconv"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
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
		return errors.Trace(err)
	}

	concurrency, err := getChecksumTableConcurrency(e.ctx)
	if err != nil {
		return errors.Trace(err)
	}

	tasks, err := e.buildTasks()
	if err != nil {
		return errors.Trace(err)
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
			log.Error(errors.ErrorStack(err))
			continue
		}
		e.handleResult(result)
	}
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// Next implements the Executor Next interface.
func (e *ChecksumTableExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.done {
		return nil
	}
	for _, t := range e.tables {
		chk.AppendString(0, t.DBInfo.Name.O)
		chk.AppendString(1, t.TableInfo.Name.O)
		chk.AppendUint64(2, t.Response.Checksum)
		chk.AppendUint64(3, t.Response.TotalKvs)
		chk.AppendUint64(4, t.Response.TotalBytes)
	}
	e.done = true
	return nil
}

func (e *ChecksumTableExec) buildTasks() ([]*checksumTask, error) {
	var tasks []*checksumTask
	for id, t := range e.tables {
		reqs, err := t.BuildRequests(e.ctx)
		if err != nil {
			return nil, errors.Trace(err)
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
	ctx := context.TODO()
	res, err := distsql.Checksum(ctx, e.ctx.GetClient(), req, e.ctx.GetSessionVars().KVVars)
	if err != nil {
		return nil, errors.Trace(err)
	}
	res.Fetch(ctx)
	defer func() {
		if err1 := res.Close(); err1 != nil {
			err = errors.Trace(err1)
		}
	}()

	resp = &tipb.ChecksumResponse{}

	for {
		data, err := res.NextRaw(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if data == nil {
			break
		}
		checksum := &tipb.ChecksumResponse{}
		if err = checksum.Unmarshal(data); err != nil {
			return nil, errors.Trace(err)
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
	var reqs []*kv.Request

	req, err := c.buildTableRequest(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	reqs = append(reqs, req)

	for _, indexInfo := range c.TableInfo.Indices {
		if indexInfo.State != model.StatePublic {
			continue
		}
		req, err = c.buildIndexRequest(ctx, indexInfo)
		if err != nil {
			return nil, errors.Trace(err)
		}
		reqs = append(reqs, req)
	}

	return reqs, nil
}

func (c *checksumContext) buildTableRequest(ctx sessionctx.Context) (*kv.Request, error) {
	checksum := &tipb.ChecksumRequest{
		StartTs:   c.StartTs,
		ScanOn:    tipb.ChecksumScanOn_Table,
		Algorithm: tipb.ChecksumAlgorithm_Crc64_Xor,
	}

	ranges := ranger.FullIntRange(false)

	var builder distsql.RequestBuilder
	return builder.SetTableRanges(c.TableInfo.ID, ranges, nil).
		SetChecksumRequest(checksum).
		SetConcurrency(ctx.GetSessionVars().DistSQLScanConcurrency).
		Build()
}

func (c *checksumContext) buildIndexRequest(ctx sessionctx.Context, indexInfo *model.IndexInfo) (*kv.Request, error) {
	checksum := &tipb.ChecksumRequest{
		StartTs:   c.StartTs,
		ScanOn:    tipb.ChecksumScanOn_Index,
		Algorithm: tipb.ChecksumAlgorithm_Crc64_Xor,
	}

	ranges := ranger.FullRange()

	var builder distsql.RequestBuilder
	return builder.SetIndexRanges(ctx.GetSessionVars().StmtCtx, c.TableInfo.ID, indexInfo.ID, ranges).
		SetChecksumRequest(checksum).
		SetConcurrency(ctx.GetSessionVars().DistSQLScanConcurrency).
		Build()
}

func (c *checksumContext) HandleResponse(update *tipb.ChecksumResponse) {
	updateChecksumResponse(c.Response, update)
}

func getChecksumTableConcurrency(ctx sessionctx.Context) (int, error) {
	sessionVars := ctx.GetSessionVars()
	concurrency, err := variable.GetSessionSystemVar(sessionVars, variable.TiDBChecksumTableConcurrency)
	if err != nil {
		return 0, errors.Trace(err)
	}
	c, err := strconv.ParseInt(concurrency, 10, 64)
	return int(c), errors.Trace(err)
}

func updateChecksumResponse(resp, update *tipb.ChecksumResponse) {
	resp.Checksum ^= update.Checksum
	resp.TotalKvs += update.TotalKvs
	resp.TotalBytes += update.TotalBytes
}
