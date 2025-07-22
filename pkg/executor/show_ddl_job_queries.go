// Copyright 2024 PingCAP, Inc.
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

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// ShowDDLJobQueriesExec represents a show DDL job queries executor.
// The jobs id that is given by 'admin show ddl job queries' statement,
// only be searched in the latest 10 history jobs.
type ShowDDLJobQueriesExec struct {
	exec.BaseExecutor

	cursor int
	jobs   []*model.Job
	jobIDs []int64
}

var _ exec.Executor = &ShowDDLJobQueriesExec{}

// Open implements the Executor Open interface.
func (e *ShowDDLJobQueriesExec) Open(ctx context.Context) error {
	var err error
	var jobs []*model.Job
	if err := e.BaseExecutor.Open(ctx); err != nil {
		return err
	}
	session, err := e.GetSysSession()
	if err != nil {
		return err
	}
	err = sessiontxn.NewTxn(context.Background(), session)
	if err != nil {
		return err
	}
	defer func() {
		// ReleaseSysSession will rollbacks txn automatically.
		e.ReleaseSysSession(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), session)
	}()
	txn, err := session.Txn(true)
	if err != nil {
		return err
	}
	session.GetSessionVars().SetInTxn(true)

	m := meta.NewMutator(txn)
	jobs, err = ddl.GetAllDDLJobs(ctx, session)
	if err != nil {
		return err
	}

	historyJobs, err := ddl.GetLastNHistoryDDLJobs(m, ddl.DefNumHistoryJobs)
	if err != nil {
		return err
	}

	appendedJobID := make(map[int64]struct{})
	// deduplicate job results
	// for situations when this operation happens at the same time with new DDLs being executed
	for _, job := range jobs {
		if _, ok := appendedJobID[job.ID]; !ok {
			appendedJobID[job.ID] = struct{}{}
			e.jobs = append(e.jobs, job)
		}
	}
	for _, historyJob := range historyJobs {
		if _, ok := appendedJobID[historyJob.ID]; !ok {
			appendedJobID[historyJob.ID] = struct{}{}
			e.jobs = append(e.jobs, historyJob)
		}
	}

	return nil
}

// Next implements the Executor Next interface.
func (e *ShowDDLJobQueriesExec) Next(_ context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.MaxChunkSize())
	if e.cursor >= len(e.jobs) {
		return nil
	}
	if len(e.jobIDs) >= len(e.jobs) {
		return nil
	}
	numCurBatch := min(req.Capacity(), len(e.jobs)-e.cursor)
	for _, id := range e.jobIDs {
		for i := e.cursor; i < e.cursor+numCurBatch; i++ {
			if id == e.jobs[i].ID {
				req.AppendString(0, e.jobs[i].Query)
			}
		}
	}
	e.cursor += numCurBatch
	return nil
}

// ShowDDLJobQueriesWithRangeExec represents a show DDL job queries with range executor.
// The jobs id that is given by 'admin show ddl job queries' statement,
// can be searched within a specified range in history jobs using offset and limit.
type ShowDDLJobQueriesWithRangeExec struct {
	exec.BaseExecutor

	cursor int
	jobs   []*model.Job
	offset uint64
	limit  uint64
}

// Open implements the Executor Open interface.
func (e *ShowDDLJobQueriesWithRangeExec) Open(ctx context.Context) error {
	var err error
	var jobs []*model.Job
	if err := e.BaseExecutor.Open(ctx); err != nil {
		return err
	}
	session, err := e.GetSysSession()
	if err != nil {
		return err
	}
	err = sessiontxn.NewTxn(context.Background(), session)
	if err != nil {
		return err
	}
	defer func() {
		// ReleaseSysSession will rollbacks txn automatically.
		e.ReleaseSysSession(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), session)
	}()
	txn, err := session.Txn(true)
	if err != nil {
		return err
	}
	session.GetSessionVars().SetInTxn(true)

	m := meta.NewMutator(txn)
	jobs, err = ddl.GetAllDDLJobs(ctx, session)
	if err != nil {
		return err
	}

	historyJobs, err := ddl.GetLastNHistoryDDLJobs(m, int(e.offset+e.limit))
	if err != nil {
		return err
	}

	appendedJobID := make(map[int64]struct{})
	// deduplicate job results
	// for situations when this operation happens at the same time with new DDLs being executed
	for _, job := range jobs {
		if _, ok := appendedJobID[job.ID]; !ok {
			appendedJobID[job.ID] = struct{}{}
			e.jobs = append(e.jobs, job)
		}
	}
	for _, historyJob := range historyJobs {
		if _, ok := appendedJobID[historyJob.ID]; !ok {
			appendedJobID[historyJob.ID] = struct{}{}
			e.jobs = append(e.jobs, historyJob)
		}
	}

	if e.cursor < int(e.offset) {
		e.cursor = int(e.offset)
	}

	return nil
}

// Next implements the Executor Next interface.
func (e *ShowDDLJobQueriesWithRangeExec) Next(_ context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.MaxChunkSize())
	if e.cursor >= len(e.jobs) {
		return nil
	}
	if int(e.offset) > len(e.jobs) {
		return nil
	}
	numCurBatch := min(req.Capacity(), len(e.jobs)-e.cursor)
	for i := e.cursor; i < e.cursor+numCurBatch; i++ {
		// i is make true to be >= int(e.offset)
		if i >= int(e.offset+e.limit) {
			break
		}
		req.AppendString(0, strconv.FormatInt(e.jobs[i].ID, 10))
		req.AppendString(1, e.jobs[i].Query)
	}
	e.cursor += numCurBatch
	return nil
}
