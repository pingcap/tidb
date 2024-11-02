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
	"fmt"
	"strconv"

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// CommandDDLJobsExec is the general struct for Cancel/Pause/Resume commands on
// DDL jobs. These command currently by admin have the very similar struct and
// operations, it should be a better idea to have them in the same struct.
type CommandDDLJobsExec struct {
	exec.BaseExecutor

	cursor int
	jobIDs []int64
	errs   []error

	execute func(ctx context.Context, se sessionctx.Context, ids []int64) (errs []error, err error)
}

// Open implements the Executor for all Cancel/Pause/Resume command on DDL jobs
// just with different processes. And, it should not be called directly by the
// Executor.
func (e *CommandDDLJobsExec) Open(ctx context.Context) error {
	// We want to use a global transaction to execute the admin command, so we don't use e.Ctx() here.
	newSess, err := e.GetSysSession()
	if err != nil {
		return err
	}
	e.errs, err = e.execute(ctx, newSess, e.jobIDs)
	e.ReleaseSysSession(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), newSess)
	return err
}

// Next implements the Executor Next interface for Cancel/Pause/Resume
func (e *CommandDDLJobsExec) Next(_ context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.MaxChunkSize())
	if e.cursor >= len(e.jobIDs) {
		return nil
	}
	numCurBatch := min(req.Capacity(), len(e.jobIDs)-e.cursor)
	for i := e.cursor; i < e.cursor+numCurBatch; i++ {
		req.AppendString(0, strconv.FormatInt(e.jobIDs[i], 10))
		if e.errs != nil && e.errs[i] != nil {
			req.AppendString(1, fmt.Sprintf("error: %v", e.errs[i]))
		} else {
			req.AppendString(1, "successful")
		}
	}
	e.cursor += numCurBatch
	return nil
}

// CancelDDLJobsExec represents a cancel DDL jobs executor.
type CancelDDLJobsExec struct {
	*CommandDDLJobsExec
}

// PauseDDLJobsExec indicates an Executor for Pause a DDL Job.
type PauseDDLJobsExec struct {
	*CommandDDLJobsExec
}

// ResumeDDLJobsExec indicates an Executor for Resume a DDL Job.
type ResumeDDLJobsExec struct {
	*CommandDDLJobsExec
}
