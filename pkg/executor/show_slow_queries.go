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

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// ShowSlowExec represents the executor of showing the slow queries.
// It is build from the "admin show slow" statement:
//
//	admin show slow top [internal | all] N
//	admin show slow recent N
type ShowSlowExec struct {
	exec.BaseExecutor

	ShowSlow *ast.ShowSlow
	result   []*domain.SlowQueryInfo
	cursor   int
}

var _ exec.Executor = &ShowSlowExec{}

// Open implements the Executor Open interface.
func (e *ShowSlowExec) Open(ctx context.Context) error {
	if err := e.BaseExecutor.Open(ctx); err != nil {
		return err
	}

	dom := domain.GetDomain(e.Ctx())
	e.result = dom.ShowSlowQuery(e.ShowSlow)
	return nil
}

// Next implements the Executor Next interface.
func (e *ShowSlowExec) Next(_ context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.cursor >= len(e.result) {
		return nil
	}

	for e.cursor < len(e.result) && req.NumRows() < e.MaxChunkSize() {
		slow := e.result[e.cursor]
		req.AppendString(0, slow.SQL)
		req.AppendTime(1, types.NewTime(types.FromGoTime(slow.Start), mysql.TypeTimestamp, types.MaxFsp))
		req.AppendDuration(2, types.Duration{Duration: slow.Duration, Fsp: types.MaxFsp})
		req.AppendString(3, slow.Detail.String())
		if slow.Succ {
			req.AppendInt64(4, 1)
		} else {
			req.AppendInt64(4, 0)
		}
		req.AppendUint64(5, slow.ConnID)
		req.AppendUint64(6, slow.TxnTS)
		req.AppendString(7, slow.User)
		req.AppendString(8, slow.DB)
		req.AppendString(9, slow.TableIDs)
		req.AppendString(10, slow.IndexNames)
		if slow.Internal {
			req.AppendInt64(11, 1)
		} else {
			req.AppendInt64(11, 0)
		}
		req.AppendString(12, slow.Digest)
		req.AppendString(13, slow.SessAlias)
		e.cursor++
	}
	return nil
}
