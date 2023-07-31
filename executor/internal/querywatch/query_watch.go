// Copyright 2023 PingCAP, Inc.
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

package querywatch

import (
	"context"

	"github.com/pingcap/tidb/executor/internal/exec"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
)

// AddExecutor is used as executor of add query watch.
type AddExecutor struct {
	QueryWatchOptionList []*ast.QueryWatchOption
	exec.BaseExecutor
	done bool
}

// Next implements the interface of Executor.
func (e *AddExecutor) Next(_ context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.done {
		return nil
	}
	e.done = true
	req.AppendUint64(0, uint64(0))
	return nil
}

// ExecDropQueryWatch is use to exec DropQueryWatchStmt.
func ExecDropQueryWatch(_ context.Context, _ sessionctx.Context, _ int64) error {
	return nil
}
