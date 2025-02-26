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

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// AdminShowBDRRoleExec represents a show BDR role executor.
type AdminShowBDRRoleExec struct {
	exec.BaseExecutor

	done bool
}

var _ exec.Executor = &AdminShowBDRRoleExec{}

// Next implements the Executor Next interface.
func (e *AdminShowBDRRoleExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.done {
		return nil
	}

	return kv.RunInNewTxn(kv.WithInternalSourceType(ctx, kv.InternalTxnAdmin), e.Ctx().GetStore(), true, func(_ context.Context, txn kv.Transaction) error {
		role, err := meta.NewMutator(txn).GetBDRRole()
		if err != nil {
			return err
		}

		req.AppendString(0, role)
		e.done = true
		return nil
	})
}
