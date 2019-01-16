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
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/chunk"
)

// CreateTableInsertExec represents an insert executor when creating table, it is simply a wrapper of `InsertExec` or
// `ReplaceExec` depends on `onDuplicate` option
// See 'https://dev.mysql.com/doc/refman/5.7/en/create-table-select.html' for more details
type CreateTableInsertExec struct {
	baseExecutor

	insert Executor
}

// Next implements Exec Next interface.
func (e *CreateTableInsertExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	return errors.Trace(e.insert.Next(ctx, req))
}

// Close implements the Executor Close interface.
func (e *CreateTableInsertExec) Close() error {
	return errors.Trace(e.insert.Close())
}

// Open implements the Executor Close interface.
func (e *CreateTableInsertExec) Open(ctx context.Context) error {
	return errors.Trace(e.insert.Open(ctx))
}
