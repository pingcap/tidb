// Copyright 2019 PingCAP, Inc.
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
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/util/chunk"
)

// CreateBindExec represents a create bind executor.
type CreateBindExec struct {
	baseExecutor

	originSQL string
	bindSQL   string
	defaultDB string
	charset   string
	collation string
	isGlobal  bool
	bindAst   ast.StmtNode
}

// Next implements the Executor Next interface.
func (e *CreateBindExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	req.Reset()
	handle := domain.GetDomain(e.ctx).BindHandle()
	if handle == nil {
		return errors.New("bind manager is nil")
	}
	var err error
	if e.isGlobal {
		err = handle.AddGlobalBind(e.originSQL, e.bindSQL, e.defaultDB, e.charset, e.collation)
	}
	return err
}
