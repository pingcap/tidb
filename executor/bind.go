// Copyright 2016 PingCAP, Inc.
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
	"github.com/pingcap/tidb/infobind"
	"github.com/pingcap/tidb/util/chunk"
)

type CreateBindExec struct {
	baseExecutor

	originSql string

	bindSql string

	defaultDb string

	done bool

	isGlobal bool

	bindAst ast.StmtNode
}

// Next implements the Executor Next interface.
func (e *CreateBindExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.done {
		return nil
	}
	e.done = true

	bm := infobind.GetBindManager(e.ctx)
	if bm == nil {
		return errors.New("bind manager is nil")
	}

	if e.isGlobal {
		err := bm.AddGlobalBind(e.originSql, e.bindSql, e.defaultDb)
		return errors.Trace(err)
	}
}

type DropBindExec struct {
	baseExecutor

	originSql string

	defaultDb string

	isGlobal bool

	done bool
}

// Next implements the Executor Next interface.
func (e *DropBindExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.done {
		return nil
	}
	e.done = true

	bm := infobind.GetBindManager(e.ctx)
	if bm == nil {
		return errors.New("bind manager is nil")
	}

	var err error
	if e.isGlobal {
		err = bm.RemoveGlobalBind(e.originSql, e.defaultDb)
	}

	return errors.Trace(err)
}
