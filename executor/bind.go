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
	"github.com/pingcap/tidb/bindinfo"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/util/chunk"
)

// SQLBindExec represents a bind executor.
type SQLBindExec struct {
	baseExecutor

	normdOrigSQL string
	bindSQL      string
	defaultDB    string
	charset      string
	collation    string
	isGlobal     bool
	bindAst      ast.StmtNode
}

// Next implements the Executor Next interface.
func (e *SQLBindExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	req.Reset()
	handle := domain.GetDomain(e.ctx).BindHandle()
	if e.isGlobal {
		bindRecord := &bindinfo.BindRecord{
			OriginalSQL: e.normdOrigSQL,
			BindSQL:     e.bindSQL,
			Db:          e.defaultDB,
			Charset:     e.charset,
			Collation:   e.collation,
		}
		return handle.AddGlobalBind(bindRecord)
	}

	return errors.New("Create non global sql bind is not supported.")
}
