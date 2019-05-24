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

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/bindinfo"
	"github.com/pingcap/tidb/domain"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/util/chunk"
)

// SQLBindExec represents a bind executor.
type SQLBindExec struct {
	baseExecutor

	sqlBindOp    plannercore.SQLBindOpType
	normdOrigSQL string
	bindSQL      string
	charset      string
	collation    string
	isGlobal     bool
	bindAst      ast.StmtNode
}

// Next implements the Executor Next interface.
func (e *SQLBindExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("SQLBindExec.Next", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}

	req.Reset()
	switch e.sqlBindOp {
	case plannercore.OpSQLBindCreate:
		return e.createSQLBind()
	case plannercore.OpSQLBindDrop:
		return e.dropSQLBind()
	default:
		return errors.Errorf("unsupported SQL bind operation: %v", e.sqlBindOp)
	}
}

func (e *SQLBindExec) dropSQLBind() error {
	record := &bindinfo.BindRecord{
		OriginalSQL: e.normdOrigSQL,
		Db:          e.ctx.GetSessionVars().CurrentDB,
	}
	if !e.isGlobal {
		handle := e.ctx.Value(bindinfo.SessionBindInfoKeyType).(*bindinfo.SessionHandle)
		handle.DropBindRecord(record)
		return nil
	}
	return domain.GetDomain(e.ctx).BindHandle().DropBindRecord(record)
}

func (e *SQLBindExec) createSQLBind() error {
	record := &bindinfo.BindRecord{
		OriginalSQL: e.normdOrigSQL,
		BindSQL:     e.bindSQL,
		Db:          e.ctx.GetSessionVars().CurrentDB,
		Charset:     e.charset,
		Collation:   e.collation,
		Status:      bindinfo.Using,
	}
	if !e.isGlobal {
		handle := e.ctx.Value(bindinfo.SessionBindInfoKeyType).(*bindinfo.SessionHandle)
		return handle.AddBindRecord(record)
	}
	return domain.GetDomain(e.ctx).BindHandle().AddBindRecord(record)
}
