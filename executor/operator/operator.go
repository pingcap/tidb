// Copyright 2015 PingCAP, Inc.
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

package operator

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"golang.org/x/net/context"
)

// Executor is the physical implementation of a algebra operator.
//
// In TiDB, all algebra operators are implemented as iterators, i.e., they
// support a simple Open-Next-Close protocol. See this paper for more details:
//
// "Volcano-An Extensible and Parallel Query Evaluation System"
//
// Different from Volcano's execution model, a "Next" function call in TiDB will
// return a batch of rows, other than a single row in Volcano.
// NOTE: Executors must call "chk.Reset()" before appending their results to it.
type Executor interface {
	Open(context.Context) error
	Next(ctx context.Context, chk *chunk.Chunk) error
	Close() error
	Schema() *expression.Schema

	RetTypes() []*types.FieldType
	NewChunk() *chunk.Chunk
}

type BaseExecutor struct {
	Sctx            sessionctx.Context
	ExplainID       string
	ChunkSize       int
	Children        []Executor
	ChildrenResults []*chunk.Chunk

	schema        *expression.Schema
	retFieldTypes []*types.FieldType
}

// Open initializes Children recursively and "ChildrenResults" according to Children's schemas.
func (e *BaseExecutor) Open(ctx context.Context) error {
	for _, child := range e.Children {
		err := child.Open(ctx)
		if err != nil {
			return errors.Trace(err)
		}
	}
	e.ChildrenResults = make([]*chunk.Chunk, 0, len(e.Children))
	for _, child := range e.Children {
		e.ChildrenResults = append(e.ChildrenResults, child.NewChunk())
	}
	return nil
}

// Close closes all executors and release all resources.
func (e *BaseExecutor) Close() error {
	for _, child := range e.Children {
		err := child.Close()
		if err != nil {
			return errors.Trace(err)
		}
	}
	e.ChildrenResults = nil
	return nil
}

// Schema returns the current BaseExecutor's schema. If it is nil, then create and return a new one.
func (e *BaseExecutor) Schema() *expression.Schema {
	if e.schema == nil {
		return expression.NewSchema()
	}
	return e.schema
}

// NewChunk creates a new chunk to buffer current executor's result.
func (e *BaseExecutor) NewChunk() *chunk.Chunk {
	return chunk.NewChunkWithCapacity(e.RetTypes(), e.ChunkSize)
}

// RetTypes returns all output column types.
func (e *BaseExecutor) RetTypes() []*types.FieldType {
	return e.retFieldTypes
}

// Next fills mutiple rows into a chunk.
func (e *BaseExecutor) Next(ctx context.Context, chk *chunk.Chunk) error {
	return nil
}

func NewBaseExecutor(ctx sessionctx.Context, schema *expression.Schema, id string, children ...Executor) BaseExecutor {
	e := BaseExecutor{
		Children:  children,
		Sctx:      ctx,
		ExplainID: id,
		schema:    schema,
		ChunkSize: ctx.GetSessionVars().MaxChunkSize,
	}
	if schema != nil {
		cols := schema.Columns
		e.retFieldTypes = make([]*types.FieldType, len(cols))
		for i := range cols {
			e.retFieldTypes[i] = cols[i].RetType
		}
	}
	return e
}
