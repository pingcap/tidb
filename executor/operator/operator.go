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

package operator

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"golang.org/x/net/context"
)

// Operator is the physical implementation of a algebra operator.
// In TiDB, all algebra operators are implemented as iterators, i.e., they
// support a simple Open-Next-Close protocol. See this paper for more details:
//
// "Volcano-An Extensible and Parallel Query Evaluation System"
//
// Different from Volcano's execution model, a "Next" function call in TiDB will
// return a batch of rows, other than a single row in Volcano.
//
// NOTE: Operator implementations should call "chk.Reset()" before appending
// their results to it.
type Operator interface {
	// Open initializes this operator and its child operators recursively.
	// For example: allocate buffers, start goroutines. It should be called
	// before Next().
	Open(context.Context) error

	// Next returns a batch of rows stored in the input Chunk, which is the
	// result of this operator. A Chunk with 0 rows indicates there is no more
	// data to be returned, and Close() should be called to release the
	// resources.
	Next(ctx context.Context, chk *chunk.Chunk) error

	// Close releases this operator and its child operators' resources
	// recursively. It should be called when there is no more data to return
	// or any errors encountered during the execution of Open() or Next() to
	// release resources and clean goroutines.
	Close() error

	// Schema returns the schema of this operator.
	Schema() *expression.Schema

	// RetTypes returns the types of all the output columns, which can be found
	// in the result of function Schema().
	RetTypes() []*types.FieldType

	// NewChunk allocates a Chunk which can store the result of this operator.
	// A typical usage of this function is:
	//   chk := operator.NewChunk()
	//   err := operator.Next(ctx, chk)
	NewChunk() *chunk.Chunk
}

// BaseOperator is the base implementation of the Operator interface. It holds
// the basic common resources that every operator needs, implements the Operator
// interface in the basic way, Operator implementations should implement their
// own methods if necessary.
type BaseOperator struct {
	Sctx            sessionctx.Context
	ExplainID       string
	ChunkSize       int
	Children        []Operator
	ChildrenResults []*chunk.Chunk

	schema   *expression.Schema
	retTypes []*types.FieldType
}

// NewBaseOperator creates a BaseOperator.
func NewBaseOperator(sctx sessionctx.Context, schema *expression.Schema, id string, children ...Operator) BaseOperator {
	e := BaseOperator{
		Children:  children,
		Sctx:      sctx,
		ExplainID: id,
		schema:    schema,
		ChunkSize: sctx.GetSessionVars().MaxChunkSize,
	}
	if schema != nil {
		cols := schema.Columns
		e.retTypes = make([]*types.FieldType, len(cols))
		for i := range cols {
			e.retTypes[i] = cols[i].RetType
		}
	}
	return e
}

// Open implements the Operator interface. It initializes the child operators
// recursively, allocate and initializes "ChildrenResults" according to the
// schema of each child operator.
func (e *BaseOperator) Open(ctx context.Context) error {
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

// Next implements the Operator interface.
func (e *BaseOperator) Next(ctx context.Context, chk *chunk.Chunk) error {
	return nil
}

// Close implements the Operator interface. It closes the child operators
// recursively, release all resources obtained.
func (e *BaseOperator) Close() error {
	for _, child := range e.Children {
		err := child.Close()
		if err != nil {
			return errors.Trace(err)
		}
	}
	e.ChildrenResults = nil
	return nil
}

// Schema returns the schema of this operator. An empty schema is returned if
// it doesn't have one.
func (e *BaseOperator) Schema() *expression.Schema {
	if e.schema == nil {
		return expression.NewSchema()
	}
	return e.schema
}

// RetTypes returns all field types of the output columns.
func (e *BaseOperator) RetTypes() []*types.FieldType {
	return e.retTypes
}

// NewChunk creates a new chunk to buffer the result of this operator.
func (e *BaseOperator) NewChunk() *chunk.Chunk {
	return chunk.NewChunkWithCapacity(e.RetTypes(), e.ChunkSize)
}
