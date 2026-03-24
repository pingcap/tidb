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

package operator

import (
	"fmt"

	"github.com/pingcap/tidb/pkg/resourcemanager/pool/workerpool"
	"golang.org/x/sync/errgroup"
)

// SimpleDataSource is a simple operator which use the given input slice as the data source.
type SimpleDataSource[T workerpool.TaskMayPanic] struct {
	ctx      *workerpool.Context
	errGroup *errgroup.Group
	inputs   []T
	target   DataChannel[T]
}

// NewSimpleDataSource creates a new SimpleOperator with the given inputs.
// The input workerpool.Context is used to quit this operator.
// By using the same context as the downstream operators, we can ensure that
// this operator will quit when other operators encounter an error or panic.
func NewSimpleDataSource[T workerpool.TaskMayPanic](
	ctx *workerpool.Context,
	inputs []T,
) *SimpleDataSource[T] {
	return &SimpleDataSource[T]{
		inputs:   inputs,
		errGroup: &errgroup.Group{},
		ctx:      ctx,
	}
}

// Open implements the Operator interface.
func (s *SimpleDataSource[T]) Open() error {
	s.errGroup.Go(func() error {
		defer s.target.Finish()

		for _, input := range s.inputs {
			select {
			case s.target.Channel() <- input:
			case <-s.ctx.Done():
				return s.ctx.Err()
			}
		}

		return nil
	})
	return nil
}

// Close implements the Operator interface.
func (s *SimpleDataSource[T]) Close() error {
	return s.errGroup.Wait()
}

// String implements the Operator interface.
func (*SimpleDataSource[T]) String() string {
	var zT T
	return fmt.Sprintf("SimpleDataSource[%T]", zT)
}

// SetSink implements the WithSink interface.
func (s *SimpleDataSource[T]) SetSink(ch DataChannel[T]) {
	s.target = ch
}

type simpleSink[R any] struct {
	ctx      *workerpool.Context
	errGroup errgroup.Group
	drainer  func(R)
	source   DataChannel[R]
}

func newSimpleSink[R any](ctx *workerpool.Context, drainer func(R)) *simpleSink[R] {
	return &simpleSink[R]{
		ctx:     ctx,
		drainer: drainer,
	}
}

func (s *simpleSink[R]) Open() error {
	s.errGroup.Go(func() error {
		for {
			select {
			case <-s.ctx.Done():
				return s.ctx.Err()
			case data, ok := <-s.source.Channel():
				if !ok {
					return nil
				}
				s.drainer(data)
			}
		}
	})
	return nil
}

func (s *simpleSink[R]) Close() error {
	return s.errGroup.Wait()
}

func (s *simpleSink[T]) SetSource(ch DataChannel[T]) {
	s.source = ch
}

func (*simpleSink[R]) String() string {
	return "simpleSink"
}

type simpleOperator[T workerpool.TaskMayPanic, R any] struct {
	*AsyncOperator[T, R]
}

func (s *simpleOperator[T, R]) String() string {
	return fmt.Sprintf("simpleOperator(%s)", s.AsyncOperator.String())
}

func newSimpleOperator[T workerpool.TaskMayPanic, R any](
	ctx *workerpool.Context,
	transform func(task T) R,
	concurrency int,
) *simpleOperator[T, R] {
	asyncOp := NewAsyncOperatorWithTransform(ctx, "simple", concurrency, transform)
	return &simpleOperator[T, R]{
		AsyncOperator: asyncOp,
	}
}
