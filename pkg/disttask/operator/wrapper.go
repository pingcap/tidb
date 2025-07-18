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

	"golang.org/x/sync/errgroup"
)

// SimpleDataSource is a simple operator which use the given input slice as the data source.
type SimpleDataSource[T any] struct {
	ctx      *Context
	errGroup *errgroup.Group
	inputs   []T
	target   DataChannel[T]
}

// NewSimpleDataSource creates a new SimpleOperator with the given inputs.
// The input Operator.Context is used to quit this operator.
// As the source of the pipeline, this operator should be able to quit
// if any of the downstream operators encounter an error or panic.
// So we need to call ctx.OnError in other operators when they encounter
// an error or panic.
func NewSimpleDataSource[T any](
	ctx *Context,
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
	//nolint: errcheck
	s.errGroup.Wait()
	close(s.target.Channel())
	return s.ctx.OperatorErr()
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
	ctx      *Context
	errGroup errgroup.Group
	drainer  func(R)
	source   DataChannel[R]
}

func newSimpleSink[R any](ctx *Context, drainer func(R)) *simpleSink[R] {
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
	//nolint: errcheck
	s.errGroup.Wait()
	return s.ctx.OperatorErr()
}

func (s *simpleSink[T]) SetSource(ch DataChannel[T]) {
	s.source = ch
}

func (*simpleSink[R]) String() string {
	return "simpleSink"
}

type simpleOperator[T, R any] struct {
	*AsyncOperator[T, R]
}

func (s *simpleOperator[T, R]) String() string {
	return fmt.Sprintf("simpleOperator(%s)", s.AsyncOperator.String())
}

func newSimpleOperator[T any, R any](
	ctx *Context,
	transform func(task T) R,
	concurrency int,
) *simpleOperator[T, R] {
	asyncOp := NewAsyncOperatorWithTransform(ctx, "simple", concurrency, transform)
	return &simpleOperator[T, R]{
		AsyncOperator: asyncOp,
	}
}
