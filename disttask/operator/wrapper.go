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
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"
)

type simpleSource[T comparable] struct {
	errGroup  errgroup.Group
	generator func() T
	sink      DataChannel[T]
}

func newSimpleSource[T comparable](generator func() T) *simpleSource[T] {
	return &simpleSource[T]{generator: generator}
}

func (s *simpleSource[T]) Open() error {
	s.errGroup.Go(func() error {
		var zT T
		for {
			res := s.generator()
			if res == zT {
				break
			}
			s.sink.Channel() <- res
		}
		s.sink.Finish()
		return nil
	})
	return nil
}

func (s *simpleSource[T]) Close() error {
	return s.errGroup.Wait()
}

func (s *simpleSource[T]) SetSink(ch DataChannel[T]) {
	s.sink = ch
}

func (*simpleSource[T]) String() string {
	return "simpleSource"
}

type simpleSink[R any] struct {
	errGroup errgroup.Group
	drainer  func(R)
	source   DataChannel[R]
}

func newSimpleSink[R any](drainer func(R)) *simpleSink[R] {
	return &simpleSink[R]{
		drainer: drainer,
	}
}

func (s *simpleSink[R]) Open() error {
	s.errGroup.Go(func() error {
		for {
			data, ok := <-s.source.Channel()
			if !ok {
				return nil
			}
			s.drainer(data)
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

type simpleOperator[T, R any] struct {
	*AsyncOperator[T, R]
}

func (s *simpleOperator[T, R]) String() string {
	return fmt.Sprintf("simpleOperator(%s)", s.AsyncOperator.String())
}

func newSimpleOperator[T, R any](transform func(task T) R, concurrency int) *simpleOperator[T, R] {
	asyncOp := NewAsyncOperatorWithTransform(context.Background(), "simple", concurrency, transform)
	return &simpleOperator[T, R]{
		AsyncOperator: asyncOp,
	}
}
