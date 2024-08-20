// Copyright 2024 PingCAP, Inc.
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

package testutil

import (
	"github.com/pingcap/tidb/pkg/disttask/operator"
	"golang.org/x/sync/errgroup"
)

// OperatorTestSource is used for dist task operator test.
type OperatorTestSource[T any] struct {
	errGroup errgroup.Group
	ch       chan T
	toBeSent []T
}

// NewOperatorTestSource creates a new OperatorTestSource.
func NewOperatorTestSource[T any](toBeSent ...T) *OperatorTestSource[T] {
	return &OperatorTestSource[T]{
		ch:       make(chan T),
		toBeSent: toBeSent,
	}
}

// SetSink implements disttask/operator.Operator.
func (s *OperatorTestSource[T]) SetSink(sink operator.DataChannel[T]) {
	s.ch = sink.Channel()
}

// Open implements disttask/operator.Operator.
func (s *OperatorTestSource[T]) Open() error {
	s.errGroup.Go(func() error {
		for _, data := range s.toBeSent {
			s.ch <- data
		}
		close(s.ch)
		return nil
	})
	return nil
}

// Close implements disttask/operator.Operator.
func (s *OperatorTestSource[T]) Close() error {
	return s.errGroup.Wait()
}

// String implements disttask/operator.Operator.
func (*OperatorTestSource[T]) String() string {
	return "testSource"
}

// OperatorTestSink is used for dist task operator test.
type OperatorTestSink[T any] struct {
	errGroup  errgroup.Group
	ch        chan T
	collected []T
}

// NewOperatorTestSink creates a new OperatorTestSink.
func NewOperatorTestSink[T any]() *OperatorTestSink[T] {
	return &OperatorTestSink[T]{
		ch: make(chan T),
	}
}

// Open implements disttask/operator.Operator.
func (s *OperatorTestSink[T]) Open() error {
	s.errGroup.Go(func() error {
		for data := range s.ch {
			s.collected = append(s.collected, data)
		}
		return nil
	})
	return nil
}

// Close implements disttask/operator.Operator.
func (s *OperatorTestSink[T]) Close() error {
	return s.errGroup.Wait()
}

// SetSource implements disttask/operator.Operator.
func (s *OperatorTestSink[T]) SetSource(dataCh operator.DataChannel[T]) {
	s.ch = dataCh.Channel()
}

// String implements disttask/operator.Operator.
func (*OperatorTestSink[T]) String() string {
	return "testSink"
}

// Collect the result from OperatorTestSink.
func (s *OperatorTestSink[T]) Collect() []T {
	return s.collected
}
