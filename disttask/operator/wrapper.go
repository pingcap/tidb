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
	"github.com/pingcap/tidb/resourcemanager/pool/workerpool"
	"github.com/pingcap/tidb/resourcemanager/util"
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

func (*simpleSource[T]) Display() string {
	return "simpleSource"
}

type simpleSink[R any] struct {
	errGroup errgroup.Group
	quitCh   chan struct{}
	drainer  func(R)
	source   DataChannel[R]
}

func newSimpleSink[R any](drainer func(R)) *simpleSink[R] {
	return &simpleSink[R]{
		drainer: drainer,
		quitCh:  make(chan struct{}),
	}
}

func (s *simpleSink[R]) Open() error {
	s.errGroup.Go(func() error {
		for {
			select {
			case data := <-s.source.Channel():
				s.drainer(data)
			case <-s.quitCh:
				return nil
			}
		}
	})
	return nil
}

func (s *simpleSink[R]) Close() error {
	close(s.quitCh)
	return s.errGroup.Wait()
}

func (s *simpleSink[T]) SetSource(ch DataChannel[T]) {
	s.source = ch
}

func (*simpleSink[R]) Display() string {
	return "simpleSink"
}

type simpleOperator[T, R any] struct {
	AsyncOperator[T, R]
	transform func(T) R
}

func (*simpleOperator[T, R]) Display() string {
	return "simpleOperator"
}

func newSimpleOperator[T, R any](transform func(task T) R, concurrency int) *simpleOperator[T, R] {
	pool, _ := workerpool.NewWorkerPool("simple", util.UNKNOWN, concurrency,
		func() workerpool.Worker[T, R] {
			return simpleWorker[T, R]{transform: transform}
		})
	return &simpleOperator[T, R]{
		AsyncOperator: AsyncOperator[T, R]{pool: pool},
	}
}

type simpleWorker[T, R any] struct {
	transform func(T) R
}

func (s simpleWorker[T, R]) HandleTask(task T) R {
	return s.transform(task)
}

func (simpleWorker[T, R]) Close() {}
