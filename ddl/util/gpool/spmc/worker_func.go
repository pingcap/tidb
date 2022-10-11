// Copyright 2022 PingCAP, Inc.
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

package spmc

import (
	"errors"
	"time"
)

var (
	// errQueueIsFull will be returned when the worker queue is full.
	errQueueIsFull = errors.New("the queue is full")

	// errQueueIsReleased will be returned when trying to insert item to a released worker queue.
	errQueueIsReleased = errors.New("the queue length is zero")
)

type workerArray[T any, U any, C any] interface {
	len() int
	isEmpty() bool
	insert(worker *goWorker[T, U, C]) error
	detach() *goWorker[T, U, C]
	retrieveExpiry(duration time.Duration) []*goWorker[T, U, C]
	reset()
}

type arrayType int

const (
	stackType arrayType = 1 << iota
	loopQueueType
)

func newWorkerArray[T any, U any, C any](aType arrayType, size int) workerArray[T, U, C] {
	switch aType {
	case stackType:
		return newWorkerStack[T, U, C](size)
	case loopQueueType:
		return newWorkerLoopQueue[T, U, C](size)
	default:
		return newWorkerStack[T, U, C](size)
	}
}
