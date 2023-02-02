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
	"time"
)

const defaultTaskChanLen = 1

// Option represents the optional function.
type Option func(opts *Options)

func loadOptions(options ...Option) *Options {
	opts := DefaultOption()
	for _, option := range options {
		option(opts)
	}
	return opts
}

// Options contains all options which will be applied when instantiating an pool.
type Options struct {
	// PanicHandler is used to handle panics from each worker goroutine.
	// if nil, panics will be thrown out again from worker goroutines.
	PanicHandler func(interface{})

	// ExpiryDuration is a period for the scavenger goroutine to clean up those expired workers,
	// the scavenger scans all workers every `ExpiryDuration` and clean up those workers that haven't been
	// used for more than `ExpiryDuration`.
	ExpiryDuration time.Duration

	// LimitDuration is a period in the limit mode.
	LimitDuration time.Duration

	// Max number of goroutine blocking on pool.Submit.
	// 0 (default value) means no such limit.
	MaxBlockingTasks int

	// When Nonblocking is true, Pool.AddProduce will never be blocked.
	// ErrPoolOverload will be returned when Pool.Submit cannot be done at once.
	// When Nonblocking is true, MaxBlockingTasks is inoperative.
	Nonblocking bool
}

// DefaultOption is the default option.
func DefaultOption() *Options {
	return &Options{
		LimitDuration: 200 * time.Millisecond,
		Nonblocking:   true,
	}
}

// WithExpiryDuration sets up the interval time of cleaning up goroutines.
func WithExpiryDuration(expiryDuration time.Duration) Option {
	return func(opts *Options) {
		opts.ExpiryDuration = expiryDuration
	}
}

// WithMaxBlockingTasks sets up the maximum number of goroutines that are blocked when it reaches the capacity of pool.
func WithMaxBlockingTasks(maxBlockingTasks int) Option {
	return func(opts *Options) {
		opts.MaxBlockingTasks = maxBlockingTasks
	}
}

// WithNonblocking indicates that pool will return nil when there is no available workers.
func WithNonblocking(nonblocking bool) Option {
	return func(opts *Options) {
		opts.Nonblocking = nonblocking
	}
}

// WithPanicHandler sets up panic handler.
func WithPanicHandler(panicHandler func(interface{})) Option {
	return func(opts *Options) {
		opts.PanicHandler = panicHandler
	}
}

// TaskOption represents the optional function.
type TaskOption func(opts *TaskOptions)

func loadTaskOptions(options ...TaskOption) *TaskOptions {
	opts := new(TaskOptions)
	for _, option := range options {
		option(opts)
	}
	if opts.Concurrency == 0 {
		opts.Concurrency = 1
	}
	if opts.ResultChanLen == 0 {
		opts.ResultChanLen = uint64(opts.Concurrency)
	}
	if opts.TaskChanLen == 0 {
		opts.TaskChanLen = defaultTaskChanLen
	}
	return opts
}

// TaskOptions contains all options
type TaskOptions struct {
	Concurrency   int
	ResultChanLen uint64
	TaskChanLen   uint64
}

// WithResultChanLen is to set the length of result channel.
func WithResultChanLen(resultChanLen uint64) TaskOption {
	return func(opts *TaskOptions) {
		opts.ResultChanLen = resultChanLen
	}
}

// WithTaskChanLen is to set the length of task channel.
func WithTaskChanLen(taskChanLen uint64) TaskOption {
	return func(opts *TaskOptions) {
		opts.TaskChanLen = taskChanLen
	}
}

// WithConcurrency is to set the concurrency of task.
func WithConcurrency(c int) TaskOption {
	return func(opts *TaskOptions) {
		opts.Concurrency = c
	}
}
