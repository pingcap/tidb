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

package spool

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
	Blocking bool
}

// DefaultOption is the default option.
func DefaultOption() *Options {
	return &Options{
		Blocking: true,
	}
}

// WithBlocking indicates whether the pool is blocking.
func WithBlocking(blocking bool) Option {
	return func(opts *Options) {
		opts.Blocking = blocking
	}
}
