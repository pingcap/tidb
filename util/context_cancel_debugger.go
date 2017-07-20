// Copyright 2017 PingCAP, Inc.
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

package util

import (
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/juju/errors"
	"golang.org/x/net/context"
)

var debug bool

func init() {
	if os.Getenv("tidb_context_cancel") != "" {
		debug = true
	}
}

func wrap(ctx context.Context, cancel context.CancelFunc) (context.Context, context.CancelFunc) {
	if !debug {
		return ctx, cancel
	}

	wrap := &wrapResult{
		Context: ctx,
		cancel:  cancel,
	}
	return wrap, wrap.cancelFunc
}

type wrapResult struct {
	context.Context
	cancel context.CancelFunc

	file string
	line int
}

func (wrap *wrapResult) Err() error {
	err := wrap.Context.Err()
	if err != nil {
		// Have to introduce errors package because caller may assert that
		// errors.Cause(err) == context.Canceled
		return errors.Annotate(err, fmt.Sprintf("cancel at %s:%d", wrap.file, wrap.line))
	}
	return err
}

func (wrap *wrapResult) cancelFunc() {
	_, wrap.file, wrap.line, _ = runtime.Caller(2)
	wrap.cancel()
}

// WithCancel is the same as standard WithCancel, but the caller of cancel is traced.
func WithCancel(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	return wrap(ctx, cancel)
}

// WithTimeout is the same as standard WithCancel, but the caller of cancel is traced.
func WithTimeout(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(parent, timeout)
	return wrap(ctx, cancel)
}
