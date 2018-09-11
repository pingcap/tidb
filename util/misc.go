// Copyright 2016 PingCAP, Inc.
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
	"runtime"
	"time"

	"fmt"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/metrics"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

const (
	// DefaultMaxRetries indicates the max retry count.
	DefaultMaxRetries = 30
	// RetryInterval indicates retry interval.
	RetryInterval uint64 = 500
)

// RunWithRetry will run the f with backoff and retry.
// retryCnt: Max retry count
// backoff: When run f failed, it will sleep backoff * triedCount time.Millisecond.
// Function f should have two return value. The first one is an bool which indicate if the err if retryable.
// The second is if the f meet any error.
func RunWithRetry(retryCnt int, backoff uint64, f func() (bool, error)) (err error) {
	for i := 1; i <= retryCnt; i++ {
		var retryAble bool
		retryAble, err = f()
		if err == nil || !retryAble {
			return errors.Trace(err)
		}
		sleepTime := time.Duration(backoff*uint64(i)) * time.Millisecond
		time.Sleep(sleepTime)
	}
	return errors.Trace(err)
}

// GetStack gets the stacktrace.
func GetStack() []byte {
	const size = 4096
	buf := make([]byte, size)
	stackSize := runtime.Stack(buf, false)
	buf = buf[:stackSize]
	return buf
}

// WithRecovery wraps goroutine startup call with recovery handle.
// There are two out of box common handlers `WithPanicLogf` and `WithPanicMetricInc`,
// Caller can custom define new handler use last optional parameters.
func WithRecovery(ctx context.Context, fn func(ctx context.Context), handlerOpts ...func(r interface{})) {
	if len(handlerOpts) == 0 {
		handlerOpts = append(handlerOpts, DefaultLogPanicHandler)
	}
	defer func() {
		r := recover()
		if r != nil {
			fmt.Println()
		}
		for _, handler := range handlerOpts {
			handler(r)
		}
	}()
	fn(ctx)
}

// DefaultLogPanicHandler will be added if call WithRecovery without any handlerOpts.
var DefaultLogPanicHandler = WithPanicLogf("goroutine")

// WithPanicLogf defines a panic handler that write a log with goroutine full stack.
func WithPanicLogf(format string, args ...interface{}) func(r interface{}) {
	return func(r interface{}) {
		if r == nil {
			return
		}
		buf := GetStack()
		log.Errorf("%s meet panic !!! result: %v, stack: %s", fmt.Sprintf(format, args...), r, buf)
	}
}

// WithPanicMetricInc defines a panic handler that record a panic counter metric.
func WithPanicMetricInc(labelValues ...string) func(r interface{}) {
	return func(r interface{}) {
		if r == nil {
			return
		}
		metrics.PanicCounter.WithLabelValues(labelValues...).Inc()
	}
}
