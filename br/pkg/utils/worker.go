// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"go.uber.org/zap"
)

// PanicToErr recovers when the execution get panicked, and set the error provided by the arg.
// generally, this would be used with named return value and `defer`, like:
//
//	func foo() (err error) {
//	  defer utils.PanicToErr(&err)
//	  return maybePanic()
//	}
//
// Before using this, there are some hints for reducing resource leakage or bugs:
//   - If any of clean work (by `defer`) relies on the error (say, when error happens, rollback some operations.), please
//     place `defer this` AFTER that.
//   - All resources allocated should be freed by the `defer` syntax, or when panicking, they may not be recycled.
func PanicToErr(err *error) {
	item := recover()
	if item != nil {
		*err = errors.Annotatef(berrors.ErrUnknown, "panicked when executing, message: %v", item)
		log.Warn("PanicToErr: panicked, recovering and returning error", zap.StackSkip("stack", 1), logutil.ShortError(*err))
	}
}

// CatchAndLogPanic recovers when the execution get panicked, and log the panic.
// generally, this would be used with `defer`, like:
//
//	func foo() {
//	  defer utils.CatchAndLogPanic()
//	  maybePanic()
//	}
func CatchAndLogPanic() {
	item := recover()
	if item != nil {
		log.Warn("CatchAndLogPanic: panicked, but ignored.", zap.StackSkip("stack", 1), zap.Any("panic", item))
	}
}

type Result[T any] struct {
	Err  error
	Item T
}

func AsyncStreamBy[T any](generator func() (T, error)) <-chan Result[T] {
	out := make(chan Result[T])
	go func() {
		defer close(out)
		for {
			item, err := generator()
			if err != nil {
				out <- Result[T]{Err: err}
				return
			}
			out <- Result[T]{Item: item}
		}
	}()
	return out
}

func BuildWorkerTokenChannel(size uint) chan struct{} {
	ch := make(chan struct{}, size)
	for i := 0; i < int(size); i += 1 {
		ch <- struct{}{}
	}
	return ch
}
