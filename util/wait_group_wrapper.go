// Copyright 2021 PingCAP, Inc.
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

package util

import (
	"sync"
	"time"

	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// WaitGroupEnhancedWrapper wrapper wg, it provides the basic ability of WaitGroupWrapper with checking unexited process
// if the `exited` signal is true by print them on log.
type WaitGroupEnhancedWrapper struct {
	sync.WaitGroup
	source          string
	registerProcess sync.Map
}

// NewWaitGroupEnhancedWrapper returns WaitGroupEnhancedWrapper, the empty source indicates the unit test, then
// the `checkUnExitedProcess` won't be executed.
func NewWaitGroupEnhancedWrapper(source string, exit chan struct{}, exitedCheck bool) *WaitGroupEnhancedWrapper {
	wgew := &WaitGroupEnhancedWrapper{
		source:          source,
		registerProcess: sync.Map{},
	}
	if exitedCheck {
		wgew.Add(1)
		go wgew.checkUnExitedProcess(exit)
	}
	return wgew
}

func (w *WaitGroupEnhancedWrapper) checkUnExitedProcess(exit chan struct{}) {
	defer func() {
		logutil.BgLogger().Info("waitGroupWrapper exit-checking exited", zap.String("source", w.source))
		w.Done()
	}()
	logutil.BgLogger().Info("waitGroupWrapper enable exit-checking", zap.String("source", w.source))
	<-exit
	logutil.BgLogger().Info("waitGroupWrapper start exit-checking", zap.String("source", w.source))
	if w.check() {
		ticker := time.NewTimer(2 * time.Second)
		defer ticker.Stop()
		for {
			<-ticker.C
			continueCheck := w.check()
			if !continueCheck {
				return
			}
		}
	}
}

func (w *WaitGroupEnhancedWrapper) check() bool {
	unexitedProcess := make([]string, 0)
	w.registerProcess.Range(func(key, value any) bool {
		unexitedProcess = append(unexitedProcess, key.(string))
		return true
	})
	if len(unexitedProcess) > 0 {
		logutil.BgLogger().Warn("background process unexited while received exited signal",
			zap.Strings("process", unexitedProcess),
			zap.String("source", w.source))
		return true
	}
	logutil.BgLogger().Info("waitGroupWrapper finish checking unexited process", zap.String("source", w.source))
	return false
}

// Run runs a function in a goroutine, adds 1 to WaitGroup
// and calls done when function returns. Please DO NOT use panic
// in the cb function.
// Note that the registered label shouldn't be duplicated.
func (w *WaitGroupEnhancedWrapper) Run(exec func(), label string) {
	w.onStart(label)
	w.Add(1)
	go func() {
		defer func() {
			w.onExit(label)
			w.Done()
		}()
		exec()
	}()
}

// RunWithRecover wraps goroutine startup call with force recovery, add 1 to WaitGroup
// and call done when function return.
// exec is that execute logic function. recoverFn is that handler will be called after recover and before dump stack,
// passing `nil` means noop.
// Note that the registered label shouldn't be duplicated.
func (w *WaitGroupEnhancedWrapper) RunWithRecover(exec func(), recoverFn func(r interface{}), label string) {
	w.onStart(label)
	w.Add(1)
	go func() {
		defer func() {
			r := recover()
			if r != nil && recoverFn != nil {
				logutil.BgLogger().Info("WaitGroupEnhancedWrapper exec panic recovered", zap.String("process", label))
				recoverFn(r)
			}
			w.onExit(label)
			w.Done()
		}()
		exec()
	}()
}

func (w *WaitGroupEnhancedWrapper) onStart(label string) {
	_, ok := w.registerProcess.Load(label)
	if ok {
		logutil.BgLogger().Panic("WaitGroupEnhancedWrapper received duplicated source process",
			zap.String("source", w.source),
			zap.String("process", label))
	}
	w.registerProcess.Store(label, struct{}{})
	logutil.BgLogger().Info("background process started",
		zap.String("source", w.source),
		zap.String("process", label))
}

func (w *WaitGroupEnhancedWrapper) onExit(label string) {
	w.registerProcess.Delete(label)
	logutil.BgLogger().Info("background process exited",
		zap.String("source", w.source),
		zap.String("process", label))
}

// WaitGroupWrapper is a wrapper for sync.WaitGroup
type WaitGroupWrapper struct {
	sync.WaitGroup
}

// Run runs a function in a goroutine, adds 1 to WaitGroup
// and calls done when function returns. Please DO NOT use panic
// in the cb function.
func (w *WaitGroupWrapper) Run(exec func()) {
	w.Add(1)
	go func() {
		defer w.Done()
		exec()
	}()
}

// RunWithRecover wraps goroutine startup call with force recovery, add 1 to WaitGroup
// and call done when function return. it will dump current goroutine stack into log if catch any recover result.
// exec is that execute logic function. recoverFn is that handler will be called after recover and before dump stack,
// passing `nil` means noop.
func (w *WaitGroupWrapper) RunWithRecover(exec func(), recoverFn func(r interface{})) {
	w.Add(1)
	go func() {
		defer func() {
			r := recover()
			if recoverFn != nil {
				recoverFn(r)
			}
			w.Done()
		}()
		exec()
	}()
}
