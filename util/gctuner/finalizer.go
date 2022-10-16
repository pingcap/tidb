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

package gctuner

import (
	"runtime"

	"go.uber.org/atomic"
)

type finalizerCallback func()

type finalizer struct {
	ref      *finalizerRef
	callback finalizerCallback
	stopped  atomic.Int32
}

type finalizerRef struct {
	parent *finalizer
}

func finalizerHandler(f *finalizerRef) {
	// stop calling callback
	if f.parent.stopped.Load() > 0 {
		return
	}
	f.parent.callback()
	runtime.SetFinalizer(f, finalizerHandler)
}

// newFinalizer return a finalizer object and caller should save it to make sure it will not be gc.
// the go runtime promise the callback function should be called every gc time.
func newFinalizer(callback finalizerCallback) *finalizer {
	f := &finalizer{
		callback: callback,
	}
	f.ref = &finalizerRef{parent: f}
	runtime.SetFinalizer(f.ref, finalizerHandler)
	f.ref = nil // trigger gc
	return f
}

func (f *finalizer) stop() {
	f.stopped.Store(1)
}
