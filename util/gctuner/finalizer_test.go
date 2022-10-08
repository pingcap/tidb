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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

type testState struct {
	count int32
}

func TestFinalizer(t *testing.T) {
	maxCount := int32(16)
	state := &testState{}
	f := newFinalizer(func() {
		n := atomic.AddInt32(&state.count, 1)
		if n > maxCount {
			t.Fatalf("cannot exec finalizer callback after f has been gc")
		}
	})
	for i := int32(1); i <= maxCount; i++ {
		runtime.GC()
		require.Equal(t, i, atomic.LoadInt32(&state.count))
	}
	require.Nil(t, f.ref)

	f.stop()
	require.Equal(t, maxCount, atomic.LoadInt32(&state.count))
	runtime.GC()
	require.Equal(t, maxCount, atomic.LoadInt32(&state.count))
	runtime.GC()
	require.Equal(t, maxCount, atomic.LoadInt32(&state.count))
}
