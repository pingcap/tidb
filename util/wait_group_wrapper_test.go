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
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestWaitGroupWrapperRun(t *testing.T) {
	var expect int32 = 4
	var val atomic.Int32
	var wg WaitGroupWrapper
	for i := int32(0); i < expect; i++ {
		wg.Run(func() {
			val.Inc()
		})
	}
	wg.Wait()
	require.Equal(t, expect, val.Load())
}

func TestWaitGroupWrapperRunWithRecover(t *testing.T) {
	var expect int32 = 2
	var val atomic.Int32
	var wg WaitGroupWrapper
	for i := int32(0); i < expect; i++ {
		wg.RunWithRecover(func() {
			panic("test1")
		}, func(r interface{}) {
			val.Inc()
		})
	}
	wg.Wait()
	require.Equal(t, expect, val.Load())
}
