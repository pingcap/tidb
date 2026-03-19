// Copyright 2025 PingCAP, Inc.
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

package issyncer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDeferFn(t *testing.T) {
	var df deferFn
	var a, b, c, d bool
	df.add(func() { a = true }, time.Now().Add(50*time.Millisecond))
	df.add(func() { b = true }, time.Now().Add(100*time.Millisecond))
	df.add(func() { c = true }, time.Now().Add(10*time.Minute))
	df.add(func() { d = true }, time.Now().Add(150*time.Millisecond))
	time.Sleep(300 * time.Millisecond)
	df.check()

	require.True(t, a)
	require.True(t, b)
	require.False(t, c)
	require.True(t, d)
	require.Len(t, df.data, 1)
}
