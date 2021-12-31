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

package server

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBallast(t *testing.T) {
	{
		// 2MB
		max := 1024 * 1024 * 2
		b := newBallast(max)
		var nowSz int
		var info string
		{
			nowSz, info = b.SetSize(-1)
			require.Equal(t, nowSz, max)
			require.True(t, len(info) > 0)
			nowSz, _ = b.SetSize(1024)
			require.Equal(t, nowSz, 1024)
			nowSz, info = b.SetSize(max * 2)
			require.Equal(t, nowSz, max)
			require.True(t, len(info) > 0)
			nowSz, _ = b.SetSize(0)
			require.Equal(t, nowSz, 0)
		}
	}
	{
		// auto
		b := newBallast(0)
		var nowSz int
		var info string
		{
			nowSz, info = b.SetSize(-1)
			require.True(t, len(info) > 0)
			require.True(t, nowSz > 0)
			require.True(t, nowSz <= b.autoMaxSizeThreshold)
		}
	}
}
