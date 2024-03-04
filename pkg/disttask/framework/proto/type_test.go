// Copyright 2024 PingCAP, Inc.
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

package proto

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTaskType(t *testing.T) {
	cases := []struct {
		tp  TaskType
		val int
	}{
		{TaskTypeExample, 1},
		{ImportInto, 2},
		{Backfill, 3},
		{"", 0},
	}
	for _, c := range cases {
		require.Equal(t, c.val, Type2Int(c.tp))
	}

	for _, c := range cases {
		require.Equal(t, c.tp, Int2Type(c.val))
	}
}
