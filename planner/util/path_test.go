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
)

func TestCompareCol2Len(t *testing.T) {
	tests := []struct {
		c1         Col2Len
		c2         Col2Len
		res        int
		comparable bool
	}{
		{
			c1:         Col2Len{1: -1, 2: -1, 3: -1},
			c2:         Col2Len{1: -1, 2: 10},
			res:        1,
			comparable: true,
		},
		{
			c1:         Col2Len{1: 5},
			c2:         Col2Len{1: 10, 2: -1},
			res:        -1,
			comparable: true,
		},
		{
			c1:         Col2Len{1: -1, 2: -1},
			c2:         Col2Len{1: -1, 2: 5, 3: -1},
			res:        0,
			comparable: false,
		},
		{
			c1:         Col2Len{1: -1, 2: 10},
			c2:         Col2Len{1: -1, 2: 5, 3: -1},
			res:        0,
			comparable: false,
		},
		{
			c1:         Col2Len{1: -1, 2: 10},
			c2:         Col2Len{1: -1, 2: 10},
			res:        0,
			comparable: true,
		},
		{
			c1:         Col2Len{1: -1, 2: -1},
			c2:         Col2Len{1: -1, 2: 10},
			res:        0,
			comparable: false,
		},
	}
	for _, tt := range tests {
		res, comparable := CompareCol2Len(tt.c1, tt.c2)
		require.Equal(t, tt.res, res)
		require.Equal(t, tt.comparable, comparable)
	}
}
