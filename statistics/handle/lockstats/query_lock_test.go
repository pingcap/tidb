// Copyright 2023 PingCAP, Inc.
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

package lockstats

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetTablesLockedStatuses(t *testing.T) {
	tests := []struct {
		name        string
		tableLocked map[int64]struct{}
		tableIDs    []int64
		want        map[int64]bool
	}{
		{
			name:        "not locked",
			tableLocked: map[int64]struct{}{},
			tableIDs:    []int64{1, 2, 3},
			want: map[int64]bool{
				1: false,
				2: false,
				3: false,
			},
		},
		{
			name:        "locked",
			tableLocked: map[int64]struct{}{1: {}, 2: {}},
			tableIDs:    []int64{1, 2, 3},
			want: map[int64]bool{
				1: true,
				2: true,
				3: false,
			},
		},
		{
			name:        "empty",
			tableLocked: map[int64]struct{}{},
			tableIDs:    []int64{},
			want:        map[int64]bool{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetTablesLockedStatuses(tt.tableLocked, tt.tableIDs...)
			require.Equal(t, tt.want, got)
		})
	}
}
