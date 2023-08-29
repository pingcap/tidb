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

package handle

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenerateDuplicateTablesMessage(t *testing.T) {
	tests := []struct {
		name          string
		totalTableIDs []int64
		dupTables     []string
		expectedMsg   string
	}{
		{
			name:          "no duplicate tables",
			totalTableIDs: []int64{1, 2, 3},
			expectedMsg:   "",
		},
		{
			name:          "one duplicate table",
			totalTableIDs: []int64{1},
			dupTables:     []string{"t1"},
			expectedMsg:   "skip locking locked table: t1",
		},
		{
			name:          "multiple duplicate tables",
			totalTableIDs: []int64{1, 2, 3, 4},
			dupTables:     []string{"t1", "t2", "t3"},
			expectedMsg:   "skip locking locked tables: t1, t2, t3, other tables locked successfully",
		},
		{
			name:          "all tables are duplicate",
			totalTableIDs: []int64{1, 2, 3, 4},
			dupTables:     []string{"t1", "t2", "t3", "t4"},
			expectedMsg:   "skip locking locked tables: t1, t2, t3, t4",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := generateDuplicateTablesMessage(tt.totalTableIDs, tt.dupTables)
			require.Equal(t, tt.expectedMsg, msg)
		})
	}
}
