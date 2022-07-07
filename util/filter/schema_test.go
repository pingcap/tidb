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

package filter

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsSystemSchema(t *testing.T) {
	cases := []struct {
		name     string
		expected bool
	}{
		{"information_schema", true},
		{"performance_schema", true},
		{"mysql", true},
		{"sys", true},
		{"INFORMATION_SCHEMA", true},
		{"PERFORMANCE_SCHEMA", true},
		{"MYSQL", true},
		{"SYS", true},
		{"not_system_schema", false},
		{"METRICS_SCHEMA", true},
		{"INSPECTION_SCHEMA", true},
	}

	for _, tt := range cases {
		require.Equalf(t, tt.expected, IsSystemSchema(tt.name), "schema name = %s", tt.name)
	}
}
