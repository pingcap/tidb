// Copyright 2026 PingCAP, Inc.
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

package sem

import (
	"testing"

	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/stretchr/testify/require"
)

func TestRestrictedHint(t *testing.T) {
	sem := buildSEMFromConfig(&Config{
		RestrictedVariables: []VariableRestriction{
			{Name: vardef.TiDBMemQuotaQuery, Hidden: true},
		},
		RestrictedHints: []string{"resource_group", "memory_quota", "max_execution_time"},
	})

	// A hint with no backing variable is restricted unconditionally.
	require.Error(t, sem.isRestrictedHint("resource_group"))
	// A variable-overriding hint whose variable is hidden is restricted.
	require.Error(t, sem.isRestrictedHint("memory_quota"))
	// A variable-overriding hint whose variable is still tunable is allowed.
	require.NoError(t, sem.isRestrictedHint("max_execution_time"))
	// A hint not listed in restricted_hints is allowed.
	require.NoError(t, sem.isRestrictedHint("use_index"))
}
