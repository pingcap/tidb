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
// See the License for the specific language governing permissions and
// limitations under the License.

package pkdbspfreshbulk

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRefinementPlanCanonicalContract(t *testing.T) {
	golden, err := os.ReadFile("testdata/rust_refinement_plan.json")
	require.NoError(t, err)
	frozen, err := DecodeRefinementPlanJSON(golden)
	require.NoError(t, err)
	require.Equal(t, uint32(2), frozen.ChildCount)
	encoded, err := EncodeRefinementPlanJSON(frozen)
	require.NoError(t, err)
	require.Equal(t, golden, encoded)
	for _, change := range []struct{ old, new string }{
		{`"child_count":2`, `"child_count":0`},
		{`"child_count":2`, `"child_count":3`},
		{`"child_count":2,`, ``},
		{`"target_rows":100,`, ``},
		{`"memory_budget_bytes":1073741824`, `"memory_budget_bytes":0`},
		{`"child_count":2`, `"child_count":2,"first_shard_id":0`},
		{`["AACAPwAAAAA=","AAAAAAAAgD8="]`, `null`},
		{`"centroids_f32_le_base64":["AACAPwAAAAA=","AAAAAAAAgD8="]`, `"centroids_f32_le_base64":[]`},
	} {
		_, err := DecodeRefinementPlanJSON(bytes.Replace(golden, []byte(change.old), []byte(change.new), 1))
		require.Error(t, err, change.old)
	}
	frozen.ChildCount = 1
	frozen.CentroidsF32LEBase64 = []string{}
	encoded, err = EncodeRefinementPlanJSON(frozen)
	require.NoError(t, err)
	_, err = DecodeRefinementPlanJSON(encoded)
	require.NoError(t, err)
}
