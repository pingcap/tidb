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

package external

import (
	"context"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"
)

func TestMergeOverlappingFiles(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/br/pkg/lightning/backend/external/mergeOverlappingFilesImpl", `return("a")`))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/br/pkg/lightning/backend/external/mergeOverlappingFilesImpl"))
	})
	require.ErrorContains(t, MergeOverlappingFiles(
		context.Background(),
		[]string{"a", "b", "c", "d", "e"},
		nil,
		1,
		1,
		"",
		1,
		1,
		1,
		1,
		nil,
		5,
		false,
	), "injected error")
}
