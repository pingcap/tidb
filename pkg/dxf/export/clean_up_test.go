// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package export

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/planner/extstore"
	"github.com/stretchr/testify/require"
)

func TestExportCleaner(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStorage()
	extstore.SetGlobalExtStorageForTest(store)
	t.Cleanup(func() { extstore.SetGlobalExtStorageForTest(nil) })

	taskFiles := []string{
		"1/plan/prepared/meta.json",
		"1/plan/dump/1/meta.json",
	}
	for _, name := range append(taskFiles, "10/plan/prepared/meta.json") {
		require.NoError(t, store.WriteFile(ctx, name, []byte("meta")))
	}

	cleaner := &exportCleaner{}
	require.NoError(t, cleaner.Clean(ctx, &proto.Task{TaskBase: proto.TaskBase{ID: 1}}))
	for _, name := range taskFiles {
		exists, err := store.FileExists(ctx, name)
		require.NoError(t, err)
		require.False(t, exists)
	}
	exists, err := store.FileExists(ctx, "10/plan/prepared/meta.json")
	require.NoError(t, err)
	require.True(t, exists)

	// Cleanup is retry-safe after the task prefix has already been removed.
	require.NoError(t, cleaner.Clean(ctx, &proto.Task{TaskBase: proto.TaskBase{ID: 1}}))
}
