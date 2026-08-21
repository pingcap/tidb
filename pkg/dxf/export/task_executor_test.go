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

package export

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/planner/extstore"
	"github.com/stretchr/testify/require"
)

// TestDecodeSubtaskMetaHydratesExternalChunks replays the exact production
// path: the scheduler's marshalSubtasks writes a subtask row with Chunks
// stripped out plus an external chunk file, and decodeSubtaskMeta (used by
// RunSubtask) must reconstruct Chunks, not silently leave it empty.
func TestDecodeSubtaskMetaHydratesExternalChunks(t *testing.T) {
	store := objstore.NewMemStorage()
	extstore.SetGlobalExtStorageForTest(store)
	t.Cleanup(func() { extstore.SetGlobalExtStorageForTest(nil) })
	ctx := context.Background()

	chunks := []Chunk{
		{TableIdx: 0, PhysicalID: 100, Start: []byte("a"), End: []byte("b"), Size: 42, Ordinal: 0},
		{TableIdx: 1, PhysicalID: 200, Start: []byte("c"), End: []byte("d"), Size: 7, Ordinal: 0},
	}
	metas, err := marshalSubtasks(ctx, 99, proto.ExportStepDump, [][]Chunk{chunks})
	require.NoError(t, err)
	require.Len(t, metas, 1)

	stMeta, err := decodeSubtaskMeta(ctx, &proto.Subtask{Meta: metas[0]})
	require.NoError(t, err)
	require.Equal(t, chunks, stMeta.Chunks)
}
