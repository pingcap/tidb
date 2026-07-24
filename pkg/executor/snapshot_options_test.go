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

package executor

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

type recordingSnapshot struct {
	kv.EmptyRetriever
	options map[int]any
}

func (*recordingSnapshot) BatchGet(context.Context, []kv.Key) (map[string][]byte, error) {
	return nil, nil
}

func (s *recordingSnapshot) SetOption(opt int, val any) {
	if s.options == nil {
		s.options = make(map[int]any)
	}
	s.options[opt] = val
}

type recordingSessionOptionsSnapshot struct {
	*recordingSnapshot
	readReplicaScope          string
	taskID                    uint64
	tikvClientReadTimeout     uint64
	resourceGroupName         string
	explicitRequestSourceType string
}

func (s *recordingSessionOptionsSnapshot) SetOptionsForSession(
	readReplicaScope string,
	taskID uint64,
	tikvClientReadTimeout uint64,
	resourceGroupName string,
	explicitRequestSourceType string,
) {
	s.readReplicaScope = readReplicaScope
	s.taskID = taskID
	s.tikvClientReadTimeout = tikvClientReadTimeout
	s.resourceGroupName = resourceGroupName
	s.explicitRequestSourceType = explicitRequestSourceType
}

func TestInitSnapshotWithSessCtxSessionOptions(t *testing.T) {
	ctx := mock.NewContext()
	vars := ctx.GetSessionVars()
	vars.StmtCtx.TaskID = 42
	vars.StmtCtx.ResourceGroupName = "rg1"
	vars.ExplicitRequestSourceType = "explicit"
	vars.TiKVClientReadTimeout = 1234
	readReplicaScope := "zone-a"

	t.Run("typed", func(t *testing.T) {
		snapshot := &recordingSessionOptionsSnapshot{
			recordingSnapshot: &recordingSnapshot{},
		}
		InitSnapshotWithSessCtx(snapshot, ctx, &readReplicaScope)

		require.Empty(t, snapshot.options)
		require.Equal(t, readReplicaScope, snapshot.readReplicaScope)
		require.Equal(t, uint64(42), snapshot.taskID)
		require.Equal(t, uint64(1234), snapshot.tikvClientReadTimeout)
		require.Equal(t, "rg1", snapshot.resourceGroupName)
		require.Equal(t, "explicit", snapshot.explicitRequestSourceType)
	})

	t.Run("fallback", func(t *testing.T) {
		snapshot := &recordingSnapshot{}
		InitSnapshotWithSessCtx(snapshot, ctx, &readReplicaScope)

		require.Equal(t, readReplicaScope, snapshot.options[kv.ReadReplicaScope])
		require.Equal(t, uint64(42), snapshot.options[kv.TaskID])
		require.Equal(t, uint64(1234), snapshot.options[kv.TiKVClientReadTimeout])
		require.Equal(t, "rg1", snapshot.options[kv.ResourceGroupName])
		require.Equal(t, "explicit", snapshot.options[kv.ExplicitRequestSourceType])
	})
}
