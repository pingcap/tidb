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

package owner

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/metadata"
)

func TestNewOwnerManagerInjectsClientSource(t *testing.T) {
	manager := NewOwnerManager(context.Background(), nil, "stats", "id", "key")
	internal, ok := manager.(*ownerManager)
	require.True(t, ok)

	md, ok := metadata.FromOutgoingContext(internal.ctx)
	require.True(t, ok)
	require.Equal(t, []string{"stats"}, md.Get(rpctypes.MetadataClientSourceKey))
}
