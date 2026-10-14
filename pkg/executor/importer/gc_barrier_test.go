// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package importer

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/stretchr/testify/require"
)

func TestQueryGCBarrier(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	ctx := context.Background()
	ctl := store.(kv.StorageWithPD).GetPDClient().GetGCInternalController(uint32(store.GetCodec().GetKeyspaceID()))
	ver, err := store.CurrentVersion(kv.GlobalTxnScope)
	require.NoError(t, err)
	readTS := ver.Ver

	require.NoError(t, SetQueryGCBarrier(ctx, store, readTS, time.Hour))
	res, err := ctl.AdvanceTxnSafePoint(ctx, readTS+100)
	require.NoError(t, err)
	require.Equal(t, readTS, res.NewTxnSafePoint)

	require.NoError(t, DeleteQueryGCBarrier(ctx, store, readTS))
	res, err = ctl.AdvanceTxnSafePoint(ctx, readTS+100)
	require.NoError(t, err)
	require.Equal(t, readTS+100, res.NewTxnSafePoint)
}
