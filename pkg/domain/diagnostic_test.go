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

package domain

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config/diagnosticmode"
	"github.com/pingcap/tidb/pkg/ddl/schemaver"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/stretchr/testify/require"
)

func TestStartDiagnostic(t *testing.T) {
	if !intest.InTest {
		t.Skip("requires intest")
	}
	t.Cleanup(diagnosticmode.SetForTest(true))

	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	dom := NewDomain(store, 80*time.Millisecond, 0, 0, mockFactory)
	defer dom.Close()
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	require.Nil(t, dom.ddl)
	require.Nil(t, dom.minJobIDRefresher)

	ctx, cancel := context.WithCancel(context.Background())
	dom.ctx = ctx
	cancel()
	info, err := infosync.GlobalInfoSyncerInit(ctx, "diagnostic", dom.ServerID,
		nil, nil, nil, nil, keyspace.CodecV1, true, dom.infoCache)
	require.NoError(t, err)
	dom.info = info
	schemaSyncer := schemaver.NewMemSyncer()
	require.NoError(t, schemaSyncer.Init(ctx))
	dom.isSyncer.InitRequiredFields(nil, schemaSyncer, nil, nil)

	require.NoError(t, dom.StartDiagnostic())
	require.Nil(t, dom.ddl)
	require.Nil(t, dom.minJobIDRefresher)
}
