// Copyright 2021 PingCAP, Inc.
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

package server

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/stretchr/testify/require"
)

func TestUptime(t *testing.T) {
	var err error

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/domain/infosync/mockServerInfo", "return(true)"))
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/domain/infosync/mockServerInfo")
		require.NoError(t, err)
	}()

	store, err := mockstore.NewMockStore()
	require.NoError(t, err)

	dom, err := session.BootstrapSession(store)
	defer func() {
		dom.Close()
		err := store.Close()
		require.NoError(t, err)
	}()
	require.NoError(t, err)

	_, err = infosync.GlobalInfoSyncerInit(context.Background(), dom.DDL().GetID(), dom.ServerID, dom.GetEtcdClient(), true)
	require.NoError(t, err)

	tidbdrv := NewTiDBDriver(store)
	cfg := newTestConfig()
	cfg.Socket = ""
	cfg.Port = 0
	cfg.Status.StatusPort = 0
	server, err := NewServer(cfg, tidbdrv)
	require.NoError(t, err)

	stats, err := server.Stats(nil)
	require.NoError(t, err)
	require.GreaterOrEqual(t, stats[upTime].(int64), int64(time.Since(time.Unix(1282967700, 0)).Seconds()))
}
