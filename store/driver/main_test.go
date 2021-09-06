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

package driver

import (
	"testing"

	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/copr"
	"github.com/pingcap/tidb/store/mockstore/unistore"
	"github.com/pingcap/tidb/util/testbridge"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	testbridge.WorkaroundGoCheckFlags()
	tikv.EnableFailpoints()
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("go.etcd.io/etcd/pkg/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	}
	goleak.VerifyTestMain(m, opts...)
}

func createTestStore(t *testing.T) (kv.Storage, *domain.Domain, func()) {
	client, pdClient, cluster, err := unistore.New("")
	require.NoError(t, err)

	unistore.BootstrapWithSingleStore(cluster)
	kvStore, err := tikv.NewTestTiKVStore(client, pdClient, nil, nil, 0)
	require.NoError(t, err)

	coprStore, err := copr.NewStore(kvStore, nil)
	require.NoError(t, err)

	store := &tikvStore{KVStore: kvStore, coprStore: coprStore}
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)

	clean := func() {
		dom.Close()
		require.NoError(t, store.Close())
	}

	return store, dom, clean
}
