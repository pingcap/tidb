// Copyright 2019 PingCAP, Inc.
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

package ddl_test

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestOptions(t *testing.T) {
	client, err := clientv3.NewFromURL("test")
	require.NoError(t, err)
	defer func() {
		err := client.Close()
		require.NoError(t, err)
	}()
	callback := &ddl.BaseCallback{}
	lease := time.Second * 3
	store := &mock.Store{}
	infoHandle := infoschema.NewCache(16)

	options := []ddl.Option{
		ddl.WithEtcdClient(client),
		ddl.WithHook(callback),
		ddl.WithLease(lease),
		ddl.WithStore(store),
		ddl.WithInfoCache(infoHandle),
	}

	opt := &ddl.Options{}
	for _, o := range options {
		o(opt)
	}

	require.Equal(t, client, opt.EtcdCli)
	require.Equal(t, callback, opt.Hook)
	require.Equal(t, lease, opt.Lease)
	require.Equal(t, store, opt.Store)
	require.Equal(t, infoHandle, opt.InfoCache)
}
