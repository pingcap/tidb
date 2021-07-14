// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package mockstore

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/mockstore/mockstorage"
	"github.com/pingcap/tidb/store/mockstore/unistore"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/util"
)

func newUnistore(opts *mockOptions) (kv.Storage, error) {
	client, pdClient, cluster, err := unistore.New(opts.path)
	if err != nil {
		return nil, errors.Trace(err)
	}
	opts.clusterInspector(cluster)
	pdClient = util.InterceptedPDClient{
		Client: pdClient,
	}

	kvstore, err := tikv.NewTestTiKVStore(newClientRedirector(client), pdClient, opts.clientHijacker, opts.pdClientHijacker, opts.txnLocalLatches)
	if err != nil {
		return nil, err
	}
	return mockstorage.NewMockStorage(kvstore)
}
