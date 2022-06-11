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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mockstore

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/mockstore/mockcopr"
	"github.com/pingcap/tidb/store/mockstore/mockstorage"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
)

// newMockTikvStore creates a mocked tikv store, the path is the file path to store the data.
// If path is an empty string, a memory storage will be created.
func newMockTikvStore(opt *mockOptions) (kv.Storage, error) {
	client, cluster, pdClient, err := testutils.NewMockTiKV(opt.path, mockcopr.NewCoprRPCHandler())
	if err != nil {
		return nil, errors.Trace(err)
	}
	opt.clusterInspector(cluster)

	kvstore, err := tikv.NewTestTiKVStore(newClientRedirector(client), pdClient, opt.clientHijacker, opt.pdClientHijacker, opt.txnLocalLatches)
	if err != nil {
		return nil, err
	}
	return mockstorage.NewMockStorage(kvstore)
}
