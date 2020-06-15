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
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/store/tikv"
)

// newMockTikvStore creates a mocked tikv store, the path is the file path to store the data.
// If path is an empty string, a memory storage will be created.
func newMockTikvStore(opt *mockOptions) (kv.Storage, error) {
	client, cluster, pdClient, err := mocktikv.NewTiKVAndPDClient(opt.path)
	if err != nil {
		return nil, errors.Trace(err)
	}
	opt.clusterInspector(cluster)

	return tikv.NewTestTiKVStore(client, pdClient, opt.clientHijacker, opt.pdClientHijacker, opt.txnLocalLatches)
}
