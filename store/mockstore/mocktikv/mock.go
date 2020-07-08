// Copyright 2018 PingCAP, Inc.
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

package mocktikv

import (
	"github.com/pingcap/errors"
	pd "github.com/pingcap/pd/v4/client"
)

// NewTiKVAndPDClient creates a TiKV client and PD client from options.
func NewTiKVAndPDClient(path string) (*RPCClient, *Cluster, pd.Client, error) {
	mvccStore, err := NewMVCCLevelDB(path)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}
	cluster := NewCluster(mvccStore)

	return NewRPCClient(cluster, mvccStore), cluster, NewPDClient(cluster), nil
}
