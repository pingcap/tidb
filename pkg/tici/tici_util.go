// Copyright 2025 PingCAP, Inc.
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

package tici

import (
	"time"

	tidb "github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/etcd"
)

const (
	etcdDialTimeout = 5 * time.Second
)

func getEtcdClient() (*etcd.Client, error) {
	tidbCfg := tidb.GetGlobalConfig()
	tls, err := util.NewTLSConfig(
		util.WithCAPath(tidbCfg.Security.ClusterSSLCA),
		util.WithCertAndKeyPath(tidbCfg.Security.ClusterSSLCert, tidbCfg.Security.ClusterSSLKey),
	)
	if err != nil {
		return nil, err
	}
	ectdEndpoints, err := util.ParseHostPortAddr(tidbCfg.Path)
	if err != nil {
		return nil, err
	}
	return etcd.NewClientFromCfg(ectdEndpoints, etcdDialTimeout, "", tls)
}
