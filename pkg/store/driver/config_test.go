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
	"time"

	metricscommon "github.com/pingcap/tidb/pkg/metrics/common"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/pd/client/opt"
)

func TestSetDefaultAndOptions(t *testing.T) {
	globalConfig := config.GetGlobalConfig()
	origSecurity := globalConfig.Security

	d := TiKVDriver{}
	security := config.Security{ClusterSSLCA: "test"}
	d.setDefaultAndOptions(WithSecurity(security))

	require.Equal(t, security, d.security)
	require.Equal(t, globalConfig.TiKVClient, d.tikvConfig)
	require.Equal(t, globalConfig.TxnLocalLatches, d.txnLocalLatches)
	require.Equal(t, globalConfig.PDClient, d.pdConfig)
	require.Equal(t, origSecurity, config.GetGlobalConfig().Security)

	metricscommon.SetConstLabels("keyspace_id", "42", "keyspace_name", "ks")
	t.Cleanup(func() {
		metricscommon.SetConstLabels()
	})
	pdOpt := opt.NewOption()
	for _, apply := range d.pdClientOptions() {
		apply(pdOpt)
	}
	require.Equal(t, metricscommon.GetConstLabels(), pdOpt.MetricsLabels)
	require.Equal(t, time.Duration(float64(time.Millisecond)*vardef.DefTiDBTSOClientBatchMaxWaitTime), pdOpt.GetMaxTSOBatchWaitInterval())
	require.Equal(t, vardef.DefTiDBEnableTSOFollowerProxy, pdOpt.GetEnableTSOFollowerProxy())
	require.Equal(t, vardef.DefPDEnableFollowerHandleRegion, pdOpt.GetEnableFollowerHandle())
	require.Equal(t, 1, pdOpt.GetTSOClientRPCConcurrency())
	require.Equal(t, vardef.DefTiDBEnableBatchQueryRegion, pdOpt.GetEnableRouterClient())
}
