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

	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/config"
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
}
