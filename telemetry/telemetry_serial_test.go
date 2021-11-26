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

package telemetry_test

import (
	"runtime"
	"testing"

	"github.com/Jeffail/gabs/v2"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/telemetry"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/integration"
)

func TestReport(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create file contains a colon which is not allowed on Windows")
	}

	etcdCluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer etcdCluster.Terminate(t)
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	defer se.Close()

	config.GetGlobalConfig().EnableTelemetry = false
	require.NoError(t, telemetry.ReportUsageData(se, etcdCluster.RandClient()))

	status, err := telemetry.GetTelemetryStatus(etcdCluster.RandClient())
	require.NoError(t, err)

	jsonParsed, err := gabs.ParseJSON([]byte(status))
	require.NoError(t, err)
	require.True(t, jsonParsed.Path("is_error").Data().(bool))
	require.Equal(t, "telemetry is disabled", jsonParsed.Path("error_msg").Data().(string))
	require.False(t, jsonParsed.Path("is_request_sent").Data().(bool))
}
