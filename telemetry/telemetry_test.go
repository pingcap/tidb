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

package telemetry_test

import (
	"context"
	"runtime"
	"testing"

	"github.com/Jeffail/gabs/v2"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/telemetry"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/tests/v3/integration"
)

func TestTrackingID(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create file contains a colon which is not allowed on Windows")
	}
	integration.BeforeTestExternal(t)

	etcdCluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer etcdCluster.Terminate(t)

	id, err := telemetry.GetTrackingID(etcdCluster.RandClient())
	require.NoError(t, err)
	require.Equal(t, "", id)

	id2, err := telemetry.ResetTrackingID(etcdCluster.RandClient())
	require.NoError(t, err)
	require.NotEqual(t, "", id2)

	id3, err := telemetry.GetTrackingID(etcdCluster.RandClient())
	require.NoError(t, err)
	require.Equal(t, id2, id3)
}

func TestPreview(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create file contains a colon which is not allowed on Windows")
	}
	integration.BeforeTestExternal(t)

	etcdCluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer etcdCluster.Terminate(t)
	store := testkit.CreateMockStore(t)
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	defer se.Close()

	config.GetGlobalConfig().EnableTelemetry = false
	r, err := telemetry.PreviewUsageData(se, etcdCluster.RandClient())
	require.NoError(t, err)
	require.Equal(t, "", r)

	// By disableing telemetry by default, the global sysvar **and** config file defaults
	// are all set to false, so that enabling telemetry in test become more complex.
	// As telemetry is a feature that almost no user will manually enable, I'd remove these
	// tests for now.
	// They should be uncommented once the default behavious changed back to enabled in the
	// future, otherwise they could just be deleted.
	/*
		trackingID, err := telemetry.ResetTrackingID(etcdCluster.RandClient())
		require.NoError(t, err)

		config.GetGlobalConfig().EnableTelemetry = true
		telemetryEnabled, err := telemetry.IsTelemetryEnabled(se)
		require.NoError(t, err)
		require.True(t, telemetryEnabled)
		r, err = telemetry.PreviewUsageData(se, etcdCluster.RandClient())
		require.NoError(t, err)

		jsonParsed, err := gabs.ParseJSON([]byte(r))
		require.NoError(t, err)
		require.Equal(t, trackingID, jsonParsed.Path("trackingId").Data().(string))
		// Apple M1 doesn't contain cpuFlags
		if !(runtime.GOARCH == "arm64" && runtime.GOOS == "darwin") {
			require.True(t, jsonParsed.ExistsP("hostExtra.cpuFlags"))
		}
		require.True(t, jsonParsed.ExistsP("hostExtra.os"))
		require.Len(t, jsonParsed.Path("instances").Children(), 2)
		require.Equal(t, "tidb", jsonParsed.Path("instances.0.instanceType").Data().(string))
		require.Equal(t, "tikv", jsonParsed.Path("instances.1.instanceType").Data().(string))
		require.True(t, jsonParsed.ExistsP("hardware"))
	*/

	_, err = se.Execute(context.Background(), "SET @@global.tidb_enable_telemetry = 0")
	require.NoError(t, err)
	r, err = telemetry.PreviewUsageData(se, etcdCluster.RandClient())
	require.NoError(t, err)
	require.Equal(t, "", r)

	_, err = se.Execute(context.Background(), "SET @@global.tidb_enable_telemetry = 1")
	config.GetGlobalConfig().EnableTelemetry = false
	require.NoError(t, err)

	r, err = telemetry.PreviewUsageData(se, etcdCluster.RandClient())
	require.NoError(t, err)
	require.Equal(t, "", r)
}

func TestReport(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create file contains a colon which is not allowed on Windows")
	}
	integration.BeforeTestExternal(t)

	etcdCluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer etcdCluster.Terminate(t)
	store := testkit.CreateMockStore(t)
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
