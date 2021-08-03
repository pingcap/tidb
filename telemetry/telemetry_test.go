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

package telemetry_test

import (
	"context"
	"runtime"
	"testing"

	"github.com/Jeffail/gabs/v2"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/telemetry"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/integration"
)

func TestTrackingID(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create file contains a colon which is not allowed on Windows")
	}

	t.Parallel()

	s := newSuite(t)
	defer s.close()

	id, err := telemetry.GetTrackingID(s.etcdCluster.RandClient())
	require.NoError(t, err)
	require.Equal(t, "", id)

	id2, err := telemetry.ResetTrackingID(s.etcdCluster.RandClient())
	require.NoError(t, err)
	require.NotEqual(t, "", id2)

	id3, err := telemetry.GetTrackingID(s.etcdCluster.RandClient())
	require.NoError(t, err)
	require.Equal(t, id2, id3)
}

func TestPreview(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create file contains a colon which is not allowed on Windows")
	}

	s := newSuite(t)
	defer s.close()

	config.GetGlobalConfig().EnableTelemetry = false
	r, err := telemetry.PreviewUsageData(s.se, s.etcdCluster.RandClient())
	require.NoError(t, err)
	require.Equal(t, "", r)

	trackingID, err := telemetry.ResetTrackingID(s.etcdCluster.RandClient())
	require.NoError(t, err)

	config.GetGlobalConfig().EnableTelemetry = true
	r, err = telemetry.PreviewUsageData(s.se, s.etcdCluster.RandClient())
	require.NoError(t, err)

	jsonParsed, err := gabs.ParseJSON([]byte(r))
	require.NoError(t, err)
	require.Equal(t, trackingID, jsonParsed.Path("trackingId").Data().(string))
	// Apple M1 doesn't contain cpuFlags
	if !(runtime.GOARCH == "arm" && runtime.GOOS == "darwin") {
		require.True(t, jsonParsed.ExistsP("hostExtra.cpuFlags"))
	}
	require.True(t, jsonParsed.ExistsP("hostExtra.os"))
	require.Len(t, jsonParsed.Path("instances").Children(), 2)
	require.Equal(t, "tidb", jsonParsed.Path("instances.0.instanceType").Data().(string))
	require.Equal(t, "tikv", jsonParsed.Path("instances.1.instanceType").Data().(string))
	require.True(t, jsonParsed.ExistsP("hardware"))

	_, err = s.se.Execute(context.Background(), "SET @@global.tidb_enable_telemetry = 0")
	require.NoError(t, err)
	r, err = telemetry.PreviewUsageData(s.se, s.etcdCluster.RandClient())
	require.NoError(t, err)
	require.Equal(t, "", r)

	_, err = s.se.Execute(context.Background(), "SET @@global.tidb_enable_telemetry = 1")
	config.GetGlobalConfig().EnableTelemetry = false
	require.NoError(t, err)

	r, err = telemetry.PreviewUsageData(s.se, s.etcdCluster.RandClient())
	require.NoError(t, err)
	require.Equal(t, "", r)
}

func TestReport(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create file contains a colon which is not allowed on Windows")
	}

	s := newSuite(t)
	defer s.close()

	config.GetGlobalConfig().EnableTelemetry = false
	err := telemetry.ReportUsageData(s.se, s.etcdCluster.RandClient())
	require.NoError(t, err)

	status, err := telemetry.GetTelemetryStatus(s.etcdCluster.RandClient())
	require.NoError(t, err)

	jsonParsed, err := gabs.ParseJSON([]byte(status))
	require.NoError(t, err)
	require.True(t, jsonParsed.Path("is_error").Data().(bool))
	require.Equal(t, "telemetry is disabled", jsonParsed.Path("error_msg").Data().(string))
	require.False(t, jsonParsed.Path("is_request_sent").Data().(bool))
}

type testSuite struct {
	store       kv.Storage
	dom         *domain.Domain
	etcdCluster *integration.ClusterV3
	se          session.Session
	close       func()
}

func newSuite(t *testing.T) *testSuite {
	suite := new(testSuite)

	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	suite.store = store

	session.SetSchemaLease(0)
	session.DisableStats4Test()

	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)
	suite.dom = dom

	etcdCluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	suite.etcdCluster = etcdCluster

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	suite.se = se

	suite.close = func() {
		suite.se.Close()
		suite.etcdCluster.Terminate(t)
		suite.dom.Close()
		err = suite.store.Close()
		require.NoError(t, err)
	}

	return suite
}
