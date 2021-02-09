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
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/telemetry"
	"go.etcd.io/etcd/integration"
)

var _ = Suite(&testSuite{})

type testSuite struct {
	store       kv.Storage
	dom         *domain.Domain
	etcdCluster *integration.ClusterV3
	se          session.Session
}

var telemetryTestT *testing.T

func TestT(t *testing.T) {
	telemetryTestT = t
	TestingT(t)
}

func (s *testSuite) SetUpSuite(c *C) {
	if runtime.GOOS == "windows" {
		c.Skip("integration.NewClusterV3 will create file contains a colon which is not allowed on Windows")
	}
	store, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)

	session.SetSchemaLease(0)
	session.DisableStats4Test()
	dom, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)

	etcdCluster := integration.NewClusterV3(telemetryTestT, &integration.ClusterConfig{Size: 1})

	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)

	s.store = store
	s.dom = dom
	s.etcdCluster = etcdCluster
	s.se = se
}

func (s *testSuite) TearDownSuite(c *C) {
	if runtime.GOOS == "windows" {
		return
	}
	s.se.Close()
	s.etcdCluster.Terminate(telemetryTestT)
	s.dom.Close()
	s.store.Close()
}

func (s *testSuite) Test01TrackingID(c *C) {
	id, err := telemetry.GetTrackingID(s.etcdCluster.RandClient())
	c.Assert(err, IsNil)
	c.Assert(id, Equals, "")

	id2, err := telemetry.ResetTrackingID(s.etcdCluster.RandClient())
	c.Assert(err, IsNil)
	c.Assert(id2 != "", Equals, true)

	id3, err := telemetry.GetTrackingID(s.etcdCluster.RandClient())
	c.Assert(err, IsNil)
	c.Assert(id3, Equals, id2)
}

func (s *testSuite) Test02Preview(c *C) {
	config.GetGlobalConfig().EnableTelemetry = false
	r, err := telemetry.PreviewUsageData(s.se, s.etcdCluster.RandClient())
	c.Assert(err, IsNil)
	c.Assert(r, Equals, "")

	trackingID, err := telemetry.ResetTrackingID(s.etcdCluster.RandClient())
	c.Assert(err, IsNil)

	config.GetGlobalConfig().EnableTelemetry = true
	r, err = telemetry.PreviewUsageData(s.se, s.etcdCluster.RandClient())
	c.Assert(err, IsNil)

	jsonParsed, err := gabs.ParseJSON([]byte(r))
	c.Assert(err, IsNil)

	c.Assert(jsonParsed.Path("trackingId").Data().(string), Equals, trackingID)
	c.Assert(jsonParsed.ExistsP("hostExtra.cpuFlags"), Equals, true)
	c.Assert(jsonParsed.ExistsP("hostExtra.os"), Equals, true)
	c.Assert(jsonParsed.Path("instances").Children(), HasLen, 2)
	c.Assert(jsonParsed.Path("instances.0.instanceType").Data().(string), Equals, "tidb")
	c.Assert(jsonParsed.Path("instances.1.instanceType").Data().(string), Equals, "tikv") // mocktikv
	c.Assert(jsonParsed.ExistsP("hardware"), Equals, true)

	_, err = s.se.Execute(context.Background(), "SET @@global.tidb_enable_telemetry = 0")
	c.Assert(err, IsNil)
	r, err = telemetry.PreviewUsageData(s.se, s.etcdCluster.RandClient())
	c.Assert(err, IsNil)
	c.Assert(r, Equals, "")

	_, err = s.se.Execute(context.Background(), "SET @@global.tidb_enable_telemetry = 1")
	config.GetGlobalConfig().EnableTelemetry = false
	c.Assert(err, IsNil)
	r, err = telemetry.PreviewUsageData(s.se, s.etcdCluster.RandClient())
	c.Assert(err, IsNil)
	c.Assert(r, Equals, "")
}

func (s *testSuite) Test03Report(c *C) {
	config.GetGlobalConfig().EnableTelemetry = false
	err := telemetry.ReportUsageData(s.se, s.etcdCluster.RandClient())
	c.Assert(err, IsNil)

	status, err := telemetry.GetTelemetryStatus(s.etcdCluster.RandClient())
	c.Assert(err, IsNil)

	jsonParsed, err := gabs.ParseJSON([]byte(status))
	c.Assert(err, IsNil)

	c.Assert(jsonParsed.Path("is_error").Data().(bool), Equals, true)
	c.Assert(jsonParsed.Path("error_msg").Data().(string), Equals, "telemetry is disabled")
	c.Assert(jsonParsed.Path("is_request_sent").Data().(bool), Equals, false)
}
