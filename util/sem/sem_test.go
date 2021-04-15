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
// See the License for the specific language governing permissions and
// limitations under the License.

package sem

import (
	"testing"

	"github.com/pingcap/parser/mysql"

	. "github.com/pingcap/check"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testSecurity{})

type testSecurity struct{}

func (s *testSecurity) TestInvisibleSchema(c *C) {
	c.Assert(IsInvisibleSchema(metricsSchema), IsTrue)
	c.Assert(IsInvisibleSchema("METRICS_ScHEma"), IsTrue)
	c.Assert(IsInvisibleSchema("mysql"), IsFalse)
	c.Assert(IsInvisibleSchema(informationSchema), IsFalse)
	c.Assert(IsInvisibleSchema("Bogusname"), IsFalse)
}

func (s *testSecurity) TestIsInvisibleTable(c *C) {
	mysqlTbls := []string{exprPushdownBlacklist, gcDeleteRange, gcDeleteRangeDone, optRuleBlacklist, tidb, globalVariables}
	infoSchemaTbls := []string{clusterConfig, clusterHardware, clusterLoad, clusterLog, clusterSystemInfo, inspectionResult,
		inspectionRules, inspectionSummary, metricsSummary, metricsSummaryByLabel, metricsTables, tidbHotRegions}
	perfSChemaTbls := []string{pdProfileAllocs, pdProfileBlock, pdProfileCPU, pdProfileGoroutines, pdProfileMemory,
		pdProfileMutex, tidbProfileAllocs, tidbProfileBlock, tidbProfileCPU, tidbProfileGoroutines,
		tidbProfileMemory, tidbProfileMutex, tikvProfileCPU}

	for _, tbl := range mysqlTbls {
		c.Assert(IsInvisibleTable(mysql.SystemDB, tbl), IsTrue)
	}
	for _, tbl := range infoSchemaTbls {
		c.Assert(IsInvisibleTable(informationSchema, tbl), IsTrue)
	}
	for _, tbl := range perfSChemaTbls {
		c.Assert(IsInvisibleTable(performanceSchema, tbl), IsTrue)
	}

	c.Assert(IsInvisibleTable(metricsSchema, "acdc"), IsTrue)
	c.Assert(IsInvisibleTable(metricsSchema, "fdsgfd"), IsTrue)
	c.Assert(IsInvisibleTable("test", "t1"), IsFalse)
}

func (s *testSecurity) TestIsRestrictedPrivilege(c *C) {
	c.Assert(IsRestrictedPrivilege("RESTRICTED_TABLES_ADMIN"), IsTrue)
	c.Assert(IsRestrictedPrivilege("RESTRICTED_STATUS_VARIABLES_ADMIN"), IsTrue)
	c.Assert(IsRestrictedPrivilege("CONNECTION_ADMIN"), IsFalse)
	c.Assert(IsRestrictedPrivilege("BACKUP_ADMIN"), IsFalse)
	c.Assert(IsRestrictedPrivilege("aa"), IsFalse)
}
