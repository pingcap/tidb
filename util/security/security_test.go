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

package security

import (
	"testing"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"

	. "github.com/pingcap/check"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testSecurity{})

type testSecurity struct{}

func (s *testSecurity) TestGetSetSysVars(c *C) {
	svList := variable.GetSysVars()
	c.Assert(IsEnabled(), IsFalse) // default OFF
	for _, name := range secureVarsList {
		_, ok := svList[name]
		c.Assert(ok, IsTrue)
	}
	Enable()
	for _, name := range secureVarsList {
		_, ok := svList[name]
		c.Assert(ok, IsFalse)
	}
	Disable()
	c.Assert(IsEnabled(), IsFalse)
}

func (s *testSecurity) TestIsReadOnlySystemTable(c *C) {

	Enable()
	tbls := []string{bindInfo, columnsPriv, db, defaultRoles, globalPriv, helpTopic,
		roleEdges, schemaIndexUsage, statsBuckets, statsExtended, statsFeedback,
		statsHistograms, statsMeta, statsTopN, tablesPriv, user}

	for _, tbl := range tbls {
		c.Assert(IsReadOnlySystemTable(tbl), IsTrue)
	}

	Disable()

	for _, tbl := range tbls {
		c.Assert(IsReadOnlySystemTable(tbl), IsFalse)
	}

}

func (s *testSecurity) TestInvisibleSchema(c *C) {

	Enable()

	c.Assert(IsInvisibleSchema(metricsSchema), IsTrue)
	c.Assert(IsInvisibleSchema("METRICS_ScHEma"), IsTrue)
	c.Assert(IsInvisibleSchema("mysql"), IsFalse)
	c.Assert(IsInvisibleSchema(informationSchema), IsFalse)
	c.Assert(IsInvisibleSchema("Bogusname"), IsFalse)

	Disable()

	c.Assert(IsInvisibleSchema(metricsSchema), IsFalse)
	c.Assert(IsInvisibleSchema("Bogusname"), IsFalse)

}

func (s *testSecurity) TestIsInvisibleTable(c *C) {

	Enable()

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

	Disable()

	for _, tbl := range mysqlTbls {
		c.Assert(IsInvisibleTable(mysql.SystemDB, tbl), IsFalse)
	}
	for _, tbl := range infoSchemaTbls {
		c.Assert(IsInvisibleTable(informationSchema, tbl), IsFalse)
	}
	for _, tbl := range perfSChemaTbls {
		c.Assert(IsInvisibleTable(performanceSchema, tbl), IsFalse)
	}

	c.Assert(IsInvisibleTable(metricsSchema, "acdc"), IsFalse)
	c.Assert(IsInvisibleTable(metricsSchema, "fdsgfd"), IsFalse)
	c.Assert(IsInvisibleTable("test", "t1"), IsFalse)

}
