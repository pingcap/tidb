// Copyright 2019 PingCAP, Inc.
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

package core_test

import (
	"context"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/set"
)

var _ = Suite(&extractorSuite{})

type extractorSuite struct {
	store kv.Storage
	dom   *domain.Domain
}

func (s *extractorSuite) SetUpSuite(c *C) {
	store, err := mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	c.Assert(store, NotNil)

	session.SetSchemaLease(0)
	session.DisableStats4Test()
	dom, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	c.Assert(dom, NotNil)

	s.store = store
	s.dom = dom
}

func (s *extractorSuite) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
}

func (s *extractorSuite) TestClusterConfigTableExtractor(c *C) {
	se, err := session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)

	parser := parser.New()
	var cases = []struct {
		sql         string
		nodeTypes   set.StringSet
		addresses   set.StringSet
		skipRequest bool
	}{
		{
			sql:       "select * from information_schema.cluster_config",
			nodeTypes: nil,
			addresses: nil,
		},
		{
			sql:       "select * from information_schema.cluster_config where type='tikv'",
			nodeTypes: set.NewStringSet("tikv"),
			addresses: set.NewStringSet(),
		},
		{
			sql:       "select * from information_schema.cluster_config where 'tikv'=type",
			nodeTypes: set.NewStringSet("tikv"),
			addresses: set.NewStringSet(),
		},
		{
			sql:       "select * from information_schema.cluster_config where 'TiKV'=type",
			nodeTypes: set.NewStringSet("tikv"),
			addresses: set.NewStringSet(),
		},
		{
			sql:       "select * from information_schema.cluster_config where type in ('tikv', 'pd')",
			nodeTypes: set.NewStringSet("tikv", "pd"),
			addresses: set.NewStringSet(),
		},
		{
			sql:       "select * from information_schema.cluster_config where type in ('tikv', 'pd') and address='123.1.1.2:1234'",
			nodeTypes: set.NewStringSet("tikv", "pd"),
			addresses: set.NewStringSet("123.1.1.2:1234"),
		},
		{
			sql:       "select * from information_schema.cluster_config where type in ('tikv', 'pd') and address in ('123.1.1.2:1234', '123.1.1.4:1234')",
			nodeTypes: set.NewStringSet("tikv", "pd"),
			addresses: set.NewStringSet("123.1.1.2:1234", "123.1.1.4:1234"),
		},
		{
			sql:       "select * from information_schema.cluster_config where type='tikv' and address in ('123.1.1.2:1234', '123.1.1.4:1234')",
			nodeTypes: set.NewStringSet("tikv"),
			addresses: set.NewStringSet("123.1.1.2:1234", "123.1.1.4:1234"),
		},
		{
			sql:       "select * from information_schema.cluster_config where type='tikv' and address='123.1.1.4:1234'",
			nodeTypes: set.NewStringSet("tikv"),
			addresses: set.NewStringSet("123.1.1.4:1234"),
		},
		{
			sql:       "select * from information_schema.cluster_config where type='tikv' and address='123.1.1.4:1234'",
			nodeTypes: set.NewStringSet("tikv"),
			addresses: set.NewStringSet("123.1.1.4:1234"),
		},
		{
			sql:       "select * from information_schema.cluster_config where type='tikv' and address='cNs2dm.tikv.pingcap.com:1234'",
			nodeTypes: set.NewStringSet("tikv"),
			addresses: set.NewStringSet("cNs2dm.tikv.pingcap.com:1234"),
		},
		{
			sql:       "select * from information_schema.cluster_config where type='TIKV' and address='cNs2dm.tikv.pingcap.com:1234'",
			nodeTypes: set.NewStringSet("tikv"),
			addresses: set.NewStringSet("cNs2dm.tikv.pingcap.com:1234"),
		},
		{
			sql:         "select * from information_schema.cluster_config where type='tikv' and type='pd'",
			nodeTypes:   set.NewStringSet(),
			addresses:   set.NewStringSet(),
			skipRequest: true,
		},
		{
			sql:       "select * from information_schema.cluster_config where type='tikv' and type in ('pd', 'tikv')",
			nodeTypes: set.NewStringSet("tikv"),
			addresses: set.NewStringSet(),
		},
		{
			sql:         "select * from information_schema.cluster_config where type='tikv' and type in ('pd', 'tidb')",
			nodeTypes:   set.NewStringSet(),
			addresses:   set.NewStringSet(),
			skipRequest: true,
		},
		{
			sql:       "select * from information_schema.cluster_config where type in ('tikv', 'tidb') and type in ('pd', 'tidb')",
			nodeTypes: set.NewStringSet("tidb"),
			addresses: set.NewStringSet(),
		},
		{
			sql:         "select * from information_schema.cluster_config where address='123.1.1.4:1234' and address='123.1.1.5:1234'",
			nodeTypes:   set.NewStringSet(),
			addresses:   set.NewStringSet(),
			skipRequest: true,
		},
		{
			sql:       "select * from information_schema.cluster_config where address='123.1.1.4:1234' and address in ('123.1.1.5:1234', '123.1.1.4:1234')",
			nodeTypes: set.NewStringSet(),
			addresses: set.NewStringSet("123.1.1.4:1234"),
		},
		{
			sql:         "select * from information_schema.cluster_config where address='123.1.1.4:1234' and address in ('123.1.1.5:1234', '123.1.1.6:1234')",
			nodeTypes:   set.NewStringSet(),
			addresses:   set.NewStringSet(),
			skipRequest: true,
		},
		{
			sql:       "select * from information_schema.cluster_config where address in ('123.1.1.5:1234', '123.1.1.4:1234') and address in ('123.1.1.5:1234', '123.1.1.6:1234')",
			nodeTypes: set.NewStringSet(),
			addresses: set.NewStringSet("123.1.1.5:1234"),
		},
		{
			sql: `select * from information_schema.cluster_config 
				where address in ('123.1.1.5:1234', '123.1.1.4:1234')
				  and address in ('123.1.1.5:1234', '123.1.1.6:1234')
				  and type in ('tikv', 'tidb')
				  and type in ('pd', 'tidb')`,
			nodeTypes: set.NewStringSet("tidb"),
			addresses: set.NewStringSet("123.1.1.5:1234"),
		},
		{
			sql: `select * from information_schema.cluster_config 
				where address in ('123.1.1.5:1234', '123.1.1.4:1234')
				  and address in ('123.1.1.5:1234', '123.1.1.6:1234')
				  and address in ('123.1.1.6:1234', '123.1.1.7:1234')
				  and address in ('123.1.1.7:1234', '123.1.1.8:1234')`,
			nodeTypes:   set.NewStringSet(),
			addresses:   set.NewStringSet(),
			skipRequest: true,
		},
	}
	for _, ca := range cases {
		stmt, err := parser.ParseOneStmt(ca.sql, "", "")
		c.Assert(err, IsNil)

		ctx := context.Background()
		builder := plannercore.NewPlanBuilder(se, s.dom.InfoSchema(), &plannercore.BlockHintProcessor{})
		plan, err := builder.Build(ctx, stmt)
		c.Assert(err, IsNil)

		logicalPlan, err := plannercore.LogicalOptimize(ctx, builder.GetOptFlag(), plan.(plannercore.LogicalPlan))
		c.Assert(err, IsNil)

		// Obtain the leaf plan
		leafPlan := logicalPlan
		for len(leafPlan.Children()) > 0 {
			leafPlan = leafPlan.Children()[0]
		}

		logicalMemTable := leafPlan.(*plannercore.LogicalMemTable)
		c.Assert(logicalMemTable.Extractor, NotNil)

		clusterConfigExtractor := logicalMemTable.Extractor.(*plannercore.ClusterConfigTableExtractor)
		c.Assert(clusterConfigExtractor.NodeTypes, DeepEquals, ca.nodeTypes, Commentf("SQL: %v", ca.sql))
		c.Assert(clusterConfigExtractor.Addresses, DeepEquals, ca.addresses, Commentf("SQL: %v", ca.sql))
		c.Assert(clusterConfigExtractor.SkipRequest, DeepEquals, ca.skipRequest, Commentf("SQL: %v", ca.sql))
	}
}
