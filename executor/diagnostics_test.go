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

package executor_test

import (
	"context"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/testkit"
)

var _ = Suite(&diagnosticsSuite{})

type diagnosticsSuite struct {
	store kv.Storage
	dom   *domain.Domain
}

func (s *diagnosticsSuite) SetUpSuite(c *C) {
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	s.store = store
	s.dom = dom
}

func (s *diagnosticsSuite) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
}

func (s *diagnosticsSuite) TestInspectionResult(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	mockData := map[string]variable.TableSnapshot{}
	// mock configuration inconsistent
	mockData[infoschema.TableClusterConfig] = variable.TableSnapshot{
		Rows: [][]types.Datum{
			types.MakeDatums("tidb", "192.168.3.22:4000", "ddl.lease", "1"),
			types.MakeDatums("tidb", "192.168.3.23:4000", "ddl.lease", "2"),
			types.MakeDatums("tidb", "192.168.3.24:4000", "ddl.lease", "1"),
			types.MakeDatums("tidb", "192.168.3.25:4000", "ddl.lease", "1"),
			types.MakeDatums("tikv", "192.168.3.32:26600", "coprocessor.high", "8"),
			types.MakeDatums("tikv", "192.168.3.33:26600", "coprocessor.high", "8"),
			types.MakeDatums("tikv", "192.168.3.34:26600", "coprocessor.high", "7"),
			types.MakeDatums("tikv", "192.168.3.35:26600", "coprocessor.high", "7"),
			types.MakeDatums("pd", "192.168.3.32:2379", "scheduler.limit", "3"),
			types.MakeDatums("pd", "192.168.3.33:2379", "scheduler.limit", "3"),
			types.MakeDatums("pd", "192.168.3.34:2379", "scheduler.limit", "3"),
			types.MakeDatums("pd", "192.168.3.35:2379", "scheduler.limit", "3"),
		},
	}
	// mock version inconsistent
	mockData[infoschema.TableClusterInfo] = variable.TableSnapshot{
		Rows: [][]types.Datum{
			types.MakeDatums("tidb", "192.168.1.11:1234", "192.168.1.11:1234", "4.0", "a234c"),
			types.MakeDatums("tidb", "192.168.1.12:1234", "192.168.1.11:1234", "4.0", "a234d"),
			types.MakeDatums("tidb", "192.168.1.13:1234", "192.168.1.11:1234", "4.0", "a234e"),
			types.MakeDatums("tikv", "192.168.1.21:1234", "192.168.1.21:1234", "4.0", "c234d"),
			types.MakeDatums("tikv", "192.168.1.22:1234", "192.168.1.22:1234", "4.0", "c234d"),
			types.MakeDatums("tikv", "192.168.1.23:1234", "192.168.1.23:1234", "4.0", "c234e"),
			types.MakeDatums("pd", "192.168.1.31:1234", "192.168.1.31:1234", "4.0", "m234c"),
			types.MakeDatums("pd", "192.168.1.32:1234", "192.168.1.32:1234", "4.0", "m234d"),
			types.MakeDatums("pd", "192.168.1.33:1234", "192.168.1.33:1234", "4.0", "m234e"),
		},
	}
	// mock load
	mockData[infoschema.TableClusterLoad] = variable.TableSnapshot{
		Rows: [][]types.Datum{
			types.MakeDatums("tidb", "192.168.1.11:1234", "memory", "virtual", "used-percent", "0.8"),
			types.MakeDatums("tidb", "192.168.1.12:1234", "memory", "virtual", "used-percent", "0.6"),
			types.MakeDatums("tidb", "192.168.1.13:1234", "memory", "swap", "used-percent", "0"),
			types.MakeDatums("tikv", "192.168.1.21:1234", "memory", "swap", "used-percent", "0.6"),
			types.MakeDatums("pd", "192.168.1.31:1234", "cpu", "cpu", "load1", "1.0"),
			types.MakeDatums("pd", "192.168.1.32:1234", "cpu", "cpu", "load5", "0.6"),
			types.MakeDatums("pd", "192.168.1.33:1234", "cpu", "cpu", "load15", "2.0"),
		},
	}
	mockData[infoschema.TableClusterHardware] = variable.TableSnapshot{
		Rows: [][]types.Datum{
			types.MakeDatums("tikv", "192.168.1.22:1234", "disk", "sda", "used-percent", "80"),
			types.MakeDatums("tikv", "192.168.1.23:1234", "disk", "sdb", "used-percent", "50"),
		},
	}

	ctx := context.WithValue(context.Background(), "__mockInspectionTables", mockData)
	fpName := "github.com/pingcap/tidb/executor/mockMergeMockInspectionTables"
	ctx = failpoint.WithHook(ctx, func(_ context.Context, fpname string) bool {
		return fpname == fpName
	})

	c.Assert(failpoint.Enable(fpName, "return"), IsNil)
	defer func() { c.Assert(failpoint.Disable(fpName), IsNil) }()

	cases := []struct {
		sql  string
		rows []string
	}{
		{
			sql: "select rule, item, type, value, reference, severity, details from information_schema.inspection_result where rule in ('config', 'version')",
			rows: []string{
				"config coprocessor.high tikv inconsistent consistent warning select * from information_schema.cluster_config where type='tikv' and `key`='coprocessor.high'",
				"config ddl.lease tidb inconsistent consistent warning select * from information_schema.cluster_config where type='tidb' and `key`='ddl.lease'",
				"version git_hash tidb inconsistent consistent critical select * from information_schema.cluster_info where type='tidb'",
				"version git_hash tikv inconsistent consistent critical select * from information_schema.cluster_info where type='tikv'",
				"version git_hash pd inconsistent consistent critical select * from information_schema.cluster_info where type='pd'",
			},
		},
		{
			sql: "select rule, item, type, value, reference, severity, details from information_schema.inspection_result where rule in ('config', 'version') and item in ('coprocessor.high', 'git_hash') and type='tikv'",
			rows: []string{
				"config coprocessor.high tikv inconsistent consistent warning select * from information_schema.cluster_config where type='tikv' and `key`='coprocessor.high'",
				"version git_hash tikv inconsistent consistent critical select * from information_schema.cluster_info where type='tikv'",
			},
		},
		{
			sql: "select rule, item, type, value, reference, severity, details from information_schema.inspection_result where rule='config'",
			rows: []string{
				"config coprocessor.high tikv inconsistent consistent warning select * from information_schema.cluster_config where type='tikv' and `key`='coprocessor.high'",
				"config ddl.lease tidb inconsistent consistent warning select * from information_schema.cluster_config where type='tidb' and `key`='ddl.lease'",
			},
		},
		{
			sql: "select rule, item, type, value, reference, severity, details from information_schema.inspection_result where rule='version' and item='git_hash' and type in ('pd', 'tidb')",
			rows: []string{
				"version git_hash tidb inconsistent consistent critical select * from information_schema.cluster_info where type='tidb'",
				"version git_hash pd inconsistent consistent critical select * from information_schema.cluster_info where type='pd'",
			},
		},
		{
			sql: "select rule, item, type, instance, value, reference, severity, details from information_schema.inspection_result where rule='current-load'",
			rows: []string{
				"current-load cpu-load1 pd 192.168.1.31:1234 1.0 <0.7 warning ",
				"current-load cpu-load15 pd 192.168.1.33:1234 2.0 <0.7 warning ",
				"current-load disk-usage tikv 192.168.1.22:1234 80 <70 warning select * from information_schema.cluster_hardware where type='tikv' and instance='192.168.1.22:1234' and device_type='disk' and device_name='sda'",
				"current-load swap-memory-usage tikv 192.168.1.21:1234 0.6 0 warning ",
				"current-load virtual-memory-usage tidb 192.168.1.11:1234 0.8 <0.7 warning ",
			},
		},
	}

	for _, cs := range cases {
		rs, err := tk.Se.Execute(ctx, cs.sql)
		c.Assert(err, IsNil)
		result := tk.ResultSetToResultWithCtx(ctx, rs[0], Commentf("SQL: %v", cs.sql))
		warnings := tk.Se.GetSessionVars().StmtCtx.GetWarnings()
		c.Assert(len(warnings), Equals, 0, Commentf("expected no warning, got: %+v", warnings))
		result.Check(testkit.Rows(cs.rows...))
	}
}
