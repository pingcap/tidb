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
	"github.com/pingcap/parser/mysql"
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

func (s *diagnosticsSuite) TestCriticalErrorInspection(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	fpName := "github.com/pingcap/tidb/executor/mockMetricsTableData"
	c.Assert(failpoint.Enable(fpName, "return"), IsNil)
	defer func() { c.Assert(failpoint.Disable(fpName), IsNil) }()

	datetime := func(s string) types.Time {
		t, err := types.ParseTime(tk.Se.GetSessionVars().StmtCtx, s, mysql.TypeDatetime, types.MaxFsp)
		c.Assert(err, IsNil)
		return t
	}

	// construct some mock data
	mockData := map[string][][]types.Datum{
		// columns: time, instance, type, value
		"tidb_failed_query_opm": {
			types.MakeDatums(datetime("2020-02-12 10:35:00"), "tidb-0", "type1", 0.0),
			types.MakeDatums(datetime("2020-02-12 10:36:00"), "tidb-0", "type2", 1.0),
			types.MakeDatums(datetime("2020-02-12 10:37:00"), "tidb-1", "type3", 5.0),
		},
		// columns: time, instance, type, value
		"tikv_critical_error": {
			types.MakeDatums(datetime("2020-02-12 10:35:00"), "tikv-0", "type1", 0.0),
			types.MakeDatums(datetime("2020-02-12 10:36:00"), "tikv-1", "type1", 1.0),
			types.MakeDatums(datetime("2020-02-12 10:37:00"), "tikv-2", "type2", 5.0),
		},
		// columns: time, instance, value
		"tidb_panic_count": {
			types.MakeDatums(datetime("2020-02-12 10:35:00"), "tidb-0", 4.0),
			types.MakeDatums(datetime("2020-02-12 10:36:00"), "tidb-0", 0.0),
			types.MakeDatums(datetime("2020-02-12 10:37:00"), "tidb-1", 1.0),
		},
		// columns: time, instance, value
		"tidb_binlog_error_count": {
			types.MakeDatums(datetime("2020-02-12 10:35:00"), "tidb-1", 4.0),
			types.MakeDatums(datetime("2020-02-12 10:36:00"), "tidb-2", 0.0),
			types.MakeDatums(datetime("2020-02-12 10:37:00"), "tidb-3", 1.0),
		},
		// columns: time, instance, type, value
		"pd_cmd_fail_ops": {
			types.MakeDatums(datetime("2020-02-12 10:35:00"), "tidb-0", "type1", 0.0),
			types.MakeDatums(datetime("2020-02-12 10:36:00"), "tidb-0", "type1", 1.0),
			types.MakeDatums(datetime("2020-02-12 10:37:00"), "tidb-1", "type2", 5.0),
		},
		// columns: time, instance, type, value
		"tidb_lock_resolver_ops": {
			types.MakeDatums(datetime("2020-02-12 10:35:00"), "tidb-0", "type1", 0.0),
			types.MakeDatums(datetime("2020-02-12 10:36:00"), "tidb-0", "type1", 1.0),
			types.MakeDatums(datetime("2020-02-12 10:37:00"), "tidb-1", "type2", 5.0),
		},
		// columns: time, instance, db, type, stage, value
		"tikv_scheduler_is_busy": {
			types.MakeDatums(datetime("2020-02-12 10:35:00"), "tikv-0", "db1", "type1", "stage1", 1.0),
			types.MakeDatums(datetime("2020-02-12 10:36:00"), "tikv-0", "db2", "type1", "stage2", 2.0),
			types.MakeDatums(datetime("2020-02-12 10:37:00"), "tikv-1", "db1", "type2", "stage1", 3.0),
			types.MakeDatums(datetime("2020-02-12 10:38:00"), "tikv-0", "db1", "type1", "stage2", 4.0),
			types.MakeDatums(datetime("2020-02-12 10:39:00"), "tikv-0", "db2", "type1", "stage1", 5.0),
			types.MakeDatums(datetime("2020-02-12 10:40:00"), "tikv-1", "db1", "type2", "stage2", 6.0),
		},
		// columns: time, instance, db, value
		"tikv_coprocessor_is_busy": {
			types.MakeDatums(datetime("2020-02-12 10:35:00"), "tikv-0", "db1", 1.0),
			types.MakeDatums(datetime("2020-02-12 10:36:00"), "tikv-0", "db2", 2.0),
			types.MakeDatums(datetime("2020-02-12 10:37:00"), "tikv-1", "db1", 3.0),
			types.MakeDatums(datetime("2020-02-12 10:38:00"), "tikv-0", "db1", 4.0),
			types.MakeDatums(datetime("2020-02-12 10:39:00"), "tikv-0", "db2", 5.0),
			types.MakeDatums(datetime("2020-02-12 10:40:00"), "tikv-1", "db1", 6.0),
		},
		// columns: time, instance, db, type, value
		"tikv_channel_full_total": {
			types.MakeDatums(datetime("2020-02-12 10:35:00"), "tikv-0", "db1", "type1", 1.0),
			types.MakeDatums(datetime("2020-02-12 10:36:00"), "tikv-0", "db2", "type1", 2.0),
			types.MakeDatums(datetime("2020-02-12 10:37:00"), "tikv-1", "db1", "type2", 3.0),
			types.MakeDatums(datetime("2020-02-12 10:38:00"), "tikv-0", "db1", "type1", 4.0),
			types.MakeDatums(datetime("2020-02-12 10:39:00"), "tikv-0", "db2", "type1", 5.0),
			types.MakeDatums(datetime("2020-02-12 10:40:00"), "tikv-1", "db1", "type2", 6.0),
		},
		// columns: time, "instance", "reason", value
		"tikv_coprocessor_request_error": {
			types.MakeDatums(datetime("2020-02-12 10:35:00"), "tikv-0", "reason1", 1.0),
			types.MakeDatums(datetime("2020-02-12 10:36:00"), "tikv-0", "reason2", 2.0),
			types.MakeDatums(datetime("2020-02-12 10:37:00"), "tikv-1", "reason3", 3.0),
		},
		// columns: time, instance, value
		"tidb_schema_lease_error_opm": {
			types.MakeDatums(datetime("2020-02-12 10:35:00"), "tidb-1", 4.0),
			types.MakeDatums(datetime("2020-02-12 10:36:00"), "tidb-2", 0.0),
			types.MakeDatums(datetime("2020-02-12 10:37:00"), "tidb-3", 1.0),
		},
		// columns: time, instance, type, sql_type, value
		"tidb_transaction_retry_error_ops": {
			types.MakeDatums(datetime("2020-02-12 10:35:00"), "tidb-0", "db1", "sql_type1", 1.0),
			types.MakeDatums(datetime("2020-02-12 10:36:00"), "tidb-0", "db2", "sql_type1", 2.0),
			types.MakeDatums(datetime("2020-02-12 10:37:00"), "tidb-1", "db1", "sql_type2", 3.0),
		},
		// columns: time, instance, type, value
		"tikv_grpc_errors": {
			types.MakeDatums(datetime("2020-02-12 10:35:00"), "tikv-0", "type1", 1.0),
			types.MakeDatums(datetime("2020-02-12 10:36:00"), "tikv-0", "type2", 2.0),
			types.MakeDatums(datetime("2020-02-12 10:37:00"), "tikv-1", "type3", 3.0),
		},
		// columns: time, instance, type, value
		"tidb_kv_region_error_ops": {
			types.MakeDatums(datetime("2020-02-12 10:35:00"), "tikv-0", "type1", 1.0),
			types.MakeDatums(datetime("2020-02-12 10:36:00"), "tikv-0", "type2", 2.0),
			types.MakeDatums(datetime("2020-02-12 10:37:00"), "tikv-1", "type3", 3.0),
		},
	}

	ctx := context.WithValue(context.Background(), "__mockMetricsTableData", mockData)
	ctx = failpoint.WithHook(ctx, func(_ context.Context, fpname string) bool {
		return fpName == fpname
	})

	rs, err := tk.Se.Execute(ctx, "select item, instance, value, details from information_schema.inspection_result where rule='critical-error'")
	c.Assert(err, IsNil)
	result := tk.ResultSetToResultWithCtx(ctx, rs[0], Commentf("execute inspect SQL failed"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(0), Commentf("unexpected warnings: %+v", tk.Se.GetSessionVars().StmtCtx.GetWarnings()))
	result.Check(testkit.Rows(
		"binlog-error tidb-3 1.00 select * from `metric_schema`.`tidb_binlog_error_count` where `instance`='tidb-3'",
		"binlog-error tidb-1 4.00 select * from `metric_schema`.`tidb_binlog_error_count` where `instance`='tidb-1'",
		"channel-is-full tikv-0 {`db`='db1',`type`='type1'}=4.00 select * from `metric_schema`.`tikv_channel_full_total` where `instance`='tikv-0' and `db`='db1' and `type`='type1'",
		"channel-is-full tikv-1 {`db`='db1',`type`='type2'}=6.00 select * from `metric_schema`.`tikv_channel_full_total` where `instance`='tikv-1' and `db`='db1' and `type`='type2'",
		"channel-is-full tikv-0 {`db`='db2',`type`='type1'}=5.00 select * from `metric_schema`.`tikv_channel_full_total` where `instance`='tikv-0' and `db`='db2' and `type`='type1'",
		"coprocessor-error tikv-0 {`reason`='reason1'}=1.00 select * from `metric_schema`.`tikv_coprocessor_request_error` where `instance`='tikv-0' and `reason`='reason1'",
		"coprocessor-error tikv-0 {`reason`='reason2'}=2.00 select * from `metric_schema`.`tikv_coprocessor_request_error` where `instance`='tikv-0' and `reason`='reason2'",
		"coprocessor-error tikv-1 {`reason`='reason3'}=3.00 select * from `metric_schema`.`tikv_coprocessor_request_error` where `instance`='tikv-1' and `reason`='reason3'",
		"coprocessor-is-busy tikv-0 {`db`='db1'}=4.00 select * from `metric_schema`.`tikv_coprocessor_is_busy` where `instance`='tikv-0' and `db`='db1'",
		"coprocessor-is-busy tikv-1 {`db`='db1'}=6.00 select * from `metric_schema`.`tikv_coprocessor_is_busy` where `instance`='tikv-1' and `db`='db1'",
		"coprocessor-is-busy tikv-0 {`db`='db2'}=5.00 select * from `metric_schema`.`tikv_coprocessor_is_busy` where `instance`='tikv-0' and `db`='db2'",
		"critical-error tikv-1 {`type`='type1'}=1.00 select * from `metric_schema`.`tikv_critical_error` where `instance`='tikv-1' and `type`='type1'",
		"critical-error tikv-2 {`type`='type2'}=5.00 select * from `metric_schema`.`tikv_critical_error` where `instance`='tikv-2' and `type`='type2'",
		"failed-query-opm tidb-0 {`type`='type2'}=1.00 select * from `metric_schema`.`tidb_failed_query_opm` where `instance`='tidb-0' and `type`='type2'",
		"failed-query-opm tidb-1 {`type`='type3'}=5.00 select * from `metric_schema`.`tidb_failed_query_opm` where `instance`='tidb-1' and `type`='type3'",
		"grpc-errors tikv-0 {`type`='type1'}=1.00 select * from `metric_schema`.`tikv_grpc_errors` where `instance`='tikv-0' and `type`='type1'",
		"grpc-errors tikv-0 {`type`='type2'}=2.00 select * from `metric_schema`.`tikv_grpc_errors` where `instance`='tikv-0' and `type`='type2'",
		"grpc-errors tikv-1 {`type`='type3'}=3.00 select * from `metric_schema`.`tikv_grpc_errors` where `instance`='tikv-1' and `type`='type3'",
		"lock-resolve tidb-0 {`type`='type1'}=1.00 select * from `metric_schema`.`tidb_lock_resolver_ops` where `instance`='tidb-0' and `type`='type1'",
		"lock-resolve tidb-1 {`type`='type2'}=5.00 select * from `metric_schema`.`tidb_lock_resolver_ops` where `instance`='tidb-1' and `type`='type2'",
		"panic-count tidb-1 1.00 select * from `metric_schema`.`tidb_panic_count` where `instance`='tidb-1'",
		"panic-count tidb-0 4.00 select * from `metric_schema`.`tidb_panic_count` where `instance`='tidb-0'",
		"pd-cmd-failed tidb-0 {`type`='type1'}=1.00 select * from `metric_schema`.`pd_cmd_fail_ops` where `instance`='tidb-0' and `type`='type1'",
		"pd-cmd-failed tidb-1 {`type`='type2'}=5.00 select * from `metric_schema`.`pd_cmd_fail_ops` where `instance`='tidb-1' and `type`='type2'",
		"scheduler-is-busy tikv-0 {`db`='db1',`type`='type1',`stage`='stage1'}=1.00 select * from `metric_schema`.`tikv_scheduler_is_busy` where `instance`='tikv-0' and `db`='db1' and `type`='type1' and `stage`='stage1'",
		"scheduler-is-busy tikv-0 {`db`='db1',`type`='type1',`stage`='stage2'}=4.00 select * from `metric_schema`.`tikv_scheduler_is_busy` where `instance`='tikv-0' and `db`='db1' and `type`='type1' and `stage`='stage2'",
		"scheduler-is-busy tikv-1 {`db`='db1',`type`='type2',`stage`='stage1'}=3.00 select * from `metric_schema`.`tikv_scheduler_is_busy` where `instance`='tikv-1' and `db`='db1' and `type`='type2' and `stage`='stage1'",
		"scheduler-is-busy tikv-1 {`db`='db1',`type`='type2',`stage`='stage2'}=6.00 select * from `metric_schema`.`tikv_scheduler_is_busy` where `instance`='tikv-1' and `db`='db1' and `type`='type2' and `stage`='stage2'",
		"scheduler-is-busy tikv-0 {`db`='db2',`type`='type1',`stage`='stage1'}=5.00 select * from `metric_schema`.`tikv_scheduler_is_busy` where `instance`='tikv-0' and `db`='db2' and `type`='type1' and `stage`='stage1'",
		"scheduler-is-busy tikv-0 {`db`='db2',`type`='type1',`stage`='stage2'}=2.00 select * from `metric_schema`.`tikv_scheduler_is_busy` where `instance`='tikv-0' and `db`='db2' and `type`='type1' and `stage`='stage2'",
		"schema-lease-error tidb-3 1.00 select * from `metric_schema`.`tidb_schema_lease_error_opm` where `instance`='tidb-3'",
		"schema-lease-error tidb-1 4.00 select * from `metric_schema`.`tidb_schema_lease_error_opm` where `instance`='tidb-1'",
		"ticlient-region-error tikv-0 {`type`='type1'}=1.00 select * from `metric_schema`.`tidb_kv_region_error_ops` where `instance`='tikv-0' and `type`='type1'",
		"ticlient-region-error tikv-0 {`type`='type2'}=2.00 select * from `metric_schema`.`tidb_kv_region_error_ops` where `instance`='tikv-0' and `type`='type2'",
		"ticlient-region-error tikv-1 {`type`='type3'}=3.00 select * from `metric_schema`.`tidb_kv_region_error_ops` where `instance`='tikv-1' and `type`='type3'",
		"txn-retry-error tidb-0 {`type`='db1',`sql_type`='sql_type1'}=1.00 select * from `metric_schema`.`tidb_transaction_retry_error_ops` where `instance`='tidb-0' and `type`='db1' and `sql_type`='sql_type1'",
		"txn-retry-error tidb-1 {`type`='db1',`sql_type`='sql_type2'}=3.00 select * from `metric_schema`.`tidb_transaction_retry_error_ops` where `instance`='tidb-1' and `type`='db1' and `sql_type`='sql_type2'",
		"txn-retry-error tidb-0 {`type`='db2',`sql_type`='sql_type1'}=2.00 select * from `metric_schema`.`tidb_transaction_retry_error_ops` where `instance`='tidb-0' and `type`='db2' and `sql_type`='sql_type1'",
	))
}
