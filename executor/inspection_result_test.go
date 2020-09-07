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
	"fmt"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/testkit"
)

var _ = SerialSuites(&inspectionResultSuite{&testClusterTableBase{}})

type inspectionResultSuite struct{ *testClusterTableBase }

func (s *inspectionResultSuite) TestInspectionResult(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	mockData := map[string]variable.TableSnapshot{}
	// mock configuration inconsistent
	mockData[infoschema.TableClusterConfig] = variable.TableSnapshot{
		Rows: [][]types.Datum{
			types.MakeDatums("tidb", "192.168.3.22:4000", "ddl.lease", "1"),
			types.MakeDatums("tidb", "192.168.3.23:4000", "ddl.lease", "2"),
			types.MakeDatums("tidb", "192.168.3.24:4000", "ddl.lease", "1"),
			types.MakeDatums("tidb", "192.168.3.25:4000", "ddl.lease", "1"),
			types.MakeDatums("tidb", "192.168.3.24:4000", "status.status-port", "10080"),
			types.MakeDatums("tidb", "192.168.3.25:4000", "status.status-port", "10081"),
			types.MakeDatums("tidb", "192.168.3.24:4000", "log.slow-threshold", "0"),
			types.MakeDatums("tidb", "192.168.3.25:4000", "log.slow-threshold", "1"),
			types.MakeDatums("tikv", "192.168.3.32:26600", "coprocessor.high", "8"),
			types.MakeDatums("tikv", "192.168.3.33:26600", "coprocessor.high", "8"),
			types.MakeDatums("tikv", "192.168.3.34:26600", "coprocessor.high", "7"),
			types.MakeDatums("tikv", "192.168.3.35:26600", "coprocessor.high", "7"),
			types.MakeDatums("tikv", "192.168.3.35:26600", "raftstore.sync-log", "false"),
			types.MakeDatums("pd", "192.168.3.32:2379", "scheduler.limit", "3"),
			types.MakeDatums("pd", "192.168.3.33:2379", "scheduler.limit", "3"),
			types.MakeDatums("pd", "192.168.3.34:2379", "scheduler.limit", "3"),
			types.MakeDatums("pd", "192.168.3.35:2379", "scheduler.limit", "3"),
			types.MakeDatums("pd", "192.168.3.34:2379", "advertise-client-urls", "0"),
			types.MakeDatums("pd", "192.168.3.35:2379", "advertise-client-urls", "1"),
			types.MakeDatums("pd", "192.168.3.34:2379", "advertise-peer-urls", "0"),
			types.MakeDatums("pd", "192.168.3.35:2379", "advertise-peer-urls", "1"),
			types.MakeDatums("pd", "192.168.3.34:2379", "client-urls", "0"),
			types.MakeDatums("pd", "192.168.3.35:2379", "client-urls", "1"),
			types.MakeDatums("pd", "192.168.3.34:2379", "log.file.filename", "0"),
			types.MakeDatums("pd", "192.168.3.35:2379", "log.file.filename", "1"),
			types.MakeDatums("pd", "192.168.3.34:2379", "metric.job", "0"),
			types.MakeDatums("pd", "192.168.3.35:2379", "metric.job", "1"),
			types.MakeDatums("pd", "192.168.3.34:2379", "name", "0"),
			types.MakeDatums("pd", "192.168.3.35:2379", "name", "1"),
			types.MakeDatums("pd", "192.168.3.34:2379", "peer-urls", "0"),
			types.MakeDatums("pd", "192.168.3.35:2379", "peer-urls", "1"),
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
	mockData[infoschema.TableClusterHardware] = variable.TableSnapshot{
		Rows: [][]types.Datum{
			types.MakeDatums("tikv", "192.168.1.22:1234", "disk", "sda", "used-percent", "80"),
			types.MakeDatums("tikv", "192.168.1.23:1234", "disk", "sdb", "used-percent", "50"),
			types.MakeDatums("pd", "192.168.1.31:1234", "cpu", "cpu", "cpu-logical-cores", "1"),
			types.MakeDatums("pd", "192.168.1.32:1234", "cpu", "cpu", "cpu-logical-cores", "4"),
			types.MakeDatums("pd", "192.168.1.33:1234", "cpu", "cpu", "cpu-logical-cores", "10"),
		},
	}

	datetime := func(str string) types.Time {
		return s.parseTime(c, tk.Se, str)
	}
	// construct some mock abnormal data
	mockMetric := map[string][][]types.Datum{
		"node_total_memory": {
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "192.168.3.33:26600", 50.0*1024*1024*1024),
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "192.168.3.34:26600", 50.0*1024*1024*1024),
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "192.168.3.35:26600", 50.0*1024*1024*1024),
		},
	}

	ctx := s.setupForInspection(c, mockMetric, mockData)
	defer s.tearDownForInspection(c)

	cases := []struct {
		sql  string
		rows []string
	}{
		{
			sql: "select rule, item, type, value, reference, severity, details from information_schema.inspection_result where rule in ('config', 'version')",
			rows: []string{
				"config coprocessor.high tikv inconsistent consistent warning 192.168.3.32:26600,192.168.3.33:26600 config value is 8\n192.168.3.34:26600,192.168.3.35:26600 config value is 7",
				"config ddl.lease tidb inconsistent consistent warning 192.168.3.22:4000,192.168.3.24:4000,192.168.3.25:4000 config value is 1\n192.168.3.23:4000 config value is 2",
				"config log.slow-threshold tidb 0 not 0 warning slow-threshold = 0 will record every query to slow log, it may affect performance",
				"config log.slow-threshold tidb inconsistent consistent warning 192.168.3.24:4000 config value is 0\n192.168.3.25:4000 config value is 1",
				"config raftstore.sync-log tikv false not false warning sync-log should be true to avoid recover region when the machine breaks down",
				"version git_hash pd inconsistent consistent critical the cluster has 3 different pd versions, execute the sql to see more detail: select * from information_schema.cluster_info where type='pd'",
				"version git_hash tidb inconsistent consistent critical the cluster has 3 different tidb versions, execute the sql to see more detail: select * from information_schema.cluster_info where type='tidb'",
				"version git_hash tikv inconsistent consistent critical the cluster has 2 different tikv versions, execute the sql to see more detail: select * from information_schema.cluster_info where type='tikv'",
			},
		},
		{
			sql: "select rule, item, type, value, reference, severity, details from information_schema.inspection_result where rule in ('config', 'version') and item in ('coprocessor.high', 'git_hash') and type='tikv'",
			rows: []string{
				"config coprocessor.high tikv inconsistent consistent warning 192.168.3.32:26600,192.168.3.33:26600 config value is 8\n192.168.3.34:26600,192.168.3.35:26600 config value is 7",
				"version git_hash tikv inconsistent consistent critical the cluster has 2 different tikv versions, execute the sql to see more detail: select * from information_schema.cluster_info where type='tikv'",
			},
		},
		{
			sql: "select rule, item, type, value, reference, severity, details from information_schema.inspection_result where rule='config'",
			rows: []string{
				"config coprocessor.high tikv inconsistent consistent warning 192.168.3.32:26600,192.168.3.33:26600 config value is 8\n192.168.3.34:26600,192.168.3.35:26600 config value is 7",
				"config ddl.lease tidb inconsistent consistent warning 192.168.3.22:4000,192.168.3.24:4000,192.168.3.25:4000 config value is 1\n192.168.3.23:4000 config value is 2",
				"config log.slow-threshold tidb 0 not 0 warning slow-threshold = 0 will record every query to slow log, it may affect performance",
				"config log.slow-threshold tidb inconsistent consistent warning 192.168.3.24:4000 config value is 0\n192.168.3.25:4000 config value is 1",
				"config raftstore.sync-log tikv false not false warning sync-log should be true to avoid recover region when the machine breaks down",
			},
		},
		{
			sql: "select rule, item, type, value, reference, severity, details from information_schema.inspection_result where rule='version' and item='git_hash' and type in ('pd', 'tidb')",
			rows: []string{
				"version git_hash pd inconsistent consistent critical the cluster has 3 different pd versions, execute the sql to see more detail: select * from information_schema.cluster_info where type='pd'",
				"version git_hash tidb inconsistent consistent critical the cluster has 3 different tidb versions, execute the sql to see more detail: select * from information_schema.cluster_info where type='tidb'",
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

func (s *inspectionResultSuite) parseTime(c *C, se session.Session, str string) types.Time {
	t, err := types.ParseTime(se.GetSessionVars().StmtCtx, str, mysql.TypeDatetime, types.MaxFsp)
	c.Assert(err, IsNil)
	return t
}

func (s *inspectionResultSuite) tearDownForInspection(c *C) {
	fpName := "github.com/pingcap/tidb/executor/mockMergeMockInspectionTables"
	c.Assert(failpoint.Disable(fpName), IsNil)

	fpName2 := "github.com/pingcap/tidb/executor/mockMetricsTableData"
	c.Assert(failpoint.Disable(fpName2), IsNil)
}

func (s *inspectionResultSuite) setupForInspection(c *C, mockData map[string][][]types.Datum, configurations map[string]variable.TableSnapshot) context.Context {
	// mock tikv configuration.
	if configurations == nil {
		configurations = map[string]variable.TableSnapshot{}
		configurations[infoschema.TableClusterConfig] = variable.TableSnapshot{
			Rows: [][]types.Datum{
				types.MakeDatums("tikv", "tikv-0", "raftstore.apply-pool-size", "2"),
				types.MakeDatums("tikv", "tikv-0", "raftstore.store-pool-size", "2"),
				types.MakeDatums("tikv", "tikv-0", "readpool.coprocessor.high-concurrency", "4"),
				types.MakeDatums("tikv", "tikv-0", "readpool.coprocessor.low-concurrency", "4"),
				types.MakeDatums("tikv", "tikv-0", "readpool.coprocessor.normal-concurrency", "4"),
				types.MakeDatums("tikv", "tikv-1", "readpool.coprocessor.normal-concurrency", "8"),
				types.MakeDatums("tikv", "tikv-0", "readpool.storage.high-concurrency", "4"),
				types.MakeDatums("tikv", "tikv-0", "readpool.storage.low-concurrency", "4"),
				types.MakeDatums("tikv", "tikv-0", "readpool.storage.normal-concurrency", "4"),
				types.MakeDatums("tikv", "tikv-0", "server.grpc-concurrency", "8"),
				types.MakeDatums("tikv", "tikv-0", "storage.scheduler-worker-pool-size", "6"),
			},
		}
		// mock cluster information
		configurations[infoschema.TableClusterInfo] = variable.TableSnapshot{
			Rows: [][]types.Datum{
				types.MakeDatums("pd", "pd-0", "pd-0", "4.0", "a234c", "", ""),
				types.MakeDatums("tidb", "tidb-0", "tidb-0s", "4.0", "a234c", "", ""),
				types.MakeDatums("tidb", "tidb-1", "tidb-1s", "4.0", "a234c", "", ""),
				types.MakeDatums("tikv", "tikv-0", "tikv-0s", "4.0", "a234c", "", ""),
				types.MakeDatums("tikv", "tikv-1", "tikv-1s", "4.0", "a234c", "", ""),
				types.MakeDatums("tikv", "tikv-2", "tikv-2s", "4.0", "a234c", "", ""),
			},
		}
	}
	fpName := "github.com/pingcap/tidb/executor/mockMergeMockInspectionTables"
	c.Assert(failpoint.Enable(fpName, "return"), IsNil)

	// Mock for metric table data.
	fpName2 := "github.com/pingcap/tidb/executor/mockMetricsTableData"
	c.Assert(failpoint.Enable(fpName2, "return"), IsNil)

	ctx := context.WithValue(context.Background(), "__mockInspectionTables", configurations)
	ctx = context.WithValue(ctx, "__mockMetricsTableData", mockData)
	ctx = failpoint.WithHook(ctx, func(_ context.Context, currName string) bool {
		return fpName2 == currName || currName == fpName
	})
	return ctx
}

func (s *inspectionResultSuite) TestThresholdCheckInspection(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	datetime := func(str string) types.Time {
		return s.parseTime(c, tk.Se, str)
	}
	// construct some mock abnormal data
	mockData := map[string][][]types.Datum{
		// columns: time, instance, name, value
		"tikv_thread_cpu": {
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "tikv-0s", "cop_normal0", 10.0),
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "tikv-0s", "cop_normal1", 10.0),
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "tikv-1s", "cop_normal0", 10.0),
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "tikv-0s", "cop_high1", 10.0),
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "tikv-0s", "cop_high2", 10.0),
			types.MakeDatums(datetime("2020-02-14 05:21:00"), "tikv-0s", "cop_high1", 5.0),
			types.MakeDatums(datetime("2020-02-14 05:22:00"), "tikv-0s", "cop_high1", 1.0),
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "tikv-0s", "cop_low1", 10.0),
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "tikv-0s", "grpc_1", 10.0),
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "tikv-0s", "raftstore_1", 10.0),
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "tikv-0s", "apply_0", 10.0),
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "tikv-0s", "store_read_norm1", 10.0),
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "tikv-0s", "store_read_high2", 10.0),
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "tikv-0s", "store_read_low0", 10.0),
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "tikv-0s", "sched_2", 10.0),
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "tikv-0s", "split_check", 10.0),
		},
		"pd_tso_wait_duration":                {},
		"tidb_get_token_duration":             {},
		"tidb_load_schema_duration":           {},
		"tikv_scheduler_command_duration":     {},
		"tikv_handle_snapshot_duration":       {},
		"tikv_storage_async_request_duration": {},
		"tikv_engine_write_duration":          {},
		"tikv_engine_max_get_duration":        {},
		"tikv_engine_max_seek_duration":       {},
		"tikv_scheduler_pending_commands":     {},
		"tikv_block_index_cache_hit":          {},
		"tikv_block_data_cache_hit":           {},
		"tikv_block_filter_cache_hit":         {},
		"pd_scheduler_store_status":           {},
		"pd_region_health":                    {},
	}

	ctx := s.setupForInspection(c, mockData, nil)
	defer s.tearDownForInspection(c)

	rs, err := tk.Se.Execute(ctx, "select /*+ time_range('2020-02-12 10:35:00','2020-02-12 10:37:00') */ item, type, instance,status_address, value, reference, details from information_schema.inspection_result where rule='threshold-check' order by item")
	c.Assert(err, IsNil)
	result := tk.ResultSetToResultWithCtx(ctx, rs[0], Commentf("execute inspect SQL failed"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(0), Commentf("unexpected warnings: %+v", tk.Se.GetSessionVars().StmtCtx.GetWarnings()))
	result.Check(testkit.Rows(
		"apply-cpu tikv tikv-0 tikv-0s 10.00 < 1.60, config: raftstore.apply-pool-size=2 the 'apply-cpu' max cpu-usage of tikv-0s tikv is too high",
		"coprocessor-high-cpu tikv tikv-0 tikv-0s 20.00 < 3.60, config: readpool.coprocessor.high-concurrency=4 the 'coprocessor-high-cpu' max cpu-usage of tikv-0s tikv is too high",
		"coprocessor-low-cpu tikv tikv-0 tikv-0s 10.00 < 3.60, config: readpool.coprocessor.low-concurrency=4 the 'coprocessor-low-cpu' max cpu-usage of tikv-0s tikv is too high",
		"coprocessor-normal-cpu tikv tikv-0 tikv-0s 20.00 < 3.60, config: readpool.coprocessor.normal-concurrency=4 the 'coprocessor-normal-cpu' max cpu-usage of tikv-0s tikv is too high",
		"coprocessor-normal-cpu tikv tikv-1 tikv-1s 10.00 < 7.20, config: readpool.coprocessor.normal-concurrency=8 the 'coprocessor-normal-cpu' max cpu-usage of tikv-1s tikv is too high",
		"grpc-cpu tikv tikv-0 tikv-0s 10.00 < 7.20, config: server.grpc-concurrency=8 the 'grpc-cpu' max cpu-usage of tikv-0s tikv is too high",
		"raftstore-cpu tikv tikv-0 tikv-0s 10.00 < 1.60, config: raftstore.store-pool-size=2 the 'raftstore-cpu' max cpu-usage of tikv-0s tikv is too high",
		"scheduler-worker-cpu tikv tikv-0 tikv-0s 10.00 < 5.10, config: storage.scheduler-worker-pool-size=6 the 'scheduler-worker-cpu' max cpu-usage of tikv-0s tikv is too high",
		"split-check-cpu tikv tikv-0 tikv-0s 10.00 < 0.00 the 'split-check-cpu' max cpu-usage of tikv-0s tikv is too high",
		"storage-readpool-high-cpu tikv tikv-0 tikv-0s 10.00 < 3.60, config: readpool.storage.high-concurrency=4 the 'storage-readpool-high-cpu' max cpu-usage of tikv-0s tikv is too high",
		"storage-readpool-low-cpu tikv tikv-0 tikv-0s 10.00 < 3.60, config: readpool.storage.low-concurrency=4 the 'storage-readpool-low-cpu' max cpu-usage of tikv-0s tikv is too high",
		"storage-readpool-normal-cpu tikv tikv-0 tikv-0s 10.00 < 3.60, config: readpool.storage.normal-concurrency=4 the 'storage-readpool-normal-cpu' max cpu-usage of tikv-0s tikv is too high",
	))

	// construct some mock normal data
	mockData["tikv_thread_cpu"] = [][]types.Datum{
		// columns: time, instance, name, value
		types.MakeDatums(datetime("2020-02-14 05:20:00"), "tikv-0s", "cop_normal0", 1.0),
		types.MakeDatums(datetime("2020-02-14 05:20:00"), "tikv-0s", "cop_high1", 0.1),
		types.MakeDatums(datetime("2020-02-14 05:20:00"), "tikv-0s", "cop_low1", 1.0),
		types.MakeDatums(datetime("2020-02-14 05:20:00"), "tikv-0s", "grpc_1", 7.21),
		types.MakeDatums(datetime("2020-02-14 05:20:00"), "tikv-0s", "grpc_2", 0.21),
		types.MakeDatums(datetime("2020-02-14 05:20:00"), "tikv-0s", "raftstore_1", 1.0),
		types.MakeDatums(datetime("2020-02-14 05:20:00"), "tikv-0s", "apply_0", 1.0),
		types.MakeDatums(datetime("2020-02-14 05:20:00"), "tikv-0s", "store_read_norm1", 1.0),
		types.MakeDatums(datetime("2020-02-14 05:20:00"), "tikv-0s", "store_read_high2", 1.0),
		types.MakeDatums(datetime("2020-02-14 05:20:00"), "tikv-0s", "store_read_low0", 1.0),
		types.MakeDatums(datetime("2020-02-14 05:20:00"), "tikv-0s", "sched_2", 0.3),
		types.MakeDatums(datetime("2020-02-14 05:20:00"), "tikv-0s", "split_check", 0.5),
	}

	ctx = context.WithValue(ctx, "__mockMetricsTableData", mockData)
	rs, err = tk.Se.Execute(ctx, "select /*+ time_range('2020-02-12 10:35:00','2020-02-12 10:37:00') */ item, type, instance,status_address, value, reference from information_schema.inspection_result where rule='threshold-check' order by item")
	c.Assert(err, IsNil)
	result = tk.ResultSetToResultWithCtx(ctx, rs[0], Commentf("execute inspect SQL failed"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(0), Commentf("unexpected warnings: %+v", tk.Se.GetSessionVars().StmtCtx.GetWarnings()))
	result.Check(testkit.Rows("grpc-cpu tikv tikv-0 tikv-0s 7.42 < 7.20, config: server.grpc-concurrency=8"))
}

func (s *inspectionResultSuite) TestThresholdCheckInspection2(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	datetime := func(s string) types.Time {
		t, err := types.ParseTime(tk.Se.GetSessionVars().StmtCtx, s, mysql.TypeDatetime, types.MaxFsp)
		c.Assert(err, IsNil)
		return t
	}

	// construct some mock abnormal data
	mockData := map[string][][]types.Datum{
		"pd_tso_wait_duration": {
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "pd-0", 0.999, 0.06),
		},
		"tidb_get_token_duration": {
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "tidb-0s", 0.999, 0.02*10e5),
		},
		"tidb_load_schema_duration": {
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "tidb-0s", 0.99, 2.0),
		},
		"tikv_scheduler_command_duration": {
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "tikv-0s", "get", 0.99, 2.0),
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "tikv-0s", "write", 0.99, 5.0),
		},
		"tikv_handle_snapshot_duration": {
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "tikv-0s", "gen", 0.999, 40.0),
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "tikv-0s", "read", 0.999, 10.0),
		},
		"tikv_storage_async_request_duration": {
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "tikv-0s", "write", 0.999, 0.2),
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "tikv-0s", "snapshot", 0.999, 0.06),
		},
		"tikv_engine_write_duration": {
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "tikv-0s", "write_max", "kv", 0.2*10e5),
		},
		"tikv_engine_max_get_duration": {
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "tikv-0s", "get_max", "kv", 0.06*10e5),
		},
		"tikv_engine_max_seek_duration": {
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "tikv-0s", "seek_max", "raft", 0.06*10e5),
		},
		"tikv_scheduler_pending_commands": {
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "tikv-0s", 1001.0),
		},
		"tikv_block_index_cache_hit": {
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "tikv-0s", "kv", 0.94),
		},
		"tikv_block_data_cache_hit": {
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "tikv-0s", "kv", 0.79),
		},
		"tikv_block_filter_cache_hit": {
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "tikv-0s", "kv", 0.93),
		},
		"tikv_thread_cpu":           {},
		"pd_scheduler_store_status": {},
		"pd_region_health":          {},
	}

	ctx := s.setupForInspection(c, mockData, nil)
	defer s.tearDownForInspection(c)

	rs, err := tk.Se.Execute(ctx, "select /*+ time_range('2020-02-12 10:35:00','2020-02-12 10:37:00') */ item, type, instance, status_address, value, reference, details from information_schema.inspection_result where rule='threshold-check' order by item")
	c.Assert(err, IsNil)
	result := tk.ResultSetToResultWithCtx(ctx, rs[0], Commentf("execute inspect SQL failed"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(0), Commentf("unexpected warnings: %+v", tk.Se.GetSessionVars().StmtCtx.GetWarnings()))
	result.Check(testkit.Rows(
		"data-block-cache-hit tikv tikv-0 tikv-0s 0.790 > 0.800 min data-block-cache-hit rate of tikv-0s tikv is too low",
		"filter-block-cache-hit tikv tikv-0 tikv-0s 0.930 > 0.950 min filter-block-cache-hit rate of tikv-0s tikv is too low",
		"get-token-duration tidb tidb-0 tidb-0s 0.020 < 0.001 max duration of tidb-0s tidb get-token-duration is too slow",
		"handle-snapshot-duration tikv tikv-0 tikv-0s 40.000 < 30.000 max duration of tikv-0s tikv handle-snapshot-duration is too slow",
		"index-block-cache-hit tikv tikv-0 tikv-0s 0.940 > 0.950 min index-block-cache-hit rate of tikv-0s tikv is too low",
		"load-schema-duration tidb tidb-0 tidb-0s 2.000 < 1.000 max duration of tidb-0s tidb load-schema-duration is too slow",
		"rocksdb-get-duration tikv tikv-0 tikv-0s 0.060 < 0.050 max duration of tikv-0s tikv rocksdb-get-duration is too slow",
		"rocksdb-seek-duration tikv tikv-0 tikv-0s 0.060 < 0.050 max duration of tikv-0s tikv rocksdb-seek-duration is too slow",
		"rocksdb-write-duration tikv tikv-0 tikv-0s 0.200 < 0.100 max duration of tikv-0s tikv rocksdb-write-duration is too slow",
		"scheduler-cmd-duration tikv tikv-0 tikv-0s 5.000 < 0.100 max duration of tikv-0s tikv scheduler-cmd-duration is too slow",
		"scheduler-pending-cmd-count tikv tikv-0 tikv-0s 1001.000 < 1000.000  tikv-0s tikv scheduler has too many pending commands",
		"storage-snapshot-duration tikv tikv-0 tikv-0s 0.060 < 0.050 max duration of tikv-0s tikv storage-snapshot-duration is too slow",
		"storage-write-duration tikv tikv-0 tikv-0s 0.200 < 0.100 max duration of tikv-0s tikv storage-write-duration is too slow",
		"tso-duration tidb pd-0 pd-0 0.060 < 0.050 max duration of pd-0 tidb tso-duration is too slow",
	))
}

func (s *inspectionResultSuite) TestThresholdCheckInspection3(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	datetime := func(s string) types.Time {
		t, err := types.ParseTime(tk.Se.GetSessionVars().StmtCtx, s, mysql.TypeDatetime, types.MaxFsp)
		c.Assert(err, IsNil)
		return t
	}

	// construct some mock abnormal data
	mockData := map[string][][]types.Datum{
		"pd_scheduler_store_status": {
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "pd-0", "tikv-0", "0", "leader_score", 100.0),
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "pd-0", "tikv-1", "1", "leader_score", 50.0),
			types.MakeDatums(datetime("2020-02-14 05:21:00"), "pd-0", "tikv-0", "0", "leader_score", 99.0),
			types.MakeDatums(datetime("2020-02-14 05:21:00"), "pd-0", "tikv-1", "1", "leader_score", 51.0),
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "pd-0", "tikv-0", "0", "region_score", 100.0),
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "pd-0", "tikv-1", "1", "region_score", 90.0),
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "pd-0", "tikv-0", "0", "store_available", 100.0),
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "pd-0", "tikv-1", "1", "store_available", 70.0),
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "pd-0", "tikv-0", "0", "region_count", 20001.0),
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "pd-0", "tikv-0", "0", "leader_count", 10000.0),
			types.MakeDatums(datetime("2020-02-14 05:21:00"), "pd-0", "tikv-0", "0", "leader_count", 5000.0),
			types.MakeDatums(datetime("2020-02-14 05:22:00"), "pd-0", "tikv-0", "0", "leader_count", 5000.0),
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "pd-0", "tikv-1", "0", "leader_count", 5000.0),
			types.MakeDatums(datetime("2020-02-14 05:21:00"), "pd-0", "tikv-1", "0", "leader_count", 10000.0),
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "pd-0", "tikv-2", "0", "leader_count", 10000.0),
			types.MakeDatums(datetime("2020-02-14 05:21:00"), "pd-0", "tikv-2", "0", "leader_count", 0.0),
		},
		"pd_region_health": {
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "pd-0", "extra-peer-region-count", 40.0),
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "pd-0", "learner-peer-region-count", 40.0),
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "pd-0", "pending-peer-region-count", 30.0),
		},
	}

	ctx := s.setupForInspection(c, mockData, nil)
	defer s.tearDownForInspection(c)

	rs, err := tk.Se.Execute(ctx, `select /*+ time_range('2020-02-14 04:20:00','2020-02-14 05:23:00') */
		item, type, instance,status_address, value, reference, details from information_schema.inspection_result
		where rule='threshold-check' and item in ('leader-score-balance','region-score-balance','region-count','region-health','store-available-balance','leader-drop')
		order by item`)
	c.Assert(err, IsNil)
	result := tk.ResultSetToResultWithCtx(ctx, rs[0], Commentf("execute inspect SQL failed"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(0), Commentf("unexpected warnings: %+v", tk.Se.GetSessionVars().StmtCtx.GetWarnings()))
	result.Check(testkit.Rows(
		"leader-drop tikv tikv-2 tikv-2s 10000 <= 50 tikv-2 tikv has too many leader-drop around time 2020-02-14 05:21:00.000000, leader count from 10000 drop to 0",
		"leader-drop tikv tikv-0 tikv-0s 5000 <= 50 tikv-0 tikv has too many leader-drop around time 2020-02-14 05:21:00.000000, leader count from 10000 drop to 5000",
		"leader-score-balance tikv tikv-1 tikv-1s 50.00% < 5.00% tikv-0 max leader_score is 100.00, much more than tikv-1 min leader_score 50.00",
		"region-count tikv tikv-0 tikv-0s 20001.00 <= 20000 tikv-0 tikv has too many regions",
		"region-health pd pd-0 pd-0 110.00 < 100 the count of extra-perr and learner-peer and pending-peer are 110, it means the scheduling is too frequent or too slow",
		"region-score-balance tikv tikv-1 tikv-1s 10.00% < 5.00% tikv-0 max region_score is 100.00, much more than tikv-1 min region_score 90.00",
		"store-available-balance tikv tikv-1 tikv-1s 30.00% < 20.00% tikv-0 max store_available is 100.00, much more than tikv-1 min store_available 70.00"))
}

func (s *inspectionResultSuite) TestCriticalErrorInspection(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	testServers := s.setupClusterGRPCServer(c)
	defer func() {
		for _, s := range testServers {
			s.server.Stop()
		}
	}()

	var servers []string
	for _, s := range testServers {
		servers = append(servers, strings.Join([]string{s.typ, s.address, s.address}, ","))
	}
	fpName2 := "github.com/pingcap/tidb/executor/mockClusterLogServerInfo"
	fpExpr := strings.Join(servers, ";")
	c.Assert(failpoint.Enable(fpName2, fmt.Sprintf(`return("%s")`, fpExpr)), IsNil)
	defer func() { c.Assert(failpoint.Disable(fpName2), IsNil) }()

	datetime := func(str string) types.Time {
		return s.parseTime(c, tk.Se, str)
	}

	// construct some mock data
	mockData := map[string][][]types.Datum{
		// columns: time, instance, type, value
		"tikv_critical_error_total_count": {
			types.MakeDatums(datetime("2020-02-12 10:35:00"), "tikv-0s", "type1", 0.0),
			types.MakeDatums(datetime("2020-02-12 10:36:00"), "tikv-1s", "type1", 1.0),
			types.MakeDatums(datetime("2020-02-12 10:37:00"), "tikv-2s", "type2", 5.0),
		},
		// columns: time, instance, value
		"tidb_panic_count_total_count": {
			types.MakeDatums(datetime("2020-02-12 10:35:00"), "tidb-0s", 4.0),
			types.MakeDatums(datetime("2020-02-12 10:36:00"), "tidb-0s", 0.0),
			types.MakeDatums(datetime("2020-02-12 10:37:00"), "tidb-1s", 1.0),
		},
		// columns: time, instance, value
		"tidb_binlog_error_total_count": {
			types.MakeDatums(datetime("2020-02-12 10:35:00"), "tidb-1s", 4.0),
			types.MakeDatums(datetime("2020-02-12 10:36:00"), "tidb-2s", 0.0),
			types.MakeDatums(datetime("2020-02-12 10:37:00"), "tidb-3s", 1.0),
		},
		// columns: time, instance, db, type, stage, value
		"tikv_scheduler_is_busy_total_count": {
			types.MakeDatums(datetime("2020-02-12 10:35:00"), "tikv-0s", "db1", "type1", "stage1", 1.0),
			types.MakeDatums(datetime("2020-02-12 10:36:00"), "tikv-0s", "db2", "type1", "stage2", 2.0),
			types.MakeDatums(datetime("2020-02-12 10:37:00"), "tikv-1s", "db1", "type2", "stage1", 3.0),
			types.MakeDatums(datetime("2020-02-12 10:38:00"), "tikv-0s", "db1", "type1", "stage2", 4.0),
			types.MakeDatums(datetime("2020-02-12 10:39:00"), "tikv-0s", "db2", "type1", "stage1", 5.0),
			types.MakeDatums(datetime("2020-02-12 10:40:00"), "tikv-1s", "db1", "type2", "stage2", 6.0),
		},
		// columns: time, instance, db, value
		"tikv_coprocessor_is_busy_total_count": {
			types.MakeDatums(datetime("2020-02-12 10:35:00"), "tikv-0s", "db1", 1.0),
			types.MakeDatums(datetime("2020-02-12 10:36:00"), "tikv-0s", "db2", 2.0),
			types.MakeDatums(datetime("2020-02-12 10:37:00"), "tikv-1s", "db1", 3.0),
			types.MakeDatums(datetime("2020-02-12 10:38:00"), "tikv-0s", "db1", 4.0),
			types.MakeDatums(datetime("2020-02-12 10:39:00"), "tikv-0s", "db2", 5.0),
			types.MakeDatums(datetime("2020-02-12 10:40:00"), "tikv-1s", "db1", 6.0),
		},
		// columns: time, instance, db, type, value
		"tikv_channel_full_total_count": {
			types.MakeDatums(datetime("2020-02-12 10:35:00"), "tikv-0s", "db1", "type1", 1.0),
			types.MakeDatums(datetime("2020-02-12 10:36:00"), "tikv-0s", "db2", "type1", 2.0),
			types.MakeDatums(datetime("2020-02-12 10:37:00"), "tikv-1s", "db1", "type2", 3.0),
			types.MakeDatums(datetime("2020-02-12 10:38:00"), "tikv-0s", "db1", "type1", 4.0),
			types.MakeDatums(datetime("2020-02-12 10:39:00"), "tikv-0s", "db2", "type1", 5.0),
			types.MakeDatums(datetime("2020-02-12 10:40:00"), "tikv-1s", "db1", "type2", 6.0),
		},
		// columns: time, instance, db, value
		"tikv_engine_write_stall": {
			types.MakeDatums(datetime("2020-02-12 10:35:00"), "tikv-0s", "kv", 1.0),
			types.MakeDatums(datetime("2020-02-12 10:36:00"), "tikv-0s", "raft", 2.0),
			types.MakeDatums(datetime("2020-02-12 10:37:00"), "tikv-1s", "reason3", 3.0),
		},
		// columns: time, instance, job, value
		"up": {
			types.MakeDatums(datetime("2020-02-12 10:35:00"), "tikv-0s", "tikv", 1.0),
			types.MakeDatums(datetime("2020-02-12 10:36:00"), "tikv-0s", "tikv", 0.0),
			types.MakeDatums(datetime("2020-02-12 10:37:00"), "tidb-0s", "tidb", 0.0),
			types.MakeDatums(datetime("2020-02-12 10:37:00"), "tidb-1s", "tidb", 0.0),
			types.MakeDatums(datetime("2020-02-12 10:38:00"), "tidb-1s", "tidb", 1.0),
		},
	}

	ctx := s.setupForInspection(c, mockData, nil)
	defer s.tearDownForInspection(c)

	rs, err := tk.Se.Execute(ctx, "select /*+ time_range('2020-02-12 10:35:00','2020-02-12 10:37:00') */ item, instance,status_address, value, details from information_schema.inspection_result where rule='critical-error'")
	c.Assert(err, IsNil)
	result := tk.ResultSetToResultWithCtx(ctx, rs[0], Commentf("execute inspect SQL failed"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(0), Commentf("unexpected warnings: %+v", tk.Se.GetSessionVars().StmtCtx.GetWarnings()))
	result.Check(testkit.Rows(
		"server-down tikv-0 tikv-0s  tikv tikv-0s disconnect with prometheus around time '2020-02-12 10:36:00.000000'",
		"server-down tidb-1 tidb-1s  tidb tidb-1s disconnect with prometheus around time '2020-02-12 10:37:00.000000'",
		"channel-is-full tikv-1 tikv-1s 9.00(db1, type2) the total number of errors about 'channel-is-full' is too many",
		"coprocessor-is-busy tikv-1 tikv-1s 9.00(db1) the total number of errors about 'coprocessor-is-busy' is too many",
		"channel-is-full tikv-0 tikv-0s 7.00(db2, type1) the total number of errors about 'channel-is-full' is too many",
		"coprocessor-is-busy tikv-0 tikv-0s 7.00(db2) the total number of errors about 'coprocessor-is-busy' is too many",
		"scheduler-is-busy tikv-1 tikv-1s 6.00(db1, type2, stage2) the total number of errors about 'scheduler-is-busy' is too many",
		"channel-is-full tikv-0 tikv-0s 5.00(db1, type1) the total number of errors about 'channel-is-full' is too many",
		"coprocessor-is-busy tikv-0 tikv-0s 5.00(db1) the total number of errors about 'coprocessor-is-busy' is too many",
		"critical-error tikv-2 tikv-2s 5.00(type2) the total number of errors about 'critical-error' is too many",
		"scheduler-is-busy tikv-0 tikv-0s 5.00(db2, type1, stage1) the total number of errors about 'scheduler-is-busy' is too many",
		"binlog-error tidb-1 tidb-1s 4.00 the total number of errors about 'binlog-error' is too many",
		"panic-count tidb-0 tidb-0s 4.00 the total number of errors about 'panic-count' is too many",
		"scheduler-is-busy tikv-0 tikv-0s 4.00(db1, type1, stage2) the total number of errors about 'scheduler-is-busy' is too many",
		"scheduler-is-busy tikv-1 tikv-1s 3.00(db1, type2, stage1) the total number of errors about 'scheduler-is-busy' is too many",
		"tikv_engine_write_stall tikv-1 tikv-1s 3.00(reason3) the total number of errors about 'tikv_engine_write_stall' is too many",
		"scheduler-is-busy tikv-0 tikv-0s 2.00(db2, type1, stage2) the total number of errors about 'scheduler-is-busy' is too many",
		"tikv_engine_write_stall tikv-0 tikv-0s 2.00(raft) the total number of errors about 'tikv_engine_write_stall' is too many",
		"binlog-error  tidb-3s 1.00 the total number of errors about 'binlog-error' is too many",
		"critical-error tikv-1 tikv-1s 1.00(type1) the total number of errors about 'critical-error' is too many",
		"panic-count tidb-1 tidb-1s 1.00 the total number of errors about 'panic-count' is too many",
		"scheduler-is-busy tikv-0 tikv-0s 1.00(db1, type1, stage1) the total number of errors about 'scheduler-is-busy' is too many",
		"tikv_engine_write_stall tikv-0 tikv-0s 1.00(kv) the total number of errors about 'tikv_engine_write_stall' is too many",
	))
}

func (s *inspectionResultSuite) TestNodeLoadInspection(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	datetime := func(s string) types.Time {
		t, err := types.ParseTime(tk.Se.GetSessionVars().StmtCtx, s, mysql.TypeDatetime, types.MaxFsp)
		c.Assert(err, IsNil)
		return t
	}

	// construct some mock abnormal data
	mockData := map[string][][]types.Datum{
		// columns: time, instance, value
		"node_load1": {
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "node-0", 28.1),
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "node-1", 13.0),
			types.MakeDatums(datetime("2020-02-14 05:21:00"), "node-0", 10.0),
		},
		// columns: time, instance, value
		"node_load5": {
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "node-0", 27.9),
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "node-1", 14.1),
			types.MakeDatums(datetime("2020-02-14 05:21:00"), "node-0", 0.0),
		},
		// columns: time, instance, value
		"node_load15": {
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "node-0", 30.0),
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "node-1", 14.1),
			types.MakeDatums(datetime("2020-02-14 05:21:00"), "node-0", 20.0),
		},
		// columns: time, instance, value
		"node_virtual_cpus": {
			types.MakeDatums(datetime("2020-02-14 05:21:00"), "node-0", 40.0),
			types.MakeDatums(datetime("2020-02-14 05:21:00"), "node-1", 20.0),
		},
		// columns: time, instance, value
		"node_memory_usage": {
			types.MakeDatums(datetime("2020-02-14 05:21:00"), "node-0", 80.0),
			types.MakeDatums(datetime("2020-02-14 05:21:00"), "node-1", 60.0),
			types.MakeDatums(datetime("2020-02-14 05:22:00"), "node-0", 60.0),
		},
		// columns: time, instance, value
		"node_memory_swap_used": {
			types.MakeDatums(datetime("2020-02-14 05:21:00"), "node-0", 0.0),
			types.MakeDatums(datetime("2020-02-14 05:21:00"), "node-1", 1.0),
			types.MakeDatums(datetime("2020-02-14 05:22:00"), "node-1", 0.0),
		},
		// columns: time, instance, device, value
		"node_disk_usage": {
			types.MakeDatums(datetime("2020-02-14 05:21:00"), "node-0", "/dev/nvme0", 80.0),
			types.MakeDatums(datetime("2020-02-14 05:22:00"), "node-0", "/dev/nvme0", 50.0),
			types.MakeDatums(datetime("2020-02-14 05:21:00"), "node-0", "tmpfs", 80.0),
			types.MakeDatums(datetime("2020-02-14 05:22:00"), "node-0", "tmpfs", 50.0),
		},
	}

	ctx := s.setupForInspection(c, mockData, nil)
	defer s.tearDownForInspection(c)

	rs, err := tk.Se.Execute(ctx, `select /*+ time_range('2020-02-14 04:20:00','2020-02-14 05:23:00') */
		item, type, instance, value, reference, details from information_schema.inspection_result
		where rule='node-load' order by item, value`)
	c.Assert(err, IsNil)
	result := tk.ResultSetToResultWithCtx(ctx, rs[0], Commentf("execute inspect SQL failed"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(0), Commentf("unexpected warnings: %+v", tk.Se.GetSessionVars().StmtCtx.GetWarnings()))
	result.Check(testkit.Rows(
		"cpu-load1 node node-0 28.1 < 28.0 cpu-load1 should less than (cpu_logical_cores * 0.7)",
		"cpu-load15 node node-1 14.1 < 14.0 cpu-load15 should less than (cpu_logical_cores * 0.7)",
		"cpu-load15 node node-0 30.0 < 28.0 cpu-load15 should less than (cpu_logical_cores * 0.7)",
		"cpu-load5 node node-1 14.1 < 14.0 cpu-load5 should less than (cpu_logical_cores * 0.7)",
		"disk-usage node node-0 80.0% < 70% the disk-usage of /dev/nvme0 is too high",
		"swap-memory-used node node-1 1.0 0 ",
		"virtual-memory-usage node node-0 80.0% < 70% the memory-usage is too high",
	))
}

func (s *inspectionResultSuite) TestConfigCheckOfStorageBlockCacheSize(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	datetime := func(s string) types.Time {
		t, err := types.ParseTime(tk.Se.GetSessionVars().StmtCtx, s, mysql.TypeDatetime, types.MaxFsp)
		c.Assert(err, IsNil)
		return t
	}

	configurations := map[string]variable.TableSnapshot{}
	configurations[infoschema.TableClusterConfig] = variable.TableSnapshot{
		Rows: [][]types.Datum{
			types.MakeDatums("tikv", "192.168.3.33:26600", "storage.block-cache.capacity", "10GiB"),
			types.MakeDatums("tikv", "192.168.3.33:26700", "storage.block-cache.capacity", "20GiB"),
			types.MakeDatums("tikv", "192.168.3.34:26600", "storage.block-cache.capacity", "1TiB"),
			types.MakeDatums("tikv", "192.168.3.35:26700", "storage.block-cache.capacity", "20GiB"),
		},
	}

	// construct some mock abnormal data
	mockData := map[string][][]types.Datum{
		"node_total_memory": {
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "192.168.3.33:26600", 50.0*1024*1024*1024),
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "192.168.3.34:26600", 50.0*1024*1024*1024),
			types.MakeDatums(datetime("2020-02-14 05:20:00"), "192.168.3.35:26600", 50.0*1024*1024*1024),
		},
	}

	ctx := s.setupForInspection(c, mockData, configurations)
	defer s.tearDownForInspection(c)

	rs, err := tk.Se.Execute(ctx, "select  /*+ time_range('2020-02-14 04:20:00','2020-02-14 05:23:00') */ * from information_schema.inspection_result where rule='config' and item='storage.block-cache.capacity' order by value")
	c.Assert(err, IsNil)
	result := tk.ResultSetToResultWithCtx(ctx, rs[0], Commentf("execute inspect SQL failed"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.WarningCount(), Equals, uint16(0), Commentf("unexpected warnings: %+v", tk.Se.GetSessionVars().StmtCtx.GetWarnings()))
	result.Check(testkit.Rows(
		"config storage.block-cache.capacity tikv 192.168.3.34  1099511627776 < 24159191040 warning There are 1 TiKV server in 192.168.3.34 node, the total 'storage.block-cache.capacity' of TiKV is more than (0.45 * total node memory)",
		"config storage.block-cache.capacity tikv 192.168.3.33  32212254720 < 24159191040 warning There are 2 TiKV server in 192.168.3.33 node, the total 'storage.block-cache.capacity' of TiKV is more than (0.45 * total node memory)",
	))
}
