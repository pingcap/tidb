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

package executor_test

import (
	"fmt"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl/placement"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/tikv/client-go/v2/oracle"
)

func (s *testStaleTxnSerialSuite) TestExactStalenessTransaction(c *C) {
	testcases := []struct {
		name             string
		preSQL           string
		sql              string
		IsStaleness      bool
		expectPhysicalTS int64
		zone             string
	}{
		{
			name:             "AsOfTimestamp",
			preSQL:           "begin",
			sql:              `START TRANSACTION READ ONLY AS OF TIMESTAMP '2020-09-06 00:00:00';`,
			IsStaleness:      true,
			expectPhysicalTS: 1599321600000,
			zone:             "sh",
		},
		{
			name:        "begin after AsOfTimestamp",
			preSQL:      `START TRANSACTION READ ONLY AS OF TIMESTAMP '2020-09-06 00:00:00';`,
			sql:         "begin",
			IsStaleness: false,
			zone:        "",
		},
		{
			name:             "AsOfTimestamp with tidb_bounded_staleness",
			preSQL:           "begin",
			sql:              `START TRANSACTION READ ONLY AS OF TIMESTAMP tidb_bounded_staleness('2015-09-21 00:07:01', NOW());`,
			IsStaleness:      true,
			expectPhysicalTS: 1442765221000,
			zone:             "bj",
		},
		{
			name:        "begin after AsOfTimestamp with tidb_bounded_staleness",
			preSQL:      `START TRANSACTION READ ONLY AS OF TIMESTAMP tidb_bounded_staleness('2015-09-21 00:07:01', NOW());`,
			sql:         "begin",
			IsStaleness: false,
			zone:        "",
		},
	}
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	for _, testcase := range testcases {
		c.Log(testcase.name)
		failpoint.Enable("github.com/pingcap/tidb/config/injectTxnScope",
			fmt.Sprintf(`return("%v")`, testcase.zone))
		tk.MustExec(testcase.preSQL)
		tk.MustExec(testcase.sql)
		c.Assert(tk.Se.GetSessionVars().TxnCtx.IsStaleness, Equals, testcase.IsStaleness)
		if testcase.expectPhysicalTS > 0 {
			c.Assert(oracle.ExtractPhysical(tk.Se.GetSessionVars().TxnCtx.StartTS), Equals, testcase.expectPhysicalTS)
		} else if !testcase.IsStaleness {
			curTS := oracle.ExtractPhysical(oracle.GoTimeToTS(time.Now()))
			startTS := oracle.ExtractPhysical(tk.Se.GetSessionVars().TxnCtx.StartTS)
			c.Assert(curTS-startTS, Less, time.Second.Milliseconds())
			c.Assert(startTS-curTS, Less, time.Second.Milliseconds())
		}
		tk.MustExec("commit")
	}
	failpoint.Disable("github.com/pingcap/tidb/config/injectTxnScope")
}

func (s *testStaleTxnSerialSuite) TestSelectAsOf(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	// For mocktikv, safe point is not initialized, we manually insert it for snapshot to use.
	safePointName := "tikv_gc_safe_point"
	safePointValue := "20160102-15:04:05 -0700"
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
	ON DUPLICATE KEY
	UPDATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	tk.MustExec(updateSafePoint)
	tk.MustExec("drop table if exists t")
	tk.MustExec(`drop table if exists b`)
	tk.MustExec("create table t (id int primary key);")
	tk.MustExec("create table b (pid int primary key);")
	defer func() {
		tk.MustExec(`drop table if exists b`)
		tk.MustExec(`drop table if exists t`)
	}()
	time.Sleep(2 * time.Second)
	now := time.Now()
	time.Sleep(2 * time.Second)

	testcases := []struct {
		setTxnSQL        string
		name             string
		sql              string
		expectPhysicalTS int64
		preSec           int64
		// IsStaleness is auto cleanup in select stmt.
		errorStr string
	}{
		{
			name:             "set transaction as of",
			setTxnSQL:        fmt.Sprintf("set transaction read only as of timestamp '%s';", now.Format("2006-1-2 15:04:05")),
			sql:              "select * from t;",
			expectPhysicalTS: now.Unix(),
		},
		{
			name:      "set transaction as of, expect error",
			setTxnSQL: fmt.Sprintf("set transaction read only as of timestamp '%s';", now.Format("2006-1-2 15:04:05")),
			sql:       fmt.Sprintf("select * from t as of timestamp '%s';", now.Format("2006-1-2 15:04:05")),
			errorStr:  ".*can't use select as of while already set transaction as of.*",
		},
		{
			name:             "TimestampExactRead1",
			sql:              fmt.Sprintf("select * from t as of timestamp '%s';", now.Format("2006-1-2 15:04:05")),
			expectPhysicalTS: now.Unix(),
		},
		{
			name:   "NormalRead",
			sql:    `select * from b;`,
			preSec: 0,
		},
		{
			name:             "TimestampExactRead2",
			sql:              fmt.Sprintf("select * from t as of timestamp TIMESTAMP('%s');", now.Format("2006-1-2 15:04:05")),
			expectPhysicalTS: now.Unix(),
		},
		{
			name:   "TimestampExactRead3",
			sql:    `select * from t as of timestamp NOW() - INTERVAL 2 SECOND;`,
			preSec: 2,
		},
		{
			name:   "TimestampExactRead4",
			sql:    `select * from t as of timestamp TIMESTAMP(NOW() - INTERVAL 2 SECOND);`,
			preSec: 2,
		},
		{
			name:   "TimestampExactRead5",
			sql:    `select * from t as of timestamp TIMESTAMP(NOW() - INTERVAL 1 SECOND), b as of timestamp TIMESTAMP(NOW() - INTERVAL 1 SECOND);`,
			preSec: 1,
		},
		{
			name:     "TimestampExactRead6",
			sql:      `select * from t as of timestamp TIMESTAMP(NOW() - INTERVAL 1 SECOND), b as of timestamp TIMESTAMP('2020-09-06 00:00:00');`,
			errorStr: ".*can not set different time in the as of.*",
		},
		{
			name:     "TimestampExactRead7",
			sql:      `select * from t as of timestamp TIMESTAMP(NOW() - INTERVAL 1 SECOND), b;`,
			errorStr: ".*can not set different time in the as of.*",
		},
		{
			name:     "TimestampExactRead8",
			sql:      `select * from t, b as of timestamp TIMESTAMP(NOW() - INTERVAL 1 SECOND);`,
			errorStr: ".*can not set different time in the as of.*",
		},
		{
			name:   "NomalRead",
			sql:    `select * from t, b;`,
			preSec: 0,
		},
		{
			name:     "TimestampExactRead9",
			sql:      `select * from (select * from t as of timestamp TIMESTAMP(NOW() - INTERVAL 1 SECOND), b as of timestamp TIMESTAMP(NOW() - INTERVAL 1 SECOND)) as c, b;`,
			errorStr: ".*can not set different time in the as of.*",
		},
		{
			name:   "TimestampExactRead10",
			sql:    `select * from (select * from t as of timestamp TIMESTAMP(NOW() - INTERVAL 2 SECOND), b as of timestamp TIMESTAMP(NOW() - INTERVAL 2 SECOND)) as c;`,
			preSec: 2,
		},
		// Cannot be supported the SubSelect
		{
			name:     "TimestampExactRead11",
			sql:      `select * from (select * from t as of timestamp TIMESTAMP(NOW() - INTERVAL 20 SECOND), b as of timestamp TIMESTAMP(NOW() - INTERVAL 20 SECOND)) as c as of timestamp Now();`,
			errorStr: ".*You have an error in your SQL syntax.*",
		},
	}

	for _, testcase := range testcases {
		c.Log(testcase.name)
		if len(testcase.setTxnSQL) > 0 {
			tk.MustExec(testcase.setTxnSQL)
		}
		if testcase.expectPhysicalTS > 0 {
			c.Assert(failpoint.Enable("github.com/pingcap/tidb/executor/assertStaleTSO", fmt.Sprintf(`return(%d)`, testcase.expectPhysicalTS)), IsNil)
		} else if testcase.preSec > 0 {
			c.Assert(failpoint.Enable("github.com/pingcap/tidb/executor/assertStaleTSOWithTolerance", fmt.Sprintf(`return(%d)`, time.Now().Unix()-testcase.preSec)), IsNil)
		}
		_, err := tk.Exec(testcase.sql)
		if len(testcase.errorStr) != 0 {
			c.Assert(err, ErrorMatches, testcase.errorStr)
			continue
		}
		c.Assert(err, IsNil, Commentf("sql:%s, error stack %v", testcase.sql, errors.ErrorStack(err)))
		if testcase.expectPhysicalTS > 0 {
			c.Assert(failpoint.Disable("github.com/pingcap/tidb/executor/assertStaleTSO"), IsNil)
		} else if testcase.preSec > 0 {
			c.Assert(failpoint.Disable("github.com/pingcap/tidb/executor/assertStaleTSOWithTolerance"), IsNil)
		}
		if len(testcase.setTxnSQL) > 0 {
			c.Assert(tk.Se.GetSessionVars().TxnReadTS.PeakTxnReadTS(), Equals, uint64(0))
		}
	}
}

func (s *testStaleTxnSerialSuite) TestStaleReadKVRequest(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	safePointName := "tikv_gc_safe_point"
	safePointValue := "20160102-15:04:05 -0700"
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
	ON DUPLICATE KEY
	UPDATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	tk.MustExec(updateSafePoint)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec(`drop table if exists t1`)
	tk.MustExec(`drop table if exists t2`)
	tk.MustExec("create table t (id int primary key);")
	tk.MustExec(`create table t1 (c int primary key, d int,e int,index idx_d(d),index idx_e(e))`)
	defer tk.MustExec(`drop table if exists t`)
	defer tk.MustExec(`drop table if exists t1`)
	conf := *config.GetGlobalConfig()
	oldConf := conf
	defer config.StoreGlobalConfig(&oldConf)
	conf.Labels = map[string]string{
		placement.DCLabelKey: "sh",
	}
	config.StoreGlobalConfig(&conf)
	testcases := []struct {
		name   string
		sql    string
		assert string
	}{
		{
			name:   "coprocessor read",
			sql:    "select * from t",
			assert: "github.com/pingcap/distsql/assertRequestBuilderStalenessOption",
		},
		{
			name:   "point get read",
			sql:    "select * from t where id = 1",
			assert: "github.com/pingcap/tidb/executor/assertPointStalenessOption",
		},
		{
			name:   "batch point get read",
			sql:    "select * from t where id in (1,2,3)",
			assert: "github.com/pingcap/tidb/executor/assertBatchPointStalenessOption",
		},
	}
	for _, testcase := range testcases {
		failpoint.Enable(testcase.assert, `return("sh")`)
		tk.MustExec(`START TRANSACTION READ ONLY AS OF TIMESTAMP NOW(3);`)
		tk.MustQuery(testcase.sql)
		tk.MustExec(`commit`)
		failpoint.Disable(testcase.assert)
	}
	for _, testcase := range testcases {
		failpoint.Enable(testcase.assert, `return("sh")`)
		tk.MustExec(`SET TRANSACTION READ ONLY AS OF TIMESTAMP NOW(3)`)
		tk.MustExec(`begin;`)
		tk.MustQuery(testcase.sql)
		tk.MustExec(`commit`)
		failpoint.Disable(testcase.assert)
	}
	tk.MustExec(`insert into t1 (c,d,e) values (1,1,1);`)
	tk.MustExec(`insert into t1 (c,d,e) values (2,3,5);`)
	time.Sleep(2 * time.Second)
	tsv := time.Now().Format("2006-1-2 15:04:05.000")
	tk.MustExec(`insert into t1 (c,d,e) values (3,3,7);`)
	tk.MustExec(`insert into t1 (c,d,e) values (4,0,5);`)
	tk.MustExec(`insert into t1 (c,d,e) values (5,0,5);`)
	// IndexLookUp Reader Executor
	rows1 := tk.MustQuery(fmt.Sprintf("select * from t1 AS OF TIMESTAMP '%v' use index (idx_d) where c < 5 and d < 5", tsv)).Rows()
	c.Assert(rows1, HasLen, 2)
	// IndexMerge Reader Executor
	rows2 := tk.MustQuery(fmt.Sprintf("select /*+ USE_INDEX_MERGE(t1, idx_d, idx_e) */ * from t1 AS OF TIMESTAMP '%v' where c <5 and (d =5 or e=5)", tsv)).Rows()
	c.Assert(rows2, HasLen, 1)
	// TableReader Executor
	rows3 := tk.MustQuery(fmt.Sprintf("select * from t1 AS OF TIMESTAMP '%v' where c < 6", tsv)).Rows()
	c.Assert(rows3, HasLen, 2)
	// IndexReader Executor
	rows4 := tk.MustQuery(fmt.Sprintf("select /*+ USE_INDEX(t1, idx_d) */ d from t1 AS OF TIMESTAMP '%v' where c < 5 and d < 1;", tsv)).Rows()
	c.Assert(rows4, HasLen, 0)
	// point get executor
	rows5 := tk.MustQuery(fmt.Sprintf("select * from t1 AS OF TIMESTAMP '%v' where c = 3;", tsv)).Rows()
	c.Assert(rows5, HasLen, 0)
	rows6 := tk.MustQuery(fmt.Sprintf("select * from t1 AS OF TIMESTAMP '%v' where c in (3,4,5);", tsv)).Rows()
	c.Assert(rows6, HasLen, 0)
}

func (s *testStaleTxnSuite) TestStalenessAndHistoryRead(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	// For mocktikv, safe point is not initialized, we manually insert it for snapshot to use.
	safePointName := "tikv_gc_safe_point"
	safePointValue := "20160102-15:04:05 -0700"
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
	ON DUPLICATE KEY
	UPDATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	tk.MustExec(updateSafePoint)

	time1 := time.Now()
	time1TS := oracle.GoTimeToTS(time1)
	schemaVer1 := tk.Se.GetInfoSchema().SchemaMetaVersion()
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int primary key);")
	tk.MustExec(`drop table if exists t`)
	time.Sleep(50 * time.Millisecond)
	time2 := time.Now()
	time2TS := oracle.GoTimeToTS(time2)
	schemaVer2 := tk.Se.GetInfoSchema().SchemaMetaVersion()

	// test set txn as of will flush/mutex tidb_snapshot
	tk.MustExec(fmt.Sprintf(`set @@tidb_snapshot="%s"`, time1.Format("2006-1-2 15:04:05.000")))
	c.Assert(tk.Se.GetSessionVars().SnapshotTS, Equals, time1TS)
	c.Assert(tk.Se.GetSessionVars().SnapshotInfoschema, NotNil)
	c.Assert(tk.Se.GetInfoSchema().SchemaMetaVersion(), Equals, schemaVer1)
	tk.MustExec(fmt.Sprintf(`SET TRANSACTION READ ONLY AS OF TIMESTAMP '%s'`, time2.Format("2006-1-2 15:04:05.000")))
	c.Assert(tk.Se.GetSessionVars().SnapshotTS, Equals, uint64(0))
	c.Assert(tk.Se.GetSessionVars().SnapshotInfoschema, NotNil)
	c.Assert(tk.Se.GetSessionVars().TxnReadTS.PeakTxnReadTS(), Equals, time2TS)
	c.Assert(tk.Se.GetInfoSchema().SchemaMetaVersion(), Equals, schemaVer2)

	// test tidb_snapshot will flush/mutex set txn as of
	tk.MustExec(fmt.Sprintf(`SET TRANSACTION READ ONLY AS OF TIMESTAMP '%s'`, time1.Format("2006-1-2 15:04:05.000")))
	c.Assert(tk.Se.GetSessionVars().TxnReadTS.PeakTxnReadTS(), Equals, time1TS)
	c.Assert(tk.Se.GetSessionVars().SnapshotInfoschema, NotNil)
	c.Assert(tk.Se.GetInfoSchema().SchemaMetaVersion(), Equals, schemaVer1)
	tk.MustExec(fmt.Sprintf(`set @@tidb_snapshot="%s"`, time2.Format("2006-1-2 15:04:05.000")))
	c.Assert(tk.Se.GetSessionVars().TxnReadTS.PeakTxnReadTS(), Equals, uint64(0))
	c.Assert(tk.Se.GetSessionVars().SnapshotTS, Equals, time2TS)
	c.Assert(tk.Se.GetSessionVars().SnapshotInfoschema, NotNil)
	c.Assert(tk.Se.GetInfoSchema().SchemaMetaVersion(), Equals, schemaVer2)

	// test start txn will flush/mutex tidb_snapshot
	tk.MustExec(fmt.Sprintf(`set @@tidb_snapshot="%s"`, time1.Format("2006-1-2 15:04:05.000")))
	c.Assert(tk.Se.GetSessionVars().SnapshotTS, Equals, time1TS)
	c.Assert(tk.Se.GetSessionVars().SnapshotInfoschema, NotNil)
	c.Assert(tk.Se.GetInfoSchema().SchemaMetaVersion(), Equals, schemaVer1)

	tk.MustExec(fmt.Sprintf(`START TRANSACTION READ ONLY AS OF TIMESTAMP '%s'`, time2.Format("2006-1-2 15:04:05.000")))
	c.Assert(tk.Se.GetSessionVars().SnapshotTS, Equals, uint64(0))
	c.Assert(tk.Se.GetSessionVars().TxnCtx.StartTS, Equals, time2TS)
	c.Assert(tk.Se.GetSessionVars().SnapshotInfoschema, IsNil)
	c.Assert(tk.Se.GetInfoSchema().SchemaMetaVersion(), Equals, schemaVer2)
	tk.MustExec("commit")
	c.Assert(tk.Se.GetSessionVars().SnapshotTS, Equals, uint64(0))
	c.Assert(tk.Se.GetSessionVars().SnapshotInfoschema, IsNil)
	c.Assert(tk.Se.GetInfoSchema().SchemaMetaVersion(), Equals, schemaVer2)

	// test snapshot mutex with txn
	tk.MustExec("START TRANSACTION")
	c.Assert(tk.Se.GetSessionVars().SnapshotTS, Equals, uint64(0))
	c.Assert(tk.Se.GetSessionVars().SnapshotInfoschema, IsNil)
	err := tk.ExecToErr(`set @@tidb_snapshot="2020-10-08 16:45:26";`)
	c.Assert(err, ErrorMatches, ".*Transaction characteristics can't be changed while a transaction is in progress")
	c.Assert(tk.Se.GetSessionVars().SnapshotTS, Equals, uint64(0))
	c.Assert(tk.Se.GetSessionVars().SnapshotInfoschema, IsNil)
	tk.MustExec("commit")

	// test set txn as of txn mutex with txn
	tk.MustExec("START TRANSACTION")
	c.Assert(tk.Se.GetSessionVars().TxnReadTS.PeakTxnReadTS(), Equals, uint64(0))
	err = tk.ExecToErr("SET TRANSACTION READ ONLY AS OF TIMESTAMP '2020-10-08 16:46:26'")
	c.Assert(err, ErrorMatches, ".*Transaction characteristics can't be changed while a transaction is in progress")
	c.Assert(tk.Se.GetSessionVars().TxnReadTS.PeakTxnReadTS(), Equals, uint64(0))
	tk.MustExec("commit")
}

func (s *testStaleTxnSerialSuite) TestTimeBoundedStalenessTxn(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int primary key);")
	defer tk.MustExec(`drop table if exists t`)
	testcases := []struct {
		name         string
		sql          string
		injectSafeTS uint64
		// compareWithSafeTS will be 0 if StartTS==SafeTS, -1 if StartTS < SafeTS, and +1 if StartTS > SafeTS.
		compareWithSafeTS int
	}{
		{
			name:              "20 seconds ago to now, safeTS 10 secs ago",
			sql:               `START TRANSACTION READ ONLY AS OF TIMESTAMP tidb_bounded_staleness(NOW() - INTERVAL 20 SECOND, NOW())`,
			injectSafeTS:      oracle.GoTimeToTS(time.Now().Add(-10 * time.Second)),
			compareWithSafeTS: 0,
		},
		{
			name:              "10 seconds ago to now, safeTS 20 secs ago",
			sql:               `START TRANSACTION READ ONLY AS OF TIMESTAMP tidb_bounded_staleness(NOW() - INTERVAL 10 SECOND, NOW())`,
			injectSafeTS:      oracle.GoTimeToTS(time.Now().Add(-20 * time.Second)),
			compareWithSafeTS: 1,
		},
		{
			name:              "20 seconds ago to 10 seconds ago, safeTS 5 secs ago",
			sql:               `START TRANSACTION READ ONLY AS OF TIMESTAMP tidb_bounded_staleness(NOW() - INTERVAL 20 SECOND, NOW() - INTERVAL 10 SECOND)`,
			injectSafeTS:      oracle.GoTimeToTS(time.Now().Add(-5 * time.Second)),
			compareWithSafeTS: -1,
		},
		{
			name:              "exact timestamp 5 seconds ago, safeTS 10 secs ago",
			sql:               `START TRANSACTION READ ONLY AS OF TIMESTAMP NOW() - INTERVAL 5 SECOND`,
			injectSafeTS:      oracle.GoTimeToTS(time.Now().Add(-10 * time.Second)),
			compareWithSafeTS: 1,
		},
		{
			name:              "exact timestamp 10 seconds ago, safeTS 5 secs ago",
			sql:               `START TRANSACTION READ ONLY AS OF TIMESTAMP NOW() - INTERVAL 10 SECOND`,
			injectSafeTS:      oracle.GoTimeToTS(time.Now().Add(-5 * time.Second)),
			compareWithSafeTS: -1,
		},
	}
	for _, testcase := range testcases {
		c.Log(testcase.name)
		c.Assert(failpoint.Enable("tikvclient/injectSafeTS",
			fmt.Sprintf("return(%v)", testcase.injectSafeTS)), IsNil)
		c.Assert(failpoint.Enable("github.com/pingcap/tidb/expression/injectSafeTS",
			fmt.Sprintf("return(%v)", testcase.injectSafeTS)), IsNil)
		tk.MustExec(testcase.sql)
		if testcase.compareWithSafeTS == 1 {
			c.Assert(tk.Se.GetSessionVars().TxnCtx.StartTS, Greater, testcase.injectSafeTS)
		} else if testcase.compareWithSafeTS == 0 {
			c.Assert(tk.Se.GetSessionVars().TxnCtx.StartTS, Equals, testcase.injectSafeTS)
		} else {
			c.Assert(tk.Se.GetSessionVars().TxnCtx.StartTS, Less, testcase.injectSafeTS)
		}
		tk.MustExec("commit")
	}
	failpoint.Disable("github.com/pingcap/tidb/expression/injectSafeTS")
	failpoint.Disable("tikvclient/injectSafeTS")
}

func (s *testStaleTxnSerialSuite) TestStalenessTransactionSchemaVer(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	defer tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int primary key);")

	schemaVer1 := tk.Se.GetInfoSchema().SchemaMetaVersion()
	time1 := time.Now()
	tk.MustExec("alter table t add c int")

	// confirm schema changed
	schemaVer2 := tk.Se.GetInfoSchema().SchemaMetaVersion()
	c.Assert(schemaVer1, Less, schemaVer2)

	// get the specific old schema
	tk.MustExec(fmt.Sprintf(`START TRANSACTION READ ONLY AS OF TIMESTAMP '%s'`, time1.Format("2006-1-2 15:04:05.000")))
	c.Assert(tk.Se.GetInfoSchema().SchemaMetaVersion(), Equals, schemaVer1)

	// schema changed back to the newest
	tk.MustExec("commit")
	c.Assert(tk.Se.GetInfoSchema().SchemaMetaVersion(), Equals, schemaVer2)

	// select does not affect the infoschema
	tk.MustExec(fmt.Sprintf(`SELECT * from t AS OF TIMESTAMP '%s'`, time1.Format("2006-1-2 15:04:05.000")))
	c.Assert(tk.Se.GetInfoSchema().SchemaMetaVersion(), Equals, schemaVer2)
}

func (s *testStaleTxnSerialSuite) TestSetTransactionReadOnlyAsOf(c *C) {
	t1, err := time.Parse(types.TimeFormat, "2016-09-21 09:53:04")
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, s.store)
	// For mocktikv, safe point is not initialized, we manually insert it for snapshot to use.
	safePointName := "tikv_gc_safe_point"
	safePointValue := "20160102-15:04:05 -0700"
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
	ON DUPLICATE KEY
	UPDATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	tk.MustExec(updateSafePoint)
	testcases := []struct {
		sql          string
		expectedTS   uint64
		injectSafeTS uint64
	}{
		{
			sql:          `SET TRANSACTION READ ONLY as of timestamp '2021-04-21 00:42:12'`,
			expectedTS:   424394603102208000,
			injectSafeTS: 0,
		},
		{
			sql:          `SET TRANSACTION READ ONLY as of timestamp tidb_bounded_staleness('2015-09-21 00:07:01', '2021-04-27 11:26:13')`,
			expectedTS:   oracle.GoTimeToTS(t1),
			injectSafeTS: oracle.GoTimeToTS(t1),
		},
	}
	for _, testcase := range testcases {
		if testcase.injectSafeTS > 0 {
			c.Assert(failpoint.Enable("github.com/pingcap/tidb/expression/injectSafeTS",
				fmt.Sprintf("return(%v)", testcase.injectSafeTS)), IsNil)
		}
		tk.MustExec(testcase.sql)
		c.Assert(tk.Se.GetSessionVars().TxnReadTS.PeakTxnReadTS(), Equals, testcase.expectedTS)
		tk.MustExec("begin")
		c.Assert(tk.Se.GetSessionVars().TxnCtx.StartTS, Equals, testcase.expectedTS)
		tk.MustExec("commit")
		c.Assert(tk.Se.GetSessionVars().TxnReadTS.PeakTxnReadTS(), Equals, uint64(0))
		tk.MustExec("begin")
		c.Assert(tk.Se.GetSessionVars().TxnCtx.StartTS, Not(Equals), testcase.expectedTS)
		tk.MustExec("commit")

		failpoint.Disable("github.com/pingcap/tidb/expression/injectSafeTS")
	}

	err = tk.ExecToErr(`SET TRANSACTION READ ONLY as of timestamp tidb_bounded_staleness(invalid1, invalid2')`)
	c.Assert(err, NotNil)
	c.Assert(tk.Se.GetSessionVars().TxnReadTS.PeakTxnReadTS(), Equals, uint64(0))

	tk.MustExec(`SET TRANSACTION READ ONLY as of timestamp '2021-04-21 00:42:12'`)
	err = tk.ExecToErr(`START TRANSACTION READ ONLY AS OF TIMESTAMP '2020-09-06 00:00:00'`)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "start transaction read only as of is forbidden after set transaction read only as of")
	tk.MustExec(`SET TRANSACTION READ ONLY as of timestamp '2021-04-21 00:42:12'`)
	err = tk.ExecToErr(`START TRANSACTION READ ONLY AS OF TIMESTAMP '2020-09-06 00:00:00'`)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "start transaction read only as of is forbidden after set transaction read only as of")

	tk.MustExec("begin")
	c.Assert(tk.Se.GetSessionVars().TxnReadTS.PeakTxnReadTS(), Equals, uint64(424394603102208000))
	tk.MustExec("commit")
	tk.MustExec(`START TRANSACTION READ ONLY AS OF TIMESTAMP '2020-09-06 00:00:00'`)
}

func (s *testStaleTxnSerialSuite) TestValidateReadOnlyInStalenessTransaction(c *C) {
	testcases := []struct {
		name       string
		sql        string
		isValidate bool
	}{
		{
			name:       "select statement",
			sql:        `select * from t;`,
			isValidate: true,
		},
		{
			name:       "explain statement",
			sql:        `explain insert into t (id) values (1);`,
			isValidate: true,
		},
		{
			name:       "explain analyze insert statement",
			sql:        `explain analyze insert into t (id) values (1);`,
			isValidate: false,
		},
		{
			name:       "explain analyze select statement",
			sql:        `explain analyze select * from t `,
			isValidate: true,
		},
		{
			name:       "execute insert statement",
			sql:        `EXECUTE stmt1;`,
			isValidate: false,
		},
		{
			name:       "execute select statement",
			sql:        `EXECUTE stmt2;`,
			isValidate: true,
		},
		{
			name:       "show statement",
			sql:        `show tables;`,
			isValidate: true,
		},
		{
			name:       "set union",
			sql:        `SELECT 1, 2 UNION SELECT 'a', 'b';`,
			isValidate: true,
		},
		{
			name:       "insert",
			sql:        `insert into t (id) values (1);`,
			isValidate: false,
		},
		{
			name:       "delete",
			sql:        `delete from t where id =1`,
			isValidate: false,
		},
		{
			name:       "update",
			sql:        "update t set id =2 where id =1",
			isValidate: false,
		},
		{
			name:       "point get",
			sql:        `select * from t where id = 1`,
			isValidate: true,
		},
		{
			name:       "batch point get",
			sql:        `select * from t where id in (1,2,3);`,
			isValidate: true,
		},
		{
			name:       "split table",
			sql:        `SPLIT TABLE t BETWEEN (0) AND (1000000000) REGIONS 16;`,
			isValidate: true,
		},
		{
			name:       "do statement",
			sql:        `DO SLEEP(1);`,
			isValidate: true,
		},
		{
			name:       "select for update",
			sql:        "select * from t where id = 1 for update",
			isValidate: false,
		},
		{
			name:       "select lock in share mode",
			sql:        "select * from t where id = 1 lock in share mode",
			isValidate: true,
		},
		{
			name:       "select for update union statement",
			sql:        "select * from t for update union select * from t;",
			isValidate: false,
		},
		{
			name:       "replace statement",
			sql:        "replace into t(id) values (1)",
			isValidate: false,
		},
		{
			name:       "load data statement",
			sql:        "LOAD DATA LOCAL INFILE '/mn/asa.csv' INTO TABLE t FIELDS TERMINATED BY x'2c' ENCLOSED BY b'100010' LINES TERMINATED BY '\r\n' IGNORE 1 LINES (id);",
			isValidate: false,
		},
		{
			name:       "update multi tables",
			sql:        "update t,t1 set t.id = 1,t1.id = 2 where t.1 = 2 and t1.id = 3;",
			isValidate: false,
		},
		{
			name:       "delete multi tables",
			sql:        "delete t from t1 where t.id = t1.id",
			isValidate: false,
		},
		{
			name:       "insert select",
			sql:        "insert into t select * from t1;",
			isValidate: false,
		},
	}
	tk := testkit.NewTestKit(c, s.store)
	// For mocktikv, safe point is not initialized, we manually insert it for snapshot to use.
	safePointName := "tikv_gc_safe_point"
	safePointValue := "20160102-15:04:05 -0700"
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
	ON DUPLICATE KEY
	UPDATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	tk.MustExec(updateSafePoint)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int);")
	tk.MustExec("create table t1 (id int);")
	tk.MustExec(`PREPARE stmt1 FROM 'insert into t(id) values (5);';`)
	tk.MustExec(`PREPARE stmt2 FROM 'select * from t';`)
	tk.MustExec(`set @@tidb_enable_noop_functions=1;`)
	for _, testcase := range testcases {
		c.Log(testcase.name)
		tk.MustExec(`START TRANSACTION READ ONLY AS OF TIMESTAMP NOW(3);`)
		if testcase.isValidate {
			_, err := tk.Exec(testcase.sql)
			c.Assert(err, IsNil)
		} else {
			err := tk.ExecToErr(testcase.sql)
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Matches, `.*only support read-only statement during read-only staleness transactions.*`)
		}
		tk.MustExec("commit")
		tk.MustExec("set transaction read only as of timestamp NOW(3);")
		if testcase.isValidate {
			_, err := tk.Exec(testcase.sql)
			c.Assert(err, IsNil)
		} else {
			err := tk.ExecToErr(testcase.sql)
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Matches, `.*only support read-only statement during read-only staleness transactions.*`)
		}
		tk.MustExec("set transaction read only as of timestamp ''")
	}
}

func (s *testStaleTxnSuite) TestSpecialSQLInStalenessTxn(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	testcases := []struct {
		name        string
		sql         string
		sameSession bool
	}{
		{
			name:        "ddl",
			sql:         "create table t (id int, b int,INDEX(b));",
			sameSession: false,
		},
		{
			name:        "set global session",
			sql:         `SET GLOBAL sql_mode = 'STRICT_TRANS_TABLES,NO_AUTO_CREATE_USER';`,
			sameSession: true,
		},
		{
			name:        "analyze table",
			sql:         "analyze table t",
			sameSession: true,
		},
		{
			name:        "session binding",
			sql:         "CREATE SESSION BINDING FOR  SELECT * FROM t WHERE b = 123 USING SELECT * FROM t IGNORE INDEX (b) WHERE b = 123;",
			sameSession: true,
		},
		{
			name:        "global binding",
			sql:         "CREATE GLOBAL BINDING FOR  SELECT * FROM t WHERE b = 123 USING SELECT * FROM t IGNORE INDEX (b) WHERE b = 123;",
			sameSession: true,
		},
		{
			name:        "grant statements",
			sql:         "GRANT ALL ON test.* TO 'newuser';",
			sameSession: false,
		},
		{
			name:        "revoke statements",
			sql:         "REVOKE ALL ON test.* FROM 'newuser';",
			sameSession: false,
		},
	}
	tk.MustExec("CREATE USER 'newuser' IDENTIFIED BY 'mypassword';")
	for _, testcase := range testcases {
		comment := Commentf(testcase.name)
		tk.MustExec(`START TRANSACTION READ ONLY AS OF TIMESTAMP NOW(3);`)
		c.Assert(tk.Se.GetSessionVars().TxnCtx.IsStaleness, Equals, true, comment)
		tk.MustExec(testcase.sql)
		c.Assert(tk.Se.GetSessionVars().TxnCtx.IsStaleness, Equals, testcase.sameSession, comment)
	}
}

func (s *testStaleTxnSuite) TestAsOfTimestampCompatibility(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	// For mocktikv, safe point is not initialized, we manually insert it for snapshot to use.
	safePointName := "tikv_gc_safe_point"
	safePointValue := "20160102-15:04:05 -0700"
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
	ON DUPLICATE KEY
	UPDATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	tk.MustExec(updateSafePoint)
	tk.MustExec("use test")
	tk.MustExec("create table t5(id int);")
	defer tk.MustExec("drop table if exists t5;")
	time1 := time.Now()
	testcases := []struct {
		beginSQL string
		sql      string
	}{
		{
			beginSQL: fmt.Sprintf("START TRANSACTION READ ONLY AS OF TIMESTAMP '%s'", time1.Format("2006-1-2 15:04:05.000")),
			sql:      fmt.Sprintf(`SET TRANSACTION READ ONLY AS OF TIMESTAMP '%s'`, time1.Format("2006-1-2 15:04:05.000")),
		},
		{
			beginSQL: "begin",
			sql:      fmt.Sprintf(`SET TRANSACTION READ ONLY AS OF TIMESTAMP '%s'`, time1.Format("2006-1-2 15:04:05.000")),
		},
		{
			beginSQL: "start transaction",
			sql:      fmt.Sprintf(`SET TRANSACTION READ ONLY AS OF TIMESTAMP '%s'`, time1.Format("2006-1-2 15:04:05.000")),
		},
		{
			beginSQL: fmt.Sprintf("START TRANSACTION READ ONLY AS OF TIMESTAMP '%s'", time1.Format("2006-1-2 15:04:05.000")),
			sql:      fmt.Sprintf("select * from t5 as of timestamp '%s'", time1.Format("2006-1-2 15:04:05.000")),
		},
		{
			beginSQL: "begin",
			sql:      fmt.Sprintf("select * from t5 as of timestamp '%s'", time1.Format("2006-1-2 15:04:05.000")),
		},
		{
			beginSQL: "start transaction",
			sql:      fmt.Sprintf("select * from t5 as of timestamp '%s'", time1.Format("2006-1-2 15:04:05.000")),
		},
	}
	for _, testcase := range testcases {
		tk.MustExec(testcase.beginSQL)
		err := tk.ExecToErr(testcase.sql)
		c.Assert(err, ErrorMatches, ".*as of timestamp can't be set in transaction.*|.*Transaction characteristics can't be changed while a transaction is in progress")
		tk.MustExec("commit")
	}
	tk.MustExec(`create table test.table1 (id int primary key, a int);`)
	defer tk.MustExec("drop table if exists test.table1;")
	time1 = time.Now()
	tk.MustExec(fmt.Sprintf("explain analyze select * from test.table1 as of timestamp '%s' where id = 1;", time1.Format("2006-1-2 15:04:05.000")))
}

func (s *testStaleTxnSuite) TestSetTransactionInfoSchema(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	// For mocktikv, safe point is not initialized, we manually insert it for snapshot to use.
	safePointName := "tikv_gc_safe_point"
	safePointValue := "20160102-15:04:05 -0700"
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
	ON DUPLICATE KEY
	UPDATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	tk.MustExec(updateSafePoint)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	defer tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int primary key);")

	schemaVer1 := tk.Se.GetInfoSchema().SchemaMetaVersion()
	time1 := time.Now()
	tk.MustExec("alter table t add c int")

	// confirm schema changed
	schemaVer2 := tk.Se.GetInfoSchema().SchemaMetaVersion()
	time2 := time.Now()
	c.Assert(schemaVer1, Less, schemaVer2)
	tk.MustExec(fmt.Sprintf(`SET TRANSACTION READ ONLY AS OF TIMESTAMP '%s'`, time1.Format("2006-1-2 15:04:05.000")))
	c.Assert(tk.Se.GetInfoSchema().SchemaMetaVersion(), Equals, schemaVer1)
	tk.MustExec("select * from t;")
	tk.MustExec("alter table t add d int")
	schemaVer3 := tk.Se.GetInfoSchema().SchemaMetaVersion()
	tk.MustExec(fmt.Sprintf(`SET TRANSACTION READ ONLY AS OF TIMESTAMP '%s'`, time1.Format("2006-1-2 15:04:05.000")))
	tk.MustExec("begin;")
	c.Assert(tk.Se.GetInfoSchema().SchemaMetaVersion(), Equals, schemaVer1)
	tk.MustExec("commit")
	tk.MustExec(fmt.Sprintf(`SET TRANSACTION READ ONLY AS OF TIMESTAMP '%s'`, time2.Format("2006-1-2 15:04:05.000")))
	tk.MustExec("begin;")
	c.Assert(tk.Se.GetInfoSchema().SchemaMetaVersion(), Equals, schemaVer2)
	tk.MustExec("commit")
	c.Assert(tk.Se.GetInfoSchema().SchemaMetaVersion(), Equals, schemaVer3)
}

func (s *testStaleTxnSuite) TestStaleSelect(c *C) {
	c.Skip("unstable, skip it and fix it before 20210702")
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	defer tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int)")

	tolerance := 50 * time.Millisecond

	tk.MustExec("insert into t values (1)")
	time.Sleep(tolerance)
	time1 := time.Now()

	tk.MustExec("insert into t values (2)")
	time.Sleep(tolerance)
	time2 := time.Now()

	tk.MustExec("insert into t values (3)")
	time.Sleep(tolerance)

	staleRows := testkit.Rows("1")
	staleSQL := fmt.Sprintf(`select * from t as of timestamp '%s'`, time1.Format("2006-1-2 15:04:05.000"))

	// test normal stale select
	tk.MustQuery(staleSQL).Check(staleRows)

	// test stale select in txn
	tk.MustExec("begin")
	c.Assert(tk.ExecToErr(staleSQL), NotNil)
	tk.MustExec("commit")

	// test prepared stale select
	tk.MustExec(fmt.Sprintf(`prepare s from "%s"`, staleSQL))
	tk.MustQuery("execute s")

	// test prepared stale select in txn
	tk.MustExec("begin")
	c.Assert(tk.ExecToErr(staleSQL), NotNil)
	tk.MustExec("commit")

	// test stale select in stale txn
	tk.MustExec(fmt.Sprintf(`start transaction read only as of timestamp '%s'`, time2.Format("2006-1-2 15:04:05.000")))
	c.Assert(tk.ExecToErr(staleSQL), NotNil)
	tk.MustExec("commit")

	// test prepared stale select in stale txn
	tk.MustExec(fmt.Sprintf(`start transaction read only as of timestamp '%s'`, time2.Format("2006-1-2 15:04:05.000")))
	c.Assert(tk.ExecToErr(staleSQL), NotNil)
	tk.MustExec("commit")

	// test prepared stale select with schema change
	tk.MustExec("alter table t add column c int")
	tk.MustExec("insert into t values (4, 5)")
	time.Sleep(10 * time.Millisecond)
	tk.MustQuery("execute s").Check(staleRows)

	// test dynamic timestamp stale select
	time3 := time.Now()
	tk.MustExec("alter table t add column d int")
	tk.MustExec("insert into t values (4, 4, 4)")
	time.Sleep(tolerance)
	time4 := time.Now()
	staleRows = testkit.Rows("1 <nil>", "2 <nil>", "3 <nil>", "4 5")
	tk.MustQuery(fmt.Sprintf("select * from t as of timestamp CURRENT_TIMESTAMP(3) - INTERVAL %d MICROSECOND", time4.Sub(time3).Microseconds())).Check(staleRows)

	// test prepared dynamic timestamp stale select
	time5 := time.Now()
	tk.MustExec(fmt.Sprintf(`prepare v from "select * from t as of timestamp CURRENT_TIMESTAMP(3) - INTERVAL %d MICROSECOND"`, time5.Sub(time3).Microseconds()))
	tk.MustQuery("execute v").Check(staleRows)

	// test point get
	time6 := time.Now()
	tk.MustExec("insert into t values (5, 5, 5)")
	time.Sleep(tolerance)
	tk.MustQuery(fmt.Sprintf("select * from t as of timestamp '%s' where c=5", time6.Format("2006-1-2 15:04:05.000"))).Check(testkit.Rows("4 5 <nil>"))
}

func (s *testStaleTxnSuite) TestStaleReadFutureTime(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	defer tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int)")

	// Setting tx_read_ts to a time in the future will fail. (One day before the 2038 problem)
	_, err := tk.Exec("start transaction read only as of timestamp '2038-01-18 03:14:07'")
	c.Assert(err, ErrorMatches, "cannot set read timestamp to a future time")
	// Transaction should not be started and read ts should not be set if check fails
	c.Assert(tk.Se.GetSessionVars().InTxn(), IsFalse)
	c.Assert(tk.Se.GetSessionVars().TxnReadTS.PeakTxnReadTS(), Equals, uint64(0))

	_, err = tk.Exec("set transaction read only as of timestamp '2038-01-18 03:14:07'")
	c.Assert(err, ErrorMatches, "cannot set read timestamp to a future time")
	c.Assert(tk.Se.GetSessionVars().TxnReadTS.PeakTxnReadTS(), Equals, uint64(0))

	_, err = tk.Exec("select * from t as of timestamp '2038-01-18 03:14:07'")
	c.Assert(err, ErrorMatches, "cannot set read timestamp to a future time")
}
