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
	"github.com/pingcap/tidb/ddl/placement"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testStaleTxnSerialSuite) TestExactStalenessTransaction(c *C) {
	testcases := []struct {
		name             string
		preSQL           string
		sql              string
		IsStaleness      bool
		expectPhysicalTS int64
		txnScope         string
		zone             string
	}{
		{
			name:             "AsOfTimestamp",
			preSQL:           "begin",
			sql:              `START TRANSACTION READ ONLY AS OF TIMESTAMP '2020-09-06 00:00:00';`,
			IsStaleness:      true,
			expectPhysicalTS: 1599321600000,
			txnScope:         "local",
			zone:             "sh",
		},
		{
			name:        "begin after AsOfTimestamp",
			preSQL:      `START TRANSACTION READ ONLY AS OF TIMESTAMP '2020-09-06 00:00:00';`,
			sql:         "begin",
			IsStaleness: false,
			txnScope:    oracle.GlobalTxnScope,
			zone:        "",
		},
		{
			name:             "AsOfTimestamp with tidb_bounded_staleness",
			preSQL:           "begin",
			sql:              `START TRANSACTION READ ONLY AS OF TIMESTAMP tidb_bounded_staleness('2015-09-21 00:07:01', NOW());`,
			IsStaleness:      true,
			expectPhysicalTS: 1442765221000,
			txnScope:         "local",
			zone:             "bj",
		},
		{
			name:        "begin after AsOfTimestamp with tidb_bounded_staleness",
			preSQL:      `START TRANSACTION READ ONLY AS OF TIMESTAMP tidb_bounded_staleness('2015-09-21 00:07:01', NOW());`,
			sql:         "begin",
			IsStaleness: false,
			txnScope:    oracle.GlobalTxnScope,
			zone:        "",
		},
	}
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	for _, testcase := range testcases {
		c.Log(testcase.name)
		failpoint.Enable("github.com/pingcap/tidb/config/injectTxnScope",
			fmt.Sprintf(`return("%v")`, testcase.zone))
		tk.MustExec(fmt.Sprintf("set @@txn_scope=%v", testcase.txnScope))
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
	tk.MustExec("drop table if exists t")
	tk.MustExec(`drop table if exists b`)
	tk.MustExec("create table t (id int primary key);")
	tk.MustExec("create table b (pid int primary key);")
	defer func() {
		tk.MustExec(`drop table if exists b`)
		tk.MustExec(`drop table if exists t`)
	}()

	time.Sleep(3 * time.Second)

	now := time.Now()

	testcases := []struct {
		name             string
		sql              string
		expectPhysicalTS int64
		preSec           int64
		// IsStaleness is auto cleanup in select stmt.
		errorStr string
	}{
		{
			name:             "TimestampExactRead1",
			sql:              fmt.Sprintf("select * from t as of timestamp '%s';", now.Format("2006-1-2 15:04:05")),
			expectPhysicalTS: now.Unix(),
		},
		{
			name:   "NomalRead",
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
	}
}

func (s *testStaleTxnSerialSuite) TestStaleReadKVRequest(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int primary key);")
	defer tk.MustExec(`drop table if exists t`)
	testcases := []struct {
		name     string
		sql      string
		txnScope string
		zone     string
	}{
		{
			name:     "coprocessor read",
			sql:      "select * from t",
			txnScope: "local",
			zone:     "sh",
		},
		{
			name:     "point get read",
			sql:      "select * from t where id = 1",
			txnScope: "local",
			zone:     "bj",
		},
		{
			name:     "batch point get read",
			sql:      "select * from t where id in (1,2,3)",
			txnScope: "local",
			zone:     "hz",
		},
	}
	for _, testcase := range testcases {
		c.Log(testcase.name)
		tk.MustExec(fmt.Sprintf("set @@txn_scope=%v", testcase.txnScope))
		failpoint.Enable("github.com/pingcap/tidb/config/injectTxnScope", fmt.Sprintf(`return("%v")`, testcase.zone))
		failpoint.Enable("github.com/pingcap/tidb/store/tikv/assertStoreLabels", fmt.Sprintf(`return("%v_%v")`, placement.DCLabelKey, testcase.txnScope))
		failpoint.Enable("github.com/pingcap/tidb/store/tikv/assertStaleReadFlag", `return(true)`)
		// Using NOW() will cause the loss of fsp precision, so we use NOW(3) to be accurate to the millisecond.
		tk.MustExec(`START TRANSACTION READ ONLY AS OF TIMESTAMP NOW(3);`)
		tk.MustQuery(testcase.sql)
		tk.MustExec(`commit`)
	}
	failpoint.Disable("github.com/pingcap/tidb/config/injectTxnScope")
	failpoint.Disable("github.com/pingcap/tidb/store/tikv/assertStoreLabels")
	failpoint.Disable("github.com/pingcap/tidb/store/tikv/assertStaleReadFlag")
}

func (s *testStaleTxnSerialSuite) TestStalenessAndHistoryRead(c *C) {
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
	// set @@tidb_snapshot before staleness txn
	tk.MustExec(`set @@tidb_snapshot="2016-10-08 16:45:26";`)
	tk.MustExec(`START TRANSACTION READ ONLY AS OF TIMESTAMP '2020-09-06 00:00:00';`)
	// 1599321600000 == 2020-09-06 00:00:00
	c.Assert(oracle.ExtractPhysical(tk.Se.GetSessionVars().TxnCtx.StartTS), Equals, int64(1599321600000))
	tk.MustExec("commit")
	// set @@tidb_snapshot during staleness txn
	tk.MustExec(`START TRANSACTION READ ONLY AS OF TIMESTAMP '2020-09-06 00:00:00';`)
	tk.MustExec(`set @@tidb_snapshot="2016-10-08 16:45:26";`)
	c.Assert(oracle.ExtractPhysical(tk.Se.GetSessionVars().TxnCtx.StartTS), Equals, int64(1599321600000))
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
		c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/injectSafeTS",
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
	failpoint.Disable("github.com/pingcap/tidb/store/tikv/injectSafeTS")
}

func (s *testStaleTxnSerialSuite) TestStalenessTransactionSchemaVer(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int primary key);")

	// test as of
	schemaVer1 := tk.Se.GetInfoSchema().SchemaMetaVersion()
	time1 := time.Now()
	tk.MustExec("drop table if exists t")
	c.Assert(schemaVer1, Less, tk.Se.GetInfoSchema().SchemaMetaVersion())
	tk.MustExec(fmt.Sprintf(`START TRANSACTION READ ONLY AS OF TIMESTAMP '%s'`, time1.Format("2006-1-2 15:04:05.000")))
	c.Assert(tk.Se.GetInfoSchema().SchemaMetaVersion(), Equals, schemaVer1)
	tk.MustExec("commit")
}
