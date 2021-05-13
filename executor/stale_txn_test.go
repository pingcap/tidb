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
	"strings"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/ddl/placement"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testStaleTxnSerialSuite) TestExactStalenessTransaction(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/executor/mockStalenessTxnSchemaVer", "return(false)"), IsNil)
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/executor/mockStalenessTxnSchemaVer")
		c.Assert(err, IsNil)
	}()

	testcases := []struct {
		name             string
		preSQL           string
		sql              string
		IsStaleness      bool
		expectPhysicalTS int64
		preSec           int64
		txnScope         string
		zone             string
	}{
		{
			name:             "TimestampBoundExactStaleness",
			preSQL:           `START TRANSACTION READ ONLY WITH TIMESTAMP BOUND EXACT STALENESS '00:00:20';`,
			sql:              `START TRANSACTION READ ONLY WITH TIMESTAMP BOUND READ TIMESTAMP '2020-09-06 00:00:00';`,
			IsStaleness:      true,
			expectPhysicalTS: 1599321600000,
			txnScope:         "local",
			zone:             "sh",
		},
		{
			name:             "TimestampBoundReadTimestamp",
			preSQL:           "begin",
			sql:              `START TRANSACTION READ ONLY WITH TIMESTAMP BOUND READ TIMESTAMP '2020-09-06 00:00:00';`,
			IsStaleness:      true,
			expectPhysicalTS: 1599321600000,
			txnScope:         "local",
			zone:             "bj",
		},
		{
			name:        "TimestampBoundExactStaleness",
			preSQL:      "begin",
			sql:         `START TRANSACTION READ ONLY WITH TIMESTAMP BOUND EXACT STALENESS '00:00:20';`,
			IsStaleness: true,
			preSec:      20,
			txnScope:    "local",
			zone:        "sh",
		},
		{
			name:        "TimestampBoundExactStaleness",
			preSQL:      `START TRANSACTION READ ONLY WITH TIMESTAMP BOUND READ TIMESTAMP '2020-09-06 00:00:00';`,
			sql:         `START TRANSACTION READ ONLY WITH TIMESTAMP BOUND EXACT STALENESS '00:00:20';`,
			IsStaleness: true,
			preSec:      20,
			txnScope:    "local",
			zone:        "sz",
		},
		{
			name:        "begin",
			preSQL:      `START TRANSACTION READ ONLY WITH TIMESTAMP BOUND READ TIMESTAMP '2020-09-06 00:00:00';`,
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
		if testcase.expectPhysicalTS > 0 {
			c.Assert(oracle.ExtractPhysical(tk.Se.GetSessionVars().TxnCtx.StartTS), Equals, testcase.expectPhysicalTS)
		} else if testcase.preSec > 0 {
			curSec := time.Now().Unix()
			startTS := oracle.ExtractPhysical(tk.Se.GetSessionVars().TxnCtx.StartTS)
			// exact stale txn tolerate 2 seconds deviation for startTS
			c.Assert(startTS, Greater, (curSec-testcase.preSec-2)*1000)
			c.Assert(startTS, Less, (curSec-testcase.preSec+2)*1000)
		} else if !testcase.IsStaleness {
			curSec := time.Now().Unix()
			startTS := oracle.ExtractPhysical(tk.Se.GetSessionVars().TxnCtx.StartTS)
			c.Assert(curSec*1000-startTS, Less, time.Second/time.Millisecond)
			c.Assert(startTS-curSec*1000, Less, time.Second/time.Millisecond)
		}
		c.Assert(tk.Se.GetSessionVars().TxnCtx.IsStaleness, Equals, testcase.IsStaleness)
		tk.MustExec("commit")
		failpoint.Disable("github.com/pingcap/tidb/config/injectTxnScope")
	}
}

func (s *testStaleTxnSerialSuite) TestSelectAsOf(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/executor/mockStalenessTxnSchemaVer", "return(false)"), IsNil)
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/executor/mockStalenessTxnSchemaVer")
		c.Assert(err, IsNil)
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec(`drop table if exists b`)
	tk.MustExec("create table t (id int primary key);")
	tk.MustExec("create table b (id int primary key);")
	defer func() {
		tk.MustExec(`drop table if exists b`)
		tk.MustExec(`drop table if exists t`)
	}()

	testcases := []struct {
		name             string
		sql              string
		expectPhysicalTS int64
		preSec           int64
		// IsStaleness is auto cleanup in select stmt.
		errorStr string
	}{
		{
			name:             "TimestampExactRead",
			sql:              `select * from t as of timestamp TIMESTAMP('2020-09-06 00:00:00');`,
			expectPhysicalTS: 1599321600000,
		},
		{
			name:   "TimestampExactRead",
			sql:    `select * from t as of timestamp TIMESTAMP(NOW() - INTERVAL 20 SECOND);`,
			preSec: 20,
		},
		{
			name:   "TimestampExactRead",
			sql:    `select * from t as of timestamp TIMESTAMP(NOW() - INTERVAL 20 SECOND), b as of timestamp TIMESTAMP(NOW() - INTERVAL 20 SECOND);`,
			preSec: 20,
		},
		{
			name:     "TimestampExactRead",
			sql:      `select * from t as of timestamp TIMESTAMP(NOW() - INTERVAL 20 SECOND), b as of timestamp TIMESTAMP('2020-09-06 00:00:00');`,
			preSec:   20,
			errorStr: "not set different timestamp",
		},
		{
			name:     "TimestampExactRead",
			sql:      `select * from t as of timestamp TIMESTAMP(NOW() - INTERVAL 20 SECOND), b;`,
			preSec:   20,
			errorStr: "not set different timestamp",
		},
		{
			name:   "NomalRead",
			sql:    `select * from t, b;`,
			preSec: 0,
		},
	}

	tk.MustExec("use test")
	for _, testcase := range testcases {
		c.Log(testcase.name)
		_, err := tk.Exec(testcase.sql)
		if len(testcase.errorStr) == 0 {
			c.Assert(err, IsNil, Commentf("sql:%s, error stack %v", testcase.sql, errors.ErrorStack(err)))
		} else {
			c.Assert(err, NotNil)
			c.Assert(strings.Contains(err.Error(), testcase.errorStr), IsTrue)
			continue
		}
		if testcase.expectPhysicalTS > 0 {
			c.Assert(oracle.ExtractPhysical(tk.Se.GetSessionVars().TxnCtx.StartTS), Equals, testcase.expectPhysicalTS)
		} else if testcase.preSec > 0 {
			curSec := time.Now().Unix()
			startTS := oracle.ExtractPhysical(tk.Se.GetSessionVars().TxnCtx.StartTS)
			// exact stale txn tolerate 2 seconds deviation for startTS
			c.Assert(startTS, Greater, (curSec-testcase.preSec-2)*1000)
			c.Assert(startTS, Less, (curSec-testcase.preSec+2)*1000)
		}
	}

	// TODO: add cases
	// tk.MustExec("begin")
	// tk.MustQuery("select count(*) from ( select * from t2 group by a, b) A group by A.b").Check(testkit.Rows("3"))
	// tk.MustQuery("select count(*) from t1 where t1.a+100 > ( select count(*) from t2 where t1.a=t2.a and t1.b=t2.b) group by t1.b").Check(testkit.Rows("4"))
	// txn, err := tk.Se.Txn(true)
	// c.Assert(err, IsNil)
	// ts := txn.StartTS()
}

func (s *testStaleTxnSerialSuite) TestStaleReadKVRequest(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/executor/mockStalenessTxnSchemaVer", "return(false)"), IsNil)
	defer failpoint.Disable("github.com/pingcap/tidb/executor/mockStalenessTxnSchemaVer")
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
		tk.MustExec(`START TRANSACTION READ ONLY WITH TIMESTAMP BOUND EXACT STALENESS '00:00:20';`)
		tk.MustQuery(testcase.sql)
		tk.MustExec(`commit`)
		failpoint.Disable("github.com/pingcap/tidb/config/injectTxnScope")
		failpoint.Disable("github.com/pingcap/tidb/store/tikv/assertStoreLabels")
		failpoint.Disable("github.com/pingcap/tidb/store/tikv/assertStaleReadFlag")
	}
}

func (s *testStaleTxnSerialSuite) TestStalenessAndHistoryRead(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/executor/mockStalenessTxnSchemaVer", "return(false)"), IsNil)
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/executor/mockStalenessTxnSchemaVer")
		c.Assert(err, IsNil)
	}()

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
	tk.MustExec(`START TRANSACTION READ ONLY WITH TIMESTAMP BOUND READ TIMESTAMP '2020-09-06 00:00:00';`)
	c.Assert(oracle.ExtractPhysical(tk.Se.GetSessionVars().TxnCtx.StartTS), Equals, int64(1599321600000))
	tk.MustExec("commit")
	// set @@tidb_snapshot during staleness txn
	tk.MustExec(`START TRANSACTION READ ONLY WITH TIMESTAMP BOUND READ TIMESTAMP '2020-09-06 00:00:00';`)
	tk.MustExec(`set @@tidb_snapshot="2016-10-08 16:45:26";`)
	c.Assert(oracle.ExtractPhysical(tk.Se.GetSessionVars().TxnCtx.StartTS), Equals, int64(1599321600000))
	tk.MustExec("commit")
}

func (s *testStaleTxnSerialSuite) TestStalenessTransactionSchemaVer(c *C) {
	testcases := []struct {
		name      string
		sql       string
		expectErr error
	}{
		{
			name:      "ddl change before stale txn",
			sql:       `START TRANSACTION READ ONLY WITH TIMESTAMP BOUND EXACT STALENESS '00:00:03'`,
			expectErr: errors.New("schema version changed after the staleness startTS"),
		},
		{
			name: "ddl change before stale txn",
			sql: fmt.Sprintf("START TRANSACTION READ ONLY WITH TIMESTAMP BOUND READ TIMESTAMP '%v'",
				time.Now().Truncate(3*time.Second).Format("2006-01-02 15:04:05")),
			expectErr: errors.New(".*schema version changed after the staleness startTS.*"),
		},
		{
			name:      "ddl change before stale txn",
			sql:       `START TRANSACTION READ ONLY WITH TIMESTAMP BOUND EXACT STALENESS '00:00:03'`,
			expectErr: nil,
		},
	}
	tk := testkit.NewTestKitWithInit(c, s.store)
	for _, testcase := range testcases {
		check := func() {
			if testcase.expectErr != nil {
				c.Assert(failpoint.Enable("github.com/pingcap/tidb/executor/mockStalenessTxnSchemaVer", "return(true)"), IsNil)
				defer func() {
					err := failpoint.Disable("github.com/pingcap/tidb/executor/mockStalenessTxnSchemaVer")
					c.Assert(err, IsNil)
				}()

			} else {
				c.Assert(failpoint.Enable("github.com/pingcap/tidb/executor/mockStalenessTxnSchemaVer", "return(false)"), IsNil)
				defer func() {
					err := failpoint.Disable("github.com/pingcap/tidb/executor/mockStalenessTxnSchemaVer")
					c.Assert(err, IsNil)
				}()

			}
			_, err := tk.Exec(testcase.sql)
			if testcase.expectErr != nil {
				c.Assert(err, NotNil)
				c.Assert(err.Error(), Matches, testcase.expectErr.Error())
			} else {
				c.Assert(err, IsNil)
			}
		}
		check()
	}
}

func (s *testStaleTxnSerialSuite) TestTimeBoundedStalenessTxn(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/executor/mockStalenessTxnSchemaVer", "return(false)"), IsNil)
	defer failpoint.Disable("github.com/pingcap/tidb/executor/mockStalenessTxnSchemaVer")
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int primary key);")
	defer tk.MustExec(`drop table if exists t`)
	tk.MustExec(`START TRANSACTION READ ONLY WITH TIMESTAMP BOUND MAX STALENESS '00:00:10'`)
	testcases := []struct {
		name         string
		sql          string
		injectSafeTS uint64
		useSafeTS    bool
	}{
		{
			name: "max 20 seconds ago, safeTS 10 secs ago",
			sql:  `START TRANSACTION READ ONLY WITH TIMESTAMP BOUND MAX STALENESS '00:00:20'`,
			injectSafeTS: func() uint64 {
				phy := time.Now().Add(-10*time.Second).Unix() * 1000
				return oracle.ComposeTS(phy, 0)
			}(),
			useSafeTS: true,
		},
		{
			name: "max 10 seconds ago, safeTS 20 secs ago",
			sql:  `START TRANSACTION READ ONLY WITH TIMESTAMP BOUND MAX STALENESS '00:00:10'`,
			injectSafeTS: func() uint64 {
				phy := time.Now().Add(-20*time.Second).Unix() * 1000
				return oracle.ComposeTS(phy, 0)
			}(),
			useSafeTS: false,
		},
		{
			name: "max 20 seconds ago, safeTS 10 secs ago",
			sql: func() string {
				return fmt.Sprintf(`START TRANSACTION READ ONLY WITH TIMESTAMP BOUND MIN READ TIMESTAMP '%v'`,
					time.Now().Add(-20*time.Second).Format("2006-01-02 15:04:05"))
			}(),
			injectSafeTS: func() uint64 {
				phy := time.Now().Add(-10*time.Second).Unix() * 1000
				return oracle.ComposeTS(phy, 0)
			}(),
			useSafeTS: true,
		},
		{
			name: "max 10 seconds ago, safeTS 20 secs ago",
			sql: func() string {
				return fmt.Sprintf(`START TRANSACTION READ ONLY WITH TIMESTAMP BOUND MIN READ TIMESTAMP '%v'`,
					time.Now().Add(-10*time.Second).Format("2006-01-02 15:04:05"))
			}(),
			injectSafeTS: func() uint64 {
				phy := time.Now().Add(-20*time.Second).Unix() * 1000
				return oracle.ComposeTS(phy, 0)
			}(),
			useSafeTS: false,
		},
	}
	for _, testcase := range testcases {
		c.Log(testcase.name)
		c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/injectSafeTS",
			fmt.Sprintf("return(%v)", testcase.injectSafeTS)), IsNil)
		tk.MustExec(testcase.sql)
		if testcase.useSafeTS {
			c.Assert(tk.Se.GetSessionVars().TxnCtx.StartTS, Equals, testcase.injectSafeTS)
		} else {
			c.Assert(oracle.CompareTS(tk.Se.GetSessionVars().TxnCtx.StartTS, testcase.injectSafeTS), Equals, 1)
		}
		tk.MustExec("commit")
		failpoint.Disable("github.com/pingcap/tidb/store/tikv/injectSafeTS")
	}
}
