// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

/*
Consider we have a table like this:
CREATE TABLE `t` (
  `c1` int DEFAULT NULL,
  `c2` int DEFAULT NULL
);
And we have a user named 'u'@'%'. Now we can grant and revoke some table/column
privilege to/from the user 'u'. The operations is enumerated in 'ops'. Then the
table and column privilege of 'u' has 16 kinds of state (see the 'states'). This
test is to verify the correctness of the states transformation.
*/

type state struct {
	description string

	initSQLs []string

	mysqlTablesPrivResult      string
	mysqlColumnsPrivResult     []string
	infoTablePrivilegesResult  []string
	infoColumnPrivilegesResult []string
	showGrantsResult           []string
}

var states = []*state{
	{description: "error state"},  // 0
	{description: "no privilege"}, // 1
	{description: "SELECT", // 2
		initSQLs:                   []string{"GRANT SELECT ON t TO u"},
		mysqlTablesPrivResult:      "Select|",
		mysqlColumnsPrivResult:     nil,
		infoTablePrivilegesResult:  []string{"SELECT"},
		infoColumnPrivilegesResult: nil,
		showGrantsResult:           []string{"GRANT SELECT ON `test`.`t` TO 'u'@'%'"},
	},
	{description: "UPDATE", // 3
		initSQLs:                   []string{"GRANT UPDATE ON t TO u"},
		mysqlTablesPrivResult:      "Update|",
		mysqlColumnsPrivResult:     nil,
		infoTablePrivilegesResult:  []string{"UPDATE"},
		infoColumnPrivilegesResult: nil,
		showGrantsResult:           []string{"GRANT UPDATE ON `test`.`t` TO 'u'@'%'"},
	},
	{description: "SELECT,UPDATE", // 4
		initSQLs:                   []string{"GRANT SELECT,UPDATE ON t TO u"},
		mysqlTablesPrivResult:      "Select,Update|",
		mysqlColumnsPrivResult:     nil,
		infoTablePrivilegesResult:  []string{"SELECT", "UPDATE"},
		infoColumnPrivilegesResult: nil,
		showGrantsResult:           []string{"GRANT SELECT,UPDATE ON `test`.`t` TO 'u'@'%'"},
	},
	{description: "SELECT(c1)", // 5
		initSQLs:                   []string{"GRANT SELECT(c1) ON t TO u"},
		mysqlTablesPrivResult:      "|Select",
		mysqlColumnsPrivResult:     []string{"c1|Select"},
		infoTablePrivilegesResult:  nil,
		infoColumnPrivilegesResult: []string{"SELECT"},
		showGrantsResult:           []string{"GRANT SELECT(`c1`) ON `test`.`t` TO 'u'@'%'"},
	},
	{description: "SELECT,SELECT(c1)", // 6
		initSQLs:                   []string{"GRANT SELECT,SELECT(c1) ON t TO u"},
		mysqlTablesPrivResult:      "Select|Select",
		mysqlColumnsPrivResult:     []string{"c1|Select"},
		infoTablePrivilegesResult:  []string{"SELECT"},
		infoColumnPrivilegesResult: []string{"SELECT"},
		showGrantsResult: []string{
			"GRANT SELECT ON `test`.`t` TO 'u'@'%'",
			"GRANT SELECT(`c1`) ON `test`.`t` TO 'u'@'%'",
		},
	},
	{description: "UPDATE,SELECT(c1)", // 7
		initSQLs:                   []string{"GRANT UPDATE,SELECT(c1) ON t TO u"},
		mysqlTablesPrivResult:      "Update|Select",
		mysqlColumnsPrivResult:     []string{"c1|Select"},
		infoTablePrivilegesResult:  []string{"UPDATE"},
		infoColumnPrivilegesResult: []string{"SELECT"},
		showGrantsResult: []string{
			"GRANT UPDATE ON `test`.`t` TO 'u'@'%'",
			"GRANT SELECT(`c1`) ON `test`.`t` TO 'u'@'%'",
		},
	},
	{description: "SELECT,UPDATE,SELECT(c1)", // 8
		initSQLs:                   []string{"GRANT SELECT,UPDATE,SELECT(c1) ON t TO u"},
		mysqlTablesPrivResult:      "Select,Update|Select",
		mysqlColumnsPrivResult:     []string{"c1|Select"},
		infoTablePrivilegesResult:  []string{"SELECT", "UPDATE"},
		infoColumnPrivilegesResult: []string{"SELECT"},
		showGrantsResult: []string{
			"GRANT SELECT,UPDATE ON `test`.`t` TO 'u'@'%'",
			"GRANT SELECT(`c1`) ON `test`.`t` TO 'u'@'%'",
		},
	},
	{description: "SELECT(c2)", // 9
		initSQLs:                   []string{"GRANT SELECT(c2) ON t TO u"},
		mysqlTablesPrivResult:      "|Select",
		mysqlColumnsPrivResult:     []string{"c2|Select"},
		infoTablePrivilegesResult:  nil,
		infoColumnPrivilegesResult: []string{"SELECT"},
		showGrantsResult:           []string{"GRANT SELECT(`c2`) ON `test`.`t` TO 'u'@'%'"},
	},
	{description: "SELECT,SELECT(c2)", // 10
		initSQLs:                   []string{"GRANT SELECT,SELECT(c2) ON t TO u"},
		mysqlTablesPrivResult:      "Select|Select",
		mysqlColumnsPrivResult:     []string{"c2|Select"},
		infoTablePrivilegesResult:  []string{"SELECT"},
		infoColumnPrivilegesResult: []string{"SELECT"},
		showGrantsResult: []string{
			"GRANT SELECT ON `test`.`t` TO 'u'@'%'",
			"GRANT SELECT(`c2`) ON `test`.`t` TO 'u'@'%'",
		},
	},
	{description: "UPDATE,SELECT(c2)", // 11
		initSQLs:                   []string{"GRANT UPDATE,SELECT(c2) ON t TO u"},
		mysqlTablesPrivResult:      "Update|Select",
		mysqlColumnsPrivResult:     []string{"c2|Select"},
		infoTablePrivilegesResult:  []string{"UPDATE"},
		infoColumnPrivilegesResult: []string{"SELECT"},
		showGrantsResult: []string{
			"GRANT UPDATE ON `test`.`t` TO 'u'@'%'",
			"GRANT SELECT(`c2`) ON `test`.`t` TO 'u'@'%'",
		},
	},
	{description: "SELECT,UPDATE,SELECT(c2)", // 12
		initSQLs:                   []string{"GRANT SELECT,UPDATE,SELECT(c2) ON t TO u"},
		mysqlTablesPrivResult:      "Select,Update|Select",
		mysqlColumnsPrivResult:     []string{"c2|Select"},
		infoTablePrivilegesResult:  []string{"SELECT", "UPDATE"},
		infoColumnPrivilegesResult: []string{"SELECT"},
		showGrantsResult: []string{
			"GRANT SELECT,UPDATE ON `test`.`t` TO 'u'@'%'",
			"GRANT SELECT(`c2`) ON `test`.`t` TO 'u'@'%'",
		},
	},
	{description: "SELECT(c1,c2)", // 13
		initSQLs:                   []string{"GRANT SELECT(c1,c2) ON t TO u"},
		mysqlTablesPrivResult:      "|Select",
		mysqlColumnsPrivResult:     []string{"c1|Select", "c2|Select"},
		infoTablePrivilegesResult:  nil,
		infoColumnPrivilegesResult: []string{"SELECT", "SELECT"},
		showGrantsResult:           []string{"GRANT SELECT(`c1`, `c2`) ON `test`.`t` TO 'u'@'%'"},
	},
	{description: "SELECT,SELECT(c1,c2)", // 14
		initSQLs:                   []string{"GRANT SELECT,SELECT(c1,c2) ON t TO u"},
		mysqlTablesPrivResult:      "Select|Select",
		mysqlColumnsPrivResult:     []string{"c1|Select", "c2|Select"},
		infoTablePrivilegesResult:  []string{"SELECT"},
		infoColumnPrivilegesResult: []string{"SELECT", "SELECT"},
		showGrantsResult: []string{
			"GRANT SELECT ON `test`.`t` TO 'u'@'%'",
			"GRANT SELECT(`c1`, `c2`) ON `test`.`t` TO 'u'@'%'",
		},
	},
	{description: "UPDATE,SELECT(c1,c2)", // 15
		initSQLs:                   []string{"GRANT UPDATE,SELECT(c1,c2) ON t TO u"},
		mysqlTablesPrivResult:      "Update|Select",
		mysqlColumnsPrivResult:     []string{"c1|Select", "c2|Select"},
		infoTablePrivilegesResult:  []string{"UPDATE"},
		infoColumnPrivilegesResult: []string{"SELECT", "SELECT"},
		showGrantsResult: []string{
			"GRANT UPDATE ON `test`.`t` TO 'u'@'%'",
			"GRANT SELECT(`c1`, `c2`) ON `test`.`t` TO 'u'@'%'",
		},
	},
	{description: "SELECT,UPDATE,SELECT(c1,c2)", // 16
		initSQLs:                   []string{"GRANT SELECT,UPDATE,SELECT(c1,c2) ON t TO u"},
		mysqlTablesPrivResult:      "Select,Update|Select",
		mysqlColumnsPrivResult:     []string{"c1|Select", "c2|Select"},
		infoTablePrivilegesResult:  []string{"SELECT", "UPDATE"},
		infoColumnPrivilegesResult: []string{"SELECT", "SELECT"},
		showGrantsResult: []string{
			"GRANT SELECT,UPDATE ON `test`.`t` TO 'u'@'%'",
			"GRANT SELECT(`c1`, `c2`) ON `test`.`t` TO 'u'@'%'",
		},
	},
}

var errState = states[0]

var ops = []string{
	"", // skip index 0

	// GRANT
	"GRANT SELECT ON t TO u", // index 1
	"GRANT UPDATE ON t TO u",
	"GRANT SELECT(c1) ON t TO u",
	"GRANT SELECT(c2) ON t TO u",
	"GRANT SELECT(c1,c2) ON t TO u",

	// REVOKE
	"REVOKE SELECT ON t FROM u", // index 6
	"REVOKE UPDATE ON t FROM u",
	"REVOKE SELECT(c1) ON t FROM u",
	"REVOKE SELECT(c2) ON t FROM u",
	"REVOKE SELECT(c1,c2) ON t FROM u",
}

const (
	checkMySQLTablePrivsSQL      = "SELECT table_priv,column_priv FROM mysql.tables_priv"
	checkMySQLColumnPrivsSQL     = "SELECT column_name, column_priv FROM mysql.columns_priv ORDER BY column_name"
	checkInfoTablePrivilegesSQL  = "SELECT privilege_type FROM information_schema.table_privileges ORDER BY privilege_type"
	checkInfoColumnPrivilegesSQL = "SELECT privilege_type FROM information_schema.column_privileges ORDER BY column_name,privilege_type"
	showGrantsSQL                = "SHOW GRANTS FOR u"
)

func initState(tk *testkit.TestKit, s *state) {
	tk.MustExec("TRUNCATE mysql.tables_priv")
	tk.MustExec("TRUNCATE mysql.columns_priv")
	for _, sql := range s.initSQLs {
		tk.MustExec(sql)
	}
	tk.MustExec("FLUSH PRIVILEGES")
}

func checkResult(t *testing.T, tk *testkit.TestKit, s *state) {
	if s.mysqlTablesPrivResult == "" {
		tk.MustQuery(checkMySQLTablePrivsSQL).Check(testkit.Rows())
	} else {
		tk.MustQuery(checkMySQLTablePrivsSQL).Check(testkit.RowsWithSep("|", s.mysqlTablesPrivResult))
	}
	if len(s.mysqlColumnsPrivResult) == 0 {
		tk.MustQuery(checkMySQLColumnPrivsSQL).Check(testkit.Rows())
	} else {
		tk.MustQuery(checkMySQLColumnPrivsSQL).Check(testkit.RowsWithSep("|", s.mysqlColumnsPrivResult...))
	}
	if len(s.infoTablePrivilegesResult) == 0 {
		tk.MustQuery(checkInfoTablePrivilegesSQL).Check(testkit.Rows())
	} else {
		tk.MustQuery(checkInfoTablePrivilegesSQL).Check(testkit.Rows(s.infoTablePrivilegesResult...))
	}
	if len(s.infoColumnPrivilegesResult) == 0 {
		tk.MustQuery(checkInfoColumnPrivilegesSQL).Check(testkit.Rows())
	} else {
		tk.MustQuery(checkInfoColumnPrivilegesSQL).Check(testkit.Rows(s.infoColumnPrivilegesResult...))
	}
	showGrantsResult := tk.MustQuery(showGrantsSQL).Rows()[1:]
	require.Len(t, showGrantsResult, len(s.showGrantsResult))
	for i := range showGrantsResult {
		require.Equal(t, s.showGrantsResult[i], showGrantsResult[i][0])
	}
}

type testcase struct {
	op  string
	dst *state
}

func testState(t *testing.T, orgState *state, testcases []*testcase) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE USER u")
	tk.MustExec("CREATE TABLE t (c1 int, c2 int)")
	tk.Session().GetSessionVars().User = &auth.UserIdentity{Username: "root", Hostname: "localhost"}
	for _, tc := range testcases {
		initState(tk, orgState)
		if tc.dst == errState { // error occurs, should rollback to the origin state
			tk.MustExecToErr(tc.op)
			checkResult(t, tk, orgState)
		} else {
			tk.MustExec(tc.op)
			checkResult(t, tk, tc.dst)
		}
	}
}

func TestState1(t *testing.T) {
	testcases := []*testcase{
		{op: ops[1], dst: states[2]},
		{op: ops[2], dst: states[3]},
		{op: ops[3], dst: states[5]},
		{op: ops[4], dst: states[9]},
		{op: ops[5], dst: states[13]},
		{op: ops[6], dst: errState},
		{op: ops[7], dst: errState},
		{op: ops[8], dst: errState},
		{op: ops[9], dst: errState},
		{op: ops[10], dst: errState},
	}
	testState(t, states[1], testcases)
}

func TestState2(t *testing.T) {
	testcases := []*testcase{
		{op: ops[1], dst: states[2]},
		{op: ops[2], dst: states[4]},
		{op: ops[3], dst: states[6]},
		{op: ops[4], dst: states[10]},
		{op: ops[5], dst: states[14]},
		{op: ops[6], dst: states[1]},
		{op: ops[7], dst: states[2]},
		{op: ops[8], dst: states[2]},
		{op: ops[9], dst: states[2]},
		{op: ops[10], dst: states[2]},
	}
	testState(t, states[2], testcases)
}

func TestState3(t *testing.T) {
	testcases := []*testcase{
		{op: ops[1], dst: states[4]},
		{op: ops[2], dst: states[3]},
		{op: ops[3], dst: states[7]},
		{op: ops[4], dst: states[11]},
		{op: ops[5], dst: states[15]},
		{op: ops[6], dst: states[3]},
		{op: ops[7], dst: states[1]},
		{op: ops[8], dst: states[3]},
		{op: ops[9], dst: states[3]},
		{op: ops[10], dst: states[3]},
	}
	testState(t, states[3], testcases)
}

func TestState4(t *testing.T) {
	testcases := []*testcase{
		{op: ops[1], dst: states[4]},
		{op: ops[2], dst: states[4]},
		{op: ops[3], dst: states[8]},
		{op: ops[4], dst: states[12]},
		{op: ops[5], dst: states[16]},
		{op: ops[6], dst: states[3]},
		{op: ops[7], dst: states[2]},
		{op: ops[8], dst: states[4]},
		{op: ops[9], dst: states[4]},
		{op: ops[10], dst: states[4]},
	}
	testState(t, states[4], testcases)
}

func TestState5(t *testing.T) {
	testcases := []*testcase{
		{op: ops[1], dst: states[6]},
		{op: ops[2], dst: states[7]},
		{op: ops[3], dst: states[5]},
		{op: ops[4], dst: states[13]},
		{op: ops[5], dst: states[13]},
		{op: ops[6], dst: states[1]},
		{op: ops[7], dst: states[5]},
		{op: ops[8], dst: states[1]},
		{op: ops[9], dst: states[5]},
		{op: ops[10], dst: states[1]},
	}
	testState(t, states[5], testcases)
}

func TestState6(t *testing.T) {
	testcases := []*testcase{
		{op: ops[1], dst: states[6]},
		{op: ops[2], dst: states[8]},
		{op: ops[3], dst: states[6]},
		{op: ops[4], dst: states[14]},
		{op: ops[5], dst: states[14]},
		{op: ops[6], dst: states[1]},
		{op: ops[7], dst: states[6]},
		{op: ops[8], dst: states[2]},
		{op: ops[9], dst: states[6]},
		{op: ops[10], dst: states[2]},
	}
	testState(t, states[6], testcases)
}

func TestState7(t *testing.T) {
	testcases := []*testcase{
		{op: ops[1], dst: states[8]},
		{op: ops[2], dst: states[7]},
		{op: ops[3], dst: states[7]},
		{op: ops[4], dst: states[15]},
		{op: ops[5], dst: states[15]},
		{op: ops[6], dst: states[3]},
		{op: ops[7], dst: states[5]},
		{op: ops[8], dst: states[3]},
		{op: ops[9], dst: states[7]},
		{op: ops[10], dst: states[3]},
	}
	testState(t, states[7], testcases)
}

func TestState8(t *testing.T) {
	testcases := []*testcase{
		{op: ops[1], dst: states[8]},
		{op: ops[2], dst: states[8]},
		{op: ops[3], dst: states[8]},
		{op: ops[4], dst: states[16]},
		{op: ops[5], dst: states[16]},
		{op: ops[6], dst: states[3]},
		{op: ops[7], dst: states[6]},
		{op: ops[8], dst: states[4]},
		{op: ops[9], dst: states[8]},
		{op: ops[10], dst: states[4]},
	}
	testState(t, states[8], testcases)
}

func TestState9(t *testing.T) {
	testcases := []*testcase{
		{op: ops[1], dst: states[10]},
		{op: ops[2], dst: states[11]},
		{op: ops[3], dst: states[13]},
		{op: ops[4], dst: states[9]},
		{op: ops[5], dst: states[13]},
		{op: ops[6], dst: states[1]},
		{op: ops[7], dst: states[9]},
		{op: ops[8], dst: states[9]},
		{op: ops[9], dst: states[1]},
		{op: ops[10], dst: states[1]},
	}
	testState(t, states[9], testcases)
}

func TestState10(t *testing.T) {
	testcases := []*testcase{
		{op: ops[1], dst: states[10]},
		{op: ops[2], dst: states[12]},
		{op: ops[3], dst: states[14]},
		{op: ops[4], dst: states[10]},
		{op: ops[5], dst: states[14]},
		{op: ops[6], dst: states[1]},
		{op: ops[7], dst: states[10]},
		{op: ops[8], dst: states[10]},
		{op: ops[9], dst: states[2]},
		{op: ops[10], dst: states[2]},
	}
	testState(t, states[10], testcases)
}

func TestState11(t *testing.T) {
	testcases := []*testcase{
		{op: ops[1], dst: states[12]},
		{op: ops[2], dst: states[11]},
		{op: ops[3], dst: states[15]},
		{op: ops[4], dst: states[11]},
		{op: ops[5], dst: states[15]},
		{op: ops[6], dst: states[3]},
		{op: ops[7], dst: states[9]},
		{op: ops[8], dst: states[11]},
		{op: ops[9], dst: states[3]},
		{op: ops[10], dst: states[3]},
	}
	testState(t, states[11], testcases)
}

func TestState12(t *testing.T) {
	testcases := []*testcase{
		{op: ops[1], dst: states[12]},
		{op: ops[2], dst: states[12]},
		{op: ops[3], dst: states[16]},
		{op: ops[4], dst: states[12]},
		{op: ops[5], dst: states[16]},
		{op: ops[6], dst: states[3]},
		{op: ops[7], dst: states[10]},
		{op: ops[8], dst: states[12]},
		{op: ops[9], dst: states[4]},
		{op: ops[10], dst: states[4]},
	}
	testState(t, states[12], testcases)
}

func TestState13(t *testing.T) {
	testcases := []*testcase{
		{op: ops[1], dst: states[14]},
		{op: ops[2], dst: states[15]},
		{op: ops[3], dst: states[13]},
		{op: ops[4], dst: states[13]},
		{op: ops[5], dst: states[13]},
		{op: ops[6], dst: states[1]},
		{op: ops[7], dst: states[13]},
		{op: ops[8], dst: states[9]},
		{op: ops[9], dst: states[5]},
		{op: ops[10], dst: states[1]},
	}
	testState(t, states[13], testcases)
}

func TestState14(t *testing.T) {
	testcases := []*testcase{
		{op: ops[1], dst: states[14]},
		{op: ops[2], dst: states[16]},
		{op: ops[3], dst: states[14]},
		{op: ops[4], dst: states[14]},
		{op: ops[5], dst: states[14]},
		{op: ops[6], dst: states[1]},
		{op: ops[7], dst: states[14]},
		{op: ops[8], dst: states[10]},
		{op: ops[9], dst: states[6]},
		{op: ops[10], dst: states[2]},
	}
	testState(t, states[14], testcases)
}

func TestState15(t *testing.T) {
	testcases := []*testcase{
		{op: ops[1], dst: states[16]},
		{op: ops[2], dst: states[15]},
		{op: ops[3], dst: states[15]},
		{op: ops[4], dst: states[15]},
		{op: ops[5], dst: states[15]},
		{op: ops[6], dst: states[3]},
		{op: ops[7], dst: states[13]},
		{op: ops[8], dst: states[11]},
		{op: ops[9], dst: states[7]},
		{op: ops[10], dst: states[3]},
	}
	testState(t, states[15], testcases)
}

func TestState16(t *testing.T) {
	testcases := []*testcase{
		{op: ops[1], dst: states[16]},
		{op: ops[2], dst: states[16]},
		{op: ops[3], dst: states[16]},
		{op: ops[4], dst: states[16]},
		{op: ops[5], dst: states[16]},
		{op: ops[6], dst: states[3]},
		{op: ops[7], dst: states[14]},
		{op: ops[8], dst: states[12]},
		{op: ops[9], dst: states[8]},
		{op: ops[10], dst: states[4]},
	}
	testState(t, states[16], testcases)
}
