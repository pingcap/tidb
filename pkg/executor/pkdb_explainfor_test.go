// Copyright 2026 PingCAP, Inc.

package executor_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestExplainForConnectionLocalTemporaryTableNoPanic(t *testing.T) {
	testCases := []struct {
		name         string
		createTable  string
		insertRows   string
		query        string
		expectedPlan string
	}{
		{
			name:         "TableReader",
			createTable:  "create temporary table tmp_t(a int primary key, b int)",
			insertRows:   "insert into tmp_t values (1, 1), (2, 2)",
			query:        "select * from tmp_t where b > 0",
			expectedPlan: "TableReader",
		},
		{
			name:         "IndexReader",
			createTable:  "create temporary table tmp_t(a int primary key, b int, key idx_b(b))",
			insertRows:   "insert into tmp_t values (1, 1), (2, 2)",
			query:        "select b from tmp_t use index(idx_b) where b > 0",
			expectedPlan: "IndexReader",
		},
		{
			name:         "IndexLookUpReader",
			createTable:  "create temporary table tmp_t(a int primary key, b int, c int, key idx_b(b))",
			insertRows:   "insert into tmp_t values (1, 1, 1), (2, 2, 2)",
			query:        "select c from tmp_t use index(idx_b) where b > 0",
			expectedPlan: "IndexLookUp",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			store := testkit.CreateMockStore(t)
			tk1 := testkit.NewTestKit(t, store)
			tk2 := testkit.NewTestKit(t, store)
			tk1.MustExec("use test")
			tk2.MustExec("use test")

			tk1.MustExec(testCase.createTable)
			tk1.MustExec(testCase.insertRows)
			plan := tk1.MustQuery("explain format = 'brief' " + testCase.query)
			require.Contains(t, fmt.Sprint(plan.Rows()), testCase.expectedPlan)
			tk1.MustQuery(testCase.query)
			tk1Process := tk1.Session().ShowProcess()
			tk2.Session().SetSessionManager(&testkit.MockSessionManager{PS: []*util.ProcessInfo{tk1Process}})

			err := tk2.ExecToErr(fmt.Sprintf("explain for connection %d", tk1Process.ID))
			require.Error(t, err)
			require.Contains(t, err.Error(), "is not found in current session infoschema")
		})
	}
}
