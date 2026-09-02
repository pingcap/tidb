// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0.

package executor_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestDateSubExtremeMonth(t *testing.T) {
	for _, vectorized := range []string{"OFF", "ON"} {
		t.Run(vectorized, func(t *testing.T) {
			store := testkit.CreateMockStore(t)
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("USE test")
			tk.MustExec("SET @@session.tidb_allow_mpp = 0")
			tk.MustExec("SET @@session.tidb_enable_vectorized_expression = " + vectorized)
			tk.MustExec("CREATE TABLE lrr_test (col1 DATETIME)")
			tk.MustExec("INSERT INTO lrr_test VALUES ('0001-01-01 00:00:00')")

			tk.MustQuery("SELECT col1, DATE_SUB(col1, INTERVAL 10 MONTH) FROM lrr_test").Check(testkit.Rows(
				"0001-01-01 00:00:00 0000-03-01 00:00:00",
			))
			tk.MustQuery("SELECT DATE_SUB(col1, INTERVAL 1 YEAR) FROM lrr_test").Check(testkit.Rows(
				"0000-01-01 00:00:00",
			))
			tk.MustQuery("SELECT DATE_ADD(col1, INTERVAL -2 HOUR) FROM lrr_test").Check(testkit.Rows(
				"0000-00-00 22:00:00",
			))
		})
	}
}
