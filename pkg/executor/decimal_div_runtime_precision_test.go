// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0.

package executor_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestDecimalDivRuntimePrecision(t *testing.T) {
	for _, vectorized := range []string{"OFF", "ON"} {
		t.Run(vectorized, func(t *testing.T) {
			store := testkit.CreateMockStore(t)
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("USE test")
			tk.MustExec("SET @@session.tidb_allow_mpp = 0")
			tk.MustExec("SET @@session.tidb_enable_vectorized_expression = " + vectorized)
			tk.MustExec("CREATE TABLE t (id INT, num DECIMAL)")
			tk.MustExec("INSERT INTO t VALUES (1, 100)")

			tk.MustQuery("SELECT CAST(CAST('1.2300' AS DECIMAL(10,4)) AS CHAR)").Check(testkit.Rows("1.2300"))
			tk.MustQuery("SELECT /*+ READ_FROM_STORAGE(TIKV[t]) */ LENGTH(SUM(num) / 10) FROM t GROUP BY id").Check(testkit.Rows("7"))
			tk.MustQuery("SELECT /*+ READ_FROM_STORAGE(TIKV[t]) */ CAST(SUM(num) / 10 AS CHAR) FROM t GROUP BY id").Check(testkit.Rows("10.0000"))
		})
	}
}
