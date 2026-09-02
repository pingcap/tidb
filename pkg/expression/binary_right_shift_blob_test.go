// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0.

package expression_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestBinaryRightShiftBlob(t *testing.T) {
	for _, vectorized := range []string{"off", "on"} {
		t.Run(vectorized, func(t *testing.T) {
			store := testkit.CreateMockStore(t)
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("create table shift_source(id int primary key, b blob, vb varbinary(2), fixed binary(2), text_value varchar(4), shift_count int)")
			tk.MustExec("insert into shift_source values (1, 0xC2A0, 0xC2A0, 0xC2A0, '16', 4), (2, NULL, NULL, NULL, NULL, 4)")
			tk.MustExec("create table t2(a blob)")
			tk.MustExec("create table t3(a blob)")
			tk.MustExec("insert into t2 values (0xC2A0)")
			tk.MustExec("insert into t3 values (0xC2)")
			tk.MustExec("set @@tidb_enable_vectorized_expression=" + vectorized)

			tk.MustQuery("select hex(b >> shift_count), hex(b >> 8), hex(b >> 16), length(b >> shift_count), hex(vb >> shift_count), hex(fixed >> shift_count) from shift_source where id = 1").Check(
				testkit.Rows("0C2A 00C2 0000 2 0C2A 0C2A"),
			)
			tk.MustQuery("select hex(b >> shift_count), hex(vb >> shift_count), hex(fixed >> shift_count) from shift_source where id = 2").Check(
				testkit.Rows("<nil> <nil> <nil>"),
			)
			tk.MustQuery("select hex(t2.a), hex(t3.a) from t2, t3 where (t2.a >> 4) = t3.a").Check(testkit.Rows())
			tk.MustQuery("select 123 >> 2, 0xC2A0 >> 4, text_value >> 2 from shift_source where id = 1").Check(
				testkit.Rows("30 3114 4"),
			)
		})
	}
}
