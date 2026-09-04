// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0.

package integration_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestDnfCommonConditionExtractionCollationHashCollision(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("DROP TABLE IF EXISTS t1")
	tk.MustExec("DROP TABLE IF EXISTS t2")
	tk.MustExec("CREATE TABLE t1(id INT PRIMARY KEY, c VARCHAR(10) COLLATE utf8mb4_general_ci, b INT)")
	tk.MustExec("CREATE TABLE t2(id INT PRIMARY KEY, x INT)")
	tk.MustExec("INSERT INTO t1 VALUES (1,'A',1),(2,'a',1),(3,'A',2),(4,'a',2)")
	tk.MustExec("INSERT INTO t2 VALUES (1,1),(2,1),(3,2),(4,2)")
	tk.MustQuery("SELECT t1.id\nFROM t1 JOIN t2\n  ON t1.id = t2.id\n AND ((c = 'A' COLLATE utf8mb4_general_ci AND t1.b = 1)\n   OR (c = 'A' COLLATE utf8mb4_bin AND t1.b = 2))\nORDER BY t1.id;").Check([][]any{
		{"1"},
		{"2"},
		{"3"},
	})

}
