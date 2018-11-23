// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2017 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

// +build go1.8

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"runtime"
	"testing"
)

func benchmarkQueryContext(b *testing.B, db *sql.DB, p int) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db.SetMaxIdleConns(p * runtime.GOMAXPROCS(0))

	tb := (*TB)(b)
	stmt := tb.checkStmt(db.PrepareContext(ctx, "SELECT val FROM foo WHERE id=?"))
	defer stmt.Close()

	b.SetParallelism(p)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var got string
		for pb.Next() {
			tb.check(stmt.QueryRow(1).Scan(&got))
			if got != "one" {
				b.Fatalf("query = %q; want one", got)
			}
		}
	})
}

func BenchmarkQueryContext(b *testing.B) {
	db := initDB(b,
		"DROP TABLE IF EXISTS foo",
		"CREATE TABLE foo (id INT PRIMARY KEY, val CHAR(50))",
		`INSERT INTO foo VALUES (1, "one")`,
		`INSERT INTO foo VALUES (2, "two")`,
	)
	defer db.Close()
	for _, p := range []int{1, 2, 3, 4} {
		b.Run(fmt.Sprintf("%d", p), func(b *testing.B) {
			benchmarkQueryContext(b, db, p)
		})
	}
}

func benchmarkExecContext(b *testing.B, db *sql.DB, p int) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db.SetMaxIdleConns(p * runtime.GOMAXPROCS(0))

	tb := (*TB)(b)
	stmt := tb.checkStmt(db.PrepareContext(ctx, "DO 1"))
	defer stmt.Close()

	b.SetParallelism(p)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := stmt.ExecContext(ctx); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkExecContext(b *testing.B) {
	db := initDB(b,
		"DROP TABLE IF EXISTS foo",
		"CREATE TABLE foo (id INT PRIMARY KEY, val CHAR(50))",
		`INSERT INTO foo VALUES (1, "one")`,
		`INSERT INTO foo VALUES (2, "two")`,
	)
	defer db.Close()
	for _, p := range []int{1, 2, 3, 4} {
		b.Run(fmt.Sprintf("%d", p), func(b *testing.B) {
			benchmarkQueryContext(b, db, p)
		})
	}
}
