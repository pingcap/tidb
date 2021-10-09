// Copyright 2020 PingCAP, Inc.
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

//+build reserved_words_test

// This file ensures that the set of reserved keywords is the same as that of
// MySQL. To run:
//
//  1. Set up a MySQL server listening at 127.0.0.1:3306 using root and no password
//  2. Run this test with:
//
//		go test -tags reserved_words_test -check.f TestReservedWords
package parser

import (
	dbsql "database/sql"

	// needed to connect to MySQL
	_ "github.com/go-sql-driver/mysql"
	. "github.com/pingcap/check"

	"github.com/pingcap/parser/ast"
)

func (s *testConsistentSuite) TestCompareReservedWordsWithMySQL(c *C) {
	p := New()
	db, err := dbsql.Open("mysql", "root@tcp(127.0.0.1:3306)/")
	c.Assert(err, IsNil)
	defer db.Close()

	for _, kw := range s.reservedKeywords {
		switch kw {
		case "CURRENT_ROLE":
			// special case: we do reserve CURRENT_ROLE but MySQL didn't,
			// and unreservering it causes legit parser conflict.
			continue
		}

		query := "do (select 1 as " + kw + ")"
		errRegexp := ".*" + kw + ".*"

		var err error

		if _, ok := windowFuncTokenMap[kw]; !ok {
			// for some reason the query does parse even then the keyword is reserved in TiDB.
			_, _, err = p.Parse(query, "", "")
			c.Assert(err, ErrorMatches, errRegexp)
		}
		_, err = db.Exec(query)
		c.Assert(err, ErrorMatches, errRegexp, Commentf("MySQL suggests that '%s' should *not* be reserved!", kw))
	}

	for _, kws := range [][]string{s.unreservedKeywords, s.notKeywordTokens, s.tidbKeywords} {
		for _, kw := range kws {
			switch kw {
			case "FUNCTION", // reserved in 8.0.1
				"SEPARATOR": // ?
				continue
			}

			query := "do (select 1 as " + kw + ")"

			stmts, _, err := p.Parse(query, "", "")
			c.Assert(err, IsNil)
			c.Assert(stmts, HasLen, 1)
			c.Assert(stmts[0], FitsTypeOf, &ast.DoStmt{})

			_, err = db.Exec(query)
			c.Assert(err, IsNil, Commentf("MySQL suggests that '%s' should be reserved!", kw))
		}
	}
}
