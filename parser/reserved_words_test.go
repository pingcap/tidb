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

//go:build reserved_words_test
// +build reserved_words_test

// This file ensures that the set of reserved keywords is the same as that of
// MySQL. To run:
//
//  1. Set up a MySQL server listening at 127.0.0.1:3306 using root and no password
//  2. Run this test with:
//
//		go test -tags reserved_words_test -run '^TestCompareReservedWordsWithMySQL$'

package parser

import (
	// needed to connect to MySQL

	dbsql "database/sql"
	"io/ioutil"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/tidb/parser/ast"
	requires "github.com/stretchr/testify/require"
)

func TestCompareReservedWordsWithMySQL(t *testing.T) {
	parserFilename := "parser.y"
	parserFile, err := os.Open(parserFilename)
	requires.NoError(t, err)
	data, err := ioutil.ReadAll(parserFile)
	requires.NoError(t, err)
	content := string(data)

	reservedKeywordStartMarker := "\t/* The following tokens belong to ReservedKeyword. Notice: make sure these tokens are contained in ReservedKeyword. */"
	unreservedKeywordStartMarker := "\t/* The following tokens belong to UnReservedKeyword. Notice: make sure these tokens are contained in UnReservedKeyword. */"
	notKeywordTokenStartMarker := "\t/* The following tokens belong to NotKeywordToken. Notice: make sure these tokens are contained in NotKeywordToken. */"
	tidbKeywordStartMarker := "\t/* The following tokens belong to TiDBKeyword. Notice: make sure these tokens are contained in TiDBKeyword. */"
	identTokenEndMarker := "%token\t<item>"

	reservedKeywords := extractKeywords(content, reservedKeywordStartMarker, unreservedKeywordStartMarker)
	unreservedKeywords := extractKeywords(content, unreservedKeywordStartMarker, notKeywordTokenStartMarker)
	notKeywordTokens := extractKeywords(content, notKeywordTokenStartMarker, tidbKeywordStartMarker)
	tidbKeywords := extractKeywords(content, tidbKeywordStartMarker, identTokenEndMarker)

	p := New()
	db, err := dbsql.Open("mysql", "root@tcp(127.0.0.1:3306)/")
	requires.NoError(t, err)
	defer func() {
		requires.NoError(t, db.Close())
	}()

	for _, kw := range reservedKeywords {
		switch kw {
		case "CURRENT_ROLE", "INTERSECT", "STATS_EXTENDED", "TABLESAMPLE":
			// special case: we do reserve these words but MySQL didn't,
			// and unreservering it causes legit parser conflict.
			continue
		}

		query := "do (select 1 as " + kw + ")"
		errRegexp := ".*" + kw + ".*"

		var err error

		if _, ok := windowFuncTokenMap[kw]; !ok {
			// for some reason the query does parse even then the keyword is reserved in TiDB.
			_, _, err = p.Parse(query, "", "")
			requires.Error(t, err)
			requires.Regexp(t, errRegexp, err.Error())
		}
		_, err = db.Exec(query)
		requires.Error(t, err)
		requires.Regexp(t, errRegexp, err.Error(), "MySQL suggests that '%s' should *not* be reserved!", kw)
	}

	for _, kws := range [][]string{unreservedKeywords, notKeywordTokens, tidbKeywords} {
		for _, kw := range kws {
			switch kw {
			case "FUNCTION", // reserved in 8.0.1
				"PURGE", "SYSTEM", "SEPARATOR": // ?
				continue
			}

			query := "do (select 1 as " + kw + ")"

			stmts, _, err := p.Parse(query, "", "")
			requires.NoError(t, err)
			requires.Len(t, stmts, 1)
			requires.IsType(t, &ast.DoStmt{}, stmts[0])

			_, err = db.Exec(query)
			println(query)
			requires.NoErrorf(t, err, "MySQL suggests that '%s' should be reserved!", kw)
		}
	}
}
