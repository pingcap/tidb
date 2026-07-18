// Copyright 2026 PingCAP, Inc.
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

// Command gorun executes SQL statements end-to-end in a real (mock-backed) TiDB
// session — parse, plan, and execute — and prints each result set, so the Rust
// tidb-exec executor can be checked for parity against actual TiDB execution
// (the design's "result ring", true end-to-end).
//
// It reads one statement per line from stdin and writes, per line:
//
//	RS:<row>;<row>;...   the result rows, each as `c1|c2|...` (NULL is <nil>).
//	                     Rows are sorted so the comparison is independent of row
//	                     order, EXCEPT when the statement has a top-level
//	                     ORDER BY, whose produced order is then preserved.
//	OK                   a statement with no result set (CREATE TABLE, INSERT)
//	ERR                  the statement failed to execute
//
// Sorting makes set-operation results order-independent for comparison; an
// explicit ORDER BY makes the order significant, so it must be kept verbatim to
// verify the executor's sort.
package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver"
)

func format(rows [][]string, ordered bool) string {
	cells := make([]string, 0, len(rows))
	for _, r := range rows {
		encoded := make([]string, len(r))
		for i, value := range r {
			encoded[i] = formatCell(value)
		}
		cells = append(cells, strings.Join(encoded, "|"))
	}
	if !ordered {
		sort.Strings(cells)
	}
	return "RS:" + strings.Join(cells, ";")
}

const (
	bytesHexPrefix   = "BYTES_HEX:"
	textEscapePrefix = "TEXT:"
)

// formatCell is the Go side of tidb_exec::ResultSet::label's byte-safe
// differential transport. Existing valid UTF-8 stays byte-for-byte stable;
// malformed UTF-8 is reversible uppercase hex. Escaping valid strings that
// already start with a marker keeps the representation unambiguous.
func formatCell(value string) string {
	if !utf8.ValidString(value) {
		return fmt.Sprintf("%s%X", bytesHexPrefix, []byte(value))
	}
	// The gorun protocol is line-framed. Preserve valid text containing a line
	// terminator as a reversible byte label instead of letting fmt.Fprintln
	// split one result cell into multiple protocol records.
	if strings.ContainsAny(value, "\r\n") {
		return fmt.Sprintf("%s%X", bytesHexPrefix, []byte(value))
	}
	if strings.HasPrefix(value, bytesHexPrefix) || strings.HasPrefix(value, textEscapePrefix) {
		return textEscapePrefix + value
	}
	return value
}

// hasOrderBy reports whether a statement has a top-level ORDER BY, so its result
// order is significant and must not be re-sorted for comparison.
func hasOrderBy(p *parser.Parser, sql string) bool {
	stmt, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		return false
	}
	switch s := stmt.(type) {
	case *ast.SelectStmt:
		return s.OrderBy != nil
	case *ast.SetOprStmt:
		return s.OrderBy != nil
	}
	return false
}

func main() {
	store, err := mockstore.NewMockStore()
	if err != nil {
		fmt.Fprintln(os.Stderr, "mockstore:", err)
		os.Exit(1)
	}
	dom, err := session.BootstrapSession(store)
	if err != nil {
		fmt.Fprintln(os.Stderr, "bootstrap:", err)
		os.Exit(1)
	}
	defer dom.Close()
	se, err := session.CreateSession4Test(store)
	if err != nil {
		fmt.Fprintln(os.Stderr, "create session:", err)
		os.Exit(1)
	}

	ctx := context.Background()
	// A current database is required for CREATE TABLE etc.
	for _, s := range []string{"create database if not exists test", "use test"} {
		if _, err := se.Execute(ctx, s); err != nil {
			fmt.Fprintln(os.Stderr, "setup:", err)
			os.Exit(1)
		}
	}
	p := parser.New()
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 1024*1024), 8*1024*1024)
	out := bufio.NewWriter(os.Stdout)
	defer out.Flush()
	for in.Scan() {
		line := in.Text()
		if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "##") {
			continue
		}
		rss, err := se.Execute(ctx, line)
		if err != nil {
			fmt.Fprintln(out, "ERR")
			continue
		}
		if len(rss) == 0 || rss[0] == nil {
			fmt.Fprintln(out, "OK") // no result set (CREATE TABLE, INSERT, ...)
			continue
		}
		rows, err := session.ResultSetToStringSlice(ctx, se, rss[0])
		if err != nil {
			fmt.Fprintln(out, "ERR")
			continue
		}
		fmt.Fprintln(out, format(rows, hasOrderBy(p, line)))
	}
}
