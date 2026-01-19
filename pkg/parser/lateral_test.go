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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package parser_test

import (
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/format"
)

func TestLateralParsing(t *testing.T) {
	p := parser.New()

	// Test case 1: Basic LATERAL with comma syntax
	sql1 := "SELECT * FROM t1, LATERAL (SELECT t1.a) AS dt"
	stmt1, err := p.ParseOneStmt(sql1, "", "")
	if err != nil {
		t.Fatalf("Failed to parse LATERAL with comma syntax: %v", err)
	}

	// Test case 2: LATERAL with LEFT JOIN
	sql2 := "SELECT * FROM t1 LEFT JOIN LATERAL (SELECT t1.b) AS dt ON true"
	_, err = p.ParseOneStmt(sql2, "", "")
	if err != nil {
		t.Fatalf("Failed to parse LATERAL with LEFT JOIN: %v", err)
	}

	// Test case 3: Verify Restore works (round-trip test)
	var sb strings.Builder
	restoreCtx := format.NewRestoreCtx(format.RestoreStringSingleQuotes, &sb)
	if err := stmt1.Restore(restoreCtx); err != nil {
		t.Fatalf("Failed to restore LATERAL statement: %v", err)
	}
	restored := sb.String()
	if !strings.Contains(restored, "LATERAL") {
		t.Errorf("LATERAL keyword missing in restored SQL: %s", restored)
	}
}
