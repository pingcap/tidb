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

package core

import (
	"sort"
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

// TestExtractTablePartitionMalformed guards against an out-of-range slice when
// the input contains ')' before '(' (e.g. a crafted table-name argument to
// tidb_encode_record_key). Previously str[start+1:end] panicked with a reversed
// slice; now it returns the whole string with an empty partition.
func TestExtractTablePartitionMalformed(t *testing.T) {
	var h tidbCodecFuncHelper
	cases := []struct {
		input     string
		wantTable string
		wantPart  string
	}{
		{"t)(", "t)(", ""}, // ')' before '(' — the panic trigger
		{")(", ")(", ""},   // leading ')'
		{"t(p)", "t", "p"}, // normal partitioned form still works
		{"t", "t", ""},     // no parens
		{"t(p", "t(p", ""}, // missing ')'
		{"tp)", "tp)", ""}, // missing '('
	}
	for _, c := range cases {
		tbl, part := h.extractTablePartition(c.input)
		require.Equal(t, c.wantTable, tbl, "input=%q", c.input)
		require.Equal(t, c.wantPart, part, "input=%q", c.input)
	}
}

// TestSchemaTableSorterKeepsPairsAligned verifies that sorting schema/table
// pairs keeps schemas[i] paired with tables[i]. The previous hand-rolled
// sort.Slice mutated the table slice inside the less func, scrambling the
// pairing.
func TestSchemaTableSorterKeepsPairsAligned(t *testing.T) {
	schemas := []ast.CIStr{
		ast.NewCIStr("db_b"),
		ast.NewCIStr("db_a"),
		ast.NewCIStr("db_a"),
	}
	tables := []*model.TableInfo{
		{Name: ast.NewCIStr("t_in_b")},
		{Name: ast.NewCIStr("t2_in_a")},
		{Name: ast.NewCIStr("t1_in_a")},
	}
	sort.Sort(&schemaTableSorter{schemas: schemas, tables: tables})

	// Expected order: (db_a, t1_in_a), (db_a, t2_in_a), (db_b, t_in_b).
	require.Equal(t, "db_a", schemas[0].L)
	require.Equal(t, "t1_in_a", tables[0].Name.L)
	require.Equal(t, "db_a", schemas[1].L)
	require.Equal(t, "t2_in_a", tables[1].Name.L)
	require.Equal(t, "db_b", schemas[2].L)
	require.Equal(t, "t_in_b", tables[2].Name.L)
}
