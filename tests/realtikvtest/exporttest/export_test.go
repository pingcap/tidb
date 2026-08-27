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

package exporttest

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

func TestExportTableClusteredPK(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	dir := t.TempDir()

	tk.MustExec("drop database if exists export_test")
	tk.MustExec("create database export_test")
	tk.MustExec("use export_test")
	tk.MustExec("create table t (id int primary key clustered, v varchar(128), d decimal(10,2))")

	const rowCnt = 20000
	var sb strings.Builder
	for i := range rowCnt {
		if i%1000 == 0 {
			if sb.Len() > 0 {
				tk.MustExec(sb.String())
			}
			sb.Reset()
			sb.WriteString("insert into t values ")
		} else {
			sb.WriteString(",")
		}
		fmt.Fprintf(&sb, "(%d,'val-%d',%d.25)", i, i, i%1000)
	}
	tk.MustExec(sb.String())
	tk.MustQuery("split table t between (0) and (20000) regions 8").Check(
		testkit.Rows("7 1"))

	rows := tk.MustQuery(fmt.Sprintf(
		"EXPORT TABLE export_test.t TO 'local://%s' WITH thread=2, file_size='128KiB'", dir)).Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "succeed", rows[0][2])

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, ent := range entries {
		if !strings.HasSuffix(ent.Name(), ".csv") {
			continue
		}
		require.Regexp(t, `^export_test\.t\.\d{11}\.csv$`, ent.Name())
		names = append(names, ent.Name())
	}
	require.NotEmpty(t, names)
	sort.Strings(names)

	// concatenating files in name order must give ids in strictly increasing
	// order: files are internally ordered, disjoint, and named in key order.
	gotRows := 0
	lastID := -1
	for _, name := range names {
		data, err := os.ReadFile(filepath.Join(dir, name))
		require.NoError(t, err)
		for line := range strings.Lines(string(data)) {
			line = strings.TrimSuffix(line, "\n")
			if line == "" {
				continue
			}
			fields := strings.SplitN(line, ",", 3)
			require.Len(t, fields, 3)
			id, err := strconv.Atoi(fields[0])
			require.NoError(t, err)
			require.Greater(t, id, lastID, "ids must be strictly increasing across files in name order")
			require.Equal(t, fmt.Sprintf(`"val-%d"`, id), fields[1])
			lastID = id
			gotRows++
		}
	}
	require.Equal(t, rowCnt, gotRows)
	t.Logf("exported %d rows into %d files", gotRows, len(names))
}

func TestExportTableNonClustered(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	dir := t.TempDir()

	tk.MustExec("drop database if exists export_test2")
	tk.MustExec("create database export_test2")
	tk.MustExec("use export_test2")
	tk.MustExec("create table t (id int, v varchar(64), key(id))")
	tk.MustExec("insert into t values (3,'c'),(1,'a'),(2,NULL)")
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("3"))

	rows := tk.MustQuery(fmt.Sprintf(
		"EXPORT TABLE export_test2.t TO 'local://%s'", dir)).Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "succeed", rows[0][2])

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var content strings.Builder
	names := make([]string, 0, len(entries))
	for _, ent := range entries {
		if strings.HasSuffix(ent.Name(), ".csv") {
			names = append(names, ent.Name())
		}
	}
	sort.Strings(names)
	for _, name := range names {
		data, err := os.ReadFile(filepath.Join(dir, name))
		require.NoError(t, err)
		content.Write(data)
	}
	// _tidb_rowid order == insert order here; _tidb_rowid itself is not exported.
	require.Equal(t, "3,\"c\"\n1,\"a\"\n2,\\N\n", content.String())
}

func TestExportSchemaMultiTable(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	dir := t.TempDir()

	tk.MustExec("drop database if exists export_schema_test")
	tk.MustExec("create database export_schema_test")
	tk.MustExec("use export_schema_test")
	const tableCnt = 5
	wantByTable := make(map[string]map[string]bool, tableCnt)
	for i := range tableCnt {
		tblName := fmt.Sprintf("t%d", i)
		tk.MustExec(fmt.Sprintf("create table %s (id int primary key, v varchar(32))", tblName))
		tk.MustExec(fmt.Sprintf("insert into %s values (1,'a-%d'),(2,'b-%d')", tblName, i, i))
		wantByTable[tblName] = map[string]bool{
			fmt.Sprintf(`1,"a-%d"`, i): true,
			fmt.Sprintf(`2,"b-%d"`, i): true,
		}
	}
	// a view and a temporary table must be skipped, not exported.
	tk.MustExec("create view v0 as select * from t0")
	tk.MustExec("create temporary table tmp0 (id int)")

	rows := tk.MustQuery(fmt.Sprintf(
		"EXPORT SCHEMA export_schema_test TO 'local://%s'", dir)).Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "succeed", rows[0][2])

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	dataRe := regexp.MustCompile(`^export_schema_test\.(t\d)\.\d{11}\.csv$`)
	schemaRe := regexp.MustCompile(`^export_schema_test\.(t\d)-schema\.sql$`)
	gotByTable := make(map[string]map[string]bool, tableCnt)
	gotSchemaFor := make(map[string]bool, tableCnt)
	gotDBCreate := false
	for _, ent := range entries {
		switch {
		case ent.Name() == "export_schema_test-schema-create.sql":
			require.False(t, gotDBCreate, "duplicate database create file")
			gotDBCreate = true
			data, err := os.ReadFile(filepath.Join(dir, ent.Name()))
			require.NoError(t, err)
			require.Contains(t, string(data), "CREATE DATABASE")
			continue
		case schemaRe.MatchString(ent.Name()):
			tblName := schemaRe.FindStringSubmatch(ent.Name())[1]
			data, err := os.ReadFile(filepath.Join(dir, ent.Name()))
			require.NoError(t, err)
			require.Contains(t, string(data), "CREATE TABLE")
			gotSchemaFor[tblName] = true
			continue
		}
		m := dataRe.FindStringSubmatch(ent.Name())
		require.NotNil(t, m, "unexpected file name %s", ent.Name())
		tblName := m[1]
		data, err := os.ReadFile(filepath.Join(dir, ent.Name()))
		require.NoError(t, err)
		if gotByTable[tblName] == nil {
			gotByTable[tblName] = map[string]bool{}
		}
		for line := range strings.Lines(string(data)) {
			line = strings.TrimSuffix(line, "\n")
			if line != "" {
				gotByTable[tblName][line] = true
			}
		}
	}
	require.Equal(t, wantByTable, gotByTable)
	require.True(t, gotDBCreate, "missing export_schema_test-schema-create.sql")
	wantSchemaFor := make(map[string]bool, tableCnt)
	for tblName := range wantByTable {
		wantSchemaFor[tblName] = true
	}
	require.Equal(t, wantSchemaFor, gotSchemaFor)
}

// TestExportSchemaBatchedScanMatchesCoprocessor exports the same table two ways
// and requires the bytes to match. A schema of many tiny tables is read with one
// scan covering all of them at once, while a single-table export still goes
// through the coprocessor, so this is what proves the two read paths agree.
func TestExportSchemaBatchedScanMatchesCoprocessor(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	schemaDir, tableDir := t.TempDir(), t.TempDir()

	tk.MustExec("drop database if exists export_batch_test")
	tk.MustExec("create database export_batch_test")
	tk.MustExec("use export_batch_test")

	// Enough tables to be worth batching, each small enough to sit whole inside
	// a shared region. Types are mixed so decoding is exercised beyond integers,
	// and one column is added afterwards so its rows fall back to the default.
	const tableCnt = 12
	for i := range tableCnt {
		tbl := fmt.Sprintf("t%02d", i)
		tk.MustExec(fmt.Sprintf(
			"create table %s (id int primary key clustered, v varchar(32), d decimal(10,2), n int)", tbl))
		for r := range 3 {
			tk.MustExec(fmt.Sprintf("insert into %s values (%d,'v-%d-%d',%d.75,%d)",
				tbl, r, i, r, r, r*7))
		}
		// Rows written before this column existed take its default when read.
		tk.MustExec(fmt.Sprintf("alter table %s add column added varchar(8) default 'dflt'", tbl))
		tk.MustExec(fmt.Sprintf("insert into %s values (99,'after',1.00,7,'set')", tbl))
	}

	rows := tk.MustQuery(fmt.Sprintf(
		"EXPORT SCHEMA export_batch_test TO 'local://%s'", schemaDir)).Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "succeed", rows[0][2])

	readCSVs := func(dir, table string) map[string]string {
		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		re := regexp.MustCompile(`^export_batch_test\.` + table + `\.(\d{11})\.csv$`)
		out := map[string]string{}
		for _, ent := range entries {
			m := re.FindStringSubmatch(ent.Name())
			if m == nil {
				continue
			}
			data, err := os.ReadFile(filepath.Join(dir, ent.Name()))
			require.NoError(t, err)
			out[m[1]] = string(data)
		}
		require.NotEmpty(t, out, "no data files for %s in %s", table, dir)
		return out
	}

	// Same table on its own: one table never batches, so this is the
	// coprocessor path reading exactly the same rows.
	const probe = "t05"
	rows = tk.MustQuery(fmt.Sprintf(
		"EXPORT TABLE export_batch_test.%s TO 'local://%s'", probe, tableDir)).Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "succeed", rows[0][2])

	require.Equal(t, readCSVs(tableDir, probe), readCSVs(schemaDir, probe),
		"batched scan and coprocessor scan must produce identical files")

	// And the content itself is right, so both paths agreeing cannot mean both
	// are wrong in the same way.
	var got []string
	for _, data := range readCSVs(schemaDir, probe) {
		for line := range strings.Lines(data) {
			if line = strings.TrimSuffix(line, "\n"); line != "" {
				got = append(got, line)
			}
		}
	}
	sort.Strings(got)
	require.Equal(t, []string{
		`0,"v-5-0",0.75,0,"dflt"`,
		`1,"v-5-1",1.75,7,"dflt"`,
		`2,"v-5-2",2.75,14,"dflt"`,
		`99,"after",1.00,7,"set"`,
	}, got)
}

// TestExportSchemaBatchedScanCommonHandle repeats the cross-path comparison for
// tables whose primary key lives in the row key rather than its value, which the
// batched scan has to rebuild itself instead of receiving it already decoded.
func TestExportSchemaBatchedScanCommonHandle(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	schemaDir, tableDir := t.TempDir(), t.TempDir()

	tk.MustExec("drop database if exists export_batch_ch")
	tk.MustExec("create database export_batch_ch")
	tk.MustExec("use export_batch_ch")

	const tableCnt = 12
	for i := range tableCnt {
		tbl := fmt.Sprintf("t%02d", i)
		tk.MustExec(fmt.Sprintf(
			"create table %s (k varchar(16), s varchar(16), v int, primary key (k, s) clustered)", tbl))
		for r := range 3 {
			tk.MustExec(fmt.Sprintf("insert into %s values ('k-%d','s-%d-%d',%d)", tbl, r, i, r, r*3))
		}
	}

	rows := tk.MustQuery(fmt.Sprintf(
		"EXPORT SCHEMA export_batch_ch TO 'local://%s'", schemaDir)).Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "succeed", rows[0][2])

	const probe = "t07"
	rows = tk.MustQuery(fmt.Sprintf(
		"EXPORT TABLE export_batch_ch.%s TO 'local://%s'", probe, tableDir)).Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "succeed", rows[0][2])

	readCSVs := func(dir string) map[string]string {
		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		re := regexp.MustCompile(`^export_batch_ch\.` + probe + `\.(\d{11})\.csv$`)
		out := map[string]string{}
		for _, ent := range entries {
			if m := re.FindStringSubmatch(ent.Name()); m != nil {
				data, err := os.ReadFile(filepath.Join(dir, ent.Name()))
				require.NoError(t, err)
				out[m[1]] = string(data)
			}
		}
		require.NotEmpty(t, out, "no data files for %s in %s", probe, dir)
		return out
	}
	require.Equal(t, readCSVs(tableDir), readCSVs(schemaDir),
		"batched scan must rebuild the clustered key exactly as the coprocessor does")

	var got []string
	for _, data := range readCSVs(schemaDir) {
		for line := range strings.Lines(data) {
			if line = strings.TrimSuffix(line, "\n"); line != "" {
				got = append(got, line)
			}
		}
	}
	sort.Strings(got)
	require.Equal(t, []string{
		`"k-0","s-7-0",0`,
		`"k-1","s-7-1",3`,
		`"k-2","s-7-2",6`,
	}, got)
}

func TestExportTableCommonHandle(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	dir := t.TempDir()

	tk.MustExec("drop database if exists export_test3")
	tk.MustExec("create database export_test3")
	tk.MustExec("use export_test3")
	tk.MustExec("create table t (k varchar(16), v int, primary key(k) clustered)")
	tk.MustExec("insert into t values ('b',2),('a',1),('c',3)")

	rows := tk.MustQuery(fmt.Sprintf(
		"EXPORT TABLE export_test3.t TO 'local://%s'", dir)).Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "succeed", rows[0][2])

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var content strings.Builder
	names := make([]string, 0, len(entries))
	for _, ent := range entries {
		if strings.HasSuffix(ent.Name(), ".csv") {
			names = append(names, ent.Name())
		}
	}
	sort.Strings(names)
	for _, name := range names {
		data, err := os.ReadFile(filepath.Join(dir, name))
		require.NoError(t, err)
		content.Write(data)
	}
	require.Equal(t, "\"a\",1\n\"b\",2\n\"c\",3\n", content.String())
}
