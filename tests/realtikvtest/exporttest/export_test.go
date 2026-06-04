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
		require.Regexp(t, `^export_test\.t\.\d{14}\.csv$`, ent.Name())
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
		names = append(names, ent.Name())
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
		names = append(names, ent.Name())
	}
	sort.Strings(names)
	for _, name := range names {
		data, err := os.ReadFile(filepath.Join(dir, name))
		require.NoError(t, err)
		content.Write(data)
	}
	require.Equal(t, "\"a\",1\n\"b\",2\n\"c\",3\n", content.String())
}
