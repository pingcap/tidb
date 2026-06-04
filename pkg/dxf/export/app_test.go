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

package export_test

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestExportTableStatement(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	dir := t.TempDir()

	tk.MustExec("use test")
	tk.MustExec("create table t (id int primary key, v varchar(64), d decimal(10,2), ts datetime)")
	tk.MustExec(`insert into t values (1,'a"b',1.50,'2026-01-01 00:00:00'),(2,NULL,2.50,NULL),(3,'c\\d',3.00,'2026-02-02 12:00:00')`)

	rows := tk.MustQuery(fmt.Sprintf(
		"EXPORT TABLE test.t TO 'local://%s' WITH thread=2, lanes=2, file_size='64MiB'", dir)).Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "succeed", rows[0][2])

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	var content strings.Builder
	for _, ent := range entries {
		names = append(names, ent.Name())
	}
	sort.Strings(names)
	for _, name := range names {
		require.Regexp(t, `^test\.t\.\d{14}\.csv$`, name)
		data, err := os.ReadFile(filepath.Join(dir, name))
		require.NoError(t, err)
		content.Write(data)
	}
	require.Equal(t, "1,\"a\\\"b\",1.50,\"2026-01-01 00:00:00\"\n"+
		"2,\\N,2.50,\\N\n"+
		"3,\"c\\\\d\",3.00,\"2026-02-02 12:00:00\"\n", content.String())
}
