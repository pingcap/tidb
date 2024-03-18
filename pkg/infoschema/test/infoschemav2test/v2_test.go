// Copyright 2024 PingCAP, Inc.
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

package infoschemav2test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestSpecialSchemas(t *testing.T) {
	t.Skip("This feature is not enabled yet")
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	tk.MustExec("use test")

	tk.MustQuery("select @@global.tidb_schema_cache_size;").Check(testkit.Rows("0"))
	tk.MustExec("set @@global.tidb_schema_cache_size = 1024;")
	tk.MustQuery("select @@global.tidb_schema_cache_size;").Check(testkit.Rows("1024"))
	tk.MustExec("create table t (id int);")
	is := domain.GetDomain(tk.Session()).InfoSchema()
	require.True(t, infoschema.IsV2(is))

	tk.MustQuery("show databases;").Check(testkit.Rows(
		"INFORMATION_SCHEMA", "METRICS_SCHEMA", "PERFORMANCE_SCHEMA", "mysql", "sys", "test"))
	tk.MustExec("use information_schema;")
	tk.MustQuery("show tables;").MultiCheckContain([]string{
		"ANALYZE_STATUS",
		"ATTRIBUTES",
		"CHARACTER_SETS",
		"COLLATIONS",
		"COLUMNS",
		"COLUMN_PRIVILEGES",
		"COLUMN_STATISTICS",
		"VIEWS"})
	tk.MustQuery("show create table tables;").MultiCheckContain([]string{
		`TABLE_CATALOG`,
		`TABLE_SCHEMA`,
		`TABLE_NAME`,
		`TABLE_TYPE`,
	})

	tk.ExecToErr("drop database information_schema;")
	tk.ExecToErr("drop table views;")

	tk.MustExec("use metrics_schema;")
	tk.MustQuery("show tables;").CheckContain("uptime")
	tk.MustQuery("show create table uptime;").CheckContain("time")

	tk.MustExec("set @@global.tidb_schema_cache_size = default;")
}
