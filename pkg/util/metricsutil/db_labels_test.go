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

package metricsutil_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/metricsutil"
	"github.com/stretchr/testify/require"
)

func TestGetDBNames(t *testing.T) {
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	newCfg.Status.RecordDBLabel = true
	config.StoreGlobalConfig(&newCfg)
	defer config.StoreGlobalConfig(originCfg)

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database DatabaseA;")
	tk.MustExec("use DatabaseA;")
	dbs := metricsutil.GetDBNames(tk.Session().GetSessionVars())
	require.Equal(t, dbs[0], "databasea")
	tk.MustExec(`create table t1(id bigint primary key, a int, b varchar(32), c text)`)
	tk.MustExec(`create table t2(id bigint primary key, a int, b varchar(32), c text)`)
	dbs = metricsutil.GetDBNames(tk.Session().GetSessionVars())
	require.Equal(t, dbs[0], "databasea")
	tk.MustExec(`insert into t1 (id, b, c) values(1, 'ab', 'ab\\\\c');`)
	dbs = metricsutil.GetDBNames(tk.Session().GetSessionVars())
	require.Equal(t, dbs[0], "databasea")
	tk.MustQuery("select * from t1 where id = 1").Check(testkit.Rows("1 <nil> ab ab\\\\c"))
	dbs = metricsutil.GetDBNames(tk.Session().GetSessionVars())
	require.Equal(t, dbs[0], "databasea")
	tk.MustExec(`insert into t1 (id, b, c) values(2, 'xy', 'ab\\c');`)
	tk.MustExec(`update t1 set a = 123 where id = 2`)
	dbs = metricsutil.GetDBNames(tk.Session().GetSessionVars())
	require.Equal(t, dbs[0], "databasea")
	tk.MustExec(`delete from t1 where id = 1;`)
	dbs = metricsutil.GetDBNames(tk.Session().GetSessionVars())
	require.Equal(t, dbs[0], "databasea")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("2 123 xy ab\\c"))
	dbs = metricsutil.GetDBNames(tk.Session().GetSessionVars())
	require.Equal(t, dbs[0], "databasea")
	require.ErrorIs(t, tk.ExecToErr("IMPORT INTO t1(a) FROM select * from t2;"),
		plannererrors.ErrWrongValueCountOnRow)
	dbs = metricsutil.GetDBNames(tk.Session().GetSessionVars())
	require.Equal(t, dbs[0], "databasea")
	tk.MustQuery("show tables")
	dbs = metricsutil.GetDBNames(tk.Session().GetSessionVars())
	require.Equal(t, dbs[0], "databasea")
	tk.MustExec(`drop table t1`)
	dbs = metricsutil.GetDBNames(tk.Session().GetSessionVars())
	require.Equal(t, dbs[0], "databasea")
}

func TestGetDBNamesLabels(t *testing.T) {
	originCfg := config.GetGlobalConfig()
	defer config.StoreGlobalConfig(originCfg)

	cases := []struct {
		name          string
		recordDBLabel bool
		vars          *variable.SessionVars
		want          []string
	}{
		{
			name:          "nil session vars",
			recordDBLabel: true,
			want:          []string{""},
		},
		{
			name: "record DB label disabled",
			vars: &variable.SessionVars{
				CurrentDB: "DatabaseA",
				StmtCtx: &stmtctx.StatementContext{
					Tables: []stmtctx.TableEntry{{DB: "databaseb", Table: "t1"}},
				},
			},
			want: []string{""},
		},
		{
			name:          "nil statement context",
			recordDBLabel: true,
			vars:          &variable.SessionVars{CurrentDB: "DatabaseA"},
			want:          []string{"databasea"},
		},
		{
			name:          "empty tables",
			recordDBLabel: true,
			vars: &variable.SessionVars{
				CurrentDB: "DatabaseA",
				StmtCtx:   &stmtctx.StatementContext{},
			},
			want: []string{"databasea"},
		},
		{
			name:          "empty current DB",
			recordDBLabel: true,
			vars:          &variable.SessionVars{},
			want:          []string{""},
		},
		{
			name:          "cross DB deduplication",
			recordDBLabel: true,
			vars: &variable.SessionVars{
				CurrentDB: "UnusedDB",
				StmtCtx: &stmtctx.StatementContext{
					Tables: []stmtctx.TableEntry{
						{DB: "databasea", Table: "t1"},
						{DB: "databaseb", Table: "t2"},
						{DB: "databasea", Table: "t3"},
					},
				},
			},
			want: []string{"databasea", "databaseb"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			newCfg := *originCfg
			newCfg.Status.RecordDBLabel = tc.recordDBLabel
			config.StoreGlobalConfig(&newCfg)
			require.ElementsMatch(t, tc.want, metricsutil.GetDBNames(tc.vars))
		})
	}
}
