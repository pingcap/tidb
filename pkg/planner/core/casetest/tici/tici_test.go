// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tici

import (
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	ingesttestutil "github.com/pingcap/tidb/pkg/ddl/ingest/testutil"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func TestTiCISearchExplain(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/tici/MockCreateTiCIIndexSuccess", `return(true)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/tici/MockFinishIndexUpload", `return(true)`))
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/pkg/tici/MockCreateTiCIIndexSuccess")
		require.NoError(t, err)
		err = failpoint.Disable("github.com/pingcap/tidb/pkg/tici/MockFinishIndexUpload")
		require.NoError(t, err)
	}()

	store := testkit.CreateMockStoreWithSchemaLease(t, 1*time.Second, mockstore.WithMockTiFlash(2))

	tk := testkit.NewTestKit(t, store)

	tiflash := infosync.NewMockTiFlash()
	infosync.SetMockTiFlash(tiflash)
	defer func() {
		tiflash.Lock()
		tiflash.StatusServer.Close()
		tiflash.Unlock()
	}()

	tk.MustExec("use test")
	tk.MustExec(`create table t1(
		id INT PRIMARY KEY, title TEXT, body TEXT, field1 int,
		FULLTEXT INDEX idx_title (title),
		index idx_field1 (field1)
	)`)
	dom := domain.GetDomain(tk.Session())
	testkit.SetTiFlashReplica(t, dom, "test", "t1")
	tk.MustExec("create table t2(a int, col text)")

	tk.MustExec(`create table t3(
		a int,
		b int,
		title text,
		primary key(a, b),
		fulltext key(title)
	)`)

	tk.MustExec("create table t4(i bigint, ts timestamp(5), d datetime(3), t text, primary key(i))")
	tk.MustExec(`create hybrid index idx1 on t4(i, ts, d, t) parameter '{
		"inverted": {
			"columns": ["i", "ts", "d", "t"]
		},
		"sort": {
			"columns": ["i", "ts", "d"],
			"order": ["asc", "desc", "asc"]
		},
		"sharding_key": {
			"columns": ["i", "ts"]
		}
	}'`)

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Warn []string
	}
	integrationSuiteData := GetFTSIndexSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			output[i].Warn = testdata.ConvertSQLWarnToStrings(tk.Session().GetSessionVars().StmtCtx.GetWarnings())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
		require.Equal(t, output[i].Warn, testdata.ConvertSQLWarnToStrings(tk.Session().GetSessionVars().StmtCtx.GetWarnings()))
	}
}

func TestTiCIWithIndexHintCases(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/tici/MockCreateTiCIIndexSuccess", `return(true)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/tici/MockFinishIndexUpload", `return(true)`))
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/pkg/tici/MockCreateTiCIIndexSuccess")
		require.NoError(t, err)
		err = failpoint.Disable("github.com/pingcap/tidb/pkg/tici/MockFinishIndexUpload")
		require.NoError(t, err)
	}()

	store := testkit.CreateMockStoreWithSchemaLease(t, 1*time.Second, mockstore.WithMockTiFlash(2))
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	tk := testkit.NewTestKit(t, store)

	tiflash := infosync.NewMockTiFlash()
	infosync.SetMockTiFlash(tiflash)
	defer func() {
		tiflash.Lock()
		tiflash.StatusServer.Close()
		tiflash.Unlock()
	}()

	tk.MustExec("use test")
	tk.MustExec(`create table t1(
		id INT PRIMARY KEY, title TEXT, body TEXT, field1 int,
		FULLTEXT INDEX idx_title (title),
		index idx_field1 (field1)
	)`)
	tk.MustExec("create table t2(i bigint, ts timestamp(5), d datetime(3), t text, primary key(i))")
	tk.MustExec(`create hybrid index idx1 on t2(i, ts, d, t) parameter '{
		"inverted": {
			"columns": ["i", "ts", "d", "t"]
		},
		"sort": {
			"columns": ["i", "ts", "d"],
			"order": ["asc", "desc", "asc"]
		},
		"sharding_key": {
			"columns": ["i", "ts"]
		}
	}'`)
	dom := domain.GetDomain(tk.Session())
	testkit.SetTiFlashReplica(t, dom, "test", "t1")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Warn []string
	}
	integrationSuiteData := GetFTSIndexSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			output[i].Warn = testdata.ConvertSQLWarnToStrings(tk.Session().GetSessionVars().StmtCtx.GetWarnings())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
		require.Equal(t, output[i].Warn, testdata.ConvertSQLWarnToStrings(tk.Session().GetSessionVars().StmtCtx.GetWarnings()))
	}
}
