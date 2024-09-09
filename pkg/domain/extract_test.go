// Copyright 2023 PingCAP, Inc.
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

package domain_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/testkit"
	stmtsummaryv2 "github.com/pingcap/tidb/pkg/util/stmtsummary/v2"
	"github.com/stretchr/testify/require"
)

func TestExtractPlanWithoutHistoryView(t *testing.T) {
	_, dom := testkit.CreateMockStoreAndDomain(t)
	extractHandler := dom.GetExtractHandle()
	task := domain.NewExtractPlanTask(time.Now(), time.Now())
	task.UseHistoryView = false
	_, err := extractHandler.ExtractTask(context.Background(), task)
	defer os.RemoveAll(domain.GetExtractTaskDirName())
	require.NoError(t, err)
}

func TestExtractWithoutStmtSummaryPersistedEnabled(t *testing.T) {
	setupStmtSummary()
	closeStmtSummary()
	_, dom := testkit.CreateMockStoreAndDomain(t)
	extractHandler := dom.GetExtractHandle()
	task := domain.NewExtractPlanTask(time.Now(), time.Now())
	task.UseHistoryView = true
	_, err := extractHandler.ExtractTask(context.Background(), task)
	defer os.RemoveAll(domain.GetExtractTaskDirName())
	require.Error(t, err)
}

func TestExtractHandlePlanTask(t *testing.T) {
	setupStmtSummary()
	defer closeStmtSummary()

	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newTestKitWithRoot(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id int);")
	// Clear all statements.
	tk.MustExec("set global tidb_enable_stmt_summary = 0")
	tk.MustExec("set global tidb_enable_stmt_summary = 1")
	tk.MustQuery("select @@global.tidb_enable_stmt_summary").Check(testkit.Rows("1"))

	// new testkit
	tk = newTestKitWithRoot(t, store)
	tk.MustExec("use test")
	tk.MustQuery("select * from t")
	startTime := time.Now()
	time.Sleep(time.Second)
	tk.MustQuery("select COUNT(*) FROM INFORMATION_SCHEMA.STATEMENTS_SUMMARY_HISTORY WHERE STMT_TYPE = 'Select' AND SCHEMA_NAME = 'test' AND TABLE_NAMES = 'test.t'").Check(testkit.Rows("1"))
	time.Sleep(time.Second)
	end := time.Now()
	extractHandler := dom.GetExtractHandle()
	task := domain.NewExtractPlanTask(startTime, end)
	task.UseHistoryView = true
	name, err := extractHandler.ExtractTask(context.Background(), task)
	defer os.RemoveAll(domain.GetExtractTaskDirName())
	require.NoError(t, err)
	require.True(t, len(name) > 0)
}

func setupStmtSummary() {
	stmtsummaryv2.Setup(&stmtsummaryv2.Config{
		Filename: "tidb-statements.log",
	})
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Instance.StmtSummaryEnablePersistent = true
	})
}

func closeStmtSummary() {
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Instance.StmtSummaryEnablePersistent = false
	})
	stmtsummaryv2.GlobalStmtSummary.Close()
	_ = os.Remove(config.GetGlobalConfig().Instance.StmtSummaryFilename)
}

func newTestKit(t *testing.T, store kv.Storage) *testkit.TestKit {
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	return tk
}

func newTestKitWithRoot(t *testing.T, store kv.Storage) *testkit.TestKit {
	tk := newTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	return tk
}
