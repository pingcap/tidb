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

package session

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/session/sessionapi"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/stretchr/testify/require"
)

func TestUpgradeToVer259BackfillsIgnoreInlistPlanDigest(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("Skip this case because there is no upgrade in the first release of next-gen kernel")
	}

	ctx := context.Background()
	store, dom := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	ver258 := version258
	seV258 := CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	err = m.FinishBootstrap(int64(ver258))
	require.NoError(t, err)
	RevertVersionAndVariables(t, seV258, ver258)

	// Simulate a cluster upgraded through the old path where the variable existed in code
	// but its row was never backfilled into mysql.global_variables.
	MustExec(t, seV258, fmt.Sprintf(
		"delete from mysql.GLOBAL_VARIABLES where variable_name='%s'",
		vardef.TiDBIgnoreInlistPlanDigest,
	))
	err = txn.Commit(ctx)
	require.NoError(t, err)
	store.SetOption(StoreBootstrappedKey, nil)

	res := MustExecToRecodeSet(t, seV258, fmt.Sprintf(
		"select * from mysql.GLOBAL_VARIABLES where variable_name='%s'",
		vardef.TiDBIgnoreInlistPlanDigest,
	))
	chk := res.NewChunk(nil)
	require.NoError(t, res.Next(ctx, chk))
	require.Equal(t, 0, chk.NumRows())
	require.NoError(t, res.Close())

	dom.Close()
	domCurVer, err := BootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()

	seCurVer := CreateSessionAndSetID(t, store)
	ver, err := GetBootstrapVersion(seCurVer)
	require.NoError(t, err)
	require.Equal(t, currentBootstrapVersion, ver)

	res = MustExecToRecodeSet(t, seCurVer, fmt.Sprintf(
		"select * from mysql.GLOBAL_VARIABLES where variable_name='%s'",
		vardef.TiDBIgnoreInlistPlanDigest,
	))
	chk = res.NewChunk(nil)
	require.NoError(t, res.Next(ctx, chk))
	require.Equal(t, 1, chk.NumRows())
	require.Equal(t, "OFF", chk.GetRow(0).GetString(1))
	require.NoError(t, res.Close())

	res = MustExecToRecodeSet(t, seCurVer, "select @@global.tidb_ignore_inlist_plan_digest")
	chk = res.NewChunk(nil)
	require.NoError(t, res.Next(ctx, chk))
	require.Equal(t, 1, chk.NumRows())
	require.Equal(t, int64(0), chk.GetRow(0).GetInt64(0))
	require.NoError(t, res.Close())
}

func TestUpgradeToVer261RefreshesBindingDigest(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("Skip this case because there is no upgrade in the first release of next-gen kernel")
	}

	ctx := context.Background()
	store, dom := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	ver260 := version260
	seV260 := CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	err = m.FinishBootstrap(int64(ver260))
	require.NoError(t, err)
	RevertVersionAndVariables(t, seV260, ver260)
	err = txn.Commit(ctx)
	require.NoError(t, err)

	ver, err := GetBootstrapVersion(seV260)
	require.NoError(t, err)
	require.Equal(t, int64(ver260), ver)

	MustExec(t, seV260, "use test")
	MustExec(t, seV260, "create table t_simple(a int, b int, key idx_simple_a(a), key idx_simple_b(b))")
	MustExec(t, seV260, "create table t_issue(a int, b int, key idx_issue_a(a), key idx_issue_b(b))")

	simpleBindSQL := "select /*+ use_index(t_simple, idx_simple_b) */ * from t_simple where a = 1"
	issueBindSQL := "select /*+ use_index(t_issue, idx_issue_b) */ * from t_issue where ((a = 1) and (b = 1))"
	MustExec(t, seV260, "create global binding for select * from t_simple where a = 1 using "+simpleBindSQL)
	MustExec(t, seV260, "create global binding for select * from t_issue where ((a = 1) and (b = 1)) using "+issueBindSQL)

	issueDigestV261 := getBindingSQLDigest(t, seV260, "idx_issue_b")
	issuePlanDigest := "issue-plan-digest-v261"
	require.NotEmpty(t, issueDigestV261)
	simpleOriginalV260, simpleDigestV260 := normalizeBindingDigestBeforeVer261(t, simpleBindSQL, "test")
	issueOriginalV260, issueDigestV260 := normalizeBindingDigestBeforeVer261(t, issueBindSQL, "test")
	MustExec(t, seV260, "update mysql.bind_info set original_sql = ?, sql_digest = ? where bind_sql like ?", simpleOriginalV260, simpleDigestV260, "%idx_simple_b%")
	MustExec(t, seV260, "update mysql.bind_info set original_sql = ?, sql_digest = ?, plan_digest = ? where bind_sql like ?", issueOriginalV260, issueDigestV260, issuePlanDigest, "%idx_issue_b%")
	invalidBindSQL := "invalid binding"
	MustExec(t, seV260, `insert into mysql.bind_info
		(original_sql, bind_sql, default_db, status, create_time, update_time, charset, collation, source, sql_digest, plan_digest)
		values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"select * from `test` . `t_issue` where `a` = ? and `b` = ?",
		invalidBindSQL,
		"test",
		"enabled",
		"2000-01-01 00:00:00.000000",
		"2000-01-01 00:00:00.000000",
		"",
		"",
		"manual",
		issueDigestV261,
		issuePlanDigest,
	)

	simpleDigestBefore := getBindingSQLDigest(t, seV260, "idx_simple_b")
	issueDigestBefore := getBindingSQLDigest(t, seV260, "idx_issue_b")

	store.SetOption(StoreBootstrappedKey, nil)
	seV260.Close()
	dom.Close()
	domCurVer, err := BootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()

	seCurVer := CreateSessionAndSetID(t, store)
	defer seCurVer.Close()
	ver, err = GetBootstrapVersion(seCurVer)
	require.NoError(t, err)
	require.Equal(t, currentBootstrapVersion, ver)

	simpleDigestAfter := getBindingSQLDigest(t, seCurVer, "idx_simple_b")
	issueDigestAfter := getBindingSQLDigest(t, seCurVer, "idx_issue_b")
	require.Equal(t, simpleDigestBefore, simpleDigestAfter)
	require.NotEqual(t, issueDigestBefore, issueDigestAfter)
	requireBindingDigestPairCleared(t, seCurVer, invalidBindSQL)

	MustExec(t, seCurVer, "admin reload bindings")
	MustExec(t, seCurVer, "use test")
	MustExec(t, seCurVer, "select * from t_simple where a = 1")
	requireLastPlanFromBinding(t, seCurVer)
	MustExec(t, seCurVer, "select * from t_issue where a = 1 and b = 1")
	requireLastPlanFromBinding(t, seCurVer)
}

func normalizeBindingDigestBeforeVer261(t *testing.T, sql, defaultDB string) (string, string) {
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	require.NoError(t, err)

	var sb strings.Builder
	restoreFlags := format.RestoreStringSingleQuotes |
		format.RestoreSpacesAroundBinaryOperation |
		format.RestoreStringWithoutCharset |
		format.RestoreNameBackQuotes
	restoreCtx := format.NewRestoreCtx(restoreFlags, &sb)
	restoreCtx.DefaultDB = defaultDB
	require.NoError(t, stmt.Restore(restoreCtx))

	normalized, digest := parser.NormalizeDigestForBinding(sb.String())
	return normalized, digest.String()
}

func getBindingSQLDigest(t *testing.T, se sessionapi.Session, indexName string) string {
	sqlDigest, _ := getBindingDigestPair(t, se, indexName)
	return sqlDigest
}

func getBindingDigestPair(t *testing.T, se sessionapi.Session, indexName string) (string, string) {
	rs := MustExecToRecodeSet(t, se, "select sql_digest, plan_digest from mysql.bind_info where bind_sql like ?", "%"+indexName+"%")
	req := rs.NewChunk(nil)
	require.NoError(t, rs.Next(context.Background(), req))
	require.Equal(t, 1, req.NumRows())
	sqlDigest := req.GetRow(0).GetString(0)
	planDigest := req.GetRow(0).GetString(1)
	require.NoError(t, rs.Close())
	return sqlDigest, planDigest
}

func requireBindingDigestPairCleared(t *testing.T, se sessionapi.Session, bindSQL string) {
	rs := MustExecToRecodeSet(t, se, "select sql_digest, plan_digest from mysql.bind_info where bind_sql = ?", bindSQL)
	req := rs.NewChunk(nil)
	require.NoError(t, rs.Next(context.Background(), req))
	require.Equal(t, 1, req.NumRows())
	require.True(t, req.GetRow(0).IsNull(0))
	require.True(t, req.GetRow(0).IsNull(1))
	require.NoError(t, rs.Close())
}

func requireLastPlanFromBinding(t *testing.T, se sessionapi.Session) {
	rs := MustExecToRecodeSet(t, se, "select @@last_plan_from_binding")
	req := rs.NewChunk(nil)
	require.NoError(t, rs.Next(context.Background(), req))
	require.Equal(t, 1, req.NumRows())
	require.Equal(t, int64(1), req.GetRow(0).GetInt64(0))
	require.NoError(t, rs.Close())
}
