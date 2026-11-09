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

// Reconciliation end-to-end: every binding source converges on
// mysql.bind_info.sql_digest values that the slow-log Binding_digest can look
// up exactly:
//
//	SELECT b.* FROM mysql.bind_info b WHERE b.sql_digest = '<Binding_digest>'
//
// Sources covered here: manual, capture, and FROM HISTORY.

package tests

import (
	"net"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/stmtsummary"

	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/stretchr/testify/require"
)

const qaReconTrapDigest = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

var qaReconBindingRe = regexp.MustCompile(`Binding_digest: ([0-9a-f]{64})`)
var qaReconDigestRe = regexp.MustCompile(`Digest: ([0-9a-f]{64})`)
var qaReconPlanDigestRe = regexp.MustCompile(`Plan_digest: ([0-9a-f]{64})`)

// qaReconSetup redirects the slow log write side to a temp file and returns
// the testkit. Session-scope bindings never persist to mysql.bind_info, so
// reconciliation cases use GLOBAL bindings.
func qaReconSetup(t *testing.T) (*testkit.TestKit, string) {
	tk, _, path := qaReconSetupWithDomain(t)
	return tk, path
}

// qaReconSetupWithDomain also returns the domain — needed by the FROM HISTORY
// case, whose cluster-table fetch goes through an in-process RPC server
// serving this very domain.
func qaReconSetupWithDomain(t *testing.T) (*testkit.TestKit, *domain.Domain, string) {
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	f, err := os.CreateTemp("", "tidb-slow-recon-*.log")
	require.NoError(t, err)
	require.NoError(t, f.Close())
	newCfg.Log.SlowQueryFile = f.Name()
	config.StoreGlobalConfig(&newCfg)
	require.NoError(t, logutil.InitLogger(newCfg.Log.ToLogConfig()))
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set tidb_slow_log_threshold=0;")
	tk.MustExec("use test")
	t.Cleanup(func() {
		config.StoreGlobalConfig(originCfg)
		require.NoError(t, os.Remove(f.Name()))
	})
	return tk, dom, f.Name()
}

// qaReconRecord returns the LAST slow log record (chunked at "# Time:")
// whose SQL line — the last non-# line of the record — contains sqlSubstr.
// Matching the SQL line keeps internal DDL/DML records (which mention table
// names in job text) out of the assertion.
func qaReconRecord(t *testing.T, path, sqlSubstr string) string {
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var found string
	for _, chunk := range strings.Split(string(raw), "# Time:") {
		if strings.Contains(qaSQLLine(chunk), sqlSubstr) {
			found = chunk
		}
	}
	require.NotEmpty(t, found, "no slow log record for: "+sqlSubstr)
	require.NotContains(t, found, qaReconTrapDigest, "digest-of-empty-string leaked")
	return found
}

func qaSQLLine(record string) string {
	sqlLine := ""
	for _, l := range strings.Split(record, "\n") {
		l = strings.TrimSpace(l)
		if l != "" && !strings.HasPrefix(l, "#") {
			sqlLine = l
		}
	}
	return sqlLine
}

func qaReconField(t *testing.T, record string, re *regexp.Regexp) string {
	m := re.FindStringSubmatch(record)
	require.NotEmpty(t, m, "field missing in record")
	return m[1]
}

// Manual global binding on a single-element in-list — the shape
// exec vocabulary could never reconcile. The workflow lookup by
// Binding_digest must hit exactly the intended row, and the slow-log exec
// Digest must NOT hit anything (vocabularies stay disjoint).
func TestQAReconciliationManualBinding(t *testing.T) {
	tk, path := qaReconSetup(t)
	tk.MustExec("create table qarecon1 (a int)") // no PK: match path
	tk.MustExec("insert into qarecon1 values (1)")
	tk.MustExec("select * from qarecon1 where a in (1)")
	rec := qaReconRecord(t, path, "select * from qarecon1 where a in (1)")
	bindingDigest := qaReconField(t, rec, qaReconBindingRe)
	execDigest := qaReconField(t, rec, qaReconDigestRe)
	require.NotEqual(t, bindingDigest, execDigest)

	tk.MustExec("create global binding for select * from qarecon1 where a in (1) using select * from qarecon1 where a in (1)")
	require.Eventually(t, func() bool {
		return len(tk.MustQuery(
			"select sql_digest from mysql.bind_info where status = 'enabled' and original_sql like '%qarecon1%'",
		).Rows()) == 1
	}, 30e9, 500e6, "global binding must persist to mysql.bind_info")

	// Main workflow: lookup by slow-log Binding_digest hits exactly one row.
	rows := tk.MustQuery(
		"select sql_digest, source from mysql.bind_info where sql_digest = ? and status = 'enabled'", bindingDigest,
	).Rows()
	require.Len(t, rows, 1, "exact-digest lookup by slow-log Binding_digest must hit the manual binding")
	require.Equal(t, "manual", rows[0][1])

	// Vocabulary disjointness: the exec Digest of the same statement hits nothing.
	tk.MustQuery("select sql_digest from mysql.bind_info where sql_digest = ?", execDigest).Check(testkit.Rows())
}

// FROM HISTORY — the third binding source. `create global binding from
// history using plan digest '<PLAN_DIGEST>'` fetches the summary row through
// information_schema.cluster_statements_summary, which in mockstore needs a
// reachable TiDB status endpoint (the domain otherwise advertises a port
// nobody listens on): the test stands up the in-process RPC server the
// clustertablestest harness uses (server.NewRPCServer + StatusPort wiring),
// so the cluster table resolves to this very domain. The created row's
// sql_digest — computed on that path via RestoreWithDefaultDB +
// NormalizeDigestForBinding (planbuilder.go) — must equal the slow-log
// Binding_digest of the same statement: the lookup reconciles the FROM
// HISTORY source exactly like the manual and capture ones.
func TestQAReconciliationFromHistory(t *testing.T) {
	tk, dom, path := qaReconSetupWithDomain(t)
	// the statement summary only records authenticated sessions
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	stmt := "select * from qarecon3 where a in (1)"
	tk.MustExec("create table qarecon3 (a int)")
	tk.MustExec("insert into qarecon3 values (1)")
	tk.MustExec(stmt)

	// In-process RPC server so cluster_statements_summary fetches the summary
	// of this domain (clustertablestest harness shape).
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	sm := &testkit.MockSessionManager{PS: make([]*util.ProcessInfo, 1)}
	sm.PS[0] = &util.ProcessInfo{ID: 1, User: "root", Host: "127.0.0.1", Command: mysql.ComQuery}
	rpcSrv := server.NewRPCServer(config.GetGlobalConfig(), dom, sm)
	go func() { _ = rpcSrv.Serve(lis) }()
	defer rpcSrv.Stop()
	port := lis.Addr().(*net.TCPAddr).Port
	originStatusPort := config.GetGlobalConfig().Status.StatusPort
	originAdvertise := config.GetGlobalConfig().AdvertiseAddress
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Status.StatusPort = uint(port)
		conf.AdvertiseAddress = "127.0.0.1"
	})
	defer config.UpdateGlobal(func(conf *config.Config) {
		conf.Status.StatusPort = originStatusPort
		conf.AdvertiseAddress = originAdvertise
	})

	rec := qaReconRecord(t, path, stmt)
	fieldDigest := qaReconField(t, rec, qaReconBindingRe)
	planDigest := qaReconField(t, rec, qaReconPlanDigestRe)

	tk.MustExec("create global binding from history using plan digest '" + planDigest + "'")
	rows := tk.MustQuery(
		"select sql_digest, source from mysql.bind_info where sql_digest = ? and status = 'enabled'", fieldDigest,
	).Rows()
	require.Len(t, rows, 1, "the FROM HISTORY row must reconcile to the slow-log Binding_digest")
	require.Equal(t, "history", rows[0][1])
}

// Capture source: captured rows land in the
// bind_info vocabulary, so the lookup by the slow-log Binding_digest
// reconciles the capture source too — and capture dedup keeps equivalent
// manual/captured rows on one digest instead of near-duplicates. The
// single-element in-list shape is load-bearing: it is the only shape where
// the vocabularies diverge, so equality here proves vocabulary correctness
// (equality-predicate shapes would stay green even on the unfixed baseline).
func TestQAReconciliationCaptureSource(t *testing.T) {
	tk, path := qaReconSetup(t)
	// capture reads the statement summary: authenticated session + capture on.
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	tk.MustExec("use test")
	tk.MustExec("create table qarecon4 (a int, b int, index idx_b(b))")
	stmt := "select * from qarecon4 where a in (1)"

	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustExec("SET GLOBAL tidb_capture_plan_baselines = on")
	t.Cleanup(func() { tk.MustExec("SET GLOBAL tidb_capture_plan_baselines = off") })

	// A captured row's sql_digest equals the slow-log Binding_digest of
	// the same statement, and the lookup by that value hits exactly the
	// captured row.
	tk.MustExec(stmt)
	tk.MustExec(stmt)
	fieldDigest := qaReconField(t, qaReconRecord(t, path, stmt), qaReconBindingRe)
	tk.MustExec("admin capture bindings")
	require.Eventually(t, func() bool {
		return len(tk.MustQuery(
			"select sql_digest from mysql.bind_info where source = 'capture' and status = 'enabled'",
		).Rows()) == 1
	}, 30e9, 500e6, "capture must create exactly one row")
	require.Equal(t, fieldDigest, tk.MustQuery(
		"select sql_digest from mysql.bind_info where source = 'capture' and status = 'enabled'",
	).Rows()[0][0], "captured sql_digest == slow-log Binding_digest (one vocabulary across sources)")
	lookup := tk.MustQuery(
		"select source from mysql.bind_info where sql_digest = ? and status = 'enabled'", fieldDigest,
	).Rows()
	require.Len(t, lookup, 1, "lookup by slow-log Binding_digest hits exactly the captured row")
	require.Equal(t, "capture", lookup[0][0])

	// An equivalent manual binding joins the SAME digest — TiDB keeps one
	// enabled binding per shape, so the manual row supersedes the captured
	// one (the captured row stays in bind_info history on the same digest),
	// and a capture pass never adds a near-duplicate. The lookup by the
	// slow-log field keeps hitting exactly one enabled row across the
	// transition: every source reconciles to one vocabulary.
	tk.MustExec("create global binding for " + stmt + " using select * from qarecon4 use index(idx_b) where a in (1)")
	require.Eventually(t, func() bool {
		rows := tk.MustQuery(
			"select source from mysql.bind_info where sql_digest = ? and status = 'enabled'", fieldDigest,
		).Rows()
		return len(rows) == 1 && rows[0][0] == "manual"
	}, 30e9, 500e6, "the manual binding must supersede the captured row on the same digest")
	tk.MustExec(stmt)
	tk.MustExec(stmt)
	tk.MustExec("admin capture bindings")
	require.Len(t, tk.MustQuery(
		"select source from mysql.bind_info where sql_digest = ? and status = 'enabled'", fieldDigest,
	).Rows(), 1, "capture never adds a near-duplicate: one enabled row per shape")
	require.GreaterOrEqual(t, len(tk.MustQuery(
		"select sql_digest from mysql.bind_info where source = 'capture' and sql_digest = ?", fieldDigest,
	).Rows()), 1, "the captured history row keeps the shared digest (cross-source equality)")
}
