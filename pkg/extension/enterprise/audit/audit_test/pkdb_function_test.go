package audit_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/extension/enterprise/audit"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/sem"
	"github.com/stretchr/testify/require"
)

func eventClasses2String(eventClasses []audit.EventClass) string {
	eventClassNames := make([]string, len(eventClasses))
	for i, eventClass := range eventClasses {
		eventClassNames[i] = audit.Class2String[eventClass]
	}
	return fmt.Sprintf(`[EVENT="[%s]"]`, strings.Join(eventClassNames, ","))
}

func TestAuditLogRotate(t *testing.T) {
	audit.Register4Test(t)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tempDir := t.TempDir()
	tk.MustExec("SET global tidb_audit_log = ?", path.Join(tempDir, audit.DefAuditLogName))
	_, err := deleteAllAuditLogs(tempDir, "tidb-audit", ".log")
	require.NoError(t, err)

	tk.MustExec("SET global tidb_audit_log_reserved_backups = 2")
	tk.MustExec("SET global tidb_audit_enabled = 1")
	for i := 0; i < 5; i++ {
		tk.MustQuery("SELECT audit_log_rotate()").Check(testkit.Rows("OK"))
		time.Sleep(time.Second)
	}

	files, err := deleteAllAuditLogs(tempDir, "tidb-audit-", ".log")
	require.NoError(t, err)
	require.Equal(t, 2, len(files), files)
}

func TestAuditLogEnableRule(t *testing.T) {
	audit.Register4Test(t)
	tempDir := t.TempDir()
	store := testkit.CreateMockStore(t)
	srv := server.CreateMockServer(t, store)
	defer srv.Close()
	conn := server.CreateMockConn(t, srv)
	defer conn.Close()
	tk := testkit.NewTestKit(t, store)
	err := tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil)
	require.NoError(t, err)
	// Configure the temp log path before enabling audit to avoid writing setup records into the package directory.
	tk.MustExec(fmt.Sprintf("SET global tidb_audit_log = '%s'", filepath.Join(tempDir, audit.DefAuditLogName)))
	tk.MustExec("SET global tidb_audit_enabled = 1")

	_, err = deleteAllAuditLogs(tempDir, "tidb-audit", ".log")
	require.NoError(t, err)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c int)")
	tk.MustQuery("select audit_log_create_filter('all', '{}')").Check(testkit.Rows("OK"))
	tk.MustQuery("select content from mysql.audit_log_filters where filter_name = 'all'").Check(testkit.Rows(`{}`))
	tk.MustQuery("select audit_log_create_rule('%@%', 'all')").Check(testkit.Rows("OK"))
	tk.MustQuery("select enabled from mysql.audit_log_filter_rules where user = '%@%' and filter_name = 'all'").Check(testkit.Rows("1"))
	tk.MustExec("SET global tidb_audit_log_redacted = 0")

	require.NoError(t, conn.HandleQuery(context.Background(), "use test"))
	require.NoError(t, conn.HandleQuery(context.Background(), "insert into t values (1)"))
	ok, _, err := containsMessage(filepath.Join(tempDir, audit.DefAuditLogName), "insert into t values (1)")
	require.NoError(t, err)
	require.True(t, ok)

	tk.MustQuery("select audit_log_disable_rule('%@%', 'all')").Check(testkit.Rows("OK"))
	require.NoError(t, conn.HandleQuery(context.Background(), "insert into t values (2)"))
	ok, _, err = containsMessage(filepath.Join(tempDir, audit.DefAuditLogName), "insert into t values (2)")
	require.NoError(t, err)
	require.False(t, ok)

	tk.MustQuery("select audit_log_enable_rule('%@%', 'all')").Check(testkit.Rows("OK"))
	require.NoError(t, conn.HandleQuery(context.Background(), "insert into t values (3)"))
	ok, _, err = containsMessage(filepath.Join(tempDir, audit.DefAuditLogName), "insert into t values (3)")
	require.NoError(t, err)
	require.True(t, ok)

	_, err = deleteAllAuditLogs(tempDir, "tidb-audit", ".log")
	require.NoError(t, err)
}

func TestAuditAdmin(t *testing.T) {
	audit.Register4Test(t)
	store := testkit.CreateMockStore(t)
	srv := server.CreateMockServer(t, store)
	defer srv.Close()
	conn := server.CreateMockConn(t, srv)
	defer conn.Close()
	tk := testkit.NewTestKit(t, store)
	err := tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil)
	require.NoError(t, err)

	tempDir := t.TempDir()
	tk.MustExec("set global tidb_audit_log=?", path.Join(tempDir, audit.DefAuditLogName))
	tk.MustExec("CREATE USER testuser")
	tk2 := testkit.NewTestKit(t, store)
	err = tk2.Session().Auth(&auth.UserIdentity{Username: "testuser", Hostname: "%"}, nil, nil, nil)
	require.NoError(t, err)
	tk.MustExec("GRANT SELECT, SYSTEM_VARIABLES_ADMIN ON *.* to testuser")

	tk2.MustGetErrCode(`select audit_log_create_filter("empty", '{"filter":[]}')`, errno.ErrSpecificAccessDenied)
	tk2.MustGetErrCode(`set global tidb_audit_enabled = 0`, errno.ErrSpecificAccessDenied)
	tk2.MustGetErrCode(`select * from mysql.audit_log_filters`, errno.ErrTableaccessDenied)

	tk.MustExec("GRANT AUDIT_ADMIN ON *.* to testuser")

	tk2.MustQuery(`select audit_log_create_filter("all_query", '{"filter":[{"class":["QUERY"]}]}')`).Check(testkit.Rows("OK"))
	tk2.MustExec(`set global tidb_audit_enabled = 0`)
	tk2.MustQuery(`select * from mysql.audit_log_filters`).Check(testkit.Rows(`all_query {"filter":[{"class":["QUERY"]}]}`))

	_, err = deleteAllAuditLogs(tempDir, "tidb-audit", ".log")
	require.NoError(t, err)
}

func TestRestrictedAuditAdmin(t *testing.T) {
	audit.Register4Test(t)
	store := testkit.CreateMockStore(t)
	sem.Enable()
	defer sem.Disable()
	srv := server.CreateMockServer(t, store)
	defer srv.Close()
	conn := server.CreateMockConn(t, srv)
	defer conn.Close()
	tk := testkit.NewTestKit(t, store)
	tempDir := t.TempDir()
	tk.MustExec("set global tidb_audit_log=?", path.Join(tempDir, audit.DefAuditLogName))

	tk.MustExec("CREATE USER testuser")
	tk2 := testkit.NewTestKit(t, store)
	err := tk2.Session().Auth(&auth.UserIdentity{Username: "testuser", Hostname: "%"}, nil, nil, nil)
	require.NoError(t, err)
	tk.MustExec("GRANT SELECT, SYSTEM_VARIABLES_ADMIN, AUDIT_ADMIN ON *.* to testuser")

	tk2.MustGetErrCode(`select audit_log_create_filter("empty", '{"filter":[]}')`, errno.ErrSpecificAccessDenied)
	tk2.MustGetErrCode(`set global tidb_audit_enabled = 0`, errno.ErrSpecificAccessDenied)
	tk2.MustGetErrCode(`select * from mysql.audit_log_filters`, errno.ErrTableaccessDenied)

	tk.MustExec("GRANT RESTRICTED_AUDIT_ADMIN ON *.* to testuser")

	tk2.MustQuery(`select audit_log_create_filter("all_query", '{"filter":[{"class":["QUERY"]}]}')`).Check(testkit.Rows("OK"))
	tk2.MustExec(`set global tidb_audit_enabled = 0`)
	tk2.MustQuery(`select * from mysql.audit_log_filters`).Check(testkit.Rows(`all_query {"filter":[{"class":["QUERY"]}]}`))

	_, err = deleteAllAuditLogs(tempDir, "tidb-audit", ".log")
	require.NoError(t, err)
}

func TestEventClass(t *testing.T) {
	audit.Register4Test(t)
	store := testkit.CreateMockStore(t)
	srv := server.CreateMockServer(t, store)
	defer srv.Close()
	conn := server.CreateMockConn(t, srv)
	defer conn.Close()

	tempDir := t.TempDir()
	logPath := path.Join(tempDir, audit.DefAuditLogName)
	require.NoError(t, conn.HandleQuery(
		context.Background(), fmt.Sprintf("SET global tidb_audit_log='%s'", logPath),
	))

	require.NoError(t, conn.HandleQuery(context.Background(), "SET global tidb_audit_enabled = 1"))
	require.NoError(t, conn.HandleQuery(context.Background(), "SELECT audit_log_create_filter('all', '{}')"))
	require.NoError(t, conn.HandleQuery(context.Background(), "SELECT audit_log_create_rule('%@%', 'all')"))

	// Rotate once to isolate setup logs from the actual testcases below.
	require.NoError(t, conn.HandleQuery(context.Background(), "SELECT audit_log_rotate()"))

	// test QUERY and AUDIT
	testcases := []struct {
		sql           string
		requireEvents []audit.EventClass
		success       bool
	}{
		{"use test", []audit.EventClass{audit.ClassQuery}, true},
		{"begin", []audit.EventClass{audit.ClassQuery, audit.ClassTransaction}, true},
		{"create table if not exists t (c int)", []audit.EventClass{audit.ClassQuery, audit.ClassDDL}, true},
		{"create user 'event_class_user'", []audit.EventClass{audit.ClassQuery, audit.ClassDCL}, true},
		{"create user 'event_class_user'", []audit.EventClass{audit.ClassQuery, audit.ClassDCL}, false},
		{"drop user 'event_class_user'", []audit.EventClass{audit.ClassQuery, audit.ClassDCL}, true},
		{"select * from t", []audit.EventClass{audit.ClassQuery, audit.ClassSelect}, true},
		{"commit", []audit.EventClass{audit.ClassQuery, audit.ClassTransaction}, true},
		{"prepare abs_func from 'select abs(?)'", []audit.EventClass{audit.ClassQuery}, true},
		{"set @num = -1", []audit.EventClass{audit.ClassQuery}, true},
		{"execute abs_func using @num", []audit.EventClass{audit.ClassQuery, audit.ClassExecute, audit.ClassSelect}, true},
		{"insert into t values (1)", []audit.EventClass{audit.ClassQuery, audit.ClassDML, audit.ClassInsert}, true},
		{"replace into t values (2)", []audit.EventClass{audit.ClassQuery, audit.ClassDML, audit.ClassReplace}, true},
		{"update t set c = 3 where c = 1", []audit.EventClass{audit.ClassQuery, audit.ClassDML, audit.ClassUpdate}, true},
		{"delete from t", []audit.EventClass{audit.ClassQuery, audit.ClassDML, audit.ClassDelete}, true},
		{`LOAD DATA LOCAL INFILE 'load_data.csv' INTO TABLE t FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\r\n'`, []audit.EventClass{audit.ClassQuery, audit.ClassDML, audit.ClassLoadData}, false},
		{"select audit_log_create_filter('empty', '{}')", []audit.EventClass{audit.ClassAudit, audit.ClassAuditFuncCall}, true},
		{"set global tidb_audit_enabled = 1", []audit.EventClass{audit.ClassAudit, audit.ClassAuditSetSysVar, audit.ClassAuditEnable}, true},
		{"set global tidb_audit_enabled = 0", []audit.EventClass{audit.ClassAudit, audit.ClassAuditSetSysVar, audit.ClassAuditDisable}, true},
	}
	for _, tc := range testcases {
		if tc.success {
			require.NoError(t, conn.HandleQuery(context.Background(), tc.sql), tc.sql)
		} else {
			require.Error(t, conn.HandleQuery(context.Background(), tc.sql), tc.sql)
		}
		eventClassesStr := eventClasses2String(tc.requireEvents)
		ok, log, err := containsMessage(logPath, eventClassesStr)
		require.NoError(t, err, "%s\n%s\n%s", tc.sql, eventClassesStr, log)
		require.True(t, ok, "%s\n%s\n%s", tc.sql, eventClassesStr, log)
		require.NoError(t, conn.HandleQuery(context.Background(), "SELECT audit_log_rotate()"), "%s\n%s\n%s", tc.sql, eventClassesStr, log)
	}
}

// TestDCLAuditFilter verifies that QUERY_DCL can be enabled without logging ordinary DQL or DML statements.
func TestDCLAuditFilter(t *testing.T) {
	audit.Register4Test(t)
	store := testkit.CreateMockStore(t)
	srv := server.CreateMockServer(t, store)
	defer srv.Close()
	conn := server.CreateMockConn(t, srv)
	defer conn.Close()

	tempDir := t.TempDir()
	logPath := path.Join(tempDir, audit.DefAuditLogName)
	require.NoError(t, conn.HandleQuery(
		context.Background(), fmt.Sprintf("SET global tidb_audit_log='%s'", logPath),
	))
	require.NoError(t, conn.HandleQuery(context.Background(), "USE test"))
	require.NoError(t, conn.HandleQuery(context.Background(), "CREATE TABLE dcl_filter_t (c INT)"))
	require.NoError(t, conn.HandleQuery(context.Background(), "SET global tidb_audit_log_redacted = 0"))
	require.NoError(t, conn.HandleQuery(context.Background(), "SET global tidb_audit_enabled = 1"))
	require.NoError(t, conn.HandleQuery(
		context.Background(),
		`SELECT audit_log_create_filter('dcl_only', '{"filter":[{"class":["QUERY_DCL"]}]}')`,
	))
	require.NoError(t, conn.HandleQuery(context.Background(), "SELECT audit_log_create_rule('%@%', 'dcl_only')"))

	require.NoError(t, conn.HandleQuery(context.Background(), "CREATE USER 'dcl_filter_user'"))
	prepared, _, _, err := conn.Context().Prepare("GRANT SELECT ON test.* TO 'dcl_filter_user'")
	require.NoError(t, err)
	binaryExecute := binary.LittleEndian.AppendUint32([]byte{mysql.ComStmtExecute}, uint32(prepared.ID()))
	binaryExecute = append(binaryExecute, 0, 1, 0, 0, 0)
	require.NoError(t, conn.Dispatch(context.Background(), binaryExecute))
	require.NoError(t, conn.HandleQuery(context.Background(), `PREPARE revoke_dcl FROM "REVOKE SELECT ON test.* FROM 'dcl_filter_user'"`))
	require.NoError(t, conn.HandleQuery(context.Background(), "EXECUTE revoke_dcl"))
	require.NoError(t, conn.HandleQuery(context.Background(), "SELECT 'ordinary_select_marker'"))
	require.NoError(t, conn.HandleQuery(context.Background(), "INSERT INTO dcl_filter_t VALUES (8675309)"))
	require.Error(t, conn.HandleQuery(context.Background(), "DROP USER 'missing_dcl_filter_user'"))

	testcases := []struct {
		message string
		exists  bool
	}{
		{message: "GRANT SELECT ON test.* TO 'dcl_filter_user'", exists: true},
		{message: "REVOKE SELECT ON test.* FROM 'dcl_filter_user'", exists: true},
		{message: "ordinary_select_marker", exists: false},
		{message: "8675309", exists: false},
	}
	for _, testcase := range testcases {
		ok, log, err := containsMessage(logPath, testcase.message)
		require.NoError(t, err, "%s\n%s", testcase.message, log)
		require.Equal(t, testcase.exists, ok, "%s\n%s", testcase.message, log)
	}
}

func TestAuditCommitFail(t *testing.T) {
	audit.Register4Test(t)
	tempDir := t.TempDir()
	store := testkit.CreateMockStore(t)
	srv := server.CreateMockServer(t, store)
	defer srv.Close()
	conn := server.CreateMockConn(t, srv)
	defer conn.Close()

	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk.MustExec("create user 'test'@'127.0.0.1'")
	tk.MustExec("grant all on *.* to 'test'@'127.0.0.1'")
	conn.Context().Session.Auth(&auth.UserIdentity{Username: "test", Hostname: "127.0.0.1"}, nil, nil, nil)
	err := tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil)
	require.NoError(t, err)
	defer func() {
		_, err := deleteAllAuditLogs(workDir, "tidb-audit", ".log")
		require.NoError(t, err)
	}()
	tk.MustExec(fmt.Sprintf("SET global tidb_audit_log = '%s'", filepath.Join(tempDir, audit.DefAuditLogName)))
	_, err = deleteAllAuditLogs(tempDir, "tidb-audit", ".log")
	logPath := filepath.Join(tempDir, audit.DefAuditLogName)
	require.NoError(t, err)
	require.NoError(t, conn.HandleQuery(context.Background(), "SET global tidb_audit_enabled = 1"))
	require.NoError(t, conn.HandleQuery(context.Background(), "SELECT audit_log_create_filter('all', '{}')"))
	require.NoError(t, conn.HandleQuery(context.Background(), "SELECT audit_log_create_rule('%@%', 'all')"))
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c int)")
	tk.MustExec("insert into t values(1),(2),(3),(4),(5)")
	originLimit := kv.TxnTotalSizeLimit.Load()
	defer func() {
		kv.TxnTotalSizeLimit.Store(originLimit)
	}()
	// Set the limitation to a small value, make it easier to reach the limitation.
	kv.TxnTotalSizeLimit.Store(1000)
	tk.MustExec("insert into t select * from t")
	tk.MustExec("insert into t select * from t")
	tk.MustExec("insert into t select * from t")
	conn.HandleQuery(context.Background(), "use test")
	conn.HandleQuery(context.Background(), "begin")
	err = conn.HandleQuery(context.Background(), "insert into t select * from t")
	require.NotNil(t, err)
	ok, _, err := containsMessage(logPath, "Commit failed")
	require.NoError(t, err)
	require.False(t, ok)
	conn.HandleQuery(context.Background(), "rollback")
	kv.TxnTotalSizeLimit.Store(originLimit)
	err = conn.HandleQuery(context.Background(), "set tidb_txn_mode='optimistic'")
	require.Nil(t, err)
	tk.MustExec(`CREATE TABLE test ( id int(11) UNSIGNED NOT NULL AUTO_INCREMENT, val int UNIQUE, PRIMARY KEY (id)); `)
	tk.MustExec("set @@tidb_disable_txn_auto_retry = 0")
	require.NoError(t, conn.HandleQuery(context.Background(), "set @@tidb_disable_txn_auto_retry = 0"))
	require.NoError(t, conn.HandleQuery(context.Background(), "begin OPTIMISTIC"))
	require.NoError(t, conn.HandleQuery(context.Background(), "insert into test(id, val) values(1, 1);"))
	tk.MustExec(`set tidb_txn_mode=''`)
	tk2.MustExec(`set tidb_txn_mode=''`)
	tk.MustExec("begin")
	tk.MustExec("insert into test(id, val) values(2, 2);")
	tk2.MustExec("begin")
	tk2.MustExec("insert into test(id, val) values(1, 2);")
	tk2.MustExec("commit")
	err = conn.HandleQuery(context.Background(), "commit")
	require.NotNil(t, err)
	ok, _, err = containsMessage(logPath, "Commit failed")
	require.NoError(t, err)
	require.True(t, ok)
	_, _ = deleteAllAuditLogs(tempDir, "tidb-audit", ".log")
}

func TestAuditResourceGroup(t *testing.T) {
	audit.Register4Test(t)
	tempDir := t.TempDir()
	store := testkit.CreateMockStore(t)
	srv := server.CreateMockServer(t, store)
	defer srv.Close()
	conn := server.CreateMockConn(t, srv)
	defer conn.Close()

	tk := testkit.NewTestKit(t, store)
	err := tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil)
	require.NoError(t, err)
	defer func() {
		_, err := deleteAllAuditLogs(workDir, "tidb-audit", ".log")
		require.NoError(t, err)
	}()
	tk.MustExec(fmt.Sprintf("SET global tidb_audit_log = '%s'", filepath.Join(tempDir, audit.DefAuditLogName)))
	_, err = deleteAllAuditLogs(tempDir, "tidb-audit", ".log")
	logPath := filepath.Join(tempDir, audit.DefAuditLogName)
	require.NoError(t, err)
	require.NoError(t, conn.HandleQuery(context.Background(), "SET global tidb_audit_enabled = 1"))
	require.NoError(t, conn.HandleQuery(context.Background(), "SELECT audit_log_create_filter('all', '{}')"))
	require.NoError(t, conn.HandleQuery(context.Background(), "SELECT audit_log_create_rule('%@%', 'all')"))
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c int)")
	tk.MustExec("insert into t values(1),(2),(3),(4),(5)")
	tk.MustExec("set global tidb_enable_resource_control='on'")
	tk.MustExec("create resource group rg1 RU_PER_SEC=1000 QUERY_LIMIT=(EXEC_ELAPSED='50ms' ACTION=KILL)")
	tk.MustExec("create resource group rg2 BURSTABLE RU_PER_SEC=2000 QUERY_LIMIT=(EXEC_ELAPSED='50ms' action KILL WATCH EXACT duration '1s')")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/copr/sleepCoprRequest", fmt.Sprintf("return(%d)", 60)))
	_ = conn.HandleQuery(context.Background(), "use test")
	err = conn.HandleQuery(context.Background(), "select /*+ resource_group(rg1) */ * from t")
	require.ErrorContains(t, err, "[executor:8253]Query execution was interrupted, identified as runaway query")
	err = conn.HandleQuery(context.Background(), "select /*+ resource_group(rg2) */ * from t")
	require.ErrorContains(t, err, "[executor:8253]Query execution was interrupted, identified as runaway query")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/copr/sleepCoprRequest"))

	ok, str, err := containsMessage(logPath, `[EVENT="[SECURITY]"]`)
	require.NoError(t, err)
	require.True(t, ok, str)
	require.Contains(t, str, "Resource group error:")
	require.Contains(t, str, "Query execution was interrupted, identified as runaway query")
	require.Contains(t, str, "[Resource group name:rg1]")
	require.Contains(t, str, "[Resource group name:rg2]")
}

func TestAuditLogRuleUsername(t *testing.T) {
	audit.Register4Test(t)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tempDir := t.TempDir()
	tk.MustExec("set global tidb_audit_log=?", path.Join(tempDir, audit.DefAuditLogName))
	_, err := deleteAllAuditLogs(tempDir, "tidb-audit", ".log")
	require.NoError(t, err)
	tk.MustQuery("select audit_log_create_filter('all', '{}')").Check(testkit.Rows("OK"))
	tk.MustQuery("select audit_log_create_rule('foo@bar@127.0.0.1', 'all')").Check(testkit.Rows("OK"))
}

func TestAuditLogRuleValidate(t *testing.T) {
	audit.Register4Test(t)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tempDir := t.TempDir()
	tk.MustExec("set global tidb_audit_log=?", path.Join(tempDir, audit.DefAuditLogName))
	tk.MustQuery("select audit_log_create_filter('f1', '{}')").Check(testkit.Rows("OK"))
	tk.MustQuery("select FILTER_NAME, CONTENT from mysql.audit_log_filters").Check(testkit.Rows("f1 {}"))
	// dup name
	err := tk.QueryToErr("select audit_log_create_filter('f1', '{}')")
	require.EqualError(t, err, "[kv:1062]Duplicate entry 'f1' for key 'audit_log_filters.PRIMARY'")
	// invalid event name
	err = tk.QueryToErr(`select audit_log_create_filter('f2', '{"filter": [{"class": ["noexisteventname"]}]}')`)
	require.EqualError(t, err, "invalid event class name 'noexisteventname'")
	// invalid status code
	err = tk.QueryToErr(`select audit_log_create_filter('f2', '{"filter": [{"status_code": [999]}]}')`)
	require.EqualError(t, err, "invalid status code '999'")
	// invalid filters should not be created
	tk.MustQuery("select FILTER_NAME, CONTENT from mysql.audit_log_filters").Check(testkit.Rows("f1 {}"))
}

func TestFilterCURD(t *testing.T) {
	audit.Register4Test(t)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tempDir := t.TempDir()
	tk.MustExec("set global tidb_audit_log=?", path.Join(tempDir, audit.DefAuditLogName))

	// filters and rules should be empty for a new cluster
	tk.MustQuery("select * from mysql.audit_log_filters").Check(testkit.Rows())
	tk.MustQuery("select * from mysql.audit_log_filter_rules").Check(testkit.Rows())

	// create filters
	tk.MustQuery("select audit_log_create_filter('f1', '{}')").Check(testkit.Rows("OK"))
	tk.MustQuery("select audit_log_create_filter('f2', ?)", `{"filter": [ {"class": [ "query" ]}]}`).
		Check(testkit.Rows("OK"))
	tk.MustQuery("select audit_log_create_filter('f3', ?, true)", `{"filter":[{"class":["CONNECTION"]}]}`).
		Check(testkit.Rows("OK"))
	tk.MustQuery("select audit_log_create_filter('f4', ?, false)",
		`{"class":[],"table":[],"class_excl":[],"table_excl":[],"status_code":[]}`).
		Check(testkit.Rows("OK"))
	tk.MustQuery("select FILTER_NAME, CONTENT from mysql.audit_log_filters order by FILTER_NAME").
		Check(testkit.Rows(
			"f1 {}",
			`f2 {"filter":[{"class":["QUERY"]}]}`,
			`f3 {"filter":[{"class":["CONNECTION"]}]}`,
			`f4 {}`,
		))

	// should not create duplicate filter names if replace is false
	err := tk.QueryToErr("select audit_log_create_filter('f2', '{}', false)")
	require.EqualError(t, err, "[kv:1062]Duplicate entry 'f2' for key 'audit_log_filters.PRIMARY'")
	err = tk.QueryToErr("select audit_log_create_filter('f3', '{}')")
	require.EqualError(t, err, "[kv:1062]Duplicate entry 'f3' for key 'audit_log_filters.PRIMARY'")
	tk.MustQuery("select FILTER_NAME, CONTENT from mysql.audit_log_filters order by FILTER_NAME").
		Check(testkit.Rows(
			"f1 {}",
			`f2 {"filter":[{"class":["QUERY"]}]}`,
			`f3 {"filter":[{"class":["CONNECTION"]}]}`,
			`f4 {}`,
		))

	// should replace filter content when replace=true
	tk.MustQuery("select audit_log_create_filter('f1', ?, true)", `{"filter":[{"table":["test.*"]}]}`).
		Check(testkit.Rows("OK"))
	tk.MustQuery("select FILTER_NAME, CONTENT from mysql.audit_log_filters order by FILTER_NAME").
		Check(testkit.Rows(
			`f1 {"filter":[{"table":["test.*"]}]}`,
			`f2 {"filter":[{"class":["QUERY"]}]}`,
			`f3 {"filter":[{"class":["CONNECTION"]}]}`,
			`f4 {}`,
		))

	// create filter rules
	tk.MustQuery("select * from mysql.audit_log_filter_rules").Check(testkit.Rows())
	tk.MustQuery("select audit_log_create_rule('%@%', 'f1')").Check(testkit.Rows("OK"))
	tk.MustQuery("select audit_log_create_rule('root@%', 'f2', true)").Check(testkit.Rows("OK"))
	tk.MustQuery("select audit_log_create_rule('user1@localhost', 'f3', false)").Check(testkit.Rows("OK"))
	tk.MustQuery("select audit_log_create_rule('%@%', 'f4')").Check(testkit.Rows("OK"))
	tk.MustQuery("select audit_log_create_rule('root@%', 'f1')").Check(testkit.Rows("OK"))
	tk.MustQuery("select USER, FILTER_NAME, ENABLED from mysql.audit_log_filter_rules order by USER, FILTER_NAME").
		Check(testkit.Rows("%@% f1 1", "%@% f4 1", "root@% f1 1", "root@% f2 1", "user1@localhost f3 1"))

	// disable rule
	tk.MustQuery("select audit_log_disable_rule('%@%', 'f1')").Check(testkit.Rows("OK"))
	tk.MustQuery("select USER, FILTER_NAME, ENABLED from mysql.audit_log_filter_rules order by USER, FILTER_NAME").
		Check(testkit.Rows("%@% f1 0", "%@% f4 1", "root@% f1 1", "root@% f2 1", "user1@localhost f3 1"))
	tk.MustQuery("select audit_log_disable_rule('root@%', 'f2')").Check(testkit.Rows("OK"))
	tk.MustQuery("select USER, FILTER_NAME, ENABLED from mysql.audit_log_filter_rules order by USER, FILTER_NAME").
		Check(testkit.Rows("%@% f1 0", "%@% f4 1", "root@% f1 1", "root@% f2 0", "user1@localhost f3 1"))

	// disable again should take no effect
	tk.MustQuery("select audit_log_disable_rule('%@%', 'f1')").Check(testkit.Rows("OK"))
	tk.MustQuery("select USER, FILTER_NAME, ENABLED from mysql.audit_log_filter_rules order by USER, FILTER_NAME").
		Check(testkit.Rows("%@% f1 0", "%@% f4 1", "root@% f1 1", "root@% f2 0", "user1@localhost f3 1"))

	// enable rule
	tk.MustQuery("select audit_log_enable_rule('%@%', 'f1')").Check(testkit.Rows("OK"))
	tk.MustQuery("select USER, FILTER_NAME, ENABLED from mysql.audit_log_filter_rules order by USER, FILTER_NAME").
		Check(testkit.Rows("%@% f1 1", "%@% f4 1", "root@% f1 1", "root@% f2 0", "user1@localhost f3 1"))
	tk.MustQuery("select audit_log_enable_rule('user1@localhost', 'f3')").Check(testkit.Rows("OK"))
	tk.MustQuery("select USER, FILTER_NAME, ENABLED from mysql.audit_log_filter_rules order by USER, FILTER_NAME").
		Check(testkit.Rows("%@% f1 1", "%@% f4 1", "root@% f1 1", "root@% f2 0", "user1@localhost f3 1"))

	// enable/disable not-exist rule should return error
	err = tk.QueryToErr("select audit_log_enable_rule('userx@%', 'f1')")
	require.EqualError(t, err, "rule does not exist")
	err = tk.QueryToErr("select audit_log_disable_rule('%@%', 'f2')")
	require.EqualError(t, err, "rule does not exist")

	// replace rule
	tk.MustQuery("select audit_log_create_rule('%@%', 'f1', true)").Check(testkit.Rows("OK"))
	tk.MustQuery("select USER, FILTER_NAME, ENABLED from mysql.audit_log_filter_rules order by USER, FILTER_NAME").
		Check(testkit.Rows("%@% f1 1", "%@% f4 1", "root@% f1 1", "root@% f2 0", "user1@localhost f3 1"))

	// cannot delete an in-use filter
	err = tk.QueryToErr("select audit_log_remove_filter('f2')")
	require.EqualError(t, err, "filter 'f2' is in use")
	tk.MustQuery("select FILTER_NAME, CONTENT from mysql.audit_log_filters order by FILTER_NAME").
		Check(testkit.Rows(
			`f1 {"filter":[{"table":["test.*"]}]}`,
			`f2 {"filter":[{"class":["QUERY"]}]}`,
			`f3 {"filter":[{"class":["CONNECTION"]}]}`,
			`f4 {}`,
		))
	tk.MustQuery("select USER, FILTER_NAME, ENABLED from mysql.audit_log_filter_rules order by USER, FILTER_NAME").
		Check(testkit.Rows("%@% f1 1", "%@% f4 1", "root@% f1 1", "root@% f2 0", "user1@localhost f3 1"))

	// delete rule
	tk.MustQuery("select audit_log_remove_rule('root@%', 'f2')").Check(testkit.Rows("OK"))
	tk.MustQuery("select FILTER_NAME, CONTENT from mysql.audit_log_filters order by FILTER_NAME").
		Check(testkit.Rows(
			`f1 {"filter":[{"table":["test.*"]}]}`,
			`f2 {"filter":[{"class":["QUERY"]}]}`,
			`f3 {"filter":[{"class":["CONNECTION"]}]}`,
			`f4 {}`,
		))
	tk.MustQuery("select USER, FILTER_NAME, ENABLED from mysql.audit_log_filter_rules order by USER, FILTER_NAME").
		Check(testkit.Rows("%@% f1 1", "%@% f4 1", "root@% f1 1", "user1@localhost f3 1"))

	// delete an unused filter
	tk.MustQuery("select audit_log_remove_filter('f2')").Check(testkit.Rows("OK"))
	tk.MustQuery("select FILTER_NAME, CONTENT from mysql.audit_log_filters order by FILTER_NAME").
		Check(testkit.Rows(
			`f1 {"filter":[{"table":["test.*"]}]}`,
			`f3 {"filter":[{"class":["CONNECTION"]}]}`,
			`f4 {}`,
		))
	tk.MustQuery("select USER, FILTER_NAME, ENABLED from mysql.audit_log_filter_rules order by USER, FILTER_NAME").
		Check(testkit.Rows("%@% f1 1", "%@% f4 1", "root@% f1 1", "user1@localhost f3 1"))

	// delete a non-existent rule or filter should take no effect
	tk.MustQuery("select audit_log_remove_rule('root@%', 'f4')").Check(testkit.Rows("OK"))
	tk.MustQuery("select audit_log_remove_filter('f5')").Check(testkit.Rows("OK"))
	tk.MustQuery("select FILTER_NAME, CONTENT from mysql.audit_log_filters order by FILTER_NAME").
		Check(testkit.Rows(
			`f1 {"filter":[{"table":["test.*"]}]}`,
			`f3 {"filter":[{"class":["CONNECTION"]}]}`,
			`f4 {}`,
		))
	tk.MustQuery("select USER, FILTER_NAME, ENABLED from mysql.audit_log_filter_rules order by USER, FILTER_NAME").
		Check(testkit.Rows("%@% f1 1", "%@% f4 1", "root@% f1 1", "user1@localhost f3 1"))
}

func TestAuditLogConvenientRule(t *testing.T) {
	audit.Register4Test(t)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	// create a filter and distinct user rules
	tk.MustQuery("select audit_log_convenient_rule('basic_security', ?, ' connection,query_dcl, QUERY_DDL ')",
		" %@%, alice@%, 'alice,bob'@'%', bob@localhost, alice@%, 'alice,bob'@'%' ").
		Check(testkit.Rows("OK"))
	tk.MustQuery("select FILTER_NAME, CONTENT from mysql.audit_log_filters").
		Check(testkit.Rows(
			`basic_security {"filter":[{"class":["CONNECTION","QUERY_DCL","QUERY_DDL"]}]}`,
		))
	tk.MustQuery("select USER, FILTER_NAME, ENABLED from mysql.audit_log_filter_rules order by USER").
		Check(testkit.Rows("%@% basic_security 1", "alice,bob@% basic_security 1", "alice@% basic_security 1", "bob@localhost basic_security 1"))

	bundle := audit.GlobalLogManagerForTest().GetFilter()
	require.True(t, filterMatched(bundle, &audit.LogEntry{
		User:    "alice",
		Host:    "127.0.0.1",
		Classes: []audit.EventClass{audit.ClassQuery, audit.ClassDCL},
	}))
	require.False(t, filterMatched(bundle, &audit.LogEntry{
		User:    "alice",
		Host:    "127.0.0.1",
		Classes: []audit.EventClass{audit.ClassQuery, audit.ClassSelect},
	}))
	require.False(t, filterMatched(bundle, &audit.LogEntry{
		User:    "alice",
		Host:    "127.0.0.1",
		Classes: []audit.EventClass{audit.ClassQuery, audit.ClassDML, audit.ClassInsert},
	}))

	// all events
	tk.MustQuery("select audit_log_convenient_rule('all_events', '%@%', ' All ')").
		Check(testkit.Rows("OK"))
	tk.MustQuery("select CONTENT from mysql.audit_log_filters where FILTER_NAME = 'all_events'").
		Check(testkit.Rows(`{}`))

	// required event types
	eventTypes := []string{
		"CONNECTION", "QUERY_DDL", "QUERY_DCL", "QUERY_DML", "EXECUTE", "TRANSACTION", "SELECT",
	}
	for _, eventType := range eventTypes {
		tk.MustQuery("select audit_log_convenient_rule(?, '%@%', ?)", "event_"+eventType, eventType).
			Check(testkit.Rows("OK"))
	}
	require.Len(t, tk.MustQuery("select FILTER_NAME from mysql.audit_log_filters").Rows(), len(eventTypes)+2)
}

func TestAuditLogConvenientRuleDefaults(t *testing.T) {
	audit.Register4Test(t)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	// omitted arguments use the default users and event classes
	tk.MustQuery("select audit_log_convenient_rule()").Check(testkit.Rows("OK"))
	tk.MustQuery("select FILTER_NAME, CONTENT from mysql.audit_log_filters").Check(testkit.Rows(
		`convenient_rule_01 {"filter":[{"class":["CONNECTION","QUERY_DDL","QUERY_DCL"]}]}`,
	))
	tk.MustQuery("select USER, FILTER_NAME, ENABLED from mysql.audit_log_filter_rules").
		Check(testkit.Rows("%@% convenient_rule_01 1"))

	// one and two arguments use defaults for the omitted trailing arguments
	tk.MustQuery("select audit_log_convenient_rule('one_arg')").Check(testkit.Rows("OK"))
	tk.MustQuery("select audit_log_convenient_rule('two_args', 'alice@%')").Check(testkit.Rows("OK"))
	tk.MustQuery("select audit_log_convenient_rule('', '', '')").Check(testkit.Rows("OK"))
	tk.MustQuery("select FILTER_NAME, CONTENT from mysql.audit_log_filters order by FILTER_NAME").
		Check(testkit.Rows(
			`convenient_rule_01 {"filter":[{"class":["CONNECTION","QUERY_DDL","QUERY_DCL"]}]}`,
			`convenient_rule_02 {"filter":[{"class":["CONNECTION","QUERY_DDL","QUERY_DCL"]}]}`,
			`one_arg {"filter":[{"class":["CONNECTION","QUERY_DDL","QUERY_DCL"]}]}`,
			`two_args {"filter":[{"class":["CONNECTION","QUERY_DDL","QUERY_DCL"]}]}`,
		))
	tk.MustQuery("select USER, FILTER_NAME, ENABLED from mysql.audit_log_filter_rules order by USER, FILTER_NAME").
		Check(testkit.Rows(
			"%@% convenient_rule_01 1",
			"%@% convenient_rule_02 1",
			"%@% one_arg 1",
			"alice@% two_args 1",
		))
}

func TestAuditLogConvenientRuleAutoNames(t *testing.T) {
	audit.Register4Test(t)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	// skip occupied names, including names with trailing spaces
	tk.MustQuery("select audit_log_create_filter('convenient_rule_01 ', '{}')").
		Check(testkit.Rows("OK"))
	tk.MustQuery("select audit_log_convenient_rule('', 'alice@%', 'QUERY_DDL')").
		Check(testkit.Rows("OK"))
	tk.MustQuery("select FILTER_NAME from mysql.audit_log_filters order by FILTER_NAME").
		Check(testkit.Rows("convenient_rule_01 ", "convenient_rule_02"))
}

func TestAuditLogConvenientRuleValidate(t *testing.T) {
	audit.Register4Test(t)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	// invalid events and users should not leave partial configuration
	for _, eventType := range []string{" ", ",", "QUERY_DCL,", "INVALID_EVENT", "all,", "all,QUERY_DCL", "QUERY_DCL,all", "all,all"} {
		err := tk.QueryToErr("select audit_log_convenient_rule('invalid_event', '%@%', ?)", eventType)
		require.Error(t, err, "event type: %q", eventType)
	}
	err := tk.QueryToErr("select audit_log_convenient_rule('invalid_user', 'good@%,bad@@', 'QUERY_DCL')")
	require.Error(t, err)

	tk.MustQuery("select * from mysql.audit_log_filters").Check(testkit.Rows())
	tk.MustQuery("select * from mysql.audit_log_filter_rules").Check(testkit.Rows())

	// a duplicate name should preserve modified content and the disabled rule
	tk.MustQuery("select audit_log_convenient_rule('basic_security', '%@%', 'QUERY_DCL')").
		Check(testkit.Rows("OK"))
	tk.MustQuery("select audit_log_disable_rule('%@%', 'basic_security')").Check(testkit.Rows("OK"))
	tk.MustQuery("select audit_log_create_filter('basic_security', ?, true)",
		`{"filter":[{"class":["SELECT"]}]}`).Check(testkit.Rows("OK"))
	err = tk.QueryToErr("select audit_log_convenient_rule('basic_security', '%@%', 'QUERY_DDL')")
	require.Error(t, err)
	tk.MustQuery("select FILTER_NAME, CONTENT from mysql.audit_log_filters").
		Check(testkit.Rows(`basic_security {"filter":[{"class":["SELECT"]}]}`))
	tk.MustQuery("select USER, FILTER_NAME, ENABLED from mysql.audit_log_filter_rules").
		Check(testkit.Rows("%@% basic_security 0"))
}

func TestAuditLogConvenientRuleRollback(t *testing.T) {
	audit.Register4Test(t)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	// An orphan rule forces the second user insert to fail after the first succeeds.
	tk.MustExec("insert into mysql.audit_log_filter_rules values ('bob@%', 'basic_security', 0)")
	err := tk.QueryToErr("select audit_log_convenient_rule('basic_security', 'alice@%,bob@%', 'QUERY_DDL')")
	require.ErrorContains(t, err, "Duplicate entry")
	tk.MustQuery("select FILTER_NAME from mysql.audit_log_filters").Check(testkit.Rows())
	tk.MustQuery("select USER, FILTER_NAME, ENABLED from mysql.audit_log_filter_rules").
		Check(testkit.Rows("bob@% basic_security 0"))
}
