package audit_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/extension/enterprise/audit"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

/*
	1. Any statement executed by MockConn.HandleQuery() would be trailed.
	2. Due to the different implementation of `testkit`, statements executed by testkit.MustExec() could only trail SET GLOBAL and FUNCTION CALL.
*/

var (
	workDir string
)

func init() {
	workDir, _ = os.Getwd()
}

// get file names as string array in the directory
func getAllAuditLogs(dir, prefix, suffix string) ([]string, error) {
	f, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	files, err := f.Readdir(0)
	if err != nil {
		return nil, err
	}

	var res []string
	for _, file := range files {
		if name := file.Name(); strings.HasPrefix(name, prefix) && strings.HasSuffix(name, suffix) {
			res = append(res, filepath.Join(dir, name))
		}
	}

	return res, nil
}

// remove and return all logs in the directory
func deleteAllAuditLogs(dir, prefix, suffix string) ([]string, error) {
	files, err := getAllAuditLogs(dir, prefix, suffix)
	if err != nil {
		return nil, err
	}
	for _, file := range files {
		err = os.Remove(file)
		if err != nil {
			return nil, err
		}
	}
	return files, nil
}

func containsMessage(logpath, msg string) (ok bool, str string, err error) {
	bytes, err := os.ReadFile(logpath)
	str = string(bytes)
	if err != nil {
		return false, str, err
	}
	return strings.Contains(str, msg), str, nil
}

func globalLogManager(t *testing.T) *audit.LogManager {
	t.Helper()
	require.True(t, intest.InTest, "requires testkit with -tags=intest")
	manager := audit.GlobalLogManagerForTest()
	require.NotNil(t, manager)
	return manager
}

func TestAuditEnabled(t *testing.T) {
	tempDir := t.TempDir()
	_, err := deleteAllAuditLogs(tempDir, "tidb-audit", ".log")
	require.NoError(t, err)

	audit.Register4Test(t)
	store := testkit.CreateMockStore(t)
	srv := server.CreateMockServer(t, store)
	defer srv.Close()
	conn := server.CreateMockConn(t, srv)
	defer conn.Close()
	tk := testkit.NewTestKit(t, store)
	err = tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil)
	require.NoError(t, err)

	tk.MustExec(fmt.Sprintf("SET global tidb_audit_log = '%s'", filepath.Join(tempDir, audit.DefAuditLogName)))
	tk.MustQuery("SELECT @@global.tidb_audit_enabled").Check(testkit.Rows("0"))
	require.False(t, globalLogManager(t).Enabled())
	require.NoError(t, conn.HandleQuery(context.Background(), "SET global tidb_audit_enabled = 1"))
	tk.MustQuery("SELECT @@global.tidb_audit_enabled").Check(testkit.Rows("1"))
	require.True(t, globalLogManager(t).Enabled())
	require.Equal(t, filepath.Join(tempDir, audit.DefAuditLogName), globalLogManager(t).GetLogPath())
	time.Sleep(time.Second)

	files, err := deleteAllAuditLogs(tempDir, "tidb-audit", ".log")
	require.NoError(t, err)
	require.True(t, len(files) > 0, files)
}

func TestAuditEnabledRuleWarning(t *testing.T) {
	audit.Register4Test(t)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("SET GLOBAL tidb_audit_enabled = 1")
	tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows(
		"Warning 1105 audit is enabled, but no effective business audit rule is configured; you can configure rules using SELECT audit_log_convenient_rule('filter_name', 'user_name', 'event_type')",
	))

	tk.MustQuery("SELECT audit_log_convenient_rule('basic_security', '%@%', 'CONNECTION')").Check(testkit.Rows("OK"))
	tk.MustExec("SET GLOBAL tidb_audit_enabled = 0")
	tk.MustExec("SET GLOBAL tidb_audit_enabled = 1")
	tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows())
}

func TestAuditLogDefault(t *testing.T) {
	audit.Register4Test(t)
	tempDir := t.TempDir()
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustQuery("SELECT @@global.tidb_audit_log").Check(testkit.Rows(audit.DefAuditLogName))
	require.Equal(t, audit.DefAuditLogName, globalLogManager(t).GetLogConfigPath())
	require.Equal(t, filepath.Join(workDir, audit.DefAuditLogName), globalLogManager(t).GetLogPath())

	// Set empty log name
	tk.MustExec("SET global tidb_audit_log = ''")
	tk.MustQuery("SELECT @@global.tidb_audit_log").Check(testkit.Rows(""))
	require.Equal(t, "", globalLogManager(t).GetLogConfigPath())
	require.Equal(t, filepath.Join(workDir, audit.DefAuditLogName), globalLogManager(t).GetLogPath())

	// Set custom log name
	tk.MustExec(fmt.Sprintf("SET global tidb_audit_log = '%s'", "custom-tidb-audit.log"))
	tk.MustQuery("SELECT @@global.tidb_audit_log").Check(testkit.Rows("custom-tidb-audit.log"))
	require.Equal(t, "custom-tidb-audit.log", globalLogManager(t).GetLogConfigPath())
	require.Equal(t, filepath.Join(workDir, "custom-tidb-audit.log"), globalLogManager(t).GetLogPath())
	rawAddr, rawPort := config.GetGlobalConfig().AdvertiseAddress, config.GetGlobalConfig().Port
	config.GetGlobalConfig().AdvertiseAddress = "127.0.0.1"
	config.GetGlobalConfig().Port = 4000
	defer func() {
		config.GetGlobalConfig().AdvertiseAddress, config.GetGlobalConfig().Port = rawAddr, rawPort
	}()
	tk.MustExec(fmt.Sprintf("SET global tidb_audit_log = '%s'", "custom-tidb-audit-%e.log"))
	tk.MustQuery("SELECT @@global.tidb_audit_log").Check(testkit.Rows("custom-tidb-audit-%e.log"))
	require.Equal(t, "custom-tidb-audit-%e.log", globalLogManager(t).GetLogConfigPath())
	require.Equal(t, filepath.Join(workDir, "custom-tidb-audit-127-0-0-1-4000.log"), globalLogManager(t).GetLogPath())

	// Write to absolute path
	tempDirLog := filepath.Join(tempDir, "temp-audit.log")
	tk.MustExec(fmt.Sprintf("SET global tidb_audit_log = '%s'", tempDirLog))
	tk.MustQuery("SELECT @@global.tidb_audit_log").Check(testkit.Rows(tempDirLog))
	require.Equal(t, tempDirLog, globalLogManager(t).GetLogConfigPath())
	require.Equal(t, tempDirLog, globalLogManager(t).GetLogPath())
}

func TestAuditLogDefaultInstanceLog(t *testing.T) {
	tempDir := t.TempDir()
	// Default to the same directory as tidb instance logs
	config.GetGlobalConfig().Log.File.Filename = filepath.Join(tempDir, "tidb.log")
	rawAddr, rawPort := config.GetGlobalConfig().AdvertiseAddress, config.GetGlobalConfig().Port
	config.GetGlobalConfig().AdvertiseAddress = "127.0.0.1"
	config.GetGlobalConfig().Port = 4000
	defer func() {
		config.GetGlobalConfig().Log.File.Filename = ""
		config.GetGlobalConfig().AdvertiseAddress, config.GetGlobalConfig().Port = rawAddr, rawPort
	}()
	audit.Register4Test(t)

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(fmt.Sprintf("SET global tidb_audit_log = '%s'", audit.DefAuditLogName))
	tk.MustQuery("SELECT @@global.tidb_audit_log").Check(testkit.Rows(audit.DefAuditLogName))
	require.Equal(t, audit.DefAuditLogName, globalLogManager(t).GetLogConfigPath())
	require.Equal(t, filepath.Join(tempDir, audit.DefAuditLogName), globalLogManager(t).GetLogPath())

	tk.MustExec("SET global tidb_audit_log = 'tidb-audit-%e.log'")
	tk.MustQuery("SELECT @@global.tidb_audit_log").Check(testkit.Rows("tidb-audit-%e.log"))
	require.Equal(t, "tidb-audit-%e.log", globalLogManager(t).GetLogConfigPath())
	require.Equal(t, filepath.Join(tempDir, "tidb-audit-127-0-0-1-4000.log"), globalLogManager(t).GetLogPath())
}

func TestAuditLogFormat(t *testing.T) {
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

	_, err = deleteAllAuditLogs(tempDir, "tidb-audit", ".log")
	require.NoError(t, err)
	_, err = deleteAllAuditLogs(tempDir, "tidb-audit", ".json")
	require.NoError(t, err)

	tk.MustExec(fmt.Sprintf("SET global tidb_audit_log = '%s'", filepath.Join(tempDir, audit.DefAuditLogName)))
	tk.MustQuery("SELECT @@global.tidb_audit_log_format").Check(testkit.Rows("TEXT"))
	require.NoError(t, conn.HandleQuery(context.Background(), "SET global tidb_audit_enabled = 1"))
	require.Equal(t, filepath.Join(tempDir, audit.DefAuditLogName), globalLogManager(t).GetLogPath())
	require.Equal(t, "TEXT", globalLogManager(t).GetLogFormat())

	require.NoError(t, conn.HandleQuery(context.Background(), "SET global tidb_audit_log_format = 'JSON'"))
	tk.MustQuery("SELECT @@global.tidb_audit_log_format").Check(testkit.Rows("JSON"))
	require.Equal(t, filepath.Join(tempDir, audit.DefAuditLogName+".json"), globalLogManager(t).GetLogPath())
	require.Equal(t, "JSON", globalLogManager(t).GetLogFormat())

	files, err := deleteAllAuditLogs(tempDir, "tidb-audit", ".log")
	require.NoError(t, err)
	require.Equal(t, 1, len(files), files)
	files, err = deleteAllAuditLogs(tempDir, "tidb-audit", ".json")
	require.NoError(t, err)
	require.Equal(t, 1, len(files), files)
}

func TestAuditLogMaxSize(t *testing.T) {
	audit.Register4Test(t)
	tempDir := t.TempDir()
	store := testkit.CreateMockStore(t)
	srv := server.CreateMockServer(t, store)
	defer srv.Close()
	conn := server.CreateMockConn(t, srv)
	defer conn.Close()
	tk := testkit.NewTestKit(t, store)

	// test setting
	tk.MustExec(fmt.Sprintf("SET global tidb_audit_log = '%s'", filepath.Join(tempDir, audit.DefAuditLogName)))
	tk.MustQuery("SELECT @@global.tidb_audit_log_max_filesize").Check(testkit.Rows("100"))
	require.Equal(t, audit.DefAuditLogFileMaxSize, globalLogManager(t).GetFileMaxSize())
	tk.MustExec("SET global tidb_audit_log_max_filesize = 102401")
	tk.MustQuery("SELECT @@global.tidb_audit_log_max_filesize").Check(testkit.Rows("102400"))
	require.Equal(t, int64(102400), globalLogManager(t).GetFileMaxSize())
	tk.MustExec("SET global tidb_audit_log_max_filesize = 0")
	tk.MustQuery("SELECT @@global.tidb_audit_log_max_filesize").Check(testkit.Rows("100"))
	require.Equal(t, audit.DefAuditLogFileMaxSize, globalLogManager(t).GetFileMaxSize())

	_, err := deleteAllAuditLogs(tempDir, "tidb-audit", ".log")
	require.NoError(t, err)
	tk.MustExec("SET global tidb_audit_enabled = 1")
	tk.MustExec("SET global tidb_audit_log_max_filesize = 1")
	tk.MustExec("SET global tidb_audit_log_redacted=0")
	tk.MustQuery("select audit_log_rotate()")
	tk.MustQuery("select audit_log_create_filter('all', '{}')")
	tk.MustQuery("select audit_log_create_rule('%@%', 'all')")
	var sb strings.Builder
	sb.WriteString("SELECT '")
	for i := 0; i < 1024; i++ {
		sb.WriteString("01234567890")
	}
	sb.WriteString("'")
	sql := sb.String()
	for i := 0; i < 128; i++ {
		require.NoError(t, conn.HandleQuery(context.Background(), sql))
	}
	files, err := deleteAllAuditLogs(tempDir, "tidb-audit", ".log")
	require.NoError(t, err)
	require.Greater(t, len(files), 1, files)
}

func TestAuditLogMaxLifetime(t *testing.T) {
	audit.Register4Test(t)
	tempDir := t.TempDir()

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	_, err := deleteAllAuditLogs(tempDir, "tidb-audit", ".log")
	require.NoError(t, err)

	tk.MustQuery("SELECT @@global.tidb_audit_enabled").Check(testkit.Rows("0"))
	tk.MustQuery("SELECT @@global.tidb_audit_log_max_lifetime").Check(testkit.Rows("86400"))

	tk.MustExec(fmt.Sprintf("SET global tidb_audit_log = '%s'", filepath.Join(tempDir, audit.DefAuditLogName)))
	tk.MustQuery("SELECT @@global.tidb_audit_log").Check(testkit.Rows(filepath.Join(tempDir, audit.DefAuditLogName)))

	tk.MustExec("SET global tidb_audit_enabled = 1")
	tk.MustQuery("SELECT @@global.tidb_audit_enabled").Check(testkit.Rows("1"))
	tk.MustExec("SET global tidb_audit_log_max_lifetime = 3")
	tk.MustQuery("SELECT @@global.tidb_audit_log_max_lifetime").Check(testkit.Rows("3"))
	require.Equal(t, int64(3), globalLogManager(t).GetFileMaxLifetime())
	// Generate 3 log files
	time.Sleep(10 * time.Second)
	files, err := getAllAuditLogs(tempDir, "tidb-audit", ".log")
	require.NoError(t, err)
	require.Equal(t, 3, len(files), files)

	tk.MustExec("SET global tidb_audit_log_max_lifetime = 0")
	tk.MustQuery("SELECT @@global.tidb_audit_log_max_lifetime").Check(testkit.Rows("0"))
	require.Equal(t, int64(0), globalLogManager(t).GetFileMaxLifetime())
	// Not generate log file anymore
	time.Sleep(7 * time.Second)

	files, err = deleteAllAuditLogs(tempDir, "tidb-audit", ".log")
	require.NoError(t, err)
	require.Equal(t, 3, len(files), files)
}

func TestAuditLogReservedBackups(t *testing.T) {
	audit.Register4Test(t)
	tempDir := t.TempDir()

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustQuery("SELECT @@global.tidb_audit_log_reserved_backups").Check(testkit.Rows("10"))

	tk.MustExec("SET global tidb_audit_enabled = 1")
	tk.MustQuery("SELECT @@global.tidb_audit_enabled").Check(testkit.Rows("1"))
	tk.MustExec(fmt.Sprintf("SET global tidb_audit_log = '%s'", filepath.Join(tempDir, audit.DefAuditLogName)))
	tk.MustQuery("SELECT @@global.tidb_audit_log").Check(testkit.Rows(filepath.Join(tempDir, audit.DefAuditLogName)))

	_, err := deleteAllAuditLogs(tempDir, "tidb-audit", ".log")
	require.NoError(t, err)

	// Generate 5 log files, only two files are reserved
	tk.MustExec("SET global tidb_audit_log_reserved_backups = 2")
	tk.MustQuery("SELECT @@global.tidb_audit_log_reserved_backups").Check(testkit.Rows("2"))
	require.Equal(t, 2, globalLogManager(t).GetFileReservedBackups())
	tk.MustExec("SET global tidb_audit_log_max_lifetime = 3")
	tk.MustQuery("SELECT @@global.tidb_audit_log_max_lifetime").Check(testkit.Rows("3"))
	time.Sleep(16 * time.Second)
	tk.MustExec("SET global tidb_audit_log_max_lifetime = 0")
	tk.MustQuery("SELECT @@global.tidb_audit_log_max_lifetime").Check(testkit.Rows("0"))
	files, err := deleteAllAuditLogs(tempDir, "tidb-audit-2", ".log")
	require.NoError(t, err)
	require.Equal(t, 2, len(files), files)

	// Generate 5 log files, all are reserved
	tk.MustExec("SET global tidb_audit_log_reserved_backups = 0")
	tk.MustQuery("SELECT @@global.tidb_audit_log_reserved_backups").Check(testkit.Rows("0"))
	require.Equal(t, 0, globalLogManager(t).GetFileReservedBackups())
	tk.MustExec("SET global tidb_audit_log_max_lifetime = 3")
	tk.MustQuery("SELECT @@global.tidb_audit_log_max_lifetime").Check(testkit.Rows("3"))
	time.Sleep(16 * time.Second)
	tk.MustExec("SET global tidb_audit_log_max_lifetime = 0")
	tk.MustQuery("SELECT @@global.tidb_audit_log_max_lifetime").Check(testkit.Rows("0"))
	files, err = deleteAllAuditLogs(tempDir, "tidb-audit-", ".log")
	require.NoError(t, err)
	require.Equal(t, 5, len(files), files)
}

func TestAuditLogReservedDays(t *testing.T) {
	audit.Register4Test(t)
	tempDir := t.TempDir()
	store := testkit.CreateMockStore(t)
	srv := server.CreateMockServer(t, store)
	defer srv.Close()
	conn := server.CreateMockConn(t, srv)
	defer conn.Close()
	tk := testkit.NewTestKit(t, store)

	require.NoError(t, conn.HandleQuery(context.Background(), fmt.Sprintf("SET global tidb_audit_log = '%s'", filepath.Join(tempDir, audit.DefAuditLogName))))
	require.NoError(t, conn.HandleQuery(context.Background(), "SET global tidb_audit_enabled = 1"))
	tk.MustQuery("SELECT @@global.tidb_audit_log_reserved_days").Check(testkit.Rows("0"))
	require.NoError(t, conn.HandleQuery(context.Background(), "SELECT audit_log_rotate()"))
	files, err := getAllAuditLogs(tempDir, "tidb-audit-", ".log")
	require.NoError(t, err)
	require.Equal(t, 1, len(files), files)
	deprecatedLog := files[0]
	oldLog := filepath.Join(filepath.Dir(deprecatedLog), "tidb-audit-2020-01-01T14-17-15.536.log")
	logutil.BgLogger().Info("TestAuditLogReservedDays", zap.String("deprecatedLog", deprecatedLog), zap.String("oldLog", oldLog))

	time.Sleep(1500 * time.Millisecond)
	require.NoError(t, os.Rename(deprecatedLog, oldLog))
	time.Sleep(1500 * time.Millisecond)
	require.NoError(t, conn.HandleQuery(context.Background(), "SELECT audit_log_rotate()"))
	time.Sleep(1500 * time.Millisecond)
	// Currently 3 logs: tidb-audit.log, oldLog, and another new one
	files, err = getAllAuditLogs(tempDir, "tidb-audit", ".log")
	require.NoError(t, err)
	require.Equal(t, 3, len(files), files)
	require.NotContains(t, files, deprecatedLog, "%v", files)
	require.Contains(t, files, oldLog, "%v", files)

	auditLogWritten := make(chan struct{})
	var closeAuditLogWritten sync.Once
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/extension/enterprise/audit/afterAuditLogEntryWritten", func() {
		closeAuditLogWritten.Do(func() {
			close(auditLogWritten)
		})
	})
	require.NoError(t, conn.HandleQuery(context.Background(), "SET global tidb_audit_log_reserved_days = 1"))
	select {
	case <-auditLogWritten:
	case <-time.After(10 * time.Second):
		require.FailNow(t, "timeout waiting for audit log write")
	}
	tk.MustQuery("SELECT @@global.tidb_audit_log_reserved_days").Check(testkit.Rows("1"))
	require.Equal(t, 1, globalLogManager(t).GetFileReservedDays())
	require.Eventually(t, func() bool {
		files, err = getAllAuditLogs(tempDir, "tidb-audit", ".log")
		return err == nil && len(files) == 2 && !slices.Contains(files, oldLog)
	}, 10*time.Second, 100*time.Millisecond)
	files, err = deleteAllAuditLogs(tempDir, "tidb-audit", ".log")
	// Currently 2 logs: tidb-audit.log, and another new one
	require.NoError(t, err)
	require.Equal(t, 2, len(files), files)
	require.NotContains(t, files, oldLog, "%v", files)
}

func TestAuditLogRedact(t *testing.T) {
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
	tk.MustExec("SET global tidb_audit_enabled = 1")
	tk.MustExec(fmt.Sprintf("SET global tidb_audit_log = '%s'", filepath.Join(tempDir, audit.DefAuditLogName)))

	_, err = deleteAllAuditLogs(tempDir, "tidb-audit", ".log")
	require.NoError(t, err)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c int)")
	tk.MustQuery("select audit_log_create_filter('all', '{}')").Check(testkit.Rows("OK"))
	tk.MustQuery("select content from mysql.audit_log_filters where filter_name = 'all'").Check(testkit.Rows(`{}`))
	tk.MustQuery("select audit_log_create_rule('%@%', 'all')").Check(testkit.Rows("OK"))
	tk.MustQuery("select enabled from mysql.audit_log_filter_rules where user = '%@%' and filter_name = 'all'").Check(testkit.Rows("1"))

	require.NoError(t, conn.HandleQuery(context.Background(), "use test"))
	testcases := []struct {
		sql         string
		redactedSQL string
	}{
		{"insert into t values (1), (2), (3)", ""},
	}

	sctx := stmtctx.NewStmtCtx()
	for i := range testcases {
		sctx.OriginalSQL = testcases[i].sql
		testcases[i].redactedSQL, _ = sctx.SQLDigest()
	}

	for _, testcase := range testcases {
		// default on
		tk.MustExec("SET global tidb_audit_log_redacted = 1")
		tk.MustQuery("select @@global.tidb_audit_log_redacted").Check(testkit.Rows("1"))
		require.NoError(t, conn.HandleQuery(context.Background(), testcase.sql))
		ok, log, err := containsMessage(filepath.Join(tempDir, audit.DefAuditLogName), testcase.redactedSQL)
		require.NoError(t, err)
		require.True(t, ok, log)

		// turn off
		tk.MustExec("SET global tidb_audit_log_redacted = 0")
		tk.MustQuery("select @@global.tidb_audit_log_redacted").Check(testkit.Rows("0"))
		require.NoError(t, conn.HandleQuery(context.Background(), testcase.sql))
		ok, log, err = containsMessage(filepath.Join(tempDir, audit.DefAuditLogName), testcase.sql)
		require.NoError(t, err)
		require.True(t, ok, log)
	}

	// Statements containing password would always be redacted
	tk.MustExec("SET global tidb_audit_log_redacted = 1")
	require.True(t, globalLogManager(t).RedactLog())
	require.NoError(t, conn.HandleQuery(context.Background(), "create user if not exists testuser identified by '1234'"))
	ok, log, err := containsMessage(filepath.Join(tempDir, audit.DefAuditLogName), "create user if not exists `testuser` identified by ?")
	require.NoError(t, err)
	require.True(t, ok, log)
	tk.MustExec("SET global tidb_audit_log_redacted = 0")
	require.False(t, globalLogManager(t).RedactLog())
	require.NoError(t, conn.HandleQuery(context.Background(), "create user if not exists testuser identified by '1234'"))
	ok, log, err = containsMessage(filepath.Join(tempDir, audit.DefAuditLogName), "create user if not exists `testuser` identified by ?")
	require.NoError(t, err)
	require.True(t, ok, log)
	require.NoError(t, conn.HandleQuery(context.Background(), "alter user testuser identified by 'alter_secret_marker'"))
	require.NoError(t, conn.HandleQuery(context.Background(), "grant select on test.* to testuser identified by 'grant_secret_marker'"))
	for _, secret := range []string{"1234", "alter_secret_marker", "grant_secret_marker"} {
		ok, log, err = containsMessage(filepath.Join(tempDir, audit.DefAuditLogName), secret)
		require.NoError(t, err)
		require.False(t, ok, log)
	}

	_, err = deleteAllAuditLogs(tempDir, "tidb-audit", ".log")
	require.NoError(t, err)
}
