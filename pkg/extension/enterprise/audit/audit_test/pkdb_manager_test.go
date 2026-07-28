package audit_test

import (
	"context"
	"fmt"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/extension/enterprise/audit"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestConfigLogFilePath(t *testing.T) {
	t.Chdir(t.TempDir())

	globalCfg := config.GetGlobalConfig()
	oldLogFile := globalCfg.Log.File.Filename
	oldAddr, oldPort := globalCfg.AdvertiseAddress, globalCfg.Port
	t.Cleanup(func() {
		globalCfg.Log.File.Filename = oldLogFile
		globalCfg.AdvertiseAddress = oldAddr
		globalCfg.Port = oldPort
	})

	globalCfg.Log.File.Filename = ""
	globalCfg.AdvertiseAddress = "127.0.0.1"
	globalCfg.Port = 4000

	wd, err := os.Getwd()
	require.NoError(t, err)
	require.True(t, path.IsAbs(wd))
	require.False(t, strings.HasSuffix(wd, "/"))

	m, err := audit.NewAuditLogManager()
	require.NoError(t, err)
	defer m.Stop()

	checkLogPath := func(expected string) {
		require.Equal(t, expected, m.GetLogPath())
	}

	absDir := t.TempDir()
	instanceLogDir := t.TempDir()
	require.Equal(t, path.Join(wd, audit.DefAuditLogName), m.GetLogPath())
	for _, c := range []struct {
		globalCfgFileName string
		setLogConfigPath  string
		format            string
		expected          string
	}{
		{
			setLogConfigPath: "",
			format:           audit.LogFormatText,
			expected:         path.Join(wd, audit.DefAuditLogName),
		},
		{
			setLogConfigPath: "audit1.log",
			format:           audit.LogFormatText,
			expected:         path.Join(wd, "audit1.log"),
		},
		{
			setLogConfigPath: "audit-%e.log",
			format:           audit.LogFormatText,
			expected:         path.Join(wd, "audit-127-0-0-1-4000.log"),
		},
		{
			setLogConfigPath: "logs/audit2.log",
			format:           audit.LogFormatJSON,
			expected:         path.Join(wd, "logs/logs/audit2.log.json"),
		},
		{
			setLogConfigPath: path.Join(absDir, "audit3.log"),
			format:           audit.LogFormatText,
			expected:         path.Join(absDir, "audit3.log"),
		},
		{
			globalCfgFileName: path.Join(instanceLogDir, "tidb.log"),
			setLogConfigPath:  audit.DefAuditLogName,
			format:            audit.LogFormatText,
			expected:          "",
		},
	} {
		t.Run(fmt.Sprintf("%s/%s", c.setLogConfigPath, c.format), func(t *testing.T) {
			if c.globalCfgFileName != "" {
				globalCfg.Log.File.Filename = c.globalCfgFileName
				c.expected = path.Join(path.Dir(c.globalCfgFileName), audit.DefAuditLogName)
			} else {
				globalCfg.Log.File.Filename = ""
			}
			require.NoError(t, m.SetLogFormat(c.format))
			require.NoError(t, m.SetLogConfigPath(c.setLogConfigPath))
			checkLogPath(c.expected)
		})
	}
}

type mockBootstrapContext struct {
	context.Context
	sessPool extension.SessionPool
}

func (c *mockBootstrapContext) ExecuteSQL(ctx context.Context, sql string) (ret []chunk.Row, err error) {
	r, err := c.sessPool.Get()
	if err != nil {
		return nil, err
	}
	defer c.sessPool.Put(r)

	sctx, ok := r.(sessionctx.Context)
	if !ok {
		return nil, errors.Errorf("type '%T' cannot be casted to 'sessionctx.Context'", sctx)
	}

	executor := sctx.GetSQLExecutor()
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnBootstrap)
	rs, err := executor.ExecuteInternal(ctx, sql)
	if err != nil {
		return nil, err
	}

	if rs == nil {
		return nil, nil
	}

	defer func() {
		closeErr := rs.Close()
		if err == nil {
			err = closeErr
		}
	}()

	return sqlexec.DrainRecordSet(ctx, rs, 8)
}

func (c *mockBootstrapContext) EtcdClient() *clientv3.Client {
	return nil
}

func (c *mockBootstrapContext) SessionPool() extension.SessionPool {
	return c.sessPool
}

func filterMatched(bundle *audit.LogFilterRuleBundle, entry *audit.LogEntry) bool {
	return bundle != nil && bundle.Filter(entry) == entry
}

func TestManagerBootstrap(t *testing.T) {
	require.True(t, intest.InTest, "requires testkit with -tags=intest")
	store, do := testkit.CreateMockStoreAndDomain(t)
	do.SysSessionPool()

	m, err := audit.NewAuditLogManager()
	require.NoError(t, err)

	tk := testkit.NewTestKit(t, store)

	ctx := &mockBootstrapContext{
		Context:  context.Background(),
		sessPool: do.SysSessionPool(),
	}

	require.NoError(t, m.Bootstrap(ctx))
	tk.MustQuery("select * from mysql.audit_log_filter_rules").Check(testkit.Rows())
	tk.MustQuery("select * from mysql.audit_log_filters").Check(testkit.Rows())
	m.Stop()

	filter1 := "{}"
	filter2 := `{"filter":[{"class":["QUERY"]}]}`
	tk.MustExec("insert into mysql.audit_log_filters values('f1', ?), ('f2', ?)", filter1, filter2)
	tk.MustExec("insert into mysql.audit_log_filter_rules values('u1@%', 'f1', 0), ('u2@127.0.0.1', 'f2', 1)")
	m, err = audit.NewAuditLogManager()
	require.NoError(t, err)
	require.NoError(t, m.Bootstrap(ctx))
	defer m.Stop()

	require.False(t, filterMatched(m.GetFilter(), &audit.LogEntry{
		User:    "u1",
		Host:    "127.0.0.1",
		Classes: []audit.EventClass{audit.ClassQuery},
	}))
	require.True(t, filterMatched(m.GetFilter(), &audit.LogEntry{
		User:    "u2",
		Host:    "127.0.0.1",
		Classes: []audit.EventClass{audit.ClassQuery},
	}))
	require.False(t, filterMatched(m.GetFilter(), &audit.LogEntry{
		User:    "u2",
		Host:    "127.0.0.1",
		Classes: []audit.EventClass{audit.ClassDDL},
	}))
}

func TestManagerReloadFilter(t *testing.T) {
	require.True(t, intest.InTest, "requires testkit with -tags=intest")
	store, do := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	m, err := audit.NewAuditLogManager()
	require.NoError(t, err)
	require.NoError(t, m.Bootstrap(&mockBootstrapContext{
		Context:  context.Background(),
		sessPool: do.SysSessionPool(),
	}))
	defer m.Stop()

	tk.MustQuery("select * from mysql.audit_log_filters").Check(testkit.Rows())
	tk.MustQuery("select * from mysql.audit_log_filter_rules").Check(testkit.Rows())
	require.False(t, filterMatched(m.GetFilter(), &audit.LogEntry{
		User:    "root",
		Host:    "127.0.0.1",
		Classes: []audit.EventClass{audit.ClassQuery},
	}))

	tk.MustExec(
		"insert into mysql.audit_log_filters values('f1', '{}'), ('f2', ?), ('f3', ?), ('f4', '{}')",
		`{"filter":[{"class":["QUERY"]}]}`, `{"filter":[{"table":["test.*"]},{"class":[]}]}`,
	)
	tk.MustExec(
		"insert into mysql.audit_log_filter_rules values" +
			"('u1@%', 'f1', 0), " +
			"('u2@localhost', 'f2', 1)," +
			"('root@%', 'f1', 1)," +
			"('u1@%', 'f3', 1)",
	)
	require.NoError(t, m.ReloadFilter())
	filter := m.GetFilter()
	require.True(t, filterMatched(filter, &audit.LogEntry{
		User:    "root",
		Host:    "127.0.0.1",
		Classes: []audit.EventClass{audit.ClassDDL},
	}))
	require.True(t, filterMatched(filter, &audit.LogEntry{
		User:    "u2",
		Host:    "localhost",
		Classes: []audit.EventClass{audit.ClassQuery},
	}))
	require.False(t, filterMatched(filter, &audit.LogEntry{
		User:    "u1",
		Host:    "127.0.0.1",
		Classes: []audit.EventClass{audit.ClassQuery},
	}))
	require.True(t, filterMatched(filter, &audit.LogEntry{
		User: "u1",
		Host: "127.0.0.1",
		Tables: []stmtctx.TableEntry{
			{DB: "test", Table: "t"},
		},
	}))

	require.NoError(t, m.ReloadFilter())
	require.Same(t, filter, m.GetFilter())

	tk.MustExec(
		"update mysql.audit_log_filters set content = ? where filter_name = 'f2'",
		`{"filter":[{"class":["QUERY", "QUERY_DDL"]}]}`,
	)
	require.NoError(t, m.ReloadFilter())
	require.NotSame(t, filter, m.GetFilter())
	filter = m.GetFilter()
	require.True(t, filterMatched(filter, &audit.LogEntry{
		User:    "u2",
		Host:    "localhost",
		Classes: []audit.EventClass{audit.ClassDDL},
	}))

	tk.MustExec("update mysql.audit_log_filter_rules set enabled=1 where user = 'u1@%' and filter_name = 'f1'")
	require.NoError(t, m.ReloadFilter())
	require.NotSame(t, filter, m.GetFilter())
	filter = m.GetFilter()
	require.True(t, filterMatched(filter, &audit.LogEntry{
		User:    "u1",
		Host:    "127.0.0.1",
		Classes: []audit.EventClass{audit.ClassSelect},
	}))

	tk.MustExec("update mysql.audit_log_filter_rules set filter_name='f2' where user = 'u1@%' and filter_name = 'f1'")
	require.NoError(t, m.ReloadFilter())
	require.NotSame(t, filter, m.GetFilter())
	filter = m.GetFilter()
	require.True(t, filterMatched(filter, &audit.LogEntry{
		User:    "u1",
		Host:    "127.0.0.1",
		Classes: []audit.EventClass{audit.ClassDDL},
	}))
}
