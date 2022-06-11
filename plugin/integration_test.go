// Copyright 2021 PingCAP, Inc.
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

package plugin_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/plugin"
	"github.com/pingcap/tidb/server"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

// Audit tests cannot run in parallel.
func TestAuditLogNormal(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	sv := server.CreateMockServer(t, store)
	defer sv.Close()
	conn := server.CreateMockConn(t, store, sv)
	defer conn.Close()
	session.DisableStats4Test()
	session.SetSchemaLease(0)

	type normalTest struct {
		sql      string
		text     string
		rows     uint64
		stmtType string
		dbs      string
		tables   string
		cmd      string
		event    plugin.GeneralEvent
		resCnt   int
	}

	tests := []normalTest{
		{
			sql:      "CREATE DATABASE mynewdatabase",
			stmtType: "CreateDatabase",
			dbs:      "mynewdatabase",
		},
		{
			sql:      "CREATE TABLE t1 (a INT NOT NULL)",
			stmtType: "CreateTable",
			dbs:      "test",
			tables:   "t1",
		},
		{
			sql:      "CREATE TABLE t2 LIKE t1",
			stmtType: "CreateTable",
			dbs:      "test,test",
			tables:   "t2,t1",
		},
		{
			sql:      "CREATE INDEX a ON t1 (a)",
			stmtType: "CreateIndex",
			dbs:      "test",
			tables:   "t1",
		},
		{
			sql:      "CREATE SEQUENCE seq",
			stmtType: "other",
			dbs:      "test",
			tables:   "seq",
		},
		{
			sql:      " create temporary table t3 (a int)",
			stmtType: "CreateTable",
			dbs:      "test",
			tables:   "t3",
		},
		{
			sql:      "create global temporary table t4 (a int) on commit delete rows",
			stmtType: "CreateTable",
			dbs:      "test",
			tables:   "t4",
		},
		{
			sql:      "CREATE VIEW v1 AS SELECT * FROM t1 WHERE  a> 2",
			stmtType: "CreateView",
			dbs:      "test,test",
			tables:   "t1,v1",
		},
		{
			sql:      "USE test",
			stmtType: "Use",
		},
		{
			sql:      "DROP DATABASE mynewdatabase",
			stmtType: "DropDatabase",
			dbs:      "mynewdatabase",
		},
		{
			sql:      "SHOW CREATE SEQUENCE seq",
			stmtType: "Show",
			dbs:      "test",
			tables:   "seq",
		},
		{
			sql:      "DROP SEQUENCE seq",
			stmtType: "other",
			dbs:      "test",
			tables:   "seq",
		},
		{
			sql:      "DROP TABLE t4",
			stmtType: "DropTable",
			dbs:      "test",
			tables:   "t4",
		},
		{
			sql:      "DROP VIEW v1",
			stmtType: "DropView",
			dbs:      "test",
			tables:   "v1",
		},
		{
			sql:      "ALTER TABLE t1 ADD COLUMN c1 INT NOT NULL",
			stmtType: "AlterTable",
			dbs:      "test",
			tables:   "t1",
		},
		{
			sql:      "ALTER TABLE t1 MODIFY c1 BIGINT",
			stmtType: "AlterTable",
			dbs:      "test",
			tables:   "t1",
		},
		{
			sql:      "ALTER TABLE t1 ADD INDEX (c1)",
			stmtType: "AlterTable",
			dbs:      "test",
			tables:   "t1",
		},
		{
			sql:      "ALTER TABLE t1 ALTER INDEX c1 INVISIBLE",
			stmtType: "AlterTable",
			dbs:      "test",
			tables:   "t1",
		},
		{
			sql:      "ALTER TABLE t1 RENAME INDEX c1 TO c2",
			stmtType: "AlterTable",
			dbs:      "test",
			tables:   "t1",
		},
		{
			sql:      "ALTER TABLE t1 DROP INDEX c2",
			stmtType: "AlterTable",
			dbs:      "test",
			tables:   "t1",
		},
		{
			sql:      "ALTER TABLE t1 CHANGE c1 c2 INT",
			stmtType: "AlterTable",
			dbs:      "test",
			tables:   "t1",
		},
		{
			sql:      "ALTER TABLE t1 DROP COLUMN c2",
			stmtType: "AlterTable",
			dbs:      "test",
			tables:   "t1",
		},
		{
			sql:      "CREATE SESSION BINDING FOR SELECT * FROM t1 WHERE a = 123 USING SELECT * FROM t1 IGNORE INDEX (a) WHERE a = 123",
			stmtType: "CreateBinding",
		},
		{
			sql:      "DROP SESSION BINDING FOR SELECT * FROM t1 WHERE a = 123",
			stmtType: "DropBinding",
		},
		// {
		//	sql: "LOAD STATS '/tmp/stats.json'",
		//	stmtType: "other",
		// },
		// {
		//	sql: "DROP STATS t",
		//	stmtType: "other",
		// },
		{
			sql:      "RENAME TABLE t2 TO t5",
			stmtType: "other",
			dbs:      "test,test",
			tables:   "t2,t5",
		},
		{
			sql:      "TRUNCATE t1",
			stmtType: "TruncateTable",
			dbs:      "test",
			tables:   "t1",
		},
		// {
		//	sql: "FLASHBACK TABLE t TO t1",
		//	stmtType: "other",
		//	dbs: "test",
		//	tables: "t1",
		// },
		// {
		//	sql: "RECOVER TABLE t1",
		//	stmtType: "other",
		//	dbs: "test",
		//	tables: "t1,t2",
		// },
		{
			sql:      "ALTER DATABASE test DEFAULT CHARACTER SET = utf8mb4",
			stmtType: "other",
			dbs:      "test",
		},
		{
			sql:      "ADMIN RELOAD opt_rule_blacklist",
			stmtType: "other",
		},
		// {
		//	sql: "ADMIN PLUGINS ENABLE audit_test",
		//	stmtType: "other",
		// },
		{
			sql:      "ADMIN FLUSH bindings",
			stmtType: "other",
		},
		// {
		//	sql: "ADMIN REPAIR TABLE t1 CREATE TABLE (id int)",
		//	stmtType: "other",
		//	dbs: "test",
		//	tables: "t1",
		// },
		{
			sql:      "ADMIN SHOW SLOW RECENT 10",
			stmtType: "other",
		},
		{
			sql:      "ADMIN SHOW DDL JOBS",
			stmtType: "other",
		},
		// {
		//	sql: "ADMIN CANCEL DDL JOBS 1",
		//	stmtType: "other",
		// },
		{
			sql:      "ADMIN CHECKSUM TABLE t1",
			stmtType: "other",
			// dbs: "test",
			// tables: "t1",
		},
		{
			sql:      "ADMIN CHECK TABLE t1",
			stmtType: "other",
			// dbs: "test",
			// tables: "t1",
		},
		{
			sql:      "ADMIN CHECK INDEX t1 a",
			stmtType: "other",
			// dbs: "test",
			// tables: "t1",
		},
		{
			sql:      "CREATE USER 'newuser' IDENTIFIED BY 'newuserpassword'",
			stmtType: "CreateUser",
		},
		{
			sql:      "ALTER USER 'newuser' IDENTIFIED BY 'newnewpassword'",
			stmtType: "other",
		},
		{
			sql:      "CREATE ROLE analyticsteam",
			stmtType: "CreateUser",
		},
		{
			sql:      "GRANT SELECT ON test.* TO analyticsteam",
			stmtType: "Grant",
			dbs:      "test",
		},
		{
			sql:      "GRANT analyticsteam TO 'newuser'",
			stmtType: "other",
		},
		{
			sql:      "SET DEFAULT ROLE analyticsteam TO newuser;",
			stmtType: "other",
		},
		{
			sql:      "REVOKE SELECT ON test.* FROM 'analyticsteam'",
			stmtType: "Revoke",
			dbs:      "test",
		},
		{
			sql:      "DROP ROLE analyticsteam",
			stmtType: "other",
		},
		{
			sql:      "FLUSH PRIVILEGES",
			stmtType: "other",
		},
		{
			sql:      "SET PASSWORD FOR 'newuser' = 'test'",
			stmtType: "Set",
		},
		// {
		//	sql: "SET ROLE ALL",
		//	stmtType: "other",
		// },
		{
			sql:      "DROP USER 'newuser'",
			stmtType: "other",
		},
		{
			sql:      "analyze table t1",
			stmtType: "AnalyzeTable",
			dbs:      "test",
			tables:   "t1",
		},
		{
			sql:      "SPLIT TABLE t1 BETWEEN (0) AND (1000000000) REGIONS 16",
			stmtType: "other",
			// dbs: "test",
			// tables: "t1",
		},
		// {
		//	sql: "BACKUP DATABASE `test` TO '.'",
		//	stmtType: "other",
		//	dbs: "test",
		// },
		// {
		//	sql: "RESTORE DATABASE * FROM '.'",
		//	stmtType: "other",
		// },
		// {
		//	sql: "CHANGE DRAINER TO NODE_STATE ='paused' FOR NODE_ID 'drainer1'",
		//	stmtType: "other",
		// },
		// {
		//	sql: "CHANGE PUMP TO NODE_STATE ='paused' FOR NODE_ID 'pump1'",
		//	stmtType: "other",
		// },
		{
			sql:      "BEGIN",
			stmtType: "Begin",
		},
		{
			sql:      "ROLLBACK",
			stmtType: "Rollback",
		},
		{
			sql:      "START TRANSACTION",
			stmtType: "Begin",
		},
		{
			sql:      "COMMIT",
			stmtType: "Commit",
		},
		// {
		//	sql: "SHOW DRAINER STATUS",
		//	stmtType: "Show",
		// },
		// {
		//	sql: "SHOW PUMP STATUS",
		//	stmtType: "Show",
		// },
		// {
		//	sql: "SHOW GRANTS",
		//	stmtType: "Show",
		// },
		{
			sql:      "SHOW PROCESSLIST",
			stmtType: "Show",
		},
		// {
		//	sql: "SHOW BACKUPS",
		//	stmtType: "Show",
		// },
		// {
		//	sql: "SHOW RESTORES",
		//	stmtType: "Show",
		// },
		{
			sql:      "show analyze status",
			stmtType: "Show",
		},
		{
			sql:      "SHOW SESSION BINDINGS",
			stmtType: "Show",
		},
		{
			sql:      "SHOW BUILTINS",
			stmtType: "Show",
		},
		{
			sql:      "SHOW CHARACTER SET",
			stmtType: "Show",
		},
		{
			sql:      "SHOW COLLATION",
			stmtType: "Show",
		},
		{
			sql:      "show columns from t1",
			stmtType: "Show",
		},
		{
			sql:      "show fields from t1",
			stmtType: "Show",
		},
		// {
		//	sql: "SHOW CONFIG",
		//	stmtType: "Show",
		// },
		{
			sql:      "SHOW CREATE TABLE t1",
			stmtType: "Show",
			dbs:      "test",
			tables:   "t1",
		},
		{
			sql:      "SHOW CREATE USER 'root'",
			stmtType: "Show",
		},
		{
			sql:      "SHOW DATABASES",
			stmtType: "Show",
		},
		{
			sql:      "SHOW ENGINES",
			stmtType: "Show",
		},
		{
			sql:      "SHOW ERRORS",
			stmtType: "Show",
		},
		{
			sql:      "SHOW INDEXES FROM t1",
			stmtType: "Show",
		},
		{
			sql:      "SHOW MASTER STATUS",
			stmtType: "Show",
		},
		{
			sql:      "SHOW PLUGINS",
			stmtType: "Show",
		},
		{
			sql:      "show privileges",
			stmtType: "Show",
		},
		{
			sql:      "SHOW PROFILES",
			stmtType: "Show",
		},
		// {
		//	sql: "SHOW PUMP STATUS",
		//	stmtType: "Show",
		// },
		{
			sql:      "SHOW SCHEMAS",
			stmtType: "Show",
		},
		{
			sql:      "SHOW STATS_HEALTHY",
			stmtType: "Show",
			dbs:      "mysql",
		},
		{
			sql:      "show stats_histograms",
			stmtType: "Show",
			dbs:      "mysql",
		},
		{
			sql:      "show stats_meta",
			stmtType: "Show",
			dbs:      "mysql",
		},
		{
			sql:      "show status",
			stmtType: "Show",
		},
		{
			sql:      "show table t1 next_row_id",
			stmtType: "Show",
			dbs:      "test",
			tables:   "t1",
		},
		{
			sql:      "show table t1 regions",
			stmtType: "Show",
		},
		{
			sql:      "SHOW TABLE STATUS LIKE 't1'",
			stmtType: "Show",
			resCnt:   3, // Start + SHOW TABLE + Internal SELECT .. FROM IS.TABLES in current session
		},
		{
			sql:      "SHOW TABLES",
			stmtType: "Show",
		},
		{
			sql:      "SHOW VARIABLES",
			stmtType: "Show",
		},
		{
			sql:      "SHOW WARNINGS",
			stmtType: "Show",
		},
		{
			sql:      "SET @number = 5",
			stmtType: "Set",
		},
		{
			sql:      "SET NAMES utf8",
			stmtType: "Set",
		},
		{
			sql:      "SET CHARACTER SET utf8mb4",
			stmtType: "Set",
		},
		{
			sql:      "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED",
			stmtType: "Set",
		},
		{
			sql:      "SET SESSION sql_mode = 'STRICT_TRANS_TABLES,NO_AUTO_CREATE_USER'",
			stmtType: "Set",
		},
		{
			sql:      "PREPARE mystmt FROM 'SELECT ? as num FROM DUAL'",
			stmtType: "Prepare",
		},
		{
			sql:      "EXECUTE mystmt USING @number",
			text:     "SELECT ? as num FROM DUAL",
			stmtType: "Select",
		},
		{
			sql:      "DEALLOCATE PREPARE mystmt",
			stmtType: "Deallocate",
		},
		{
			sql:      "INSERT INTO t1 VALUES (1), (2)",
			stmtType: "Insert",
			dbs:      "test",
			tables:   "t1",
			rows:     2,
		},
		{
			sql:      "DELETE FROM t1 WHERE a = 2",
			stmtType: "Delete",
			dbs:      "test",
			tables:   "t1",
			rows:     1,
		},
		{
			sql:      "REPLACE INTO t1 VALUES(3)",
			stmtType: "Replace",
			dbs:      "test",
			tables:   "t1",
			rows:     1,
		},
		{
			sql:      "UPDATE t1 SET a=5 WHERE a=1",
			stmtType: "Update",
			dbs:      "test",
			tables:   "t1",
			rows:     1,
		},
		{
			sql:      "DO 1",
			stmtType: "other",
		},
		// {
		//	sql: "LOAD DATA LOCAL INFILE 'data.csv' INTO TABLE t1 FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\r\n' IGNORE 1 LINES (id)",
		//	stmtType: "LoadData",
		//	dbs: "test",
		//	tables: "t1",
		// },
		{
			sql:      "SELECT * FROM t1",
			stmtType: "Select",
			dbs:      "test",
			tables:   "t1",
		},
		{
			sql:      "SELECT 1",
			stmtType: "Select",
		},
		{
			sql:      "TABLE t1",
			stmtType: "Select",
			dbs:      "test",
			tables:   "t1",
		},
		{
			sql:      "EXPLAIN ANALYZE SELECT * FROM t1 WHERE a = 1",
			stmtType: "ExplainAnalyzeSQL",
			// dbs: "test",
			// tables: "t1",
		},
		{
			sql:      "EXPLAIN SELECT * FROM t1",
			stmtType: "ExplainSQL",
			// dbs: "test",
			// tables: "t1",
		},
		{
			sql:      "EXPLAIN SELECT * FROM t1 WHERE a = 1",
			stmtType: "ExplainSQL",
			// dbs: "test",
			// tables: "t1",
		},
		{
			sql:      "DESC SELECT * FROM t1 WHERE a = 1",
			stmtType: "ExplainSQL",
			// dbs: "test",
			// tables: "t1",
		},
		{
			sql:      "DESCRIBE SELECT * FROM t1 WHERE a = 1",
			stmtType: "ExplainSQL",
			// dbs: "test",
			// tables: "t1",
		},
		{
			sql:      "trace format='row' select * from t1",
			stmtType: "Trace",
			// dbs: "test",
			// tables: "t1",
		},
		{
			sql:      "flush status",
			stmtType: "other",
		},
		{
			sql:      "FLUSH TABLES",
			stmtType: "other",
		},
		// {
		//	sql: "KILL TIDB 2",
		//	stmtType: "other",
		// },
		// {
		//	sql: "SHUTDOWN",
		//	stmtType: "Shutdow",
		// },
		// {
		//	sql: "ALTER INSTANCE RELOAD TLS",
		//	stmtType: "other",
		// },
	}

	testResults := make([]normalTest, 0)
	dbNames := make([]string, 0)
	tableNames := make([]string, 0)
	onGeneralEvent := func(ctx context.Context, sctx *variable.SessionVars, event plugin.GeneralEvent, cmd string) {
		dbNames = dbNames[:0]
		tableNames = tableNames[:0]
		for _, value := range sctx.StmtCtx.Tables {
			dbNames = append(dbNames, value.DB)
			tableNames = append(tableNames, value.Table)
		}
		audit := normalTest{
			text:     sctx.StmtCtx.OriginalSQL,
			rows:     sctx.StmtCtx.AffectedRows(),
			stmtType: sctx.StmtCtx.StmtType,
			dbs:      strings.Join(dbNames, ","),
			tables:   strings.Join(tableNames, ","),
			cmd:      cmd,
			event:    event,
		}
		testResults = append(testResults, audit)
	}
	loadPlugin(t, onGeneralEvent)
	defer plugin.Shutdown(context.Background())

	require.NoError(t, conn.HandleQuery(context.Background(), "use test"))
	for _, test := range tests {
		testResults = testResults[:0]
		errMsg := fmt.Sprintf("statement: %s", test.sql)
		query := append([]byte{mysql.ComQuery}, []byte(test.sql)...)
		err := conn.Dispatch(context.Background(), query)
		require.NoError(t, err, errMsg)
		resultCount := test.resCnt
		if resultCount == 0 {
			resultCount = 2
		}
		require.Equal(t, resultCount, len(testResults), errMsg)

		result := testResults[0]
		require.Equal(t, "Query", result.cmd, errMsg)
		require.Equal(t, plugin.Starting, result.event, errMsg)

		result = testResults[resultCount-1]
		require.Equal(t, "Query", result.cmd, errMsg)
		if test.text == "" {
			require.Equal(t, test.sql, result.text, errMsg)
		} else {
			require.Equal(t, test.text, result.text, errMsg)
		}
		require.Equal(t, test.rows, result.rows, errMsg)
		require.Equal(t, test.stmtType, result.stmtType, errMsg)
		require.Equal(t, test.dbs, result.dbs, errMsg)
		require.Equal(t, test.tables, result.tables, errMsg)
		require.Equal(t, "Query", result.cmd, errMsg)
		require.Equal(t, plugin.Completed, result.event, errMsg)
		for i := 1; i < resultCount-1; i++ {
			result = testResults[i]
			require.Equal(t, "Query", result.cmd, errMsg)
			require.Equal(t, plugin.Completed, result.event, errMsg)
		}
	}
}

func loadPlugin(t *testing.T, onGeneralEvent func(context.Context, *variable.SessionVars, plugin.GeneralEvent, string)) {
	ctx := context.Background()
	pluginName := "audit_test"
	pluginVersion := uint16(1)
	pluginSign := pluginName + "-" + strconv.Itoa(int(pluginVersion))

	cfg := plugin.Config{
		Plugins:    []string{pluginSign},
		PluginDir:  "",
		EnvVersion: map[string]uint16{"go": 1112},
	}

	validate := func(ctx context.Context, manifest *plugin.Manifest) error {
		return nil
	}
	onInit := func(ctx context.Context, manifest *plugin.Manifest) error {
		return nil
	}
	onShutdown := func(ctx context.Context, manifest *plugin.Manifest) error {
		return nil
	}
	onConnectionEvent := func(ctx context.Context, event plugin.ConnectionEvent, info *variable.ConnectionInfo) error {
		return nil
	}

	// setup load test hook.
	loadOne := func(p *plugin.Plugin, dir string, pluginID plugin.ID) (manifest func() *plugin.Manifest, err error) {
		return func() *plugin.Manifest {
			m := &plugin.AuditManifest{
				Manifest: plugin.Manifest{
					Kind:       plugin.Audit,
					Name:       pluginName,
					Version:    pluginVersion,
					OnInit:     onInit,
					OnShutdown: onShutdown,
					Validate:   validate,
				},
				OnGeneralEvent:    onGeneralEvent,
				OnConnectionEvent: onConnectionEvent,
			}
			return plugin.ExportManifest(m)
		}, nil
	}
	plugin.SetTestHook(loadOne)

	// trigger load.
	err := plugin.Load(ctx, cfg)
	require.NoErrorf(t, err, "load plugin [%s] fail, error [%s]\n", pluginSign, err)

	err = plugin.Init(ctx, cfg)
	require.NoErrorf(t, err, "init plugin [%s] fail, error [%s]\n", pluginSign, err)
}
