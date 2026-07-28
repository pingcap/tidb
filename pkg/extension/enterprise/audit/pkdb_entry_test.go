package audit

import (
	"fmt"
	"slices"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func checkContainField(t *testing.T, fields []zap.Field, field zap.Field) {
	require.True(t, slices.ContainsFunc(fields, func(f zap.Field) bool {
		if f.Key == field.Key {
			require.Equal(t, field, f)
			return true
		}
		return false
	}), "field %q not found", field.Key)
}

func checkNotContainField(t *testing.T, fields []zap.Field, field string) {
	require.False(t, slices.ContainsFunc(fields, func(f zap.Field) bool {
		return f.Key == field
	}), "field %q found", field)
}

func checkLogCommonFields(t *testing.T, entry *LogEntry, fields []zap.Field) {
	classes := make([]string, 0, len(entry.Classes))
	for _, c := range entry.Classes {
		classes = append(classes, c.String())
	}
	require.NotEmpty(t, classes)

	roles := make([]string, 0, len(entry.Roles))
	for _, r := range entry.Roles {
		roles = append(roles, r.String())
	}
	if len(roles) == 0 {
		roles = nil
	}

	tables := make([]string, 0, len(entry.Tables))
	for _, tb := range entry.Tables {
		tables = append(tables, fmt.Sprintf("`%s`.`%s`", tb.DB, tb.Table))
	}
	if len(tables) == 0 {
		tables = nil
	}

	checkContainField(t, fields, zap.String("USER", entry.User))
	checkContainField(t, fields, zap.String("CONNECTION_ID", fmt.Sprintf("%d", entry.ConnID)))
	checkContainField(t, fields, zap.String("SESSION_ALIAS", entry.SessionAlias))
	checkContainField(t, fields, zap.Strings("EVENT", classes))
	checkContainField(t, fields, zap.Strings("ROLES", roles))
	checkContainField(t, fields, zap.Strings("TABLES", tables))
	if entry.Err != nil {
		checkContainField(t, fields, zap.Int("STATUS_CODE", 0))
		checkContainField(t, fields, zap.String("REASON", entry.Err.Error()))
	} else {
		checkContainField(t, fields, zap.Int("STATUS_CODE", 1))
	}

	require.True(t, slices.ContainsFunc(fields, func(f zap.Field) bool {
		if f.Key == "ID" {
			require.NotEmpty(t, f.String)
			return true
		}
		return false
	}), "field %q not found", "ID")
}

func TestConnEventEntry(t *testing.T) {
	cases := []struct {
		tp    extension.ConnEventTp
		class []EventClass
	}{
		{
			tp:    extension.ConnConnected,
			class: strClassesToEnums(t, []string{"CONNECTION"}),
		},
		{
			tp:    extension.ConnHandshakeAccepted,
			class: strClassesToEnums(t, []string{"CONNECTION", "CONNECT"}),
		},
		{
			tp:    extension.ConnHandshakeRejected,
			class: strClassesToEnums(t, []string{"CONNECTION", "CONNECT"}),
		},
		{
			tp:    extension.ConnReset,
			class: strClassesToEnums(t, []string{"CONNECTION", "CHANGE_USER"}),
		},
		{
			tp:    extension.ConnDisconnected,
			class: strClassesToEnums(t, []string{"CONNECTION", "DISCONNECT"}),
		},
	}

	g := newEntryIDGenerator()
	for _, c := range cases {
		for _, redact := range []bool{true, false} {
			for _, err := range []error{nil, errors.New("mockErr1")} {
				isErr := err != nil
				t.Run(fmt.Sprintf("%v-redact:%v-err:%v", c.tp, redact, isErr), func(t *testing.T) {
					connInfo := &extension.ConnEventInfo{
						ConnectionInfo: &variable.ConnectionInfo{
							ConnectionType: "TCP",
							User:           "u1",
							Host:           "127.0.0.1",
							ConnectionID:   123,
							ClientIP:       "192.168.1.1",
							ClientPort:     "8989",
							ServerID:       567,
							ServerIP:       "192.168.2.1",
							ServerPort:     4123,
							PID:            987,
							ServerVersion:  "v7.1.0",
							SSLVersion:     "TLSv1.3",
							AuthMethod:     "plugin1",
							DB:             "db1",
							Attributes: map[string]string{
								"attr1": "val1",
								"attr2": "val2",
							},
						},
						SessionAlias: "alias123",
						ActiveRoles: []*auth.RoleIdentity{
							{Username: "u1", Hostname: "h1"},
							{Username: "u2", Hostname: "h2"},
						},
						Error: err,
					}
					entry := newConnEventEntry(c.tp, connInfo)
					if c.class == nil {
						require.Nil(t, entry)
					} else {
						fields := entry.Fields(g, redact)
						require.Equal(t, "u1", entry.User)
						require.Equal(t, "127.0.0.1", entry.Host)
						require.Equal(t, connInfo.ActiveRoles, entry.Roles)
						require.Equal(t, uint64(123), entry.ConnID)
						require.Equal(t, "alias123", entry.SessionAlias)
						require.Nil(t, entry.Tables)
						if err == nil {
							require.Nil(t, entry.Err)
						} else {
							require.Equal(t, err.Error(), entry.Err.Error())
						}
						checkLogCommonFields(t, entry, fields)
						checkContainField(t, fields, zap.String("CONNECTION_TYPE", "TCP"))
						checkContainField(t, fields, zap.String("PID", "987"))
						checkContainField(t, fields, zap.String("SERVER_VERSION", "v7.1.0"))
						checkContainField(t, fields, zap.String("SSL_VERSION", "TLSv1.3"))
						checkContainField(t, fields, zap.String("HOST_IP", "192.168.2.1"))
						checkContainField(t, fields, zap.String("HOST_PORT", "4123"))
						checkContainField(t, fields, zap.String("CLIENT_IP", "192.168.1.1"))
						checkContainField(t, fields, zap.String("CLIENT_PORT", "8989"))
						checkContainField(t, fields, zap.String("AUTH_METHOD", "plugin1"))
						checkContainField(t, fields, zap.Any("CONN_ATTRS", connInfo.Attributes))
						if c.tp != extension.ConnDisconnected {
							checkContainField(t, fields, zap.String("CURRENT_DB", "db1"))
						}
					}
				})
			}
		}
	}
}

type mockStmtEventInfo struct {
	extension.StmtEventInfo
	user            *auth.UserIdentity
	activeRoles     []*auth.RoleIdentity
	currentDB       string
	connInfo        *variable.ConnectionInfo
	sessionAlias    string
	isExecute       bool
	stmtNode        ast.StmtNode
	prepareParams   []types.Datum
	originalSQLText string
	normalizedSQL   string
	affectedRows    uint64
	tables          []stmtctx.TableEntry
	err             error
}

// User returns the user of the session
func (i *mockStmtEventInfo) User() *auth.UserIdentity {
	return i.user
}

// ActiveRoles returns the active roles of the user
func (i *mockStmtEventInfo) ActiveRoles() []*auth.RoleIdentity {
	return i.activeRoles
}

// CurrentDB returns the current database
func (i *mockStmtEventInfo) CurrentDB() string {
	return i.currentDB
}

// ConnectionInfo returns the connection info of the current session
func (i *mockStmtEventInfo) ConnectionInfo() *variable.ConnectionInfo {
	return i.connInfo
}

// SessionAlias returns the session alias value set by user
func (i *mockStmtEventInfo) SessionAlias() string {
	return i.sessionAlias
}

// StmtNode returns the parsed ast of the statement
// When parse error, this method will return a nil value
func (i *mockStmtEventInfo) StmtNode() ast.StmtNode {
	if i.isExecute {
		return &ast.ExecuteStmt{}
	}
	return i.stmtNode
}

// ExecuteStmtNode will return the `ast.ExecuteStmt` node when the current statement is EXECUTE,
// otherwise a nil value will be returned
func (i *mockStmtEventInfo) ExecuteStmtNode() *ast.ExecuteStmt {
	if i.isExecute {
		return &ast.ExecuteStmt{}
	}
	return nil
}

// ExecutePreparedStmt will return the prepared stmt node for the EXECUTE statement.
// If the current statement is not EXECUTE or prepared statement is not found, a nil value will be returned
func (i *mockStmtEventInfo) ExecutePreparedStmt() ast.StmtNode {
	if i.isExecute {
		return i.stmtNode
	}
	return nil
}

// PreparedParams will return the params for the EXECUTE statement
func (i *mockStmtEventInfo) PreparedParams() []types.Datum {
	return i.prepareParams
}

// OriginalText will return the text of the statement.
// Notice that for the EXECUTE statement, the prepared statement text will be used as the return value
func (i *mockStmtEventInfo) OriginalText() string {
	return i.originalSQLText
}

// SQLDigest will return the normalized and redact text of the `OriginalText()`
func (i *mockStmtEventInfo) RedactedText() string {
	return i.normalizedSQL
}

// AffectedRows will return the affected rows of the current statement
func (i *mockStmtEventInfo) AffectedRows() uint64 {
	return i.affectedRows
}

func (i *mockStmtEventInfo) RelatedTables() []stmtctx.TableEntry {
	return i.tables
}

// GetError will return the error when the current statement is failed
func (i *mockStmtEventInfo) GetError() error {
	return i.err
}

func strClassesToEnums(t *testing.T, classes []string) []EventClass {
	m := make(map[string]EventClass, len(classes))
	for i := EventClass(0); i < ClassCount; i++ {
		m[i.String()] = i
	}

	enums := make([]EventClass, 0, len(classes))
	for _, c := range classes {
		class, ok := m[c]
		require.True(t, ok)
		enums = append(enums, class)
	}

	return enums
}

func parseSQL(t *testing.T, sql string) ast.StmtNode {
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	return stmt
}

func TestNewStmtEventEntry(t *testing.T) {
	cases := []struct {
		stmtNode        ast.StmtNode
		expectedClasses []EventClass

		forceRedact         bool
		containAffectedRows bool
	}{
		{
			stmtNode:        parseSQL(t, "select * from t"),
			expectedClasses: strClassesToEnums(t, []string{"QUERY", "SELECT"}),
		},
		{
			stmtNode:            parseSQL(t, "insert into t values(1)"),
			expectedClasses:     strClassesToEnums(t, []string{"QUERY", "QUERY_DML", "INSERT"}),
			containAffectedRows: true,
		},
		{
			stmtNode:            parseSQL(t, "update t set a = 1"),
			expectedClasses:     strClassesToEnums(t, []string{"QUERY", "QUERY_DML", "UPDATE"}),
			containAffectedRows: true,
		},
		{
			stmtNode:            parseSQL(t, "delete from t where id=1"),
			expectedClasses:     strClassesToEnums(t, []string{"QUERY", "QUERY_DML", "DELETE"}),
			containAffectedRows: true,
		},
		{
			stmtNode:            parseSQL(t, "BATCH ON id LIMIT 2 DELETE FROM t WHERE v < 6;"),
			expectedClasses:     strClassesToEnums(t, []string{"QUERY", "QUERY_DML", "DELETE"}),
			containAffectedRows: true,
		},
		{
			stmtNode:        parseSQL(t, "begin"),
			expectedClasses: strClassesToEnums(t, []string{"QUERY", "TRANSACTION"}),
		},
		{
			stmtNode:        parseSQL(t, "commit"),
			expectedClasses: strClassesToEnums(t, []string{"QUERY", "TRANSACTION"}),
		},
		{
			stmtNode:        parseSQL(t, "rollback"),
			expectedClasses: strClassesToEnums(t, []string{"QUERY", "TRANSACTION"}),
		},
		{
			stmtNode:        parseSQL(t, "create table t(a int)"),
			expectedClasses: strClassesToEnums(t, []string{"QUERY", "QUERY_DDL"}),
		},
		{
			stmtNode:        parseSQL(t, "SET PASSWORD FOR 'bob'@'%.example.org' = 'auth_string'"),
			expectedClasses: strClassesToEnums(t, []string{"QUERY"}),
			forceRedact:     true,
		},
		{
			stmtNode:        parseSQL(t, "create user 'u1'@'%'"),
			expectedClasses: strClassesToEnums(t, []string{"QUERY"}),
		},
		{
			stmtNode:        parseSQL(t, "create user 'u1'@'%' IDENTIFIED BY 'auth_string'"),
			expectedClasses: strClassesToEnums(t, []string{"QUERY"}),
			forceRedact:     true,
		},
		{
			stmtNode:        parseSQL(t, "create user 'u1'@'%' IDENTIFIED WITH caching_sha2_password AS 'xx'"),
			expectedClasses: strClassesToEnums(t, []string{"QUERY"}),
			forceRedact:     true,
		},
		{
			stmtNode:        parseSQL(t, "alter user 'u1'@'%'"),
			expectedClasses: strClassesToEnums(t, []string{"QUERY"}),
		},
		{
			stmtNode:        parseSQL(t, "ALTER USER 'u1'@'%' IDENTIFIED BY 'auth_string'"),
			expectedClasses: strClassesToEnums(t, []string{"QUERY"}),
			forceRedact:     true,
		},
		{
			stmtNode:        parseSQL(t, "ALTER USER user IDENTIFIED WITH caching_sha2_password AS 'xx'"),
			expectedClasses: strClassesToEnums(t, []string{"QUERY"}),
			forceRedact:     true,
		},
	}

	g := newEntryIDGenerator()
	for _, c := range cases {
		for _, tp := range []extension.StmtEventTp{extension.StmtError, extension.StmtSuccess} {
			for _, isExecute := range []bool{true, false} {
				for _, redact := range []bool{true, false} {
					name := fmt.Sprintf("%s %v-redact:%v-execute:%v", c.stmtNode.Text(), tp, redact, isExecute)
					t.Run(name, func(t *testing.T) {
						info := &mockStmtEventInfo{
							isExecute: isExecute,
							stmtNode:  c.stmtNode,
							tables: []stmtctx.TableEntry{
								{DB: "db1", Table: "t1"},
								{DB: "db2", Table: "t2"},
							},
							sessionAlias: "alias765",
							activeRoles: []*auth.RoleIdentity{
								{Username: "u1", Hostname: "h1"},
								{Username: "u2", Hostname: "h2"},
							},
							connInfo: &variable.ConnectionInfo{
								ConnectionID: 789,
								User:         "u1",
								Host:         "h1",
							},
							originalSQLText: "some not redact sql text",
							normalizedSQL:   "redact sql text",
							currentDB:       "db22",
							prepareParams: []types.Datum{
								{},
								types.NewStringDatum("str1"),
								types.NewIntDatum(367),
								types.NewBytesDatum([]byte{7, 9}),
								types.NewTimeDatum(types.NewTime(
									types.FromDate(2017, 12, 12, 12, 59, 59, 0),
									mysql.TypeDatetime, 0,
								)),
							},
							affectedRows: 128,
						}

						if tp == extension.StmtError {
							info.err = errors.New("mockErr1")
						}

						entry := newStmtEventEntry(tp, info)
						fields := entry.Fields(g, redact)
						require.Equal(t, "u1", entry.User)
						require.Equal(t, "h1", entry.Host)
						require.Equal(t, info.activeRoles, entry.Roles)
						require.Equal(t, uint64(789), entry.ConnID)
						require.Equal(t, "alias765", entry.SessionAlias)
						require.Equal(t, info.tables, entry.Tables)
						expectedClasses := c.expectedClasses
						if isExecute {
							expectedClasses = []EventClass{c.expectedClasses[0], ClassExecute}
							expectedClasses = append(expectedClasses, c.expectedClasses[1:]...)
						}
						require.Equal(t, expectedClasses, entry.Classes)
						if info.err == nil {
							require.Nil(t, entry.Err)
						} else {
							require.Equal(t, info.err.Error(), entry.Err.Error())
						}
						checkLogCommonFields(t, entry, fields)
						checkContainField(t, fields, zap.String("CURRENT_DB", "db22"))
						if c.forceRedact || redact {
							checkContainField(t, fields, zap.String("SQL_TEXT", info.normalizedSQL))
						} else {
							checkContainField(t, fields, zap.String("SQL_TEXT", info.originalSQLText))
						}

						if isExecute && !redact && !c.forceRedact {
							checkContainField(t, fields, zap.Strings("EXECUTE_PARAMS", []string{
								"NULL",
								"str1",
								"367",
								"[7 9]",
								"2017-12-12 12:59:59",
							}))
						} else {
							checkNotContainField(t, fields, "EXECUTE_PARAMS")
						}

						if c.containAffectedRows {
							checkContainField(t, fields, zap.Uint64("AFFECTED_ROWS", 128))
						} else {
							checkNotContainField(t, fields, "AFFECTED_ROWS")
						}
					})
				}
			}
		}
	}
}

func TestNewAuditEntryWithSetGlobalVar(t *testing.T) {
	vars := variable.NewSessionVars(nil)
	vars.ActiveRoles = []*auth.RoleIdentity{
		{Username: "u1", Hostname: "h1"},
		{Username: "u2", Hostname: "h2"},
	}

	// vars without ConnectionInfo should not generate any entry
	require.Nil(t, newAuditEntryWithSetGlobalVar("tidb_audit_enabled", "ON", vars, nil))
	require.Nil(t, newAuditEntryWithSetGlobalVar("tidb_audit_enabled", "OFF", vars, errors.New("err1")))

	cases := []struct {
		varName      string
		varVal       string
		extraClasses []string
	}{
		{varName: "tidb_audit_enabled", varVal: "ON", extraClasses: []string{"AUDIT_ENABLE"}},
		{varName: "tidb_audit_enabled", varVal: "OFF", extraClasses: []string{"AUDIT_DISABLE"}},
		{varName: "tidb_audit_enabled", varVal: "1", extraClasses: []string{"AUDIT_ENABLE"}},
		{varName: "tidb_audit_enabled", varVal: "0", extraClasses: []string{"AUDIT_DISABLE"}},
		{varName: "tidb_audit_log", varVal: "/tmp/audit1.log"},
	}

	for _, c := range cases {
		for _, redact := range []bool{false, true} {
			for _, err := range []error{nil, errors.New("mockErr1")} {
				t.Run(fmt.Sprintf("%s-%s", cases[0].varName, cases[0].varVal), func(t *testing.T) {
					vars.ConnectionInfo = &variable.ConnectionInfo{
						ConnectionID: 66677888,
						User:         "user1",
						Host:         "host1",
					}
					entry := newAuditEntryWithSetGlobalVar(c.varName, c.varVal, vars, err)
					require.NotNil(t, entry)
					fields := entry.Fields(newEntryIDGenerator(), redact)
					require.Equal(t, "user1", entry.User)
					require.Equal(t, "host1", entry.Host)
					require.Equal(t, uint64(66677888), entry.ConnID)
					require.Equal(t, vars.ActiveRoles, entry.Roles)
					events := append([]string{"AUDIT", "AUDIT_SET_SYS_VAR"}, c.extraClasses...)
					require.Equal(t, strClassesToEnums(t, events), entry.Classes)
					require.Nil(t, entry.Tables)
					if err == nil {
						require.Nil(t, entry.Err)
					} else {
						require.Equal(t, err.Error(), entry.Err.Error())
					}
					checkLogCommonFields(t, entry, fields)
					checkContainField(t, fields, zap.String("AUDIT_OP_TARGET", c.varName))
					checkContainField(t, fields, zap.Strings("AUDIT_OP_ARGS", []string{c.varVal}))
				})
			}
		}
	}
}

type mockFuncContext struct {
	extension.FunctionContext
	activeRoles []*auth.RoleIdentity
	connInfo    *variable.ConnectionInfo
}

func (ctx *mockFuncContext) ConnectionInfo() *variable.ConnectionInfo {
	return ctx.connInfo
}

func (ctx *mockFuncContext) ActiveRoles() []*auth.RoleIdentity {
	return ctx.activeRoles
}

func TestNewAuditFuncCallEntry(t *testing.T) {
	cases := []struct {
		funcName     string
		args         []types.Datum
		expectedArgs []string
	}{
		{
			funcName: "audit_log_create_filter",
			args: []types.Datum{
				types.NewStringDatum("all"), types.NewStringDatum("{}"), types.NewIntDatum(1),
			},
			expectedArgs: []string{"all", "{}", "1"},
		},
		{
			funcName: "f2",
			args: []types.Datum{
				types.NewStringDatum("s1"),
				types.NewTimeDatum(types.NewTime(
					types.FromDate(2018, 12, 12, 12, 59, 59, 0),
					mysql.TypeDatetime, 0,
				)),
			},
			expectedArgs: []string{"s1", "2018-12-12 12:59:59"},
		},
		{
			funcName:     "f3",
			args:         []types.Datum{{}, types.NewFloat64Datum(3)},
			expectedArgs: []string{"NULL", "3"},
		},
	}

	for _, c := range cases {
		for _, redact := range []bool{false, true} {
			for _, err := range []error{nil, errors.New("mockErr1")} {
				funcCtx := &mockFuncContext{
					connInfo: &variable.ConnectionInfo{
						ConnectionID: 66677888,
						User:         "user7",
						Host:         "host8",
					},
					activeRoles: []*auth.RoleIdentity{
						{Username: "u12", Hostname: "h34"},
					},
				}
				entry := newAuditFuncCallEntry(c.funcName, c.args, funcCtx, err)
				require.NotNil(t, entry)
				fields := entry.Fields(newEntryIDGenerator(), redact)
				require.Equal(t, "user7", entry.User)
				require.Equal(t, "host8", entry.Host)
				require.Equal(t, uint64(66677888), entry.ConnID)
				require.Equal(t, funcCtx.activeRoles, entry.Roles)
				require.Equal(t, strClassesToEnums(t, []string{"AUDIT", "AUDIT_FUNC_CALL"}), entry.Classes)
				require.Nil(t, entry.Tables)
				if err == nil {
					require.Nil(t, entry.Err)
				} else {
					require.Equal(t, err.Error(), entry.Err.Error())
				}
				checkLogCommonFields(t, entry, fields)
				checkContainField(t, fields, zap.String("AUDIT_OP_TARGET", c.funcName))
				checkContainField(t, fields, zap.Strings("AUDIT_OP_ARGS", c.expectedArgs))
			}
		}
	}
}
