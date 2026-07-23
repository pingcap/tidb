package audit

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"go.uber.org/zap"
)

// EventClass is the class for event
type EventClass int

// The below defines all event classes
const (
	ClassUnknown EventClass = iota

	// SQL audit
	ClassConnection
	ClassConnect
	ClassDisconnect
	ClassChangeUser
	ClassQuery
	ClassTransaction
	ClassExecute
	ClassDML
	ClassInsert
	ClassReplace
	ClassUpdate
	ClassDelete
	ClassLoadData
	ClassSelect
	ClassDDL
	ClassAudit
	ClassAuditSetSysVar
	ClassAuditFuncCall
	ClassAuditEnable
	ClassAuditDisable

	// EAL2 audit
	ClassSecurity
	ClassDataOperation

	// ClassCount is the count of all classes
	// New class should be added before it
	ClassCount
)

const (
	// StatusCodeFailed indicates the failed status of the event.
	StatusCodeFailed = 0
	// StatusCodeSuccess indicates the success status of the event.
	StatusCodeSuccess = 1
)

// Class2String is exported for test
var Class2String = map[EventClass]string{
	ClassConnection:     "CONNECTION",
	ClassConnect:        "CONNECT",
	ClassDisconnect:     "DISCONNECT",
	ClassChangeUser:     "CHANGE_USER",
	ClassQuery:          "QUERY",
	ClassTransaction:    "TRANSACTION",
	ClassExecute:        "EXECUTE",
	ClassDML:            "QUERY_DML",
	ClassInsert:         "INSERT",
	ClassReplace:        "REPLACE",
	ClassUpdate:         "UPDATE",
	ClassDelete:         "DELETE",
	ClassLoadData:       "LOAD_DATA",
	ClassSelect:         "SELECT",
	ClassDDL:            "QUERY_DDL",
	ClassAudit:          "AUDIT",
	ClassAuditSetSysVar: "AUDIT_SET_SYS_VAR",
	ClassAuditFuncCall:  "AUDIT_FUNC_CALL",
	ClassAuditEnable:    "AUDIT_ENABLE",
	ClassAuditDisable:   "AUDIT_DISABLE",
	ClassSecurity:       "SECURITY",
	ClassDataOperation:  "DATA_OPERATION",
}

var string2class map[string]EventClass

func init() {
	string2class = make(map[string]EventClass, ClassCount)
	for class := ClassUnknown + 1; class < ClassCount; class++ {
		str, ok := Class2String[class]
		if !ok {
			panic(fmt.Sprintf("string for audit class '%d' not set", class))
		}
		string2class[strings.ToUpper(str)] = class
	}
}

func getEventClass(s string) (EventClass, bool) {
	if c, ok := string2class[strings.ToUpper(s)]; ok {
		return c, true
	}
	return ClassUnknown, false
}

func (EventClass) displayInLog() bool {
	return true
}

func (c EventClass) String() string {
	return Class2String[c]
}

func getStmtClasses(stmt ast.StmtNode, execute bool) []EventClass {
	classes := append(make([]EventClass, 0, 4), ClassQuery)
	if execute {
		classes = append(classes, ClassExecute)
	}
	switch stmt.(type) {
	case *ast.SelectStmt:
		classes = append(classes, ClassSelect)
	case ast.DMLNode:
		classes = appendDMLClass(classes, stmt)
	case ast.DDLNode:
		classes = append(classes, ClassDDL)
	case *ast.BeginStmt, *ast.CommitStmt, *ast.RollbackStmt, *ast.SavepointStmt, *ast.ReleaseSavepointStmt:
		classes = append(classes, ClassTransaction)
	}
	return classes
}

func appendDMLClass(classes []EventClass, stmt ast.StmtNode) []EventClass {
	switch n := stmt.(type) {
	case *ast.InsertStmt:
		if n.IsReplace {
			classes = append(classes, ClassDML, ClassReplace)
		} else {
			classes = append(classes, ClassDML, ClassInsert)
		}
	case *ast.UpdateStmt:
		classes = append(classes, ClassDML, ClassUpdate)
	case *ast.DeleteStmt:
		classes = append(classes, ClassDML, ClassDelete)
	case *ast.LoadDataStmt:
		classes = append(classes, ClassDML, ClassLoadData)
	case *ast.NonTransactionalDMLStmt:
		classes = appendDMLClass(classes, n.DMLStmt)
	}
	return classes
}

// log keys
const (
	LogKeyID           = "ID"
	LogKeyTime         = "TIME"
	LogKeyEvent        = "EVENT"
	LogKeyUser         = "USER"
	LogKeyRoles        = "ROLES"
	LogKeyConnectionID = "CONNECTION_ID"
	LogKeyCurrentDB    = "CURRENT_DB"
	LogKeyTables       = "TABLES"
	LogKeyReason       = "REASON"
	LogKeyStatusCode   = "STATUS_CODE"
	LogKeySessionAlias = "SESSION_ALIAS"

	LogKeyConnectionTYPE = "CONNECTION_TYPE"
	LogKeyPID            = "PID"
	LogKeyServerVersion  = "SERVER_VERSION"
	LogKeySSLVersion     = "SSL_VERSION"
	LogKeyHostIP         = "HOST_IP"
	LogKeyHostPort       = "HOST_PORT"
	LogKeyClientIP       = "CLIENT_IP"
	LogKeyClientPort     = "CLIENT_PORT"
	LogKeySQLText        = "SQL_TEXT"
	LogKeyExecuteParams  = "EXECUTE_PARAMS"
	LogKeyAffectedRows   = "AFFECTED_ROWS"
	LogKeyAuthMethod     = "AUTH_METHOD"
	LogKeyAttributes     = "CONN_ATTRS"

	LogKeyAuditOpTarget = "AUDIT_OP_TARGET"
	LogKeyAuditOpArgs   = "AUDIT_OP_ARGS"

	LogKeySecurityInfo = "SECURITY_INFO"
)

var logFieldsPool = createLogFieldsPool(1024)

func createLogFieldsPool(capacity int) *pools.ResourcePool {
	return pools.NewResourcePool(
		func() (pools.Resource, error) {
			return newLogFields(), nil
		},
		capacity, capacity, 0,
	)
}

type logFields []zap.Field

func newLogFields() logFields {
	return make([]zap.Field, 0, 12)
}

func fieldsFromPool(pool *pools.ResourcePool) (logFields, func()) {
	resource, err := pool.TryGet()
	if err != nil {
		terror.Log(err)
		return newLogFields(), func() {}
	}

	if fields, ok := resource.(logFields); ok {
		return fields, func() {
			pool.Put(fields)
		}
	}

	return newLogFields(), func() {}
}

func (logFields) Close() {}

// LogEntry is the entry for the audit log
type LogEntry struct {
	User         string
	Host         string
	Roles        []*auth.RoleIdentity
	ConnID       uint64
	SessionAlias string
	Classes      []EventClass
	Tables       []stmtctx.TableEntry
	Err          error
	FieldsFunc   func(fields []zap.Field, cfg *LoggerConfig) []zap.Field
}

func newAuditEntryWithSetGlobalVar(name, value string, sessVars *variable.SessionVars, err error) *LogEntry {
	connInfo := sessVars.ConnectionInfo
	if connInfo == nil {
		// if connInfo is nil, it means it is a background setting, only log this event when user manually set it.
		return nil
	}

	var classes []EventClass
	switch name {
	case TiDBAuditEnabled:
		var auditEnableClass EventClass
		if variable.TiDBOptOn(value) {
			auditEnableClass = ClassAuditEnable
		} else {
			auditEnableClass = ClassAuditDisable
		}
		classes = []EventClass{ClassAudit, ClassAuditSetSysVar, auditEnableClass}
	default:
		classes = []EventClass{ClassAudit, ClassAuditSetSysVar}
	}

	return &LogEntry{
		User:    connInfo.User,
		Host:    connInfo.Host,
		Roles:   sessVars.ActiveRoles,
		ConnID:  connInfo.ConnectionID,
		Classes: classes,
		Err:     err,
		FieldsFunc: func(fields []zap.Field, _ *LoggerConfig) []zap.Field {
			return append(
				fields,
				zap.String(LogKeyAuditOpTarget, name),
				zap.Strings(LogKeyAuditOpArgs, []string{value}),
			)
		},
	}
}

func newAuditFuncCallEntry(name string, args []types.Datum, ctx extension.FunctionContext, err error) *LogEntry {
	var connID uint64
	var user, host string
	if conn := ctx.ConnectionInfo(); conn != nil {
		connID = conn.ConnectionID
		user = conn.User
		host = conn.Host
	}

	return &LogEntry{
		User:    user,
		Host:    host,
		Roles:   ctx.ActiveRoles(),
		ConnID:  connID,
		Err:     err,
		Classes: []EventClass{ClassAudit, ClassAuditFuncCall},
		FieldsFunc: func(fields []zap.Field, _ *LoggerConfig) []zap.Field {
			return append(
				fields,
				zap.String(LogKeyAuditOpTarget, name),
				zap.Strings(LogKeyAuditOpArgs, toStringArgs(args)),
			)
		},
	}
}

func newConnEventEntry(tp extension.ConnEventTp, info *extension.ConnEventInfo) *LogEntry {
	e := &LogEntry{}

	switch tp {
	case extension.ConnHandshakeAccepted, extension.ConnHandshakeRejected:
		e.Classes = []EventClass{ClassConnection, ClassConnect}
	case extension.ConnReset:
		e.Classes = []EventClass{ClassConnection, ClassChangeUser}
	case extension.ConnDisconnected:
		e.Classes = []EventClass{ClassConnection, ClassDisconnect}
	case extension.ConnConnected:
		e.Classes = []EventClass{ClassConnection}
	default:
		return nil
	}

	if info.ConnectionInfo != nil {
		e.User = info.User
		e.Host = info.Host
		e.ConnID = info.ConnectionID
		e.SessionAlias = info.SessionAlias
	} else {
		info.ConnectionInfo = &variable.ConnectionInfo{}
		e.User = ""
		e.Host = "%"
	}
	e.Roles = info.ActiveRoles
	e.Err = info.Error
	e.FieldsFunc = func(fields []zap.Field, cfg *LoggerConfig) []zap.Field {
		if tp != extension.ConnDisconnected {
			// only log current db when connect or change user
			fields = append(fields, zap.String(LogKeyCurrentDB, info.DB))
		}
		if len(info.Info) > 0 {
			fields = append(fields, zap.String(LogKeySecurityInfo, info.Info))
		}
		return append(
			fields,
			zap.String(LogKeyConnectionTYPE, info.ConnectionType),
			zap.String(LogKeyPID, strconv.Itoa(info.PID)),
			zap.String(LogKeyServerVersion, info.ServerVersion),
			zap.String(LogKeySSLVersion, info.SSLVersion),
			zap.String(LogKeyHostIP, normalizeIP(info.ServerIP)),
			zap.String(LogKeyHostPort, strconv.Itoa(info.ServerPort)),
			zap.String(LogKeyClientIP, normalizeIP(info.ClientIP)),
			zap.String(LogKeyClientPort, info.ClientPort),
			zap.String(LogKeyAuthMethod, info.AuthMethod),
			zap.Any(LogKeyAttributes, info.Attributes),
		)
	}

	return e
}

// Statements containing password should always be masked.
func isPasswordStmt(node ast.StmtNode) bool {
	if node == nil {
		return false
	}

	findInSpecs := func(specs []*ast.UserSpec) bool {
		for _, spec := range specs {
			if authOpt := spec.AuthOpt; authOpt != nil && (authOpt.ByAuthString || authOpt.ByHashString) {
				return true
			}
		}
		return false
	}

	switch stmt := node.(type) {
	case *ast.SetPwdStmt:
		return true
	case *ast.CreateUserStmt:
		return findInSpecs(stmt.Specs)
	case *ast.AlterUserStmt:
		return findInSpecs(stmt.Specs)
	}
	return false
}

func newStmtEventEntry(tp extension.StmtEventTp, info extension.StmtEventInfo) *LogEntry {
	if tp != extension.StmtSuccess && tp != extension.StmtError {
		return nil
	}

	isExecute := info.ExecuteStmtNode() != nil
	innerStmtNode := info.ExecutePreparedStmt()
	if innerStmtNode == nil {
		innerStmtNode = info.StmtNode()
	}

	entry := &LogEntry{
		Classes: getStmtClasses(innerStmtNode, isExecute),
		Tables:  info.RelatedTables(),
		Err:     info.GetError(),
	}

	if connInfo := info.ConnectionInfo(); connInfo != nil {
		entry.ConnID = connInfo.ConnectionID
		entry.SessionAlias = info.SessionAlias()
		entry.User = connInfo.User
		entry.Host = connInfo.Host
		entry.Roles = info.ActiveRoles()
	}
	entry.FieldsFunc = func(fields []zap.Field, cfg *LoggerConfig) []zap.Field {
		var sqlText string
		if cfg.Redact || isPasswordStmt(info.StmtNode()) {
			sqlText = info.RedactedText()
		} else {
			sqlText = info.OriginalText()
		}

		fields = append(
			fields,
			zap.String(LogKeyCurrentDB, info.CurrentDB()),
			zap.String(LogKeySQLText, sqlText),
		)

		if isExecute && !cfg.Redact {
			fields = append(fields, zap.Strings(LogKeyExecuteParams, toStringArgs(info.PreparedParams())))
		}

		for _, class := range entry.Classes {
			if class == ClassDML {
				fields = append(fields, zap.Uint64(LogKeyAffectedRows, info.AffectedRows()))
				break
			}
		}

		return fields
	}
	return entry
}

func newSecurityEventEntry(tp extension.SecurityEventTp, info extension.SecurityEventInfo) *LogEntry {
	if tp != extension.SecurityEvent {
		return nil
	}
	user, host := "", "%"
	if u := info.User(); len(u) > 0 {
		user = u
	}
	if h := info.Host(); len(h) > 0 {
		host = h
	}
	return &LogEntry{
		User:    user,
		Host:    host,
		Classes: []EventClass{ClassSecurity},
		FieldsFunc: func(fields []zap.Field, cfg *LoggerConfig) []zap.Field {
			var sqlText string
			if cfg.Redact {
				sqlText = info.RedactedText()
			} else {
				sqlText = info.OriginalText()
			}
			if len(sqlText) > 0 {
				fields = append(fields, zap.String(LogKeySQLText, sqlText))
			}
			if securityInfo := info.SecurityInfo(); len(securityInfo) > 0 {
				fields = append(fields, zap.String(LogKeySecurityInfo, securityInfo))
			}
			return fields
		},
	}
}

func newDataOpEventEntry(tp extension.DataOpEventTp, info extension.DataOpEventInfo) *LogEntry {
	if tp != extension.DataOpEvent {
		return nil
	}
	user, host := "root", "%"
	if u := info.User(); len(u) > 0 {
		user = u
	}
	if h := info.Host(); len(h) > 0 {
		host = h
	}

	var strs []string
	if c := info.Component(); len(c) > 0 {
		strs = append(strs, fmt.Sprintf("Component: %s", c))
	}
	if r := info.Result(); len(r) > 0 {
		strs = append(strs, fmt.Sprintf("Result: %s", r))
	}
	if t := info.TargetList(); len(t) > 0 {
		strs = append(strs, fmt.Sprintf("TargetList: %s", t))
	}
	if i := info.InputDir(); len(i) > 0 {
		strs = append(strs, fmt.Sprintf("InputDir: %s", i))
	}
	if o := info.OutputDir(); len(o) > 0 {
		strs = append(strs, fmt.Sprintf("OutputDir: %s", o))
	}

	res := &LogEntry{
		User:    user,
		Host:    host,
		Classes: []EventClass{ClassDataOperation},
		FieldsFunc: func(fields []zap.Field, _ *LoggerConfig) []zap.Field {
			if connectionInfo := info.ConnectionInfo(); connectionInfo != nil {
				fields = append(fields,
					zap.String(LogKeyHostIP, normalizeIP(connectionInfo.ServerIP)),
					zap.String(LogKeyHostPort, strconv.Itoa(connectionInfo.ServerPort)),
					zap.String(LogKeyClientIP, normalizeIP(connectionInfo.ClientIP)),
					zap.String(LogKeyClientPort, connectionInfo.ClientPort))
			}
			return append(fields, zap.String(LogKeySecurityInfo, strings.Join(strs, ", ")))
		},
	}
	if connectionInfo := info.ConnectionInfo(); connectionInfo != nil {
		res.ConnID = connectionInfo.ConnectionID
	}
	return res
}

// Filter filters this log entry by filter
func (e *LogEntry) Filter(filter *LogFilterRuleBundle) *LogEntry {
	return filter.Filter(e)
}

// Log logs this entry
func (e *LogEntry) Log(logger *Logger, id *entryIDGenerator) {
	if e == nil {
		return
	}

	events := make([]string, 0, len(e.Classes))
	for _, class := range e.Classes {
		if class.displayInLog() {
			events = append(events, class.String())
		}
	}

	var roles []string
	if len(e.Roles) > 0 {
		roles = make([]string, len(e.Roles))
		for i, role := range e.Roles {
			roles[i] = role.String()
		}
	}

	var tables []string
	if len(e.Tables) > 0 {
		tables = make([]string, len(e.Tables))
		for i, tb := range e.Tables {
			tables[i] = fmt.Sprintf("`%s`.`%s`", tb.DB, tb.Table)
		}
	}

	fields, closeFields := fieldsFromPool(logFieldsPool)
	defer closeFields()

	fields = append(
		fields,
		zap.String(LogKeyID, id.Next()),
		zap.Strings(LogKeyEvent, events),
		zap.String(LogKeyUser, e.User),
		zap.Strings(LogKeyRoles, roles),
		zap.String(LogKeyConnectionID, strconv.FormatUint(e.ConnID, 10)),
		zap.String(LogKeySessionAlias, e.SessionAlias),
		zap.Strings(LogKeyTables, tables),
		zap.Int(LogKeyStatusCode, getStatusCode(e.Err)),
	)

	if err := e.Err; err != nil {
		fields = append(fields, zap.String(LogKeyReason, err.Error()))
	}

	if e.FieldsFunc != nil {
		fields = e.FieldsFunc(fields, &logger.cfg)
	}

	logger.Event(fields)
}

func getStatusCode(err error) int {
	if err != nil {
		return StatusCodeFailed
	}
	return StatusCodeSuccess
}
