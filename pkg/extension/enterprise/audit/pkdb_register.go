package audit

import (
	"context"
	"encoding/json"
	"flag"
	"strconv"

	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// ExtensionName is the extension name for audit log
const ExtensionName = "Audit"

const (
	// PrivAuditAdmin is the dynamic privilege that allows the admin of audit log
	PrivAuditAdmin = "AUDIT_ADMIN"
	// PrivRestrictedAuditAdmin is the same with `PRIV_AUDIT_ADMIN` but used in sem mode.
	PrivRestrictedAuditAdmin = "RESTRICTED_AUDIT_ADMIN"
)

const (
	// TiDBAuditEnabled is a system variable that used to enable/disable audit feature
	TiDBAuditEnabled = "tidb_audit_enabled"
	// TiDBAuditLog is a system variable that used to set audit log file path
	TiDBAuditLog = "tidb_audit_log"
	// TiDBAuditLogFormat is a system variable that used to set the output format of the audit log
	TiDBAuditLogFormat = "tidb_audit_log_format"
	// TiDBAuditLogMaxFileSize is a system variable that used to control the max size of a log file
	TiDBAuditLogMaxFileSize = "tidb_audit_log_max_filesize"
	// TiDBAuditLogMaxLifetime is a system variable that used to control the max lifetime of a log file
	TiDBAuditLogMaxLifetime = "tidb_audit_log_max_lifetime"
	// TiDBAuditLogReservedBackups is a system variable that used to control the max backup count of the logs
	TiDBAuditLogReservedBackups = "tidb_audit_log_reserved_backups"
	// TiDBAuditLogMaxReservedDays is a system variable that used to control the max days of the reserved log
	TiDBAuditLogMaxReservedDays = "tidb_audit_log_reserved_days"
	// TiDBAuditRedactLog is a system variable that used to switch on/off of redact log
	TiDBAuditRedactLog = "tidb_audit_log_redacted"
)

const (
	// FuncAuditLogRotate is used to rotate audit log
	FuncAuditLogRotate = "audit_log_rotate"
)

// GlobalLogManager manages the global state of the audit log
var GlobalLogManager LogManager

var enableAuditExtension bool

// Register registers enterprise extension
func Register() {
	flag.BoolVar(&enableAuditExtension, "enable-audit-extension", true, "enable audit extension or not")
	err := extension.RegisterFactory(ExtensionName, extensionOptionsFactory)
	terror.MustNil(err)
}

// Register4Test is the mock function that is used to test.
func Register4Test() {
	extension.Reset()
	enableAuditExtension = true
	err := extension.RegisterFactory(ExtensionName, extensionOptionsFactory)
	terror.MustNil(err)
}

func requirePrivileges(sem bool) []string {
	if sem {
		return []string{PrivRestrictedAuditAdmin}
	}
	return []string{PrivAuditAdmin}
}

func extensionOptionsFactory() ([]extension.Option, error) {
	if !enableAuditExtension {
		return nil, nil
	}

	if err := GlobalLogManager.Init(); err != nil {
		return nil, err
	}

	setGlobalFunc := func(sysVar string, fn func(context.Context, *variable.SessionVars, string) error) func(context.Context, *variable.SessionVars, string) error {
		return func(ctx context.Context, vars *variable.SessionVars, val string) (err error) {
			defer func() {
				newAuditEntryWithSetGlobalVar(sysVar, val, vars, err).Log(GlobalLogManager.logger(), newEntryIDGenerator())
			}()
			return fn(ctx, vars, val)
		}
	}

	statusFunc := func(name string, fn func(extension.FunctionContext, []types.Datum) error) func(ctx extension.FunctionContext, row chunk.Row) (string, bool, error) {
		return func(ctx extension.FunctionContext, row chunk.Row) (_ string, _ bool, err error) {
			var args []types.Datum
			defer func() {
				newAuditFuncCallEntry(name, args, ctx, err).Log(GlobalLogManager.logger(), newEntryIDGenerator())
			}()

			args, err = ctx.EvalArgs(row)
			if err != nil {
				return "", false, err
			}

			if err = fn(ctx, args); err != nil {
				return "", false, err
			}
			return "OK", false, nil
		}
	}

	sysVarPrivileges := func(_ bool, sem bool) []string {
		return requirePrivileges(sem)
	}

	sysVars := []*variable.SysVar{
		{
			Name:  TiDBAuditEnabled,
			Scope: variable.ScopeGlobal,
			Type:  variable.TypeBool,
			Value: variable.BoolToOnOff(GlobalLogManager.Enabled()),
			SetGlobal: setGlobalFunc(TiDBAuditEnabled, func(_ context.Context, sessVars *variable.SessionVars, s string) (err error) {
				enable := variable.TiDBOptOn(s)
				return GlobalLogManager.SetEnabled(enable)
			}),
			RequireDynamicPrivileges: sysVarPrivileges,
		},
		{
			Name:  TiDBAuditLog,
			Scope: variable.ScopeGlobal,
			Type:  variable.TypeStr,
			Value: GlobalLogManager.GetLogConfigPath(),
			SetGlobal: setGlobalFunc(TiDBAuditLog, func(_ context.Context, _ *variable.SessionVars, s string) error {
				return GlobalLogManager.SetLogConfigPath(s)
			}),
			RequireDynamicPrivileges: sysVarPrivileges,
		},
		{
			Name:           TiDBAuditLogFormat,
			Scope:          variable.ScopeGlobal,
			Type:           variable.TypeEnum,
			Value:          LogFormatText,
			PossibleValues: []string{LogFormatText, LogFormatJSON},
			SetGlobal: setGlobalFunc(TiDBAuditLogFormat, func(_ context.Context, _ *variable.SessionVars, s string) error {
				return GlobalLogManager.SetLogFormat(s)
			}),
			RequireDynamicPrivileges: sysVarPrivileges,
		},
		{
			Name:     TiDBAuditLogMaxFileSize,
			Scope:    variable.ScopeGlobal,
			Type:     variable.TypeInt,
			Value:    strconv.FormatInt(GlobalLogManager.GetFileMaxSize(), 10),
			MinValue: 0,
			MaxValue: MaxAuditLogFileMaxSize,
			Validation: func(vars *variable.SessionVars, normalizedValue string, originalValue string, scope variable.ScopeFlag) (string, error) {
				val, err := strconv.ParseInt(normalizedValue, 10, 64)
				if err != nil {
					return normalizedValue, err
				}
				if val == 0 {
					return strconv.FormatInt(DefAuditLogFileMaxSize, 10), err
				}
				return normalizedValue, err
			},
			SetGlobal: setGlobalFunc(TiDBAuditLogMaxFileSize, func(_ context.Context, _ *variable.SessionVars, s string) error {
				val, err := strconv.ParseInt(s, 10, 64)
				if err != nil {
					return err
				}
				return GlobalLogManager.SetFileMaxSize(val)
			}),
			RequireDynamicPrivileges: sysVarPrivileges,
		},
		{
			Name:     TiDBAuditLogMaxLifetime,
			Scope:    variable.ScopeGlobal,
			Type:     variable.TypeInt,
			Value:    strconv.FormatInt(GlobalLogManager.GetFileMaxLifetime(), 10),
			MinValue: 0,
			MaxValue: MaxAuditLogFileMaxLifetime,
			SetGlobal: setGlobalFunc(TiDBAuditLogMaxLifetime, func(_ context.Context, _ *variable.SessionVars, s string) error {
				val, err := strconv.ParseInt(s, 10, 64)
				if err != nil {
					return err
				}
				return GlobalLogManager.SetFileMaxLifetime(val)
			}),
			RequireDynamicPrivileges: sysVarPrivileges,
		},
		{
			Name:     TiDBAuditLogReservedBackups,
			Scope:    variable.ScopeGlobal,
			Type:     variable.TypeInt,
			Value:    strconv.Itoa(GlobalLogManager.GetFileReservedBackups()),
			MinValue: 0,
			MaxValue: MaxAuditLogFileReservedBackups,
			SetGlobal: setGlobalFunc(TiDBAuditLogReservedBackups, func(ctx context.Context, vars *variable.SessionVars, s string) error {
				val, err := strconv.Atoi(s)
				if err != nil {
					return err
				}
				return GlobalLogManager.SetFileReservedBackups(val)
			}),
			RequireDynamicPrivileges: sysVarPrivileges,
		},
		{
			Name:     TiDBAuditLogMaxReservedDays,
			Scope:    variable.ScopeGlobal,
			Type:     variable.TypeInt,
			Value:    strconv.Itoa(GlobalLogManager.GetFileReservedDays()),
			MinValue: 0,
			MaxValue: MaxAuditLogFileReservedDays,
			SetGlobal: setGlobalFunc(TiDBAuditLogMaxReservedDays, func(ctx context.Context, vars *variable.SessionVars, s string) error {
				val, err := strconv.Atoi(s)
				if err != nil {
					return err
				}
				return GlobalLogManager.SetFileReservedDays(val)
			}),
			RequireDynamicPrivileges: sysVarPrivileges,
		},
		{
			Name:  TiDBAuditRedactLog,
			Scope: variable.ScopeGlobal,
			Type:  variable.TypeBool,
			Value: variable.BoolToOnOff(GlobalLogManager.RedactLog()),
			SetGlobal: setGlobalFunc(TiDBAuditRedactLog, func(_ context.Context, _ *variable.SessionVars, s string) error {
				return GlobalLogManager.SetRedactLog(variable.TiDBOptOn(s))
			}),
			RequireDynamicPrivileges: sysVarPrivileges,
		},
	}

	functions := []*extension.FunctionDef{
		{
			Name:   FuncAuditLogRotate,
			EvalTp: types.ETString,
			EvalStringFunc: statusFunc(FuncAuditLogRotate, func(ctx extension.FunctionContext, _ []types.Datum) error {
				return GlobalLogManager.GlobalRotateLog()
			}),
			RequireDynamicPrivileges: requirePrivileges,
		},
		{
			Name:            FuncAuditLogCreateFilter,
			EvalTp:          types.ETString,
			ArgTps:          []types.EvalType{types.ETString, types.ETString, types.ETInt},
			OptionalArgsLen: 1,
			EvalStringFunc: statusFunc(FuncAuditLogCreateFilter, func(ctx extension.FunctionContext, args []types.Datum) error {
				filterName := args[0].GetString()
				var filter LogFilter
				if err := json.Unmarshal(args[1].GetBytes(), &filter); err != nil {
					return err
				}

				replace := false
				if len(args) > 2 {
					replace = args[2].GetInt64() == 1
				}

				filter.Name = filterName
				return GlobalLogManager.CreateLogFilter(&filter, replace)
			}),
			RequireDynamicPrivileges: requirePrivileges,
		},
		{
			Name:   FuncAuditLogRemoveFilter,
			EvalTp: types.ETString,
			ArgTps: []types.EvalType{types.ETString},
			EvalStringFunc: statusFunc(FuncAuditLogRemoveFilter, func(ctx extension.FunctionContext, args []types.Datum) error {
				return GlobalLogManager.RemoveLogFilter(args[0].GetString())
			}),
			RequireDynamicPrivileges: requirePrivileges,
		},
		{
			Name:            FuncAuditLogCreateRule,
			EvalTp:          types.ETString,
			ArgTps:          []types.EvalType{types.ETString, types.ETString, types.ETInt},
			OptionalArgsLen: 1,
			EvalStringFunc: statusFunc(FuncAuditLogCreateRule, func(ctx extension.FunctionContext, args []types.Datum) error {
				replace := false
				if len(args) > 2 {
					replace = args[2].GetInt64() == 1
				}
				return GlobalLogManager.CreateLogFilterRule(args[0].GetString(), args[1].GetString(), replace)
			}),
			RequireDynamicPrivileges: requirePrivileges,
		},
		{
			Name:   FuncAuditLogRemoveRule,
			EvalTp: types.ETString,
			ArgTps: []types.EvalType{types.ETString, types.ETString},
			EvalStringFunc: statusFunc(FuncAuditLogRemoveRule, func(ctx extension.FunctionContext, args []types.Datum) error {
				return GlobalLogManager.RemoveLogFilterRule(args[0].GetString(), args[1].GetString())
			}),
			RequireDynamicPrivileges: requirePrivileges,
		},
		{
			Name:   FuncAuditLogEnableRule,
			EvalTp: types.ETString,
			ArgTps: []types.EvalType{types.ETString, types.ETString},
			EvalStringFunc: statusFunc(FuncAuditLogEnableRule, func(ctx extension.FunctionContext, args []types.Datum) error {
				return GlobalLogManager.SwitchLogFilterRuleEnabled(args[0].GetString(), args[1].GetString(), true)
			}),
			RequireDynamicPrivileges: requirePrivileges,
		},
		{
			Name:   FuncAuditLogDisableRule,
			EvalTp: types.ETString,
			ArgTps: []types.EvalType{types.ETString, types.ETString},
			EvalStringFunc: statusFunc(FuncAuditLogDisableRule, func(ctx extension.FunctionContext, args []types.Datum) error {
				return GlobalLogManager.SwitchLogFilterRuleEnabled(args[0].GetString(), args[1].GetString(), false)
			}),
			RequireDynamicPrivileges: requirePrivileges,
		},
	}

	return []extension.Option{
		extension.WithCustomDynPrivs([]string{PrivAuditAdmin, PrivRestrictedAuditAdmin}),
		extension.WithCustomSysVariables(sysVars),
		extension.WithSessionHandlerFactory(GlobalLogManager.GetSessionHandler),
		extension.WithCustomFunctions(functions),
		extension.WithBootstrap(GlobalLogManager.Bootstrap),
		extension.WithCustomAccessCheck(checkTableAccess),
		extension.WithClose(GlobalLogManager.Close),
	}, nil
}
