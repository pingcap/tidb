package audit

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	// EtcdKeyNotifyPrefix is the key prefix to notify audit log admin events to all instances
	EtcdKeyNotifyPrefix = "/tidb/audit/log/notify/"
	// EtcdKeyNotifyRotate is used to notify to rotate logs
	EtcdKeyNotifyRotate = EtcdKeyNotifyPrefix + "rotate"
	// EtcdKeyNotifyFilter is used to notify filter changes
	EtcdKeyNotifyFilter = EtcdKeyNotifyPrefix + "filter"
	// GlobalNotifyTimeout is the timeout to notify global event
	GlobalNotifyTimeout = time.Minute
	// MinEtcdReWatchInterval is the min interval for retry watch
	MinEtcdReWatchInterval = 5 * time.Second

	convenientRuleNamePrefix = "convenient_rule_"
)

// EtcdCli is an interface for etcd client
type EtcdCli interface {
	clientv3.KV
	clientv3.Watcher
}

// LogManager is used to manage audit logs
type LogManager struct {
	ctx    context.Context
	cancel func()

	logger *Logger
	mu     struct {
		sync.Mutex
		stopFlag  bool
		loggerCfg LoggerConfig
		etcdCli   EtcdCli
		sessPool  extension.SessionPool
	}
}

// NewAuditLogManager creates a new LogManager
func NewAuditLogManager() (*LogManager, error) {
	logConfigPath := DefAuditLogName
	logFilepath, err := getFilePath(logConfigPath, DefAuditLogFormat)
	if err != nil {
		return nil, err
	}

	cfg := LoggerConfig{
		Enabled:    false,
		ConfigPath: logConfigPath,
		zapLoggerConfig: zapLoggerConfig{
			Filepath:            logFilepath,
			FileMaxSize:         DefAuditLogFileMaxSize,
			FileMaxLifetime:     DefAuditLogFileMaxLifetime,
			FileReservedBackups: DefAuditLogFileReservedBackups,
			FileReservedDays:    DefAuditLogFileReservedDays,
			LogFormat:           DefAuditLogFormat,
		},
		Redact: DefAuditLogRedact,
	}

	logger, err := NewAuditLogger(cfg)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers))
	m := &LogManager{
		ctx:    ctx,
		cancel: cancel,
		logger: logger,
	}
	m.mu.loggerCfg = cfg
	return m, nil
}

// injectedEtcdCliForTest is only used for testing
var injectedEtcdCliForTest EtcdCli

// Bootstrap bootstraps audit log manager
func (m *LogManager) Bootstrap(ctx extension.BootstrapContext) error {
	var etcdCli EtcdCli
	if cli := ctx.EtcdClient(); cli != nil {
		// DO NOT assign etcdCli to mu.etcdCli directly.
		// A `*clientv3.Client(nil)` will cause `m.etcdCli != nil`
		etcdCli = ctx.EtcdClient()
	}

	if intest.InTest && injectedEtcdCliForTest != nil {
		etcdCli = injectedEtcdCliForTest
	}
	sessPool := ctx.SessionPool()
	mu := &m.mu
	mu.Lock()
	if mu.stopFlag {
		mu.Unlock()
		return errors.New("cannot Bootstrap because LogManager is stopped")
	}
	mu.etcdCli = etcdCli
	mu.sessPool = sessPool
	mu.Unlock()

	if err := m.bootstrapSQL(); err != nil {
		return err
	}

	if err := m.ReloadFilter(); err != nil {
		return err
	}

	go m.handleNotifyLoop(etcdCli)
	return nil
}

func (m *LogManager) bootstrapSQL() error {
	return m.runSQLInExecutor(func(exec sqlexec.SQLExecutor) error {
		if _, err := executeSQL(m.ctx, exec, CreateFilterTableSQL); err != nil {
			return err
		}
		if _, err := executeSQL(m.ctx, exec, CreateFilterRuleTableSQL); err != nil {
			return err
		}
		return nil
	})
}

func (m *LogManager) runSQLInExecutor(fn func(sqlexec.SQLExecutor) error) error {
	m.mu.Lock()
	sessPool := m.mu.sessPool
	m.mu.Unlock()

	if sessPool == nil {
		return errors.New("sessPool is not initialized")
	}

	sess, err := sessPool.Get()
	if err != nil {
		return err
	}

	defer func() {
		if sess != nil {
			sessPool.Put(sess)
		}
	}()

	exec, ok := sess.(sqlexec.SQLExecutor)
	if !ok {
		return errors.Errorf("type %T cannot be casted to SQLExecutor", sess)
	}

	return fn(exec)
}

func (m *LogManager) runInTxn(ctx context.Context, fn func(sqlexec.SQLExecutor) error) error {
	return m.runSQLInExecutor(func(exec sqlexec.SQLExecutor) (err error) {
		done := false
		defer func() {
			if done {
				_, err = executeSQL(ctx, exec, "COMMIT")
			} else {
				_, rollbackErr := exec.ExecuteInternal(ctx, "ROLLBACK")
				terror.Log(rollbackErr)
			}
		}()

		if _, err = executeSQL(ctx, exec, "BEGIN"); err != nil {
			return err
		}

		if err = fn(exec); err != nil {
			return err
		}

		done = true
		return nil
	})
}

func (m *LogManager) globalNotify(key string) (bool, error) {
	var cli EtcdCli
	m.mu.Lock()
	cli = m.mu.etcdCli
	m.mu.Unlock()

	if cli == nil {
		// `cli == nil` it is not a TiKV cluster, return false, nil to indicate global notify is invalid
		return false, nil
	}

	ctx, cancel := context.WithTimeout(m.ctx, GlobalNotifyTimeout)
	defer cancel()

	if _, err := cli.Put(ctx, key, fmt.Sprintf(`{"ts": %d}`, time.Now().UnixMilli())); err != nil {
		return false, err
	}

	return true, nil
}

func (m *LogManager) handleNotifyLoop(etcd EtcdCli) {
	if etcd == nil || m.ctx.Err() != nil {
		return
	}

	watcher := newKeyWatcher(m.ctx, etcd, EtcdKeyNotifyPrefix)
	reloadFilterTicker := time.NewTicker(5 * time.Minute)
Loop:
	for {
		select {
		case <-m.ctx.Done():
			break Loop
		case resp, ok := <-watcher.Chan:
			if !ok {
				watcher.RenewClosedChan()
				continue
			}

			processedKeys := make(map[string]struct{}, len(resp.Events))
			for _, event := range resp.Events {
				if event.Type != mvccpb.PUT {
					continue
				}

				key := string(event.Kv.Key)
				logutil.BgLogger().Info("audit notify message received", zap.String("key", key), zap.ByteString("value", event.Kv.Value))
				if _, ok = processedKeys[key]; ok {
					continue
				}

				switch key {
				case EtcdKeyNotifyRotate:
					logutil.BgLogger().Info("rotate local audit log because notify message received")
					terror.Log(m.RotateLog())
				case EtcdKeyNotifyFilter:
					terror.Log(m.ReloadFilter())
				}
			}
		case <-reloadFilterTicker.C:
			terror.Log(m.ReloadFilter())
		}
	}
	logutil.BgLogger().Info("audit globalRotateNotifyLoop exit")
}

// Stop stops the LogManager, it will stop the internal `Logger` and exit the background loops.
func (m *LogManager) Stop() {
	mu := &m.mu
	mu.Lock()
	if mu.stopFlag {
		mu.Unlock()
		return
	}
	mu.stopFlag = true
	mu.Unlock()

	m.cancel()
	m.logger.Stop()
}

// Enabled returns whether the log is enabled
func (m *LogManager) Enabled() bool {
	return m.GetConfig().Enabled
}

// SetEnabled sets whether the log should be enabled
func (m *LogManager) SetEnabled(enabled bool) error {
	return m.editConfig(func(cfg *LoggerConfig) error {
		cfg.Enabled = enabled
		return nil
	})
}

// SetLogConfigPath sets the log config path
func (m *LogManager) SetLogConfigPath(path string) error {
	return m.editConfig(func(cfg *LoggerConfig) error {
		logFilepath, err := getFilePath(path, cfg.LogFormat)
		if err != nil {
			return err
		}

		cfg.ConfigPath = path
		cfg.Filepath = logFilepath
		return nil
	})
}

// GetLogConfigPath gets the log config path
func (m *LogManager) GetLogConfigPath() string {
	return m.GetConfig().ConfigPath
}

// GetLogPath gets the absolute path of the log
func (m *LogManager) GetLogPath() string {
	return m.GetConfig().zapLoggerConfig.Filepath
}

// SetLogFormat sets the log format
func (m *LogManager) SetLogFormat(format string) error {
	return m.editConfig(func(cfg *LoggerConfig) error {
		format = strings.ToUpper(format)
		logFilePath, err := getFilePath(cfg.ConfigPath, format)
		if err != nil {
			return err
		}

		cfg.LogFormat = format
		cfg.Filepath = logFilePath
		return nil
	})
}

// GetLogFormat gets the format of log
func (m *LogManager) GetLogFormat() string {
	return m.GetConfig().LogFormat
}

// SetFileMaxSize sets the max size of a log file
func (m *LogManager) SetFileMaxSize(val int64) error {
	return m.editConfig(func(cfg *LoggerConfig) error {
		cfg.FileMaxSize = val
		return nil
	})
}

// SetFileMaxLifetime sets the max rotation time of a log file
func (m *LogManager) SetFileMaxLifetime(val int64) error {
	return m.editConfig(func(cfg *LoggerConfig) error {
		cfg.FileMaxLifetime = val
		return nil
	})
}

// GetFileMaxSize returns the max size of a log file
func (m *LogManager) GetFileMaxSize() int64 {
	return m.GetConfig().FileMaxSize
}

// GetFileMaxLifetime returns the max rotation time of a log file
func (m *LogManager) GetFileMaxLifetime() int64 {
	return m.GetConfig().FileMaxLifetime
}

// SetFileReservedBackups sets the max backup count
func (m *LogManager) SetFileReservedBackups(val int) error {
	return m.editConfig(func(cfg *LoggerConfig) error {
		cfg.FileReservedBackups = val
		return nil
	})
}

// GetFileReservedBackups returns the max backup count
func (m *LogManager) GetFileReservedBackups() int {
	return m.GetConfig().FileReservedBackups
}

// SetFileReservedDays sets the max reserved days for logs
func (m *LogManager) SetFileReservedDays(val int) error {
	return m.editConfig(func(cfg *LoggerConfig) error {
		cfg.FileReservedDays = val
		return nil
	})
}

// GetFileReservedDays returns the max reserved days for logs
func (m *LogManager) GetFileReservedDays() int {
	return m.GetConfig().FileReservedDays
}

// RedactLog indicates whether to redact the log
func (m *LogManager) RedactLog() bool {
	return m.GetConfig().Redact
}

// SetRedactLog sets whether redact the log
func (m *LogManager) SetRedactLog(val bool) error {
	return m.editConfig(func(cfg *LoggerConfig) error {
		cfg.Redact = val
		return nil
	})
}

// GetFilter returns the filter
func (m *LogManager) GetFilter() *LogFilterRuleBundle {
	return m.GetConfig().Filter
}

// hasEnabledRuleExceptAuditMgmt reports whether an enabled rule's filter includes
// event classes other than AUDIT and its subclasses. Exclusions are not evaluated.
func (m *LogManager) hasEnabledRuleExceptAuditMgmt() bool {
	bundle := m.GetFilter()
	if bundle == nil {
		return false
	}
	for _, rule := range bundle.rules {
		if !rule.Enabled || rule.Filter == nil {
			continue
		}
		if len(rule.Filter.Filter) == 0 {
			return true
		}
		for _, spec := range rule.Filter.Filter {
			if len(spec.Classes) == 0 {
				return true
			}
			for _, className := range spec.Classes {
				class, ok := getEventClass(className)
				if !ok {
					continue
				}
				switch class {
				case ClassAudit, ClassAuditSetSysVar, ClassAuditFuncCall, ClassAuditEnable, ClassAuditDisable:
					continue
				default:
					return true
				}
			}
		}
	}
	return false
}

// SetFilter sets the filter
func (m *LogManager) SetFilter(f *LogFilterRuleBundle) error {
	return m.editConfig(func(cfg *LoggerConfig) error {
		if !FilterRuleBundlesEquivalent(f, m.logger.GetConfig().Filter) {
			// Only update the Filter when the new bundle is different from the old one
			// to avoid unnecessary write lock in the logger.
			cfg.Filter = f
		}
		return nil
	})
}

// GetConfig returns the logger config
func (m *LogManager) GetConfig() LoggerConfig {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.mu.loggerCfg
}

func (m *LogManager) editConfig(fn func(*LoggerConfig) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := fn(&m.mu.loggerCfg); err != nil {
		return err
	}
	return m.logger.SetConfig(m.mu.loggerCfg)
}

// RotateLog rotates the log in current instance
func (m *LogManager) RotateLog() error {
	return m.logger.Rotate()
}

// LogEntryWithFilter filter the input entry and log it if possible.
func (m *LogManager) LogEntryWithFilter(entry func() *LogEntry, id *entryIDGenerator) {
	m.logger.LogEntryFunc(entry, id, false)
}

// ForceLogEntry forces to log the entry
func (m *LogManager) ForceLogEntry(entry *LogEntry) {
	m.logger.LogEntry(entry, newEntryIDGenerator(), true)
}

// GlobalRotateLog rotates all logs in cluster
func (m *LogManager) GlobalRotateLog() error {
	ok, err := m.globalNotify(EtcdKeyNotifyRotate)
	if err != nil {
		return err
	}

	if !ok {
		// !ok means the not a TiKV cluster, only rotate in local
		logutil.BgLogger().Info("only rotate local audit log because global notify not supported")
		return m.RotateLog()
	}
	return nil
}

func (m *LogManager) notifyFilterConfigUpdated() error {
	ok, err := m.globalNotify(EtcdKeyNotifyFilter)
	if err != nil {
		return err
	}

	if !ok {
		return m.ReloadFilter()
	}

	return nil
}

// injectReloadFilterForTest is only used for test
var injectReloadFilterForTest func(error) error

// ReloadFilter reloads filters from table
func (m *LogManager) ReloadFilter() error {
	err := m.runInTxn(m.ctx, func(exec sqlexec.SQLExecutor) error {
		rules, err := ListFilterRules(m.ctx, exec)
		if err != nil {
			return err
		}
		filter := NewLogFilterRuleBundle(rules)
		return m.SetFilter(filter)
	})
	if intest.InTest && injectReloadFilterForTest != nil {
		return injectReloadFilterForTest(err)
	}

	return err
}

// CreateLogFilter creates log filter
func (m *LogManager) CreateLogFilter(filter *LogFilter, replace bool) error {
	if err := filter.Validate(); err != nil {
		return err
	}

	normalizedFilter, err := filter.Normalize()
	if err != nil {
		return err
	}

	content, err := normalizedFilter.ToJSON()
	if err != nil {
		return err
	}

	err = m.runSQLInExecutor(func(exec sqlexec.SQLExecutor) error {
		insertSQL := InsertFilterSQL
		if replace {
			insertSQL = ReplaceFilterSQL
		}

		if _, err = executeSQL(m.ctx, exec, insertSQL, filter.Name, content); err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return err
	}

	return m.notifyFilterConfigUpdated()
}

// CreateConvenientRule creates one audit filter and one rule for each user pattern.
// users is a comma-separated list. An empty filter name is generated automatically.
// It does not change the audit enabled system variable.
func (m *LogManager) CreateConvenientRule(filter *LogFilter, users string) error {
	normalizedUsers, err := normalizeUserAndHostList(users)
	if err != nil {
		return err
	}

	if filter.Name == "" {
		err = m.runSQLInExecutor(func(exec sqlexec.SQLExecutor) error {
			rows, err := executeSQL(m.ctx, exec, SelectFilterNameListSQL)
			if err != nil {
				return err
			}
			occupied := make(map[string]struct{}, len(rows))
			for _, row := range rows {
				// Match the database comparison, which ignores trailing spaces.
				occupied[strings.TrimRight(row.GetString(0), " ")] = struct{}{}
			}
			// Choose the first unused numbered name; insertion checks for conflicts.
			for index := 1; ; index++ {
				name := fmt.Sprintf("%s%02d", convenientRuleNamePrefix, index)
				if _, ok := occupied[name]; !ok {
					filter.Name = name
					return nil
				}
			}
		})
		if err != nil {
			return err
		}
	}

	if err := filter.Validate(); err != nil {
		return err
	}
	normalizedFilter, err := filter.Normalize()
	if err != nil {
		return err
	}
	content, err := normalizedFilter.ToJSON()
	if err != nil {
		return err
	}

	err = m.runInTxn(m.ctx, func(exec sqlexec.SQLExecutor) error {
		if _, err := executeSQL(m.ctx, exec, InsertFilterSQL, filter.Name, content); err != nil {
			return err
		}
		for _, user := range normalizedUsers {
			if _, err := executeSQL(m.ctx, exec, InsertFilterRuleSQL, user, filter.Name); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	return m.notifyFilterConfigUpdated()
}

// RemoveLogFilter removes a log filter
func (m *LogManager) RemoveLogFilter(name string) error {
	err := m.runInTxn(m.ctx, func(exec sqlexec.SQLExecutor) error {
		rows, err := executeSQL(m.ctx, exec, SelectFilterRuleByFilterSQL, name)
		if err != nil {
			return err
		}

		if len(rows) > 0 {
			return errors.Errorf("filter '%s' is in use", name)
		}

		if _, err = executeSQL(m.ctx, exec, DeleteFilterByNameSQL, name); err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return err
	}

	return m.notifyFilterConfigUpdated()
}

// CreateLogFilterRule creates a log filter rule
func (m *LogManager) CreateLogFilterRule(user, filter string, replace bool) error {
	user, _, _, err := normalizeUserAndHost(user)
	if err != nil {
		return err
	}

	err = m.runInTxn(m.ctx, func(exec sqlexec.SQLExecutor) error {
		rows, err := executeSQL(m.ctx, exec, SelectFilterByNameSQL, filter)
		if err != nil {
			return err
		}

		if len(rows) == 0 {
			return errors.Errorf("cannot find filter with name '%s'", filter)
		}

		insertSQL := InsertFilterRuleSQL
		if replace {
			insertSQL = ReplaceFilterRuleSQL
		}

		if _, err = executeSQL(m.ctx, exec, insertSQL, user, filter); err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return err
	}

	return m.notifyFilterConfigUpdated()
}

// RemoveLogFilterRule removes a log filter rule
func (m *LogManager) RemoveLogFilterRule(user, filter string) error {
	err := m.runSQLInExecutor(func(exec sqlexec.SQLExecutor) error {
		if _, err := executeSQL(m.ctx, exec, DeleteFilterRuleSQL, user, filter); err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return err
	}

	return m.notifyFilterConfigUpdated()
}

// SwitchLogFilterRuleEnabled switches a log filter rule to enabled/disabled
func (m *LogManager) SwitchLogFilterRuleEnabled(user, filter string, enabled bool) error {
	err := m.runInTxn(m.ctx, func(exec sqlexec.SQLExecutor) error {
		rows, err := executeSQL(m.ctx, exec, SelectFilterRuleByUserAndFilterSQL, user, filter)
		if err != nil {
			return err
		}

		if len(rows) == 0 {
			return errors.New("rule does not exist")
		}

		enabledVal := 0
		if enabled {
			enabledVal = 1
		}

		if _, err = executeSQL(m.ctx, exec, UpdateFilterRuleEnabledSQL, enabledVal, user, filter); err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return err
	}

	return m.notifyFilterConfigUpdated()
}

// GetSessionHandler returns `*extension.SessionHandler` to register
func (m *LogManager) GetSessionHandler() *extension.SessionHandler {
	id := newEntryIDGenerator()
	return &extension.SessionHandler{
		OnConnectionEvent: func(tp extension.ConnEventTp, info *extension.ConnEventInfo) {
			m.LogEntryWithFilter(func() *LogEntry {
				return newConnEventEntry(tp, info)
			}, id)
		},
		OnStmtEvent: func(tp extension.StmtEventTp, info extension.StmtEventInfo) {
			m.LogEntryWithFilter(func() *LogEntry {
				return newStmtEventEntry(tp, info)
			}, id)
		},
		OnSecurityEvent: func(tp extension.SecurityEventTp, info extension.SecurityEventInfo) {
			m.LogEntryWithFilter(func() *LogEntry {
				return newSecurityEventEntry(tp, info)
			}, id)
		},
		OnDataOpEvent: func(tp extension.DataOpEventTp, info extension.DataOpEventInfo) {
			m.LogEntryWithFilter(func() *LogEntry {
				return newDataOpEventEntry(tp, info)
			}, id)
		},
	}
}
