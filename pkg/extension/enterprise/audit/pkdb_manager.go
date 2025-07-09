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
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"
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
)

// LogManager is used to manage audit logs
type LogManager struct {
	sync.Mutex

	ctx    context.Context
	cancel func()

	loggerRef atomic.Value
	loggerCfg LoggerConfig
	etcdCli   *clientv3.Client
	sessPool  extension.SessionPool
}

// Init inits a manager
func (m *LogManager) Init() (err error) {
	m.Lock()
	defer m.Unlock()

	// m.ctx != nil means already inited
	if m.ctx != nil {
		return nil
	}

	m.ctx = kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers)
	m.ctx, m.cancel = context.WithCancel(m.ctx)
	defer func() {
		if err != nil {
			m.Close()
		}
	}()

	logConfigPath := DefAuditLogName
	logFilepath, err := getFilePath(logConfigPath, DefAuditLogFormat)
	if err != nil {
		return err
	}

	m.loggerCfg = LoggerConfig{
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

	return m.refreshLoggerRef()
}

// Bootstrap bootstraps audit log manager
func (m *LogManager) Bootstrap(ctx extension.BootstrapContext) error {
	etcdCli := ctx.EtcdClient()
	sessPool := ctx.SessionPool()

	m.Lock()
	if m.ctx == nil {
		m.Unlock()
		return errors.New("not initialized")
	}
	m.etcdCli = etcdCli
	m.sessPool = sessPool
	m.Unlock()

	if err := m.ReloadFilter(); err != nil {
		return err
	}

	go m.handleNotifyLoop(ctx, etcdCli)
	return nil
}

func (m *LogManager) runSQLInExecutor(fn func(sqlexec.SQLExecutor) error) error {
	m.Lock()
	sessPool := m.sessPool
	m.Unlock()

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
	var cli *clientv3.Client
	m.Lock()
	cli = m.etcdCli
	m.Unlock()

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

func (m *LogManager) handleNotifyLoop(ctx context.Context, etcd *clientv3.Client) {
	if etcd == nil {
		return
	}

	watcher := newKeyWatcher(ctx, etcd, EtcdKeyNotifyPrefix)
	reloadFilterTicker := time.NewTicker(5 * time.Minute)
Loop:
	for {
		select {
		case <-ctx.Done():
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

// Close closes the log manager
func (m *LogManager) Close() {
	m.Lock()
	defer m.Unlock()
	m.ctx = nil
	m.loggerCfg = LoggerConfig{}
	m.loggerRef.Store(&Logger{})
	m.etcdCli = nil
	if m.cancel != nil {
		m.cancel()
		m.cancel = nil
	}
}

// Enabled returns whether the log is enabled
func (m *LogManager) Enabled() bool {
	m.Lock()
	defer m.Unlock()
	return m.loggerCfg.Enabled
}

// SetEnabled sets whether the log should be enabled
func (m *LogManager) SetEnabled(enabled bool) error {
	m.Lock()
	defer m.Unlock()
	m.loggerCfg.Enabled = enabled
	return m.refreshLoggerRef()
}

// SetLogConfigPath sets the log config path
func (m *LogManager) SetLogConfigPath(path string) error {
	m.Lock()
	defer m.Unlock()

	logFilepath, err := getFilePath(path, m.loggerCfg.LogFormat)
	if err != nil {
		return err
	}

	m.loggerCfg.ConfigPath = path
	m.loggerCfg.Filepath = logFilepath
	return m.refreshLoggerRef()
}

// GetLogConfigPath gets the log config path
func (m *LogManager) GetLogConfigPath() string {
	m.Lock()
	defer m.Unlock()
	return m.loggerCfg.ConfigPath
}

// GetLogPath gets the absolute path of the log
func (m *LogManager) GetLogPath() string {
	m.Lock()
	defer m.Unlock()
	return m.loggerCfg.zapLoggerConfig.Filepath
}

// SetLogFormat sets the log format
func (m *LogManager) SetLogFormat(format string) error {
	m.Lock()
	defer m.Unlock()
	format = strings.ToUpper(format)

	logFilePath, err := getFilePath(m.loggerCfg.ConfigPath, format)
	if err != nil {
		return err
	}

	m.loggerCfg.LogFormat = format
	m.loggerCfg.Filepath = logFilePath
	return m.refreshLoggerRef()
}

// GetLogFormat gets the format of log
func (m *LogManager) GetLogFormat() string {
	m.Lock()
	defer m.Unlock()
	return m.loggerCfg.LogFormat
}

// SetFileMaxSize sets the max size of a log file
func (m *LogManager) SetFileMaxSize(val int64) error {
	m.Lock()
	defer m.Unlock()
	m.loggerCfg.FileMaxSize = val
	return m.refreshLoggerRef()
}

// SetFileMaxLifetime sets the max rotation time of a log file
func (m *LogManager) SetFileMaxLifetime(val int64) error {
	m.Lock()
	defer m.Unlock()
	m.loggerCfg.FileMaxLifetime = val
	return m.refreshLoggerRef()
}

// GetFileMaxSize returns the max size of a log file
func (m *LogManager) GetFileMaxSize() int64 {
	m.Lock()
	defer m.Unlock()
	return m.loggerCfg.FileMaxSize
}

// GetFileMaxLifetime returns the max rotation time of a log file
func (m *LogManager) GetFileMaxLifetime() int64 {
	m.Lock()
	defer m.Unlock()
	return m.loggerCfg.FileMaxLifetime
}

// SetFileReservedBackups sets the max backup count
func (m *LogManager) SetFileReservedBackups(val int) error {
	m.Lock()
	defer m.Unlock()
	m.loggerCfg.FileReservedBackups = val
	return m.refreshLoggerRef()
}

// GetFileReservedBackups returns the max backup count
func (m *LogManager) GetFileReservedBackups() int {
	m.Lock()
	defer m.Unlock()
	return m.loggerCfg.FileReservedBackups
}

// SetFileReservedDays sets the max reserved days for logs
func (m *LogManager) SetFileReservedDays(val int) error {
	m.Lock()
	defer m.Unlock()
	m.loggerCfg.FileReservedDays = val
	return m.refreshLoggerRef()
}

// GetFileReservedDays returns the max reserved days for logs
func (m *LogManager) GetFileReservedDays() int {
	m.Lock()
	defer m.Unlock()
	return m.loggerCfg.FileReservedDays
}

// RedactLog indicates whether to redact the log
func (m *LogManager) RedactLog() bool {
	m.Lock()
	defer m.Unlock()
	return m.loggerCfg.Redact
}

// SetRedactLog sets whether redact the log
func (m *LogManager) SetRedactLog(val bool) error {
	m.Lock()
	defer m.Unlock()
	m.loggerCfg.Redact = val
	return m.refreshLoggerRef()
}

// GetFilter returns the filter
func (m *LogManager) GetFilter() *LogFilterRuleBundle {
	m.Lock()
	defer m.Unlock()
	return m.loggerCfg.Filter
}

// SetFilter sets the filter
func (m *LogManager) SetFilter(f *LogFilterRuleBundle) error {
	m.Lock()
	defer m.Unlock()
	m.loggerCfg.Filter = f
	return m.refreshLoggerRef()
}

// RotateLog rotates the log in current instance
func (m *LogManager) RotateLog() error {
	logutil.BgLogger().Info("rotate audit log in local")
	return m.logger().Rotate()
}

// GlobalRotateLog rotates all logs in cluster
func (m *LogManager) GlobalRotateLog() error {
	ok, err := m.globalNotify(EtcdKeyNotifyRotate)
	if err != nil {
		return err
	}

	if !ok {
		// !ok means the not a TiKV cluster, only rotate in local
		return m.RotateLog()
	}
	return nil
}

func (m *LogManager) refreshLoggerRef() error {
	oldLogger := m.logger()
	newLogger, err := LoggerWithConfig(m.ctx, m.loggerCfg, oldLogger)
	if err != nil {
		return err
	}

	if newLogger != oldLogger {
		m.loggerRef.Store(newLogger)

		if cfg := m.loggerCfg; cfg.Enabled && cfg.FileMaxLifetime > 0 {
			newLogger.loopCheckLogMaxLifetime(time.Duration(m.loggerCfg.FileMaxLifetime) * time.Second)
		}
		if oldLogger != nil && oldLogger.cfg.Enabled && oldLogger.cancel != nil {
			oldLogger.cancel()
		}
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

// ReloadFilter reloads filters from table
func (m *LogManager) ReloadFilter() error {
	return m.runInTxn(m.ctx, func(exec sqlexec.SQLExecutor) error {
		rules, err := ListFilterRules(m.ctx, exec)
		if err != nil {
			return err
		}
		filter := NewLogFilterRuleBundle(rules)
		return m.SetFilter(filter)
	})
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

func (m *LogManager) logger() *Logger {
	if ref, ok := m.loggerRef.Load().(*Logger); ok {
		return ref
	}
	return nil
}

// GetSessionHandler returns `*extension.SessionHandler` to register
func (m *LogManager) GetSessionHandler() *extension.SessionHandler {
	id := newEntryIDGenerator()
	return &extension.SessionHandler{
		OnConnectionEvent: func(tp extension.ConnEventTp, info *extension.ConnEventInfo) {
			if logger := m.logger(); logger.cfg.Enabled {
				newConnEventEntry(tp, info).Filter(logger.cfg.Filter).Log(logger, id)
			}
		},
		OnStmtEvent: func(tp extension.StmtEventTp, info extension.StmtEventInfo) {
			if logger := m.logger(); logger.cfg.Enabled {
				newStmtEventEntry(tp, info).Filter(logger.cfg.Filter).Log(logger, id)
			}
		},
		OnSecurityEvent: func(tp extension.SecurityEventTp, info extension.SecurityEventInfo) {
			if logger := m.logger(); logger.cfg.Enabled {
				newSecurityEventEntry(tp, info).Filter(logger.cfg.Filter).Log(logger, id)
			}
		},
		OnDataOpEvent: func(tp extension.DataOpEventTp, info extension.DataOpEventInfo) {
			if logger := m.logger(); logger.cfg.Enabled {
				newDataOpEventEntry(tp, info).Filter(logger.cfg.Filter).Log(logger, id)
			}
		},
	}
}
