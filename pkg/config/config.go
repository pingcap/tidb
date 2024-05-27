// Copyright 2017 PingCAP, Inc.
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

package config

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"os/user"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	zaplog "github.com/pingcap/log"
	logbackupconf "github.com/pingcap/tidb/br/pkg/streamhelper/config"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/tiflashcompute"
	"github.com/pingcap/tidb/pkg/util/tikvutil"
	"github.com/pingcap/tidb/pkg/util/versioninfo"
	tikvcfg "github.com/tikv/client-go/v2/config"
	tracing "github.com/uber/jaeger-client-go/config"
	atomicutil "go.uber.org/atomic"
	"go.uber.org/zap"
)

// Config number limitations
const (
	MaxLogFileSize = 4096 // MB
	// MaxTxnEntrySize is the max value of TxnEntrySizeLimit.
	MaxTxnEntrySizeLimit = 120 * 1024 * 1024 // 120MB
	// DefTxnEntrySizeLimit is the default value of TxnEntrySizeLimit.
	DefTxnEntrySizeLimit = 6 * 1024 * 1024
	// DefTxnTotalSizeLimit is the default value of TxnTxnTotalSizeLimit.
	DefTxnTotalSizeLimit        = 100 * 1024 * 1024
	SuperLargeTxnSize    uint64 = 100 * 1024 * 1024 * 1024 * 1024 // 100T, we expect a txn can never be this large
	// DefMaxIndexLength is the maximum index length(in bytes). This value is consistent with MySQL.
	DefMaxIndexLength = 3072
	// DefMaxOfMaxIndexLength is the maximum index length(in bytes) for TiDB v3.0.7 and previous version.
	DefMaxOfMaxIndexLength = 3072 * 4
	// DefIndexLimit is the limitation of index on a single table. This value is consistent with MySQL.
	DefIndexLimit = 64
	// DefMaxOfIndexLimit is the maximum limitation of index on a single table for TiDB.
	DefMaxOfIndexLimit = 64 * 8
	// DefPort is the default port of TiDB
	DefPort = 4000
	// DefStatusPort is the default status port of TiDB
	DefStatusPort = 10080
	// DefHost is the default host of TiDB
	DefHost = "0.0.0.0"
	// DefStatusHost is the default status host of TiDB
	DefStatusHost = "0.0.0.0"
	// DefTableColumnCountLimit is limit of the number of columns in a table
	DefTableColumnCountLimit = 1017
	// DefMaxOfTableColumnCountLimit is maximum limitation of the number of columns in a table
	DefMaxOfTableColumnCountLimit = 4096
	// DefStatsLoadConcurrencyLimit is limit of the concurrency of stats-load. When it is set to 0, it will be set by syncload.GetSyncLoadConcurrencyByCPU.
	DefStatsLoadConcurrencyLimit = 0
	// DefMaxOfStatsLoadConcurrencyLimit is maximum limitation of the concurrency of stats-load
	DefMaxOfStatsLoadConcurrencyLimit = 128
	// DefStatsLoadQueueSizeLimit is limit of the size of stats-load request queue
	DefStatsLoadQueueSizeLimit = 1
	// DefMaxOfStatsLoadQueueSizeLimit is maximum limitation of the size of stats-load request queue
	DefMaxOfStatsLoadQueueSizeLimit = 100000
	// DefDDLSlowOprThreshold sets log DDL operations whose execution time exceeds the threshold value.
	DefDDLSlowOprThreshold = 300
	// DefExpensiveQueryTimeThreshold indicates the time threshold of expensive query.
	DefExpensiveQueryTimeThreshold = 60
	// DefExpensiveTxnTimeThreshold indicates the time threshold of expensive txn.
	DefExpensiveTxnTimeThreshold = 600
	// DefMemoryUsageAlarmRatio is the threshold triggering an alarm which the memory usage of tidb-server instance exceeds.
	DefMemoryUsageAlarmRatio = 0.8
	// DefTempDir is the default temporary directory path for TiDB.
	DefTempDir = "/tmp/tidb"
	// DefAuthTokenRefreshInterval is the default time interval to refresh tidb auth token.
	DefAuthTokenRefreshInterval = time.Hour
	// EnvVarKeyspaceName is the system env name for keyspace name.
	EnvVarKeyspaceName = "KEYSPACE_NAME"
	// MaxTokenLimit is the max token limit value.
	MaxTokenLimit = 1024 * 1024
)

// Valid config maps
var (
	ValidStorage = map[string]bool{
		"mocktikv": true,
		"tikv":     true,
		"unistore": true,
	}
	// CheckTableBeforeDrop enable to execute `admin check table` before `drop table`.
	CheckTableBeforeDrop = false
	// checkBeforeDropLDFlag is a go build flag.
	checkBeforeDropLDFlag = "None"
	// tempStorageDirName is the default temporary storage dir name by base64 encoding a string `port/statusPort`
	tempStorageDirName = encodeDefTempStorageDir(os.TempDir(), DefHost, DefStatusHost, DefPort, DefStatusPort)
)

// InstanceConfigSection indicates a config session that has options moved to [instance] session.
type InstanceConfigSection struct {
	// SectionName indicates the origin section name.
	SectionName string
	// NameMappings maps the origin name to the name in [instance].
	NameMappings map[string]string
}

var (
	// sectionMovedToInstance records all config section and options that should be moved to [instance].
	sectionMovedToInstance = []InstanceConfigSection{
		{
			"",
			map[string]string{
				"check-mb4-value-in-utf8":       "tidb_check_mb4_value_in_utf8",
				"enable-collect-execution-info": "tidb_enable_collect_execution_info",
				"max-server-connections":        "max_connections",
				"run-ddl":                       "tidb_enable_ddl",
			},
		},
		{
			"log",
			map[string]string{
				"enable-slow-log":         "tidb_enable_slow_log",
				"slow-threshold":          "tidb_slow_log_threshold",
				"record-plan-in-slow-log": "tidb_record_plan_in_slow_log",
			},
		},
		{
			"performance",
			map[string]string{
				"force-priority":           "tidb_force_priority",
				"memory-usage-alarm-ratio": "tidb_memory_usage_alarm_ratio",
			},
		},
		{
			"plugin",
			map[string]string{
				"load": "plugin_load",
				"dir":  "plugin_dir",
			},
		},
	}

	// ConflictOptions indicates the conflict config options existing in both [instance] and other sections in config file.
	ConflictOptions []InstanceConfigSection

	// DeprecatedOptions indicates the config options existing in some other sections in config file.
	// They should be moved to [instance] section.
	DeprecatedOptions []InstanceConfigSection

	// TikvConfigLock protects against concurrent tikv config refresh
	TikvConfigLock sync.Mutex
)

// Config contains configuration options.
type Config struct {
	Host             string `toml:"host" json:"host"`
	AdvertiseAddress string `toml:"advertise-address" json:"advertise-address"`
	Port             uint   `toml:"port" json:"port"`
	Cors             string `toml:"cors" json:"cors"`
	Store            string `toml:"store" json:"store"`
	Path             string `toml:"path" json:"path"`
	Socket           string `toml:"socket" json:"socket"`
	Lease            string `toml:"lease" json:"lease"`
	SplitTable       bool   `toml:"split-table" json:"split-table"`
	TokenLimit       uint   `toml:"token-limit" json:"token-limit"`
	TempDir          string `toml:"temp-dir" json:"temp-dir"`
	TempStoragePath  string `toml:"tmp-storage-path" json:"tmp-storage-path"`
	// TempStorageQuota describe the temporary storage Quota during query exector when TiDBEnableTmpStorageOnOOM is enabled
	// If the quota exceed the capacity of the TempStoragePath, the tidb-server would exit with fatal error
	TempStorageQuota           int64                   `toml:"tmp-storage-quota" json:"tmp-storage-quota"` // Bytes
	TxnLocalLatches            tikvcfg.TxnLocalLatches `toml:"-" json:"-"`
	ServerVersion              string                  `toml:"server-version" json:"server-version"`
	VersionComment             string                  `toml:"version-comment" json:"version-comment"`
	TiDBEdition                string                  `toml:"tidb-edition" json:"tidb-edition"`
	TiDBReleaseVersion         string                  `toml:"tidb-release-version" json:"tidb-release-version"`
	KeyspaceName               string                  `toml:"keyspace-name" json:"keyspace-name"`
	Log                        Log                     `toml:"log" json:"log"`
	Instance                   Instance                `toml:"instance" json:"instance"`
	Security                   Security                `toml:"security" json:"security"`
	Status                     Status                  `toml:"status" json:"status"`
	Performance                Performance             `toml:"performance" json:"performance"`
	PreparedPlanCache          PreparedPlanCache       `toml:"prepared-plan-cache" json:"prepared-plan-cache"`
	OpenTracing                OpenTracing             `toml:"opentracing" json:"opentracing"`
	ProxyProtocol              ProxyProtocol           `toml:"proxy-protocol" json:"proxy-protocol"`
	PDClient                   tikvcfg.PDClient        `toml:"pd-client" json:"pd-client"`
	TiKVClient                 tikvcfg.TiKVClient      `toml:"tikv-client" json:"tikv-client"`
	Binlog                     Binlog                  `toml:"binlog" json:"binlog"`
	CompatibleKillQuery        bool                    `toml:"compatible-kill-query" json:"compatible-kill-query"`
	PessimisticTxn             PessimisticTxn          `toml:"pessimistic-txn" json:"pessimistic-txn"`
	MaxIndexLength             int                     `toml:"max-index-length" json:"max-index-length"`
	IndexLimit                 int                     `toml:"index-limit" json:"index-limit"`
	TableColumnCountLimit      uint32                  `toml:"table-column-count-limit" json:"table-column-count-limit"`
	GracefulWaitBeforeShutdown int                     `toml:"graceful-wait-before-shutdown" json:"graceful-wait-before-shutdown"`
	// AlterPrimaryKey is used to control alter primary key feature.
	AlterPrimaryKey bool `toml:"alter-primary-key" json:"alter-primary-key"`
	// TreatOldVersionUTF8AsUTF8MB4 is use to treat old version table/column UTF8 charset as UTF8MB4. This is for compatibility.
	// Currently not support dynamic modify, because this need to reload all old version schema.
	TreatOldVersionUTF8AsUTF8MB4 bool `toml:"treat-old-version-utf8-as-utf8mb4" json:"treat-old-version-utf8-as-utf8mb4"`
	// EnableTableLock indicate whether enable table lock.
	// TODO: remove this after table lock features stable.
	EnableTableLock     bool   `toml:"enable-table-lock" json:"enable-table-lock"`
	DelayCleanTableLock uint64 `toml:"delay-clean-table-lock" json:"delay-clean-table-lock"`
	SplitRegionMaxNum   uint64 `toml:"split-region-max-num" json:"split-region-max-num"`
	TopSQL              TopSQL `toml:"top-sql" json:"top-sql"`
	// RepairMode indicates that the TiDB is in the repair mode for table meta.
	RepairMode      bool     `toml:"repair-mode" json:"repair-mode"`
	RepairTableList []string `toml:"repair-table-list" json:"repair-table-list"`
	// IsolationRead indicates that the TiDB reads data from which isolation level(engine and label).
	IsolationRead IsolationRead `toml:"isolation-read" json:"isolation-read"`
	// NewCollationsEnabledOnFirstBootstrap indicates if the new collations are enabled, it effects only when a TiDB cluster bootstrapped on the first time.
	NewCollationsEnabledOnFirstBootstrap bool `toml:"new_collations_enabled_on_first_bootstrap" json:"new_collations_enabled_on_first_bootstrap"`
	// Experimental contains parameters for experimental features.
	Experimental Experimental `toml:"experimental" json:"experimental"`
	// SkipRegisterToDashboard tells TiDB don't register itself to the dashboard.
	SkipRegisterToDashboard bool `toml:"skip-register-to-dashboard" json:"skip-register-to-dashboard"`
	// EnableTelemetry enables the usage data report to PingCAP. Deprecated: Telemetry has been removed.
	EnableTelemetry bool `toml:"enable-telemetry" json:"enable-telemetry"`
	// Labels indicates the labels set for the tidb server. The labels describe some specific properties for the tidb
	// server like `zone`/`rack`/`host`. Currently, labels won't affect the tidb server except for some special
	// label keys. Now we have following special keys:
	// 1. 'group' is a special label key which should be automatically set by tidb-operator. We don't suggest
	// users to set 'group' in labels.
	// 2. 'zone' is a special key that indicates the DC location of this tidb-server. If it is set, the value for this
	// key will be the default value of the session variable `txn_scope` for this tidb-server.
	Labels map[string]string `toml:"labels" json:"labels"`

	// EnableGlobalIndex is deprecated.
	EnableGlobalIndex bool `toml:"enable-global-index" json:"enable-global-index"`

	// DeprecateIntegerDisplayWidth indicates whether deprecating the max display length for integer.
	DeprecateIntegerDisplayWidth bool `toml:"deprecate-integer-display-length" json:"deprecate-integer-display-length"`
	// EnableEnumLengthLimit indicates whether the enum/set element length is limited.
	// According to MySQL 8.0 Refman:
	// The maximum supported length of an individual SET element is M <= 255 and (M x w) <= 1020,
	// where M is the element literal length and w is the number of bytes required for the maximum-length character in the character set.
	// See https://dev.mysql.com/doc/refman/8.0/en/string-type-syntax.html for more details.
	EnableEnumLengthLimit bool `toml:"enable-enum-length-limit" json:"enable-enum-length-limit"`
	// StoresRefreshInterval indicates the interval of refreshing stores info, the unit is second.
	StoresRefreshInterval uint64 `toml:"stores-refresh-interval" json:"stores-refresh-interval"`
	// EnableTCP4Only enables net.Listen("tcp4",...)
	// Note that: it can make lvs with toa work and thus tidb can get real client ip.
	EnableTCP4Only bool `toml:"enable-tcp4-only" json:"enable-tcp4-only"`
	// The client will forward the requests through the follower
	// if one of the following conditions happens:
	// 1. there is a network partition problem between TiDB and PD leader.
	// 2. there is a network partition problem between TiDB and TiKV leader.
	EnableForwarding bool `toml:"enable-forwarding" json:"enable-forwarding"`
	// MaxBallastObjectSize set the max size of the ballast object, the unit is byte.
	// The default value is the smallest of the following two values: 2GB or
	// one quarter of the total physical memory in the current system.
	MaxBallastObjectSize int `toml:"max-ballast-object-size" json:"max-ballast-object-size"`
	// BallastObjectSize set the initial size of the ballast object, the unit is byte.
	BallastObjectSize int        `toml:"ballast-object-size" json:"ballast-object-size"`
	TrxSummary        TrxSummary `toml:"transaction-summary" json:"transaction-summary"`
	// EnableGlobalKill indicates whether to enable global kill.
	EnableGlobalKill bool `toml:"enable-global-kill" json:"enable-global-kill"`
	// Enable32BitsConnectionID indicates whether to enable 32bits connection ID for global kill.
	Enable32BitsConnectionID bool `toml:"enable-32bits-connection-id" json:"enable-32bits-connection-id"`
	// InitializeSQLFile is a file that will be executed after first bootstrap only.
	// It can be used to set GLOBAL system variable values
	InitializeSQLFile string `toml:"initialize-sql-file" json:"initialize-sql-file"`

	// The following items are deprecated. We need to keep them here temporarily
	// to support the upgrade process. They can be removed in future.

	// EnableBatchDML, MemQuotaQuery, OOMAction unused since bootstrap v90
	EnableBatchDML bool   `toml:"enable-batch-dml" json:"enable-batch-dml"`
	MemQuotaQuery  int64  `toml:"mem-quota-query" json:"mem-quota-query"`
	OOMAction      string `toml:"oom-action" json:"oom-action"`

	// OOMUseTmpStorage unused since bootstrap v93
	OOMUseTmpStorage bool `toml:"oom-use-tmp-storage" json:"oom-use-tmp-storage"`

	// These items are deprecated because they are turned into instance system variables.
	CheckMb4ValueInUTF8        AtomicBool `toml:"check-mb4-value-in-utf8" json:"check-mb4-value-in-utf8"`
	EnableCollectExecutionInfo bool       `toml:"enable-collect-execution-info" json:"enable-collect-execution-info"`
	Plugin                     Plugin     `toml:"plugin" json:"plugin"`
	MaxServerConnections       uint32     `toml:"max-server-connections" json:"max-server-connections"`
	RunDDL                     bool       `toml:"run-ddl" json:"run-ddl"`

	// These configs are related to disaggregated-tiflash mode.
	DisaggregatedTiFlash         bool   `toml:"disaggregated-tiflash" json:"disaggregated-tiflash"`
	TiFlashComputeAutoScalerType string `toml:"autoscaler-type" json:"autoscaler-type"`
	TiFlashComputeAutoScalerAddr string `toml:"autoscaler-addr" json:"autoscaler-addr"`
	IsTiFlashComputeFixedPool    bool   `toml:"is-tiflashcompute-fixed-pool" json:"is-tiflashcompute-fixed-pool"`
	AutoScalerClusterID          string `toml:"autoscaler-cluster-id" json:"autoscaler-cluster-id"`
	UseAutoScaler                bool   `toml:"use-autoscaler" json:"use-autoscaler"`

	// TiDBMaxReuseChunk indicates max cached chunk num
	TiDBMaxReuseChunk uint32 `toml:"tidb-max-reuse-chunk" json:"tidb-max-reuse-chunk"`
	// TiDBMaxReuseColumn indicates max cached column num
	TiDBMaxReuseColumn uint32 `toml:"tidb-max-reuse-column" json:"tidb-max-reuse-column"`
	// TiDBEnableExitCheck indicates whether exit-checking in domain for background process
	TiDBEnableExitCheck bool `toml:"tidb-enable-exit-check" json:"tidb-enable-exit-check"`

	// InMemSlowQueryTopNNum indicates the number of TopN slow queries stored in memory.
	InMemSlowQueryTopNNum int `toml:"in-mem-slow-query-topn-num" json:"in-mem-slow-query-topn-num"`
	// InMemSlowQueryRecentNum indicates the number of recent slow queries stored in memory.
	InMemSlowQueryRecentNum int `toml:"in-mem-slow-query-recent-num" json:"in-mem-slow-query-recent-num"`
}

// UpdateTempStoragePath is to update the `TempStoragePath` if port/statusPort was changed
// and the `tmp-storage-path` was not specified in the conf.toml or was specified the same as the default value.
func (c *Config) UpdateTempStoragePath() {
	if c.TempStoragePath == tempStorageDirName {
		c.TempStoragePath = encodeDefTempStorageDir(os.TempDir(), c.Host, c.Status.StatusHost, c.Port, c.Status.StatusPort)
	} else {
		c.TempStoragePath = encodeDefTempStorageDir(c.TempStoragePath, c.Host, c.Status.StatusHost, c.Port, c.Status.StatusPort)
	}
}

// GetTiKVConfig returns configuration options from tikvcfg
func (c *Config) GetTiKVConfig() *tikvcfg.Config {
	return &tikvcfg.Config{
		CommitterConcurrency:  int(tikvutil.CommitterConcurrency.Load()),
		MaxTxnTTL:             c.Performance.MaxTxnTTL,
		TiKVClient:            c.TiKVClient,
		Security:              c.Security.ClusterSecurity(),
		PDClient:              c.PDClient,
		PessimisticTxn:        tikvcfg.PessimisticTxn{MaxRetryCount: c.PessimisticTxn.MaxRetryCount},
		TxnLocalLatches:       c.TxnLocalLatches,
		StoresRefreshInterval: c.StoresRefreshInterval,
		OpenTracingEnable:     c.OpenTracing.Enable,
		Path:                  c.Path,
		EnableForwarding:      c.EnableForwarding,
		TxnScope:              c.Labels["zone"],
	}
}

func encodeDefTempStorageDir(tempDir string, host, statusHost string, port, statusPort uint) string {
	dirName := base64.URLEncoding.EncodeToString([]byte(fmt.Sprintf("%v:%v/%v:%v", host, port, statusHost, statusPort)))
	osUID := ""
	currentUser, err := user.Current()
	if err == nil {
		osUID = currentUser.Uid
	}
	return filepath.Join(tempDir, osUID+"_tidb", dirName, "tmp-storage")
}

// nullableBool defaults unset bool options to unset instead of false, which enables us to know if the user has set 2
// conflict options at the same time.
type nullableBool struct {
	IsValid bool
	IsTrue  bool
}

var (
	nbUnset = nullableBool{false, false}
	nbFalse = nullableBool{true, false}
	nbTrue  = nullableBool{true, true}
)

func (b *nullableBool) toBool() bool {
	return b.IsValid && b.IsTrue
}

func (b nullableBool) MarshalJSON() ([]byte, error) {
	switch b {
	case nbTrue:
		return json.Marshal(true)
	case nbFalse:
		return json.Marshal(false)
	default:
		return json.Marshal(nil)
	}
}

func (b *nullableBool) UnmarshalText(text []byte) error {
	str := string(text)
	switch str {
	case "", "null":
		*b = nbUnset
		return nil
	case "true":
		*b = nbTrue
	case "false":
		*b = nbFalse
	default:
		*b = nbUnset
		return errors.New("Invalid value for bool type: " + str)
	}
	return nil
}

func (b nullableBool) MarshalText() ([]byte, error) {
	if !b.IsValid {
		return []byte(""), nil
	}
	if b.IsTrue {
		return []byte("true"), nil
	}
	return []byte("false"), nil
}

func (b *nullableBool) UnmarshalJSON(data []byte) error {
	var err error
	var v any
	if err = json.Unmarshal(data, &v); err != nil {
		return err
	}
	switch raw := v.(type) {
	case bool:
		*b = nullableBool{true, raw}
	default:
		*b = nbUnset
	}
	return err
}

// AtomicBool is a helper type for atomic operations on a boolean value.
type AtomicBool struct {
	atomicutil.Bool
}

// NewAtomicBool creates an AtomicBool.
func NewAtomicBool(v bool) *AtomicBool {
	return &AtomicBool{*atomicutil.NewBool(v)}
}

// MarshalText implements the encoding.TextMarshaler interface.
func (b AtomicBool) MarshalText() ([]byte, error) {
	if b.Load() {
		return []byte("true"), nil
	}
	return []byte("false"), nil
}

// UnmarshalText implements the encoding.TextUnmarshaler interface.
func (b *AtomicBool) UnmarshalText(text []byte) error {
	str := string(text)
	switch str {
	case "", "null":
		*b = AtomicBool{*atomicutil.NewBool(false)}
	case "true":
		*b = AtomicBool{*atomicutil.NewBool(true)}
	case "false":
		*b = AtomicBool{*atomicutil.NewBool(false)}
	default:
		*b = AtomicBool{*atomicutil.NewBool(false)}
		return errors.New("Invalid value for bool type: " + str)
	}
	return nil
}

// LogBackup is the config for log backup service.
// For now, it includes the embed advancer.
type LogBackup struct {
	Advancer logbackupconf.Config `toml:"advancer" json:"advancer"`
	Enabled  bool                 `toml:"enabled" json:"enabled"`
}

// Log is the log section of config.
type Log struct {
	// Log level.
	Level string `toml:"level" json:"level"`
	// Log format, one of json or text.
	Format string `toml:"format" json:"format"`
	// Disable automatic timestamps in output. Deprecated: use EnableTimestamp instead.
	DisableTimestamp nullableBool `toml:"disable-timestamp" json:"disable-timestamp"`
	// EnableTimestamp enables automatic timestamps in log output.
	EnableTimestamp nullableBool `toml:"enable-timestamp" json:"enable-timestamp"`
	// DisableErrorStack stops annotating logs with the full stack error
	// message. Deprecated: use EnableErrorStack instead.
	DisableErrorStack nullableBool `toml:"disable-error-stack" json:"disable-error-stack"`
	// EnableErrorStack enables annotating logs with the full stack error
	// message.
	EnableErrorStack nullableBool `toml:"enable-error-stack" json:"enable-error-stack"`
	// File log config.
	File logutil.FileLogConfig `toml:"file" json:"file"`

	SlowQueryFile string `toml:"slow-query-file" json:"slow-query-file"`
	// ExpensiveThreshold is deprecated.
	ExpensiveThreshold uint `toml:"expensive-threshold" json:"expensive-threshold"`

	GeneralLogFile string `toml:"general-log-file" json:"general-log-file"`

	// The following items are deprecated. We need to keep them here temporarily
	// to support the upgrade process. They can be removed in future.

	// QueryLogMaxLen unused since bootstrap v90
	QueryLogMaxLen uint64 `toml:"query-log-max-len" json:"query-log-max-len"`
	// EnableSlowLog, SlowThreshold, RecordPlanInSlowLog are deprecated.
	EnableSlowLog       AtomicBool `toml:"enable-slow-log" json:"enable-slow-log"`
	SlowThreshold       uint64     `toml:"slow-threshold" json:"slow-threshold"`
	RecordPlanInSlowLog uint32     `toml:"record-plan-in-slow-log" json:"record-plan-in-slow-log"`

	// Make tidb panic if write log operation hang in `Timeout` seconds
	Timeout int `toml:"timeout" json:"timeout"`
}

// Instance is the section of instance scope system variables.
type Instance struct {
	// These variables only exist in [instance] section.

	// TiDBGeneralLog is used to log every query in the server in info level.
	TiDBGeneralLog bool `toml:"tidb_general_log" json:"tidb_general_log"`
	// EnablePProfSQLCPU is used to add label sql label to pprof result.
	EnablePProfSQLCPU bool `toml:"tidb_pprof_sql_cpu" json:"tidb_pprof_sql_cpu"`
	// DDLSlowOprThreshold sets log DDL operations whose execution time exceeds the threshold value.
	DDLSlowOprThreshold uint32 `toml:"ddl_slow_threshold" json:"ddl_slow_threshold"`
	// ExpensiveQueryTimeThreshold indicates the time threshold of expensive query.
	ExpensiveQueryTimeThreshold uint64 `toml:"tidb_expensive_query_time_threshold" json:"tidb_expensive_query_time_threshold"`
	// ExpensiveTxnTimeThreshold indicates the time threshold of expensive transaction.
	ExpensiveTxnTimeThreshold uint64 `toml:"tidb_expensive_txn_time_threshold" json:"tidb_expensive_txn_time_threshold"`
	// StmtSummaryEnablePersistent indicates whether to enable file persistence for stmtsummary.
	StmtSummaryEnablePersistent bool `toml:"tidb_stmt_summary_enable_persistent" json:"tidb_stmt_summary_enable_persistent"`
	// StmtSummaryFilename indicates the file name written by stmtsummary
	// when StmtSummaryEnablePersistent is true.
	StmtSummaryFilename string `toml:"tidb_stmt_summary_filename" json:"tidb_stmt_summary_filename"`
	// StmtSummaryFileMaxDays indicates how many days the files written by
	// stmtsummary will be kept when StmtSummaryEnablePersistent is true.
	StmtSummaryFileMaxDays int `toml:"tidb_stmt_summary_file_max_days" json:"tidb_stmt_summary_file_max_days"`
	// StmtSummaryFileMaxSize indicates the maximum size (in mb) of a single file
	// written by stmtsummary when StmtSummaryEnablePersistent is true.
	StmtSummaryFileMaxSize int `toml:"tidb_stmt_summary_file_max_size" json:"tidb_stmt_summary_file_max_size"`
	// StmtSummaryFileMaxBackups indicates the maximum number of files written
	// by stmtsummary when StmtSummaryEnablePersistent is true.
	StmtSummaryFileMaxBackups int `toml:"tidb_stmt_summary_file_max_backups" json:"tidb_stmt_summary_file_max_backups"`

	// These variables exist in both 'instance' section and another place.
	// The configuration in 'instance' section takes precedence.

	EnableSlowLog         AtomicBool `toml:"tidb_enable_slow_log" json:"tidb_enable_slow_log"`
	SlowThreshold         uint64     `toml:"tidb_slow_log_threshold" json:"tidb_slow_log_threshold"`
	RecordPlanInSlowLog   uint32     `toml:"tidb_record_plan_in_slow_log" json:"tidb_record_plan_in_slow_log"`
	CheckMb4ValueInUTF8   AtomicBool `toml:"tidb_check_mb4_value_in_utf8" json:"tidb_check_mb4_value_in_utf8"`
	ForcePriority         string     `toml:"tidb_force_priority" json:"tidb_force_priority"`
	MemoryUsageAlarmRatio float64    `toml:"tidb_memory_usage_alarm_ratio" json:"tidb_memory_usage_alarm_ratio"`
	// EnableCollectExecutionInfo enables the TiDB to collect execution info.
	EnableCollectExecutionInfo AtomicBool `toml:"tidb_enable_collect_execution_info" json:"tidb_enable_collect_execution_info"`
	PluginDir                  string     `toml:"plugin_dir" json:"plugin_dir"`
	PluginLoad                 string     `toml:"plugin_load" json:"plugin_load"`
	// MaxConnections is the maximum permitted number of simultaneous client connections.
	MaxConnections    uint32     `toml:"max_connections" json:"max_connections"`
	TiDBEnableDDL     AtomicBool `toml:"tidb_enable_ddl" json:"tidb_enable_ddl"`
	TiDBRCReadCheckTS bool       `toml:"tidb_rc_read_check_ts" json:"tidb_rc_read_check_ts"`
	// TiDBServiceScope indicates the role for tidb for distributed task framework.
	TiDBServiceScope string `toml:"tidb_service_scope" json:"tidb_service_scope"`
}

func (l *Log) getDisableTimestamp() bool {
	if l.EnableTimestamp == nbUnset && l.DisableTimestamp == nbUnset {
		return false
	}
	if l.EnableTimestamp == nbUnset {
		return l.DisableTimestamp.toBool()
	}
	return !l.EnableTimestamp.toBool()
}

func (l *Log) getDisableErrorStack() bool {
	if l.EnableErrorStack == nbUnset && l.DisableErrorStack == nbUnset {
		return true
	}
	if l.EnableErrorStack == nbUnset {
		return l.DisableErrorStack.toBool()
	}
	return !l.EnableErrorStack.toBool()
}

// The following constants represents the valid action configurations for Security.SpilledFileEncryptionMethod.
// "plaintext" means encryption is disabled.
// NOTE: Although the values is case insensitive, we should use lower-case
// strings because the configuration value will be transformed to lower-case
// string and compared with these constants in the further usage.
const (
	SpilledFileEncryptionMethodPlaintext = "plaintext"
	SpilledFileEncryptionMethodAES128CTR = "aes128-ctr"
)

// Security is the security section of the config.
type Security struct {
	SkipGrantTable  bool     `toml:"skip-grant-table" json:"skip-grant-table"`
	SSLCA           string   `toml:"ssl-ca" json:"ssl-ca"`
	SSLCert         string   `toml:"ssl-cert" json:"ssl-cert"`
	SSLKey          string   `toml:"ssl-key" json:"ssl-key"`
	ClusterSSLCA    string   `toml:"cluster-ssl-ca" json:"cluster-ssl-ca"`
	ClusterSSLCert  string   `toml:"cluster-ssl-cert" json:"cluster-ssl-cert"`
	ClusterSSLKey   string   `toml:"cluster-ssl-key" json:"cluster-ssl-key"`
	ClusterVerifyCN []string `toml:"cluster-verify-cn" json:"cluster-verify-cn"`
	// Used for auth plugin `tidb_session_token`.
	SessionTokenSigningCert string `toml:"session-token-signing-cert" json:"session-token-signing-cert"`
	SessionTokenSigningKey  string `toml:"session-token-signing-key" json:"session-token-signing-key"`
	// If set to "plaintext", the spilled files will not be encrypted.
	SpilledFileEncryptionMethod string `toml:"spilled-file-encryption-method" json:"spilled-file-encryption-method"`
	// EnableSEM prevents SUPER users from having full access.
	EnableSEM bool `toml:"enable-sem" json:"enable-sem"`
	// Allow automatic TLS certificate generation
	AutoTLS         bool   `toml:"auto-tls" json:"auto-tls"`
	MinTLSVersion   string `toml:"tls-version" json:"tls-version"`
	RSAKeySize      int    `toml:"rsa-key-size" json:"rsa-key-size"`
	SecureBootstrap bool   `toml:"secure-bootstrap" json:"secure-bootstrap"`
	// The path of the JWKS for tidb_auth_token authentication
	AuthTokenJWKS string `toml:"auth-token-jwks" json:"auth-token-jwks"`
	// The refresh time interval of JWKS
	AuthTokenRefreshInterval string `toml:"auth-token-refresh-interval" json:"auth-token-refresh-interval"`
	// Disconnect directly when the password is expired
	DisconnectOnExpiredPassword bool `toml:"disconnect-on-expired-password" json:"disconnect-on-expired-password"`
}

// The ErrConfigValidationFailed error is used so that external callers can do a type assertion
// to defer handling of this specific error when someone does not want strict type checking.
// This is needed only because logging hasn't been set up at the time we parse the config file.
// This should all be ripped out once strict config checking is made the default behavior.
type ErrConfigValidationFailed struct {
	confFile       string
	UndecodedItems []string
}

func (e *ErrConfigValidationFailed) Error() string {
	return fmt.Sprintf("config file %s contained invalid configuration options: %s; check "+
		"TiDB manual to make sure this option has not been deprecated and removed from your TiDB "+
		"version if the option does not appear to be a typo", e.confFile, strings.Join(
		e.UndecodedItems, ", "))
}

// ErrConfigInstanceSection error is used to warning the user
// which config options should be moved to 'instance'.
type ErrConfigInstanceSection struct {
	confFile           string
	configSections     *[]InstanceConfigSection
	deprecatedSections *[]InstanceConfigSection
}

func (e *ErrConfigInstanceSection) Error() string {
	var builder strings.Builder
	if len(*e.configSections) > 0 {
		builder.WriteString("Conflict configuration options exists on both [instance] section and some other sections. ")
	}
	if len(*e.deprecatedSections) > 0 {
		builder.WriteString("Some configuration options should be moved to [instance] section. ")
	}
	builder.WriteString("Please use the latter config options in [instance] instead: ")
	for _, configSection := range *e.configSections {
		for oldName, newName := range configSection.NameMappings {
			builder.WriteString(fmt.Sprintf(" (%s, %s)", oldName, newName))
		}
	}
	for _, configSection := range *e.deprecatedSections {
		for oldName, newName := range configSection.NameMappings {
			builder.WriteString(fmt.Sprintf(" (%s, %s)", oldName, newName))
		}
	}
	builder.WriteString(".")

	return builder.String()
}

// ClusterSecurity returns Security info for cluster
func (s *Security) ClusterSecurity() tikvcfg.Security {
	return tikvcfg.NewSecurity(s.ClusterSSLCA, s.ClusterSSLCert, s.ClusterSSLKey, s.ClusterVerifyCN)
}

// Status is the status section of the config.
type Status struct {
	StatusHost      string `toml:"status-host" json:"status-host"`
	MetricsAddr     string `toml:"metrics-addr" json:"metrics-addr"`
	StatusPort      uint   `toml:"status-port" json:"status-port"`
	MetricsInterval uint   `toml:"metrics-interval" json:"metrics-interval"`
	ReportStatus    bool   `toml:"report-status" json:"report-status"`
	RecordQPSbyDB   bool   `toml:"record-db-qps" json:"record-db-qps"`
	RecordDBLabel   bool   `toml:"record-db-label" json:"record-db-label"`
	// After a duration of this time in seconds if the server doesn't see any activity it pings
	// the client to see if the transport is still alive.
	GRPCKeepAliveTime uint `toml:"grpc-keepalive-time" json:"grpc-keepalive-time"`
	// After having pinged for keepalive check, the server waits for a duration of timeout in seconds
	// and if no activity is seen even after that the connection is closed.
	GRPCKeepAliveTimeout uint `toml:"grpc-keepalive-timeout" json:"grpc-keepalive-timeout"`
	// The number of max concurrent streams/requests on a client connection.
	GRPCConcurrentStreams uint `toml:"grpc-concurrent-streams" json:"grpc-concurrent-streams"`
	// Sets window size for stream. The default value is 2MB.
	GRPCInitialWindowSize int `toml:"grpc-initial-window-size" json:"grpc-initial-window-size"`
	// Set maximum message length in bytes that gRPC can send. `-1` means unlimited. The default value is 10MB.
	GRPCMaxSendMsgSize int `toml:"grpc-max-send-msg-size" json:"grpc-max-send-msg-size"`
}

// Performance is the performance section of the config.
type Performance struct {
	MaxProcs uint `toml:"max-procs" json:"max-procs"`
	// Deprecated: use ServerMemoryQuota instead
	MaxMemory         uint64 `toml:"max-memory" json:"max-memory"`
	ServerMemoryQuota uint64 `toml:"server-memory-quota" json:"server-memory-quota"`
	StatsLease        string `toml:"stats-lease" json:"stats-lease"`
	// Deprecated: transaction auto retry is deprecated.
	StmtCountLimit      uint    `toml:"stmt-count-limit" json:"stmt-count-limit"`
	PseudoEstimateRatio float64 `toml:"pseudo-estimate-ratio" json:"pseudo-estimate-ratio"`
	BindInfoLease       string  `toml:"bind-info-lease" json:"bind-info-lease"`
	TxnEntrySizeLimit   uint64  `toml:"txn-entry-size-limit" json:"txn-entry-size-limit"`
	TxnTotalSizeLimit   uint64  `toml:"txn-total-size-limit" json:"txn-total-size-limit"`
	TCPKeepAlive        bool    `toml:"tcp-keep-alive" json:"tcp-keep-alive"`
	TCPNoDelay          bool    `toml:"tcp-no-delay" json:"tcp-no-delay"`
	CrossJoin           bool    `toml:"cross-join" json:"cross-join"`
	DistinctAggPushDown bool    `toml:"distinct-agg-push-down" json:"distinct-agg-push-down"`
	// Whether enable projection push down for coprocessors (both tikv & tiflash), default false.
	ProjectionPushDown bool   `toml:"projection-push-down" json:"projection-push-down"`
	MaxTxnTTL          uint64 `toml:"max-txn-ttl" json:"max-txn-ttl"`
	// Deprecated
	MemProfileInterval string `toml:"-" json:"-"`

	// Deprecated: this config will not have any effect
	IndexUsageSyncLease               string `toml:"index-usage-sync-lease" json:"index-usage-sync-lease"`
	PlanReplayerGCLease               string `toml:"plan-replayer-gc-lease" json:"plan-replayer-gc-lease"`
	GOGC                              int    `toml:"gogc" json:"gogc"`
	EnforceMPP                        bool   `toml:"enforce-mpp" json:"enforce-mpp"`
	StatsLoadConcurrency              int    `toml:"stats-load-concurrency" json:"stats-load-concurrency"`
	StatsLoadQueueSize                uint   `toml:"stats-load-queue-size" json:"stats-load-queue-size"`
	AnalyzePartitionConcurrencyQuota  uint   `toml:"analyze-partition-concurrency-quota" json:"analyze-partition-concurrency-quota"`
	PlanReplayerDumpWorkerConcurrency uint   `toml:"plan-replayer-dump-worker-concurrency" json:"plan-replayer-dump-worker-concurrency"`
	EnableStatsCacheMemQuota          bool   `toml:"enable-stats-cache-mem-quota" json:"enable-stats-cache-mem-quota"`
	// The following items are deprecated. We need to keep them here temporarily
	// to support the upgrade process. They can be removed in future.

	// CommitterConcurrency, RunAutoAnalyze unused since bootstrap v90
	CommitterConcurrency int  `toml:"committer-concurrency" json:"committer-concurrency"`
	RunAutoAnalyze       bool `toml:"run-auto-analyze" json:"run-auto-analyze"`

	// ForcePriority, MemoryUsageAlarmRatio are deprecated.
	ForcePriority         string  `toml:"force-priority" json:"force-priority"`
	MemoryUsageAlarmRatio float64 `toml:"memory-usage-alarm-ratio" json:"memory-usage-alarm-ratio"`

	EnableLoadFMSketch bool `toml:"enable-load-fmsketch" json:"enable-load-fmsketch"`

	// LiteInitStats indicates whether to use the lite version of stats.
	// 1. Basic stats meta data is loaded.(count, modify count, etc.)
	// 2. Column/index stats are loaded. (only histogram)
	// 3. TopN, Bucket, FMSketch are not loaded.
	// The lite version of stats is enabled by default.
	LiteInitStats bool `toml:"lite-init-stats" json:"lite-init-stats"`

	// If ForceInitStats is true, when tidb starts up, it doesn't provide service until init stats is finished.
	// If ForceInitStats is false, tidb can provide service before init stats is finished. Note that during the period
	// of init stats the optimizer may make bad decisions due to pseudo stats.
	ForceInitStats bool `toml:"force-init-stats" json:"force-init-stats"`

	// ConcurrentlyInitStats indicates whether to use concurrency to init stats.
	ConcurrentlyInitStats bool `toml:"concurrently-init-stats" json:"concurrently-init-stats"`
}

// PlanCache is the PlanCache section of the config.
type PlanCache struct {
	Enabled  bool `toml:"enabled" json:"enabled"`
	Capacity uint `toml:"capacity" json:"capacity"`
	Shards   uint `toml:"shards" json:"shards"`
}

// PreparedPlanCache is the PreparedPlanCache section of the config.
type PreparedPlanCache struct {
	Enabled          bool    `toml:"enabled" json:"enabled"`
	Capacity         uint    `toml:"capacity" json:"capacity"`
	MemoryGuardRatio float64 `toml:"memory-guard-ratio" json:"memory-guard-ratio"`
}

// OpenTracing is the opentracing section of the config.
type OpenTracing struct {
	Enable     bool                `toml:"enable" json:"enable"`
	RPCMetrics bool                `toml:"rpc-metrics" json:"rpc-metrics"`
	Sampler    OpenTracingSampler  `toml:"sampler" json:"sampler"`
	Reporter   OpenTracingReporter `toml:"reporter" json:"reporter"`
}

// OpenTracingSampler is the config for opentracing sampler.
// See https://godoc.org/github.com/uber/jaeger-client-go/config#SamplerConfig
type OpenTracingSampler struct {
	Type                    string        `toml:"type" json:"type"`
	Param                   float64       `toml:"param" json:"param"`
	SamplingServerURL       string        `toml:"sampling-server-url" json:"sampling-server-url"`
	MaxOperations           int           `toml:"max-operations" json:"max-operations"`
	SamplingRefreshInterval time.Duration `toml:"sampling-refresh-interval" json:"sampling-refresh-interval"`
}

// OpenTracingReporter is the config for opentracing reporter.
// See https://godoc.org/github.com/uber/jaeger-client-go/config#ReporterConfig
type OpenTracingReporter struct {
	QueueSize           int           `toml:"queue-size" json:"queue-size"`
	BufferFlushInterval time.Duration `toml:"buffer-flush-interval" json:"buffer-flush-interval"`
	LogSpans            bool          `toml:"log-spans" json:"log-spans"`
	LocalAgentHostPort  string        `toml:"local-agent-host-port" json:"local-agent-host-port"`
}

// ProxyProtocol is the PROXY protocol section of the config.
type ProxyProtocol struct {
	// PROXY protocol acceptable client networks.
	// Empty string means disable PROXY protocol,
	// * means all networks.
	Networks string `toml:"networks" json:"networks"`
	// PROXY protocol header read timeout, Unit is second.
	HeaderTimeout uint `toml:"header-timeout" json:"header-timeout"`
	// PROXY protocol header process fallback-able.
	// If set to true and not send PROXY protocol header, connection will return connection's client IP.
	Fallbackable bool `toml:"fallbackable" json:"fallbackable"`
}

// Binlog is the config for binlog.
type Binlog struct {
	Enable bool `toml:"enable" json:"enable"`
	// If IgnoreError is true, when writing binlog meets error, TiDB would
	// ignore the error.
	IgnoreError  bool   `toml:"ignore-error" json:"ignore-error"`
	WriteTimeout string `toml:"write-timeout" json:"write-timeout"`
	// Use socket file to write binlog, for compatible with kafka version tidb-binlog.
	BinlogSocket string `toml:"binlog-socket" json:"binlog-socket"`
	// The strategy for sending binlog to pump, value can be "range" or "hash" now.
	Strategy string `toml:"strategy" json:"strategy"`
}

// PessimisticTxn is the config for pessimistic transaction.
type PessimisticTxn struct {
	// The max count of retry for a single statement in a pessimistic transaction.
	MaxRetryCount uint `toml:"max-retry-count" json:"max-retry-count"`
	// The max count of deadlock events that will be recorded in the information_schema.deadlocks table.
	DeadlockHistoryCapacity uint `toml:"deadlock-history-capacity" json:"deadlock-history-capacity"`
	// Whether retryable deadlocks (in-statement deadlocks) are collected to the information_schema.deadlocks table.
	DeadlockHistoryCollectRetryable bool `toml:"deadlock-history-collect-retryable" json:"deadlock-history-collect-retryable"`
	// PessimisticAutoCommit represents if true it means the auto-commit transactions will be in pessimistic mode.
	PessimisticAutoCommit AtomicBool `toml:"pessimistic-auto-commit" json:"pessimistic-auto-commit"`
	// ConstraintCheckInPlacePessimistic is the default value for the session variable `tidb_constraint_check_in_place_pessimistic`
	ConstraintCheckInPlacePessimistic bool `toml:"constraint-check-in-place-pessimistic" json:"constraint-check-in-place-pessimistic"`
}

// TrxSummary is the config for transaction summary collecting.
type TrxSummary struct {
	// how many transaction summary in `transaction_summary` each TiDB node should keep.
	TransactionSummaryCapacity uint `toml:"transaction-summary-capacity" json:"transaction-summary-capacity"`
	// how long a transaction should be executed to make it be recorded in `transaction_id_digest`.
	TransactionIDDigestMinDuration uint `toml:"transaction-id-digest-min-duration" json:"transaction-id-digest-min-duration"`
}

// Valid Validatse TrxSummary configs
func (config *TrxSummary) Valid() error {
	if config.TransactionSummaryCapacity > 5000 {
		return errors.New("transaction-summary.transaction-summary-capacity should not be larger than 5000")
	}
	return nil
}

// DefaultPessimisticTxn returns the default configuration for PessimisticTxn
func DefaultPessimisticTxn() PessimisticTxn {
	return PessimisticTxn{
		MaxRetryCount:                     256,
		DeadlockHistoryCapacity:           10,
		DeadlockHistoryCollectRetryable:   false,
		PessimisticAutoCommit:             *NewAtomicBool(false),
		ConstraintCheckInPlacePessimistic: true,
	}
}

// DefaultTrxSummary returns the default configuration for TrxSummary collector
func DefaultTrxSummary() TrxSummary {
	// TrxSummary is not enabled by default before GA
	return TrxSummary{
		TransactionSummaryCapacity:     500,
		TransactionIDDigestMinDuration: 2147483647,
	}
}

// Plugin is the config for plugin
type Plugin struct {
	Dir  string `toml:"dir" json:"dir"`
	Load string `toml:"load" json:"load"`
}

// TopSQL is the config for TopSQL.
type TopSQL struct {
	// The TopSQL's data receiver address.
	ReceiverAddress string `toml:"receiver-address" json:"receiver-address"`
}

// IsolationRead is the config for isolation read.
type IsolationRead struct {
	// Engines filters tidb-server access paths by engine type.
	Engines []string `toml:"engines" json:"engines"`
}

// Experimental controls the features that are still experimental: their semantics, interfaces are subject to change.
// Using these features in the production environment is not recommended.
type Experimental struct {
	// Whether enable creating expression index.
	AllowsExpressionIndex bool `toml:"allow-expression-index" json:"allow-expression-index"`
	// Whether enable charset feature.
	EnableNewCharset bool `toml:"enable-new-charset" json:"-"`
}

var defTiKVCfg = tikvcfg.DefaultConfig()
var defaultConf = Config{
	Host:                         DefHost,
	AdvertiseAddress:             "",
	Port:                         DefPort,
	Socket:                       "/tmp/tidb-{Port}.sock",
	Cors:                         "",
	Store:                        "unistore",
	Path:                         "/tmp/tidb",
	RunDDL:                       true,
	SplitTable:                   true,
	Lease:                        "45s",
	TokenLimit:                   1000,
	OOMUseTmpStorage:             true,
	TempDir:                      DefTempDir,
	TempStorageQuota:             -1,
	TempStoragePath:              tempStorageDirName,
	MemQuotaQuery:                1 << 30,
	OOMAction:                    "cancel",
	EnableBatchDML:               false,
	CheckMb4ValueInUTF8:          *NewAtomicBool(true),
	MaxIndexLength:               3072,
	IndexLimit:                   64,
	TableColumnCountLimit:        1017,
	AlterPrimaryKey:              false,
	TreatOldVersionUTF8AsUTF8MB4: true,
	EnableTableLock:              false,
	DelayCleanTableLock:          0,
	SplitRegionMaxNum:            1000,
	RepairMode:                   false,
	RepairTableList:              []string{},
	MaxServerConnections:         0,
	TxnLocalLatches:              defTiKVCfg.TxnLocalLatches,
	GracefulWaitBeforeShutdown:   0,
	ServerVersion:                "",
	TiDBEdition:                  "",
	VersionComment:               "",
	TiDBReleaseVersion:           "",
	Log: Log{
		Level:               "info",
		Format:              "text",
		File:                logutil.NewFileLogConfig(logutil.DefaultLogMaxSize),
		SlowQueryFile:       "tidb-slow.log",
		SlowThreshold:       logutil.DefaultSlowThreshold,
		ExpensiveThreshold:  10000, // ExpensiveThreshold is deprecated.
		DisableErrorStack:   nbUnset,
		EnableErrorStack:    nbUnset, // If both options are nbUnset, getDisableErrorStack() returns true
		EnableTimestamp:     nbUnset,
		DisableTimestamp:    nbUnset, // If both options are nbUnset, getDisableTimestamp() returns false
		QueryLogMaxLen:      logutil.DefaultQueryLogMaxLen,
		RecordPlanInSlowLog: logutil.DefaultRecordPlanInSlowLog,
		EnableSlowLog:       *NewAtomicBool(logutil.DefaultTiDBEnableSlowLog),
	},
	Instance: Instance{
		TiDBGeneralLog:              false,
		EnablePProfSQLCPU:           false,
		DDLSlowOprThreshold:         DefDDLSlowOprThreshold,
		ExpensiveQueryTimeThreshold: DefExpensiveQueryTimeThreshold,
		ExpensiveTxnTimeThreshold:   DefExpensiveTxnTimeThreshold,
		StmtSummaryEnablePersistent: false,
		StmtSummaryFilename:         "tidb-statements.log",
		StmtSummaryFileMaxDays:      3,
		StmtSummaryFileMaxSize:      64,
		StmtSummaryFileMaxBackups:   0,
		EnableSlowLog:               *NewAtomicBool(logutil.DefaultTiDBEnableSlowLog),
		SlowThreshold:               logutil.DefaultSlowThreshold,
		RecordPlanInSlowLog:         logutil.DefaultRecordPlanInSlowLog,
		CheckMb4ValueInUTF8:         *NewAtomicBool(true),
		ForcePriority:               "NO_PRIORITY",
		MemoryUsageAlarmRatio:       DefMemoryUsageAlarmRatio,
		EnableCollectExecutionInfo:  *NewAtomicBool(true),
		PluginDir:                   "/data/deploy/plugin",
		PluginLoad:                  "",
		MaxConnections:              0,
		TiDBEnableDDL:               *NewAtomicBool(true),
		TiDBRCReadCheckTS:           false,
		TiDBServiceScope:            "",
	},
	Status: Status{
		ReportStatus:          true,
		StatusHost:            DefStatusHost,
		StatusPort:            DefStatusPort,
		MetricsInterval:       15,
		RecordQPSbyDB:         false,
		RecordDBLabel:         false,
		GRPCKeepAliveTime:     10,
		GRPCKeepAliveTimeout:  3,
		GRPCConcurrentStreams: 1024,
		GRPCInitialWindowSize: 2 * 1024 * 1024,
		GRPCMaxSendMsgSize:    math.MaxInt32,
	},
	Performance: Performance{
		MaxMemory:                         0,
		ServerMemoryQuota:                 0,
		MemoryUsageAlarmRatio:             DefMemoryUsageAlarmRatio,
		TCPKeepAlive:                      true,
		TCPNoDelay:                        true,
		CrossJoin:                         true,
		StatsLease:                        "3s",
		StmtCountLimit:                    5000,
		PseudoEstimateRatio:               0.8,
		ForcePriority:                     "NO_PRIORITY",
		BindInfoLease:                     "3s",
		TxnEntrySizeLimit:                 DefTxnEntrySizeLimit,
		TxnTotalSizeLimit:                 DefTxnTotalSizeLimit,
		DistinctAggPushDown:               false,
		ProjectionPushDown:                false,
		CommitterConcurrency:              defTiKVCfg.CommitterConcurrency,
		MaxTxnTTL:                         defTiKVCfg.MaxTxnTTL, // 1hour
		GOGC:                              100,
		EnforceMPP:                        false,
		PlanReplayerGCLease:               "10m",
		StatsLoadConcurrency:              0, // 0 is auto mode.
		StatsLoadQueueSize:                1000,
		AnalyzePartitionConcurrencyQuota:  16,
		PlanReplayerDumpWorkerConcurrency: 1,
		EnableStatsCacheMemQuota:          true,
		RunAutoAnalyze:                    true,
		EnableLoadFMSketch:                false,
		LiteInitStats:                     true,
		ForceInitStats:                    true,
		ConcurrentlyInitStats:             true,
	},
	ProxyProtocol: ProxyProtocol{
		Networks:      "",
		HeaderTimeout: 5,
		Fallbackable:  false,
	},
	PreparedPlanCache: PreparedPlanCache{
		Enabled:          true,
		Capacity:         100,
		MemoryGuardRatio: 0.1,
	},
	OpenTracing: OpenTracing{
		Enable: false,
		Sampler: OpenTracingSampler{
			Type:  "const",
			Param: 1.0,
		},
		Reporter: OpenTracingReporter{},
	},
	PDClient:   defTiKVCfg.PDClient,
	TiKVClient: defTiKVCfg.TiKVClient,
	Binlog: Binlog{
		WriteTimeout: "15s",
		Strategy:     "range",
	},
	Plugin: Plugin{
		Dir:  "/data/deploy/plugin",
		Load: "",
	},
	PessimisticTxn: DefaultPessimisticTxn(),
	IsolationRead: IsolationRead{
		Engines: []string{"tikv", "tiflash", "tidb"},
	},
	Experimental:               Experimental{},
	EnableCollectExecutionInfo: true,
	EnableTelemetry:            false,
	Labels:                     make(map[string]string),
	EnableGlobalIndex:          false,
	Security: Security{
		SpilledFileEncryptionMethod: SpilledFileEncryptionMethodPlaintext,
		EnableSEM:                   false,
		AutoTLS:                     false,
		RSAKeySize:                  4096,
		AuthTokenJWKS:               "",
		AuthTokenRefreshInterval:    DefAuthTokenRefreshInterval.String(),
		DisconnectOnExpiredPassword: true,
	},
	DeprecateIntegerDisplayWidth:         false,
	EnableEnumLengthLimit:                true,
	StoresRefreshInterval:                defTiKVCfg.StoresRefreshInterval,
	EnableForwarding:                     defTiKVCfg.EnableForwarding,
	NewCollationsEnabledOnFirstBootstrap: true,
	EnableGlobalKill:                     true,
	Enable32BitsConnectionID:             true,
	TrxSummary:                           DefaultTrxSummary(),
	DisaggregatedTiFlash:                 false,
	TiFlashComputeAutoScalerType:         tiflashcompute.DefASStr,
	TiFlashComputeAutoScalerAddr:         tiflashcompute.DefAWSAutoScalerAddr,
	IsTiFlashComputeFixedPool:            false,
	AutoScalerClusterID:                  "",
	UseAutoScaler:                        false,
	TiDBMaxReuseChunk:                    64,
	TiDBMaxReuseColumn:                   256,
	TiDBEnableExitCheck:                  false,
	InMemSlowQueryTopNNum:                30,
	InMemSlowQueryRecentNum:              500,
}

var (
	globalConf atomic.Pointer[Config]
)

// NewConfig creates a new config instance with default value.
func NewConfig() *Config {
	conf := defaultConf
	return &conf
}

// GetGlobalConfig returns the global configuration for this server.
// It should store configuration from command line and configuration file.
// Other parts of the system can read the global configuration use this function.
func GetGlobalConfig() *Config {
	return globalConf.Load()
}

// StoreGlobalConfig stores a new config to the globalConf. It mostly uses in the test to avoid some data races.
func StoreGlobalConfig(config *Config) {
	globalConf.Store(config)
	TikvConfigLock.Lock()
	defer TikvConfigLock.Unlock()
	cfg := *config.GetTiKVConfig()
	tikvcfg.StoreGlobalConfig(&cfg)
}

// removedConfig contains items that are no longer supported.
// they might still be in the config struct to support import,
// but are not actively used.
var removedConfig = map[string]struct{}{
	"pessimistic-txn.ttl":                {},
	"pessimistic-txn.enable":             {},
	"log.file.log-rotate":                {},
	"log.log-slow-query":                 {},
	"txn-local-latches":                  {},
	"txn-local-latches.enabled":          {},
	"txn-local-latches.capacity":         {},
	"performance.max-memory":             {},
	"max-txn-time-use":                   {},
	"experimental.allow-auto-random":     {},
	"enable-redact-log":                  {}, // use variable tidb_redact_log instead
	"enable-streaming":                   {},
	"performance.mem-profile-interval":   {},
	"security.require-secure-transport":  {},
	"lower-case-table-names":             {},
	"stmt-summary":                       {},
	"stmt-summary.enable":                {},
	"stmt-summary.enable-internal-query": {},
	"stmt-summary.max-stmt-count":        {},
	"stmt-summary.max-sql-length":        {},
	"stmt-summary.refresh-interval":      {},
	"stmt-summary.history-size":          {},
	"enable-batch-dml":                   {}, // use tidb_enable_batch_dml
	"mem-quota-query":                    {},
	"log.query-log-max-len":              {},
	"performance.committer-concurrency":  {},
	"experimental.enable-global-kill":    {},
	"performance.run-auto-analyze":       {}, // use tidb_enable_auto_analyze
	// use tidb_enable_prepared_plan_cache, tidb_prepared_plan_cache_size and tidb_prepared_plan_cache_memory_guard_ratio
	"prepared-plan-cache.enabled":            {},
	"prepared-plan-cache.capacity":           {},
	"prepared-plan-cache.memory-guard-ratio": {},
	"oom-action":                             {},
	"check-mb4-value-in-utf8":                {}, // use tidb_check_mb4_value_in_utf8
	"enable-collect-execution-info":          {}, // use tidb_enable_collect_execution_info
	"log.enable-slow-log":                    {}, // use tidb_enable_slow_log
	"log.slow-threshold":                     {}, // use tidb_slow_log_threshold
	"log.record-plan-in-slow-log":            {}, // use tidb_record_plan_in_slow_log
	"log.expensive-threshold":                {},
	"performance.force-priority":             {}, // use tidb_force_priority
	"performance.memory-usage-alarm-ratio":   {}, // use tidb_memory_usage_alarm_ratio
	"plugin.load":                            {}, // use plugin_load
	"plugin.dir":                             {}, // use plugin_dir
	"performance.feedback-probability":       {}, // This feature is deprecated
	"performance.query-feedback-limit":       {},
	"oom-use-tmp-storage":                    {}, // use tidb_enable_tmp_storage_on_oom
	"max-server-connections":                 {}, // use sysvar max_connections
	"run-ddl":                                {}, // use sysvar tidb_enable_ddl
	"instance.tidb_memory_usage_alarm_ratio": {}, // use sysvar tidb_memory_usage_alarm_ratio
	"enable-global-index":                    {}, // use sysvar tidb_enable_global_index
}

// isAllRemovedConfigItems returns true if all the items that couldn't validate
// belong to the list of removedConfig items.
func isAllRemovedConfigItems(items []string) bool {
	for _, item := range items {
		if _, ok := removedConfig[item]; !ok {
			return false
		}
	}
	return true
}

// InitializeConfig initialize the global config handler.
// The function enforceCmdArgs is used to merge the config file with command arguments:
// For example, if you start TiDB by the command "./tidb-server --port=3000", the port number should be
// overwritten to 3000 and ignore the port number in the config file.
func InitializeConfig(confPath string, configCheck, configStrict bool, enforceCmdArgs func(*Config, *flag.FlagSet), fset *flag.FlagSet) {
	cfg := GetGlobalConfig()
	var err error
	if confPath != "" {
		if err = cfg.Load(confPath); err != nil {
			// Unused config item error turns to warnings.
			if tmp, ok := err.(*ErrConfigValidationFailed); ok {
				// This block is to accommodate an interim situation where strict config checking
				// is not the default behavior of TiDB. The warning message must be deferred until
				// logging has been set up. After strict config checking is the default behavior,
				// This should all be removed.
				if (!configCheck && !configStrict) || isAllRemovedConfigItems(tmp.UndecodedItems) {
					fmt.Fprintln(os.Stderr, err.Error())
					err = nil
				}
			} else if tmp, ok := err.(*ErrConfigInstanceSection); ok {
				logutil.BgLogger().Warn(tmp.Error())
				err = nil
			}
		}
		// In configCheck we always print out which options in the config file
		// have been removed. This helps users upgrade better.
		if configCheck {
			err = cfg.RemovedVariableCheck(confPath)
			if err != nil {
				logutil.BgLogger().Warn(err.Error())
				err = nil // treat as warning
			}
		}

		terror.MustNil(err)
	} else {
		// configCheck should have the config file specified.
		if configCheck {
			fmt.Fprintln(os.Stderr, "config check failed", errors.New("no config file specified for config-check"))
			os.Exit(1)
		}
	}
	enforceCmdArgs(cfg, fset)

	if err := cfg.Valid(); err != nil {
		if !filepath.IsAbs(confPath) {
			if tmp, err := filepath.Abs(confPath); err == nil {
				confPath = tmp
			}
		}
		fmt.Fprintln(os.Stderr, "load config file:", confPath)
		fmt.Fprintln(os.Stderr, "invalid config", err)
		os.Exit(1)
	}
	if configCheck {
		fmt.Println("config check successful")
		os.Exit(0)
	}
	StoreGlobalConfig(cfg)
}

// RemovedVariableCheck checks if the config file contains any items
// which have been removed. These will not take effect any more.
func (c *Config) RemovedVariableCheck(confFile string) error {
	metaData, err := toml.DecodeFile(confFile, c)
	if err != nil {
		return err
	}
	var removed []string
	for item := range removedConfig {
		// We need to split the string to account for the top level
		// and the section hierarchy of config.
		tmp := strings.Split(item, ".")
		if len(tmp) == 2 && metaData.IsDefined(tmp[0], tmp[1]) {
			removed = append(removed, item)
		} else if len(tmp) == 1 && metaData.IsDefined(tmp[0]) {
			removed = append(removed, item)
		}
	}
	if len(removed) > 0 {
		sort.Strings(removed) // deterministic for tests
		return fmt.Errorf("The following configuration options are no longer supported in this version of TiDB. Check the release notes for more information: %s", strings.Join(removed, ", "))
	}
	return nil
}

// Load loads config options from a toml file.
func (c *Config) Load(confFile string) error {
	metaData, err := toml.DecodeFile(confFile, c)
	if err != nil {
		return err
	}
	if c.TokenLimit == 0 {
		c.TokenLimit = 1000
	} else if c.TokenLimit > MaxTokenLimit {
		c.TokenLimit = MaxTokenLimit
	}
	// If any items in confFile file are not mapped into the Config struct, issue
	// an error and stop the server from starting.
	undecoded := metaData.Undecoded()
	if len(undecoded) > 0 {
		var undecodedItems []string
		for _, item := range undecoded {
			undecodedItems = append(undecodedItems, item.String())
		}
		err = &ErrConfigValidationFailed{confFile, undecodedItems}
	}

	for _, section := range sectionMovedToInstance {
		newConflictSection := InstanceConfigSection{SectionName: section.SectionName, NameMappings: map[string]string{}}
		newDeprecatedSection := InstanceConfigSection{SectionName: section.SectionName, NameMappings: map[string]string{}}
		for oldName, newName := range section.NameMappings {
			if section.SectionName == "" && metaData.IsDefined(oldName) ||
				section.SectionName != "" && metaData.IsDefined(section.SectionName, oldName) {
				if metaData.IsDefined("instance", newName) {
					newConflictSection.NameMappings[oldName] = newName
				} else {
					newDeprecatedSection.NameMappings[oldName] = newName
				}
			}
		}
		if len(newConflictSection.NameMappings) > 0 {
			ConflictOptions = append(ConflictOptions, newConflictSection)
		}
		if len(newDeprecatedSection.NameMappings) > 0 {
			DeprecatedOptions = append(DeprecatedOptions, newDeprecatedSection)
		}
	}
	if len(ConflictOptions) > 0 || len(DeprecatedOptions) > 0 {
		// Give a warning that the 'instance' section should be used.
		err = &ErrConfigInstanceSection{confFile, &ConflictOptions, &DeprecatedOptions}
	}

	return err
}

// Valid checks if this config is valid.
func (c *Config) Valid() error {
	if c.Log.EnableErrorStack == c.Log.DisableErrorStack && c.Log.EnableErrorStack != nbUnset {
		logutil.BgLogger().Warn(fmt.Sprintf("\"enable-error-stack\" (%v) conflicts \"disable-error-stack\" (%v). \"disable-error-stack\" is deprecated, please use \"enable-error-stack\" instead. disable-error-stack is ignored.", c.Log.EnableErrorStack, c.Log.DisableErrorStack))
		// if two options conflict, we will use the value of EnableErrorStack
		c.Log.DisableErrorStack = nbUnset
	}
	if c.Log.EnableTimestamp == c.Log.DisableTimestamp && c.Log.EnableTimestamp != nbUnset {
		logutil.BgLogger().Warn(fmt.Sprintf("\"enable-timestamp\" (%v) conflicts \"disable-timestamp\" (%v). \"disable-timestamp\" is deprecated, please use \"enable-timestamp\" instead", c.Log.EnableTimestamp, c.Log.DisableTimestamp))
		// if two options conflict, we will use the value of EnableTimestamp
		c.Log.DisableTimestamp = nbUnset
	}
	if c.Security.SkipGrantTable && !hasRootPrivilege() {
		return fmt.Errorf("TiDB run with skip-grant-table need root privilege")
	}
	if !ValidStorage[c.Store] {
		nameList := make([]string, 0, len(ValidStorage))
		for k, v := range ValidStorage {
			if v {
				nameList = append(nameList, k)
			}
		}
		return fmt.Errorf("invalid store=%s, valid storages=%v", c.Store, nameList)
	}
	if c.Store == "mocktikv" && !c.Instance.TiDBEnableDDL.Load() {
		return fmt.Errorf("can't disable DDL on mocktikv")
	}
	if c.MaxIndexLength < DefMaxIndexLength || c.MaxIndexLength > DefMaxOfMaxIndexLength {
		return fmt.Errorf("max-index-length should be [%d, %d]", DefMaxIndexLength, DefMaxOfMaxIndexLength)
	}
	if c.IndexLimit < DefIndexLimit || c.IndexLimit > DefMaxOfIndexLimit {
		return fmt.Errorf("index-limit should be [%d, %d]", DefIndexLimit, DefMaxOfIndexLimit)
	}
	if c.Log.File.MaxSize > MaxLogFileSize {
		return fmt.Errorf("invalid max log file size=%v which is larger than max=%v", c.Log.File.MaxSize, MaxLogFileSize)
	}
	if c.TableColumnCountLimit < DefTableColumnCountLimit || c.TableColumnCountLimit > DefMaxOfTableColumnCountLimit {
		return fmt.Errorf("table-column-limit should be [%d, %d]", DefIndexLimit, DefMaxOfTableColumnCountLimit)
	}

	// txn-local-latches
	if err := c.TxnLocalLatches.Valid(); err != nil {
		return err
	}

	// For tikvclient.
	if err := c.TiKVClient.Valid(); err != nil {
		return err
	}
	if err := c.TrxSummary.Valid(); err != nil {
		return err
	}

	if c.Performance.TxnTotalSizeLimit > 1<<40 {
		return fmt.Errorf("txn-total-size-limit should be less than %d", 1<<40)
	}

	if c.Instance.MemoryUsageAlarmRatio > 1 || c.Instance.MemoryUsageAlarmRatio < 0 {
		return fmt.Errorf("tidb_memory_usage_alarm_ratio in [Instance] must be greater than or equal to 0 and less than or equal to 1")
	}

	if len(c.IsolationRead.Engines) < 1 {
		return fmt.Errorf("the number of [isolation-read]engines for isolation read should be at least 1")
	}
	for _, engine := range c.IsolationRead.Engines {
		if engine != "tidb" && engine != "tikv" && engine != "tiflash" {
			return fmt.Errorf("type of [isolation-read]engines can't be %v should be one of tidb or tikv or tiflash", engine)
		}
	}

	// test security
	c.Security.SpilledFileEncryptionMethod = strings.ToLower(c.Security.SpilledFileEncryptionMethod)
	switch c.Security.SpilledFileEncryptionMethod {
	case SpilledFileEncryptionMethodPlaintext, SpilledFileEncryptionMethodAES128CTR:
	default:
		return fmt.Errorf("unsupported [security]spilled-file-encryption-method %v, TiDB only supports [%v, %v]",
			c.Security.SpilledFileEncryptionMethod, SpilledFileEncryptionMethodPlaintext, SpilledFileEncryptionMethodAES128CTR)
	}

	// check stats load config
	if c.Performance.StatsLoadConcurrency < DefStatsLoadConcurrencyLimit || c.Performance.StatsLoadConcurrency > DefMaxOfStatsLoadConcurrencyLimit {
		return fmt.Errorf("stats-load-concurrency should be [%d, %d]", DefStatsLoadConcurrencyLimit, DefMaxOfStatsLoadConcurrencyLimit)
	}
	if c.Performance.StatsLoadQueueSize < DefStatsLoadQueueSizeLimit || c.Performance.StatsLoadQueueSize > DefMaxOfStatsLoadQueueSizeLimit {
		return fmt.Errorf("stats-load-queue-size should be [%d, %d]", DefStatsLoadQueueSizeLimit, DefMaxOfStatsLoadQueueSizeLimit)
	}

	// Check tiflash_compute topo fetch is valid.
	if c.DisaggregatedTiFlash && c.UseAutoScaler {
		if !tiflashcompute.IsValidAutoScalerConfig(c.TiFlashComputeAutoScalerType) {
			return fmt.Errorf("invalid AutoScaler type, expect %s, %s or %s, got %s",
				tiflashcompute.MockASStr, tiflashcompute.AWSASStr, tiflashcompute.GCPASStr, c.TiFlashComputeAutoScalerType)
		}
		if c.TiFlashComputeAutoScalerAddr == "" {
			return fmt.Errorf("autoscaler-addr cannot be empty when disaggregated-tiflash mode is true")
		}
	}

	// test log level
	l := zap.NewAtomicLevel()
	return l.UnmarshalText([]byte(c.Log.Level))
}

// UpdateGlobal updates the global config, and provide a restore function that can be used to restore to the original.
func UpdateGlobal(f func(conf *Config)) {
	g := GetGlobalConfig()
	newConf := *g
	f(&newConf)
	StoreGlobalConfig(&newConf)
}

// RestoreFunc gets a function that restore the config to the current value.
func RestoreFunc() (restore func()) {
	g := GetGlobalConfig()
	return func() {
		StoreGlobalConfig(g)
	}
}

func hasRootPrivilege() bool {
	return os.Geteuid() == 0
}

// TableLockEnabled uses to check whether enabled the table lock feature.
func TableLockEnabled() bool {
	return GetGlobalConfig().EnableTableLock
}

// TableLockDelayClean uses to get the time of delay clean table lock.
var TableLockDelayClean = func() uint64 {
	return GetGlobalConfig().DelayCleanTableLock
}

// ToLogConfig converts *Log to *logutil.LogConfig.
func (l *Log) ToLogConfig() *logutil.LogConfig {
	return logutil.NewLogConfig(l.Level, l.Format, l.SlowQueryFile, l.GeneralLogFile, l.File, l.getDisableTimestamp(),
		func(config *zaplog.Config) { config.DisableErrorVerbose = l.getDisableErrorStack() },
		func(config *zaplog.Config) { config.Timeout = l.Timeout },
	)
}

// ToTracingConfig converts *OpenTracing to *tracing.Configuration.
func (t *OpenTracing) ToTracingConfig() *tracing.Configuration {
	ret := &tracing.Configuration{
		Disabled:   !t.Enable,
		RPCMetrics: t.RPCMetrics,
		Reporter:   &tracing.ReporterConfig{},
		Sampler:    &tracing.SamplerConfig{},
	}
	ret.Reporter.QueueSize = t.Reporter.QueueSize
	ret.Reporter.BufferFlushInterval = t.Reporter.BufferFlushInterval
	ret.Reporter.LogSpans = t.Reporter.LogSpans
	ret.Reporter.LocalAgentHostPort = t.Reporter.LocalAgentHostPort

	ret.Sampler.Type = t.Sampler.Type
	ret.Sampler.Param = t.Sampler.Param
	ret.Sampler.SamplingServerURL = t.Sampler.SamplingServerURL
	ret.Sampler.MaxOperations = t.Sampler.MaxOperations
	ret.Sampler.SamplingRefreshInterval = t.Sampler.SamplingRefreshInterval
	return ret
}

func init() {
	initByLDFlags(versioninfo.TiDBEdition, checkBeforeDropLDFlag)
}

func initByLDFlags(edition, checkBeforeDropLDFlag string) {
	conf := defaultConf
	StoreGlobalConfig(&conf)
	if checkBeforeDropLDFlag == "1" {
		CheckTableBeforeDrop = true
	}
}

// hideConfig is used to filter a single line of config for hiding.
var hideConfig = []string{
	"performance.index-usage-sync-lease",
}

// GetJSONConfig returns the config as JSON with hidden items removed
// It replaces the earlier HideConfig() which used strings.Split() in
// an way that didn't work for similarly named items (like enable).
func GetJSONConfig() (string, error) {
	j, err := json.Marshal(GetGlobalConfig())
	if err != nil {
		return "", err
	}

	jsonValue := make(map[string]any)
	err = json.Unmarshal(j, &jsonValue)
	if err != nil {
		return "", err
	}

	removedPaths := make([]string, 0, len(removedConfig)+len(hideConfig))
	for removedItem := range removedConfig {
		removedPaths = append(removedPaths, removedItem)
	}
	removedPaths = append(removedPaths, hideConfig...)

	for _, path := range removedPaths {
		s := strings.Split(path, ".")
		curValue := jsonValue
		for i, key := range s {
			if i == len(s)-1 {
				delete(curValue, key)
			}
			if curValue[key] == nil {
				break
			}
			mapValue, ok := curValue[key].(map[string]any)
			if !ok {
				break
			}
			curValue = mapValue
		}
	}

	buf, err := json.Marshal(jsonValue)
	if err != nil {
		return "", err
	}

	var resBuf bytes.Buffer
	if err = json.Indent(&resBuf, buf, "", "\t"); err != nil {
		return "", err
	}
	return resBuf.String(), nil
}

// ContainHiddenConfig checks whether it contains the configuration that needs to be hidden.
func ContainHiddenConfig(s string) bool {
	s = strings.ToLower(s)
	for _, hc := range hideConfig {
		if strings.Contains(s, hc) {
			return true
		}
	}
	for dc := range removedConfig {
		if strings.Contains(s, dc) {
			return true
		}
	}
	return false
}

// GetGlobalKeyspaceName is used to get global keyspace name
// from config file or command line.
func GetGlobalKeyspaceName() string {
	return GetGlobalConfig().KeyspaceName
}
