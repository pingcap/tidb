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
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	zaplog "github.com/pingcap/log"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/versioninfo"
	tikvcfg "github.com/tikv/client-go/v2/config"
	tracing "github.com/uber/jaeger-client-go/config"
	atomicutil "go.uber.org/atomic"
	"go.uber.org/zap"
)

// Config number limitations
const (
	MaxLogFileSize = 4096 // MB
	// DefTxnEntrySizeLimit is the default value of TxnEntrySizeLimit.
	DefTxnEntrySizeLimit = 6 * 1024 * 1024
	// DefTxnTotalSizeLimit is the default value of TxnTxnTotalSizeLimit.
	DefTxnTotalSizeLimit = 100 * 1024 * 1024
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
	// DefStatsLoadConcurrencyLimit is limit of the concurrency of stats-load
	DefStatsLoadConcurrencyLimit = 1
	// DefMaxOfStatsLoadConcurrencyLimit is maximum limitation of the concurrency of stats-load
	DefMaxOfStatsLoadConcurrencyLimit = 128
	// DefStatsLoadQueueSizeLimit is limit of the size of stats-load request queue
	DefStatsLoadQueueSizeLimit = 1
	// DefMaxOfStatsLoadQueueSizeLimit is maximum limitation of the size of stats-load request queue
	DefMaxOfStatsLoadQueueSizeLimit = 100000
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
	RunDDL           bool   `toml:"run-ddl" json:"run-ddl"`
	SplitTable       bool   `toml:"split-table" json:"split-table"`
	TokenLimit       uint   `toml:"token-limit" json:"token-limit"`
	OOMUseTmpStorage bool   `toml:"oom-use-tmp-storage" json:"oom-use-tmp-storage"`
	TempStoragePath  string `toml:"tmp-storage-path" json:"tmp-storage-path"`
	OOMAction        string `toml:"oom-action" json:"oom-action"`
	MemQuotaQuery    int64  `toml:"mem-quota-query" json:"mem-quota-query"`
	// TempStorageQuota describe the temporary storage Quota during query exector when OOMUseTmpStorage is enabled
	// If the quota exceed the capacity of the TempStoragePath, the tidb-server would exit with fatal error
	TempStorageQuota int64 `toml:"tmp-storage-quota" json:"tmp-storage-quota"` // Bytes
	// Deprecated
	EnableStreaming bool                    `toml:"-" json:"-"`
	EnableBatchDML  bool                    `toml:"enable-batch-dml" json:"enable-batch-dml"`
	TxnLocalLatches tikvcfg.TxnLocalLatches `toml:"-" json:"-"`
	// Set sys variable lower-case-table-names, ref: https://dev.mysql.com/doc/refman/5.7/en/identifier-case-sensitivity.html.
	// TODO: We actually only support mode 2, which keeps the original case, but the comparison is case-insensitive.
	LowerCaseTableNames        int                `toml:"lower-case-table-names" json:"lower-case-table-names"`
	ServerVersion              string             `toml:"server-version" json:"server-version"`
	Log                        Log                `toml:"log" json:"log"`
	Security                   Security           `toml:"security" json:"security"`
	Status                     Status             `toml:"status" json:"status"`
	Performance                Performance        `toml:"performance" json:"performance"`
	PreparedPlanCache          PreparedPlanCache  `toml:"prepared-plan-cache" json:"prepared-plan-cache"`
	OpenTracing                OpenTracing        `toml:"opentracing" json:"opentracing"`
	ProxyProtocol              ProxyProtocol      `toml:"proxy-protocol" json:"proxy-protocol"`
	PDClient                   tikvcfg.PDClient   `toml:"pd-client" json:"pd-client"`
	TiKVClient                 tikvcfg.TiKVClient `toml:"tikv-client" json:"tikv-client"`
	Binlog                     Binlog             `toml:"binlog" json:"binlog"`
	CompatibleKillQuery        bool               `toml:"compatible-kill-query" json:"compatible-kill-query"`
	Plugin                     Plugin             `toml:"plugin" json:"plugin"`
	PessimisticTxn             PessimisticTxn     `toml:"pessimistic-txn" json:"pessimistic-txn"`
	CheckMb4ValueInUTF8        AtomicBool         `toml:"check-mb4-value-in-utf8" json:"check-mb4-value-in-utf8"`
	MaxIndexLength             int                `toml:"max-index-length" json:"max-index-length"`
	IndexLimit                 int                `toml:"index-limit" json:"index-limit"`
	TableColumnCountLimit      uint32             `toml:"table-column-count-limit" json:"table-column-count-limit"`
	GracefulWaitBeforeShutdown int                `toml:"graceful-wait-before-shutdown" json:"graceful-wait-before-shutdown"`
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
	// MaxServerConnections is the maximum permitted number of simultaneous client connections.
	MaxServerConnections uint32 `toml:"max-server-connections" json:"max-server-connections"`
	// NewCollationsEnabledOnFirstBootstrap indicates if the new collations are enabled, it effects only when a TiDB cluster bootstrapped on the first time.
	NewCollationsEnabledOnFirstBootstrap bool `toml:"new_collations_enabled_on_first_bootstrap" json:"new_collations_enabled_on_first_bootstrap"`
	// Experimental contains parameters for experimental features.
	Experimental Experimental `toml:"experimental" json:"experimental"`
	// EnableCollectExecutionInfo enables the TiDB to collect execution info.
	EnableCollectExecutionInfo bool `toml:"enable-collect-execution-info" json:"enable-collect-execution-info"`
	// SkipRegisterToDashboard tells TiDB don't register itself to the dashboard.
	SkipRegisterToDashboard bool `toml:"skip-register-to-dashboard" json:"skip-register-to-dashboard"`
	// EnableTelemetry enables the usage data report to PingCAP.
	EnableTelemetry bool `toml:"enable-telemetry" json:"enable-telemetry"`
	// Labels indicates the labels set for the tidb server. The labels describe some specific properties for the tidb
	// server like `zone`/`rack`/`host`. Currently, labels won't affect the tidb server except for some special
	// label keys. Now we have following special keys:
	// 1. 'group' is a special label key which should be automatically set by tidb-operator. We don't suggest
	// users to set 'group' in labels.
	// 2. 'zone' is a special key that indicates the DC location of this tidb-server. If it is set, the value for this
	// key will be the default value of the session variable `txn_scope` for this tidb-server.
	Labels map[string]string `toml:"labels" json:"labels"`
	// EnableGlobalIndex enables creating global index.
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
	BallastObjectSize int `toml:"ballast-object-size" json:"ballast-object-size"`
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

func (c *Config) getTiKVConfig() *tikvcfg.Config {
	return &tikvcfg.Config{
		CommitterConcurrency:  c.Performance.CommitterConcurrency,
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
	var v interface{}
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

	EnableSlowLog       AtomicBool `toml:"enable-slow-log" json:"enable-slow-log"`
	SlowQueryFile       string     `toml:"slow-query-file" json:"slow-query-file"`
	SlowThreshold       uint64     `toml:"slow-threshold" json:"slow-threshold"`
	ExpensiveThreshold  uint       `toml:"expensive-threshold" json:"expensive-threshold"`
	QueryLogMaxLen      uint64     `toml:"query-log-max-len" json:"query-log-max-len"`
	RecordPlanInSlowLog uint32     `toml:"record-plan-in-slow-log" json:"record-plan-in-slow-log"`
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
	SkipGrantTable         bool     `toml:"skip-grant-table" json:"skip-grant-table"`
	SSLCA                  string   `toml:"ssl-ca" json:"ssl-ca"`
	SSLCert                string   `toml:"ssl-cert" json:"ssl-cert"`
	SSLKey                 string   `toml:"ssl-key" json:"ssl-key"`
	RequireSecureTransport bool     `toml:"require-secure-transport" json:"require-secure-transport"`
	ClusterSSLCA           string   `toml:"cluster-ssl-ca" json:"cluster-ssl-ca"`
	ClusterSSLCert         string   `toml:"cluster-ssl-cert" json:"cluster-ssl-cert"`
	ClusterSSLKey          string   `toml:"cluster-ssl-key" json:"cluster-ssl-key"`
	ClusterVerifyCN        []string `toml:"cluster-verify-cn" json:"cluster-verify-cn"`
	// If set to "plaintext", the spilled files will not be encrypted.
	SpilledFileEncryptionMethod string `toml:"spilled-file-encryption-method" json:"spilled-file-encryption-method"`
	// EnableSEM prevents SUPER users from having full access.
	EnableSEM bool `toml:"enable-sem" json:"enable-sem"`
	// Allow automatic TLS certificate generation
	AutoTLS         bool   `toml:"auto-tls" json:"auto-tls"`
	MinTLSVersion   string `toml:"tls-version" json:"tls-version"`
	RSAKeySize      int    `toml:"rsa-key-size" json:"rsa-key-size"`
	SecureBootstrap bool   `toml:"secure-bootstrap" json:"secure-bootstrap"`
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
	MaxMemory             uint64  `toml:"max-memory" json:"max-memory"`
	ServerMemoryQuota     uint64  `toml:"server-memory-quota" json:"server-memory-quota"`
	MemoryUsageAlarmRatio float64 `toml:"memory-usage-alarm-ratio" json:"memory-usage-alarm-ratio"`
	StatsLease            string  `toml:"stats-lease" json:"stats-lease"`
	StmtCountLimit        uint    `toml:"stmt-count-limit" json:"stmt-count-limit"`
	FeedbackProbability   float64 `toml:"feedback-probability" json:"feedback-probability"`
	QueryFeedbackLimit    uint    `toml:"query-feedback-limit" json:"query-feedback-limit"`
	PseudoEstimateRatio   float64 `toml:"pseudo-estimate-ratio" json:"pseudo-estimate-ratio"`
	ForcePriority         string  `toml:"force-priority" json:"force-priority"`
	BindInfoLease         string  `toml:"bind-info-lease" json:"bind-info-lease"`
	TxnEntrySizeLimit     uint64  `toml:"txn-entry-size-limit" json:"txn-entry-size-limit"`
	TxnTotalSizeLimit     uint64  `toml:"txn-total-size-limit" json:"txn-total-size-limit"`
	TCPKeepAlive          bool    `toml:"tcp-keep-alive" json:"tcp-keep-alive"`
	TCPNoDelay            bool    `toml:"tcp-no-delay" json:"tcp-no-delay"`
	CrossJoin             bool    `toml:"cross-join" json:"cross-join"`
	RunAutoAnalyze        bool    `toml:"run-auto-analyze" json:"run-auto-analyze"`
	DistinctAggPushDown   bool    `toml:"distinct-agg-push-down" json:"distinct-agg-push-down"`
	CommitterConcurrency  int     `toml:"committer-concurrency" json:"committer-concurrency"`
	MaxTxnTTL             uint64  `toml:"max-txn-ttl" json:"max-txn-ttl"`
	// Deprecated
	MemProfileInterval   string `toml:"-" json:"-"`
	IndexUsageSyncLease  string `toml:"index-usage-sync-lease" json:"index-usage-sync-lease"`
	PlanReplayerGCLease  string `toml:"plan-replayer-gc-lease" json:"plan-replayer-gc-lease"`
	GOGC                 int    `toml:"gogc" json:"gogc"`
	EnforceMPP           bool   `toml:"enforce-mpp" json:"enforce-mpp"`
	StatsLoadConcurrency uint   `toml:"stats-load-concurrency" json:"stats-load-concurrency"`
	StatsLoadQueueSize   uint   `toml:"stats-load-queue-size" json:"stats-load-queue-size"`
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
}

// DefaultPessimisticTxn returns the default configuration for PessimisticTxn
func DefaultPessimisticTxn() PessimisticTxn {
	return PessimisticTxn{
		MaxRetryCount:                   256,
		DeadlockHistoryCapacity:         10,
		DeadlockHistoryCollectRetryable: false,
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
	// Whether enable global kill.
	EnableGlobalKill bool `toml:"enable-global-kill" json:"-"`
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
	TempStorageQuota:             -1,
	TempStoragePath:              tempStorageDirName,
	OOMAction:                    OOMActionCancel,
	MemQuotaQuery:                1 << 30,
	EnableStreaming:              false,
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
	LowerCaseTableNames:          2,
	GracefulWaitBeforeShutdown:   0,
	ServerVersion:                "",
	Log: Log{
		Level:               "info",
		Format:              "text",
		File:                logutil.NewFileLogConfig(logutil.DefaultLogMaxSize),
		SlowQueryFile:       "tidb-slow.log",
		SlowThreshold:       logutil.DefaultSlowThreshold,
		ExpensiveThreshold:  10000,
		DisableErrorStack:   nbUnset,
		EnableErrorStack:    nbUnset, // If both options are nbUnset, getDisableErrorStack() returns true
		EnableTimestamp:     nbUnset,
		DisableTimestamp:    nbUnset, // If both options are nbUnset, getDisableTimestamp() returns false
		QueryLogMaxLen:      logutil.DefaultQueryLogMaxLen,
		RecordPlanInSlowLog: logutil.DefaultRecordPlanInSlowLog,
		EnableSlowLog:       *NewAtomicBool(logutil.DefaultTiDBEnableSlowLog),
	},
	Status: Status{
		ReportStatus:          true,
		StatusHost:            DefStatusHost,
		StatusPort:            DefStatusPort,
		MetricsInterval:       15,
		RecordQPSbyDB:         false,
		GRPCKeepAliveTime:     10,
		GRPCKeepAliveTimeout:  3,
		GRPCConcurrentStreams: 1024,
		GRPCInitialWindowSize: 2 * 1024 * 1024,
		GRPCMaxSendMsgSize:    10 * 1024 * 1024,
	},
	Performance: Performance{
		MaxMemory:             0,
		ServerMemoryQuota:     0,
		MemoryUsageAlarmRatio: 0.8,
		TCPKeepAlive:          true,
		TCPNoDelay:            true,
		CrossJoin:             true,
		StatsLease:            "3s",
		RunAutoAnalyze:        true,
		StmtCountLimit:        5000,
		FeedbackProbability:   0.0,
		QueryFeedbackLimit:    512,
		PseudoEstimateRatio:   0.8,
		ForcePriority:         "NO_PRIORITY",
		BindInfoLease:         "3s",
		TxnEntrySizeLimit:     DefTxnEntrySizeLimit,
		TxnTotalSizeLimit:     DefTxnTotalSizeLimit,
		DistinctAggPushDown:   false,
		CommitterConcurrency:  defTiKVCfg.CommitterConcurrency,
		MaxTxnTTL:             defTiKVCfg.MaxTxnTTL, // 1hour
		// TODO: set indexUsageSyncLease to 60s.
		IndexUsageSyncLease:  "0s",
		GOGC:                 100,
		EnforceMPP:           false,
		PlanReplayerGCLease:  "10m",
		StatsLoadConcurrency: 5,
		StatsLoadQueueSize:   1000,
	},
	ProxyProtocol: ProxyProtocol{
		Networks:      "",
		HeaderTimeout: 5,
	},
	PreparedPlanCache: PreparedPlanCache{
		Enabled:          false,
		Capacity:         1000,
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
	Experimental: Experimental{
		EnableGlobalKill: false,
	},
	EnableCollectExecutionInfo: true,
	EnableTelemetry:            true,
	Labels:                     make(map[string]string),
	EnableGlobalIndex:          false,
	Security: Security{
		SpilledFileEncryptionMethod: SpilledFileEncryptionMethodPlaintext,
		EnableSEM:                   false,
		AutoTLS:                     false,
		RSAKeySize:                  4096,
	},
	DeprecateIntegerDisplayWidth: false,
	EnableEnumLengthLimit:        true,
	StoresRefreshInterval:        defTiKVCfg.StoresRefreshInterval,
	EnableForwarding:             defTiKVCfg.EnableForwarding,
}

var (
	globalConf atomic.Value
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
	return globalConf.Load().(*Config)
}

// StoreGlobalConfig stores a new config to the globalConf. It mostly uses in the test to avoid some data races.
func StoreGlobalConfig(config *Config) {
	globalConf.Store(config)
	cfg := *config.getTiKVConfig()
	tikvcfg.StoreGlobalConfig(&cfg)
}

var deprecatedConfig = map[string]struct{}{
	"pessimistic-txn.ttl":              {},
	"pessimistic-txn.enable":           {},
	"log.file.log-rotate":              {},
	"log.log-slow-query":               {},
	"txn-local-latches":                {},
	"txn-local-latches.enabled":        {},
	"txn-local-latches.capacity":       {},
	"performance.max-memory":           {},
	"max-txn-time-use":                 {},
	"experimental.allow-auto-random":   {},
	"enable-redact-log":                {}, // use variable tidb_redact_log instead
	"tikv-client.copr-cache.enable":    {},
	"alter-primary-key":                {}, // use NONCLUSTERED keyword instead
	"enable-streaming":                 {},
	"performance.mem-profile-interval": {},
}

func isAllDeprecatedConfigItems(items []string) bool {
	for _, item := range items {
		if _, ok := deprecatedConfig[item]; !ok {
			return false
		}
	}
	return true
}

// IsMemoryQuotaQuerySetByUser indicates whether the config item mem-quota-query
// is set by the user.
var IsMemoryQuotaQuerySetByUser bool

// IsOOMActionSetByUser indicates whether the config item mem-action is set by
// the user.
var IsOOMActionSetByUser bool

// InitializeConfig initialize the global config handler.
// The function enforceCmdArgs is used to merge the config file with command arguments:
// For example, if you start TiDB by the command "./tidb-server --port=3000", the port number should be
// overwritten to 3000 and ignore the port number in the config file.
func InitializeConfig(confPath string, configCheck, configStrict bool, enforceCmdArgs func(*Config)) {
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
				if (!configCheck && !configStrict) || isAllDeprecatedConfigItems(tmp.UndecodedItems) {
					fmt.Fprintln(os.Stderr, err.Error())
					err = nil
				}
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
	enforceCmdArgs(cfg)

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

// Load loads config options from a toml file.
func (c *Config) Load(confFile string) error {
	metaData, err := toml.DecodeFile(confFile, c)
	if c.TokenLimit == 0 {
		c.TokenLimit = 1000
	}
	if metaData.IsDefined("mem-quota-query") {
		IsMemoryQuotaQuerySetByUser = true
	}
	if metaData.IsDefined("oom-action") {
		IsOOMActionSetByUser = true
	}
	// If any items in confFile file are not mapped into the Config struct, issue
	// an error and stop the server from starting.
	undecoded := metaData.Undecoded()
	if len(undecoded) > 0 && err == nil {
		var undecodedItems []string
		for _, item := range undecoded {
			undecodedItems = append(undecodedItems, item.String())
		}
		err = &ErrConfigValidationFailed{confFile, undecodedItems}
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
	if c.Store == "mocktikv" && !c.RunDDL {
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
	c.OOMAction = strings.ToLower(c.OOMAction)
	if c.OOMAction != OOMActionLog && c.OOMAction != OOMActionCancel {
		return fmt.Errorf("unsupported OOMAction %v, TiDB only supports [%v, %v]", c.OOMAction, OOMActionLog, OOMActionCancel)
	}
	if c.TableColumnCountLimit < DefTableColumnCountLimit || c.TableColumnCountLimit > DefMaxOfTableColumnCountLimit {
		return fmt.Errorf("table-column-limit should be [%d, %d]", DefIndexLimit, DefMaxOfTableColumnCountLimit)
	}

	// lower_case_table_names is allowed to be 0, 1, 2
	if c.LowerCaseTableNames < 0 || c.LowerCaseTableNames > 2 {
		return fmt.Errorf("lower-case-table-names should be 0 or 1 or 2")
	}

	// txn-local-latches
	if err := c.TxnLocalLatches.Valid(); err != nil {
		return err
	}

	// For tikvclient.
	if err := c.TiKVClient.Valid(); err != nil {
		return err
	}

	if c.Performance.TxnTotalSizeLimit > 1<<40 {
		return fmt.Errorf("txn-total-size-limit should be less than %d", 1<<40)
	}

	if c.Performance.MemoryUsageAlarmRatio > 1 || c.Performance.MemoryUsageAlarmRatio < 0 {
		return fmt.Errorf("memory-usage-alarm-ratio in [Performance] must be greater than or equal to 0 and less than or equal to 1")
	}

	if c.PreparedPlanCache.Capacity < 1 {
		return fmt.Errorf("capacity in [prepared-plan-cache] should be at least 1")
	}
	if c.PreparedPlanCache.MemoryGuardRatio < 0 || c.PreparedPlanCache.MemoryGuardRatio > 1 {
		return fmt.Errorf("memory-guard-ratio in [prepared-plan-cache] must be NOT less than 0 and more than 1")
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
	return logutil.NewLogConfig(l.Level, l.Format, l.SlowQueryFile, l.File, l.getDisableTimestamp(), func(config *zaplog.Config) { config.DisableErrorVerbose = l.getDisableErrorStack() })
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
	if edition != versioninfo.CommunityEdition {
		defaultConf.EnableTelemetry = false
	}
	conf := defaultConf
	StoreGlobalConfig(&conf)
	if checkBeforeDropLDFlag == "1" {
		CheckTableBeforeDrop = true
	}
}

// The following constants represents the valid action configurations for OOMAction.
// NOTE: Although the values is case insensitive, we should use lower-case
// strings because the configuration value will be transformed to lower-case
// string and compared with these constants in the further usage.
const (
	OOMActionCancel = "cancel"
	OOMActionLog    = "log"
)

// hideConfig is used to filter a single line of config for hiding.
var hideConfig = []string{
	"index-usage-sync-lease",
}

// HideConfig is used to filter the configs that needs to be hidden.
func HideConfig(s string) string {
	configs := strings.Split(s, "\n")
	hideMap := make([]bool, len(configs))
	for i, c := range configs {
		for _, hc := range hideConfig {
			if strings.Contains(c, hc) {
				hideMap[i] = true
				break
			}
		}
	}
	var buf bytes.Buffer
	for i, c := range configs {
		if hideMap[i] {
			continue
		}
		if i != 0 {
			buf.WriteString("\n")
		}
		buf.WriteString(c)
	}
	return buf.String()
}

// ContainHiddenConfig checks whether it contains the configuration that needs to be hidden.
func ContainHiddenConfig(s string) bool {
	s = strings.ToLower(s)
	for _, hc := range hideConfig {
		if strings.Contains(s, hc) {
			return true
		}
	}
	return false
}
