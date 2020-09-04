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
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	zaplog "github.com/pingcap/log"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/versioninfo"
	tracing "github.com/uber/jaeger-client-go/config"

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
	// DefPort is the default port of TiDB
	DefPort = 4000
	// DefStatusPort is the default status port of TiDB
	DefStatusPort = 10080
	// DefHost is the default host of TiDB
	DefHost = "0.0.0.0"
	// DefStatusHost is the default status host of TiDB
	DefStatusHost = "0.0.0.0"
	// DefStoreLivenessTimeout is the default value for store liveness timeout.
	DefStoreLivenessTimeout = "5s"
	// DefTiDBRedactLog is the default value for redact log.
	DefTiDBRedactLog = false
)

// Valid config maps
var (
	ValidStorage = map[string]bool{
		"mocktikv": true,
		"tikv":     true,
		"unistore": true,
	}
	// checkTableBeforeDrop enable to execute `admin check table` before `drop table`.
	CheckTableBeforeDrop = false
	// checkBeforeDropLDFlag is a go build flag.
	checkBeforeDropLDFlag = "None"
	// tempStorageDirName is the default temporary storage dir name by base64 encoding a string `port/statusPort`
	tempStorageDirName = encodeDefTempStorageDir(os.TempDir(), DefHost, DefStatusHost, DefPort, DefStatusPort)
)

// Config contains configuration options.
type Config struct {
	Host                        string `toml:"host" json:"host"`
	AdvertiseAddress            string `toml:"advertise-address" json:"advertise-address"`
	Port                        uint   `toml:"port" json:"port"`
	Cors                        string `toml:"cors" json:"cors"`
	Store                       string `toml:"store" json:"store"`
	Path                        string `toml:"path" json:"path"`
	Socket                      string `toml:"socket" json:"socket"`
	Lease                       string `toml:"lease" json:"lease"`
	RunDDL                      bool   `toml:"run-ddl" json:"run-ddl"`
	SplitTable                  bool   `toml:"split-table" json:"split-table"`
	TokenLimit                  uint   `toml:"token-limit" json:"token-limit"`
	OOMUseTmpStorage            bool   `toml:"oom-use-tmp-storage" json:"oom-use-tmp-storage"`
	TempStoragePath             string `toml:"tmp-storage-path" json:"tmp-storage-path"`
	OOMAction                   string `toml:"oom-action" json:"oom-action"`
	MemQuotaQuery               int64  `toml:"mem-quota-query" json:"mem-quota-query"`
	NestedLoopJoinCacheCapacity int64  `toml:"nested-loop-join-cache-capacity" json:"nested-loop-join-cache-capacity"`
	// TempStorageQuota describe the temporary storage Quota during query exector when OOMUseTmpStorage is enabled
	// If the quota exceed the capacity of the TempStoragePath, the tidb-server would exit with fatal error
	TempStorageQuota int64           `toml:"tmp-storage-quota" json:"tmp-storage-quota"` // Bytes
	EnableStreaming  bool            `toml:"enable-streaming" json:"enable-streaming"`
	EnableBatchDML   bool            `toml:"enable-batch-dml" json:"enable-batch-dml"`
	TxnLocalLatches  TxnLocalLatches `toml:"-" json:"-"`
	// Set sys variable lower-case-table-names, ref: https://dev.mysql.com/doc/refman/5.7/en/identifier-case-sensitivity.html.
	// TODO: We actually only support mode 2, which keeps the original case, but the comparison is case-insensitive.
	LowerCaseTableNames int               `toml:"lower-case-table-names" json:"lower-case-table-names"`
	ServerVersion       string            `toml:"server-version" json:"server-version"`
	Log                 Log               `toml:"log" json:"log"`
	Security            Security          `toml:"security" json:"security"`
	Status              Status            `toml:"status" json:"status"`
	Performance         Performance       `toml:"performance" json:"performance"`
	PreparedPlanCache   PreparedPlanCache `toml:"prepared-plan-cache" json:"prepared-plan-cache"`
	OpenTracing         OpenTracing       `toml:"opentracing" json:"opentracing"`
	ProxyProtocol       ProxyProtocol     `toml:"proxy-protocol" json:"proxy-protocol"`
	TiKVClient          TiKVClient        `toml:"tikv-client" json:"tikv-client"`
	Binlog              Binlog            `toml:"binlog" json:"binlog"`
	CompatibleKillQuery bool              `toml:"compatible-kill-query" json:"compatible-kill-query"`
	Plugin              Plugin            `toml:"plugin" json:"plugin"`
	PessimisticTxn      PessimisticTxn    `toml:"pessimistic-txn" json:"pessimistic-txn"`
	CheckMb4ValueInUTF8 bool              `toml:"check-mb4-value-in-utf8" json:"check-mb4-value-in-utf8"`
	MaxIndexLength      int               `toml:"max-index-length" json:"max-index-length"`
	// AlterPrimaryKey is used to control alter primary key feature.
	AlterPrimaryKey bool `toml:"alter-primary-key" json:"alter-primary-key"`
	// TreatOldVersionUTF8AsUTF8MB4 is use to treat old version table/column UTF8 charset as UTF8MB4. This is for compatibility.
	// Currently not support dynamic modify, because this need to reload all old version schema.
	TreatOldVersionUTF8AsUTF8MB4 bool `toml:"treat-old-version-utf8-as-utf8mb4" json:"treat-old-version-utf8-as-utf8mb4"`
	// EnableTableLock indicate whether enable table lock.
	// TODO: remove this after table lock features stable.
	EnableTableLock     bool        `toml:"enable-table-lock" json:"enable-table-lock"`
	DelayCleanTableLock uint64      `toml:"delay-clean-table-lock" json:"delay-clean-table-lock"`
	SplitRegionMaxNum   uint64      `toml:"split-region-max-num" json:"split-region-max-num"`
	StmtSummary         StmtSummary `toml:"stmt-summary" json:"stmt-summary"`
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
	// label keys. Now we only have `group` as a special label key.
	// Note that: 'group' is a special label key which should be automatically set by tidb-operator. We don't suggest
	// users to set 'group' in labels.
	Labels map[string]string `toml:"labels" json:"labels"`
	// EnableGlobalIndex enables creating global index.
	EnableGlobalIndex bool `toml:"enable-global-index" json:"enable-global-index"`
	// DeprecateIntegerDisplayWidth indicates whether deprecating the max display length for integer.
	DeprecateIntegerDisplayWidth bool `toml:"deprecate-integer-display-length" json:"deprecate-integer-display-length"`
	// EnableRedactLog indicates that whether redact log.
	EnableRedactLog bool `toml:"enable-redact-log" json:"enable-redact-log"`
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

func encodeDefTempStorageDir(tempDir string, host, statusHost string, port, statusPort uint) string {
	dirName := base64.URLEncoding.EncodeToString([]byte(fmt.Sprintf("%v:%v/%v:%v", host, port, statusHost, statusPort)))
	var osUID string
	currentUser, err := user.Current()
	if err != nil {
		osUID = ""
	} else {
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

// Log is the log section of config.
type Log struct {
	// Log level.
	Level string `toml:"level" json:"level"`
	// Log format. one of json, text, or console.
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

	EnableSlowLog       bool   `toml:"enable-slow-log" json:"enable-slow-log"`
	SlowQueryFile       string `toml:"slow-query-file" json:"slow-query-file"`
	SlowThreshold       uint64 `toml:"slow-threshold" json:"slow-threshold"`
	ExpensiveThreshold  uint   `toml:"expensive-threshold" json:"expensive-threshold"`
	QueryLogMaxLen      uint64 `toml:"query-log-max-len" json:"query-log-max-len"`
	RecordPlanInSlowLog uint32 `toml:"record-plan-in-slow-log" json:"record-plan-in-slow-log"`
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

// ToTLSConfig generates tls's config based on security section of the config.
func (s *Security) ToTLSConfig() (tlsConfig *tls.Config, err error) {
	if len(s.ClusterSSLCA) != 0 {
		certPool := x509.NewCertPool()
		// Create a certificate pool from the certificate authority
		var ca []byte
		ca, err = ioutil.ReadFile(s.ClusterSSLCA)
		if err != nil {
			err = errors.Errorf("could not read ca certificate: %s", err)
			return
		}
		// Append the certificates from the CA
		if !certPool.AppendCertsFromPEM(ca) {
			err = errors.New("failed to append ca certs")
			return
		}
		tlsConfig = &tls.Config{
			RootCAs:   certPool,
			ClientCAs: certPool,
		}

		if len(s.ClusterSSLCert) != 0 && len(s.ClusterSSLKey) != 0 {
			getCert := func() (*tls.Certificate, error) {
				// Load the client certificates from disk
				cert, err := tls.LoadX509KeyPair(s.ClusterSSLCert, s.ClusterSSLKey)
				if err != nil {
					return nil, errors.Errorf("could not load client key pair: %s", err)
				}
				return &cert, nil
			}
			// pre-test cert's loading.
			if _, err = getCert(); err != nil {
				return
			}
			tlsConfig.GetClientCertificate = func(info *tls.CertificateRequestInfo) (certificate *tls.Certificate, err error) {
				return getCert()
			}
			tlsConfig.GetCertificate = func(info *tls.ClientHelloInfo) (certificate *tls.Certificate, err error) {
				return getCert()
			}
		}
	}
	return
}

// Status is the status section of the config.
type Status struct {
	StatusHost      string `toml:"status-host" json:"status-host"`
	MetricsAddr     string `toml:"metrics-addr" json:"metrics-addr"`
	StatusPort      uint   `toml:"status-port" json:"status-port"`
	MetricsInterval uint   `toml:"metrics-interval" json:"metrics-interval"`
	ReportStatus    bool   `toml:"report-status" json:"report-status"`
	RecordQPSbyDB   bool   `toml:"record-db-qps" json:"record-db-qps"`
}

// Performance is the performance section of the config.
type Performance struct {
	MaxProcs uint `toml:"max-procs" json:"max-procs"`
	// Deprecated: use ServerMemoryQuota instead
	MaxMemory            uint64  `toml:"max-memory" json:"max-memory"`
	ServerMemoryQuota    uint64  `toml:"server-memory-quota" json:"server-memory-quota"`
	StatsLease           string  `toml:"stats-lease" json:"stats-lease"`
	StmtCountLimit       uint    `toml:"stmt-count-limit" json:"stmt-count-limit"`
	FeedbackProbability  float64 `toml:"feedback-probability" json:"feedback-probability"`
	QueryFeedbackLimit   uint    `toml:"query-feedback-limit" json:"query-feedback-limit"`
	PseudoEstimateRatio  float64 `toml:"pseudo-estimate-ratio" json:"pseudo-estimate-ratio"`
	ForcePriority        string  `toml:"force-priority" json:"force-priority"`
	BindInfoLease        string  `toml:"bind-info-lease" json:"bind-info-lease"`
	TxnEntrySizeLimit    uint64  `toml:"txn-entry-size-limit" json:"txn-entry-size-limit"`
	TxnTotalSizeLimit    uint64  `toml:"txn-total-size-limit" json:"txn-total-size-limit"`
	TCPKeepAlive         bool    `toml:"tcp-keep-alive" json:"tcp-keep-alive"`
	CrossJoin            bool    `toml:"cross-join" json:"cross-join"`
	RunAutoAnalyze       bool    `toml:"run-auto-analyze" json:"run-auto-analyze"`
	DistinctAggPushDown  bool    `toml:"distinct-agg-push-down" json:"agg-push-down-join"`
	CommitterConcurrency int     `toml:"committer-concurrency" json:"committer-concurrency"`
	MaxTxnTTL            uint64  `toml:"max-txn-ttl" json:"max-txn-ttl"`
	MemProfileInterval   string  `toml:"mem-profile-interval" json:"mem-profile-interval"`
}

// PlanCache is the PlanCache section of the config.
type PlanCache struct {
	Enabled  bool `toml:"enabled" json:"enabled"`
	Capacity uint `toml:"capacity" json:"capacity"`
	Shards   uint `toml:"shards" json:"shards"`
}

// TxnLocalLatches is the TxnLocalLatches section of the config.
type TxnLocalLatches struct {
	Enabled  bool `toml:"-" json:"-"`
	Capacity uint `toml:"-" json:"-"`
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

// TiKVClient is the config for tikv client.
type TiKVClient struct {
	// GrpcConnectionCount is the max gRPC connections that will be established
	// with each tikv-server.
	GrpcConnectionCount uint `toml:"grpc-connection-count" json:"grpc-connection-count"`
	// After a duration of this time in seconds if the client doesn't see any activity it pings
	// the server to see if the transport is still alive.
	GrpcKeepAliveTime uint `toml:"grpc-keepalive-time" json:"grpc-keepalive-time"`
	// After having pinged for keepalive check, the client waits for a duration of Timeout in seconds
	// and if no activity is seen even after that the connection is closed.
	GrpcKeepAliveTimeout uint `toml:"grpc-keepalive-timeout" json:"grpc-keepalive-timeout"`
	// CommitTimeout is the max time which command 'commit' will wait.
	CommitTimeout string `toml:"commit-timeout" json:"commit-timeout"`
	// EnableAsyncCommit enables async commit for all transactions.
	EnableAsyncCommit bool `toml:"enable-async-commit" json:"enable-async-commit"`

	// MaxBatchSize is the max batch size when calling batch commands API.
	MaxBatchSize uint `toml:"max-batch-size" json:"max-batch-size"`
	// If TiKV load is greater than this, TiDB will wait for a while to avoid little batch.
	OverloadThreshold uint `toml:"overload-threshold" json:"overload-threshold"`
	// MaxBatchWaitTime in nanosecond is the max wait time for batch.
	MaxBatchWaitTime time.Duration `toml:"max-batch-wait-time" json:"max-batch-wait-time"`
	// BatchWaitSize is the max wait size for batch.
	BatchWaitSize uint `toml:"batch-wait-size" json:"batch-wait-size"`
	// EnableChunkRPC indicate the data encode in chunk format for coprocessor requests.
	EnableChunkRPC bool `toml:"enable-chunk-rpc" json:"enable-chunk-rpc"`
	// If a Region has not been accessed for more than the given duration (in seconds), it
	// will be reloaded from the PD.
	RegionCacheTTL uint `toml:"region-cache-ttl" json:"region-cache-ttl"`
	// If a store has been up to the limit, it will return error for successive request to
	// prevent the store occupying too much token in dispatching level.
	StoreLimit int64 `toml:"store-limit" json:"store-limit"`
	// StoreLivenessTimeout is the timeout for store liveness check request.
	StoreLivenessTimeout string           `toml:"store-liveness-timeout" json:"store-liveness-timeout"`
	CoprCache            CoprocessorCache `toml:"copr-cache" json:"copr-cache"`
	// TTLRefreshedTxnSize controls whether a transaction should update its TTL or not.
	TTLRefreshedTxnSize int64 `toml:"ttl-refreshed-txn-size" json:"ttl-refreshed-txn-size"`
}

// CoprocessorCache is the config for coprocessor cache.
type CoprocessorCache struct {
	// Whether to enable the copr cache. The copr cache saves the result from TiKV Coprocessor in the memory and
	// reuses the result when corresponding data in TiKV is unchanged, on a region basis.
	Enable bool `toml:"enable" json:"enable"`
	// The capacity in MB of the cache.
	CapacityMB float64 `toml:"capacity-mb" json:"capacity-mb"`
	// Only cache requests whose result set is small.
	AdmissionMaxResultMB float64 `toml:"admission-max-result-mb" json:"admission-max-result-mb"`
	// Only cache requests takes notable time to process.
	AdmissionMinProcessMs uint64 `toml:"admission-min-process-ms" json:"admission-min-process-ms"`
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

// Plugin is the config for plugin
type Plugin struct {
	Dir  string `toml:"dir" json:"dir"`
	Load string `toml:"load" json:"load"`
}

// PessimisticTxn is the config for pessimistic transaction.
type PessimisticTxn struct {
	// Enable must be true for 'begin lock' or session variable to start a pessimistic transaction.
	Enable bool `toml:"enable" json:"enable"`
	// The max count of retry for a single statement in a pessimistic transaction.
	MaxRetryCount uint `toml:"max-retry-count" json:"max-retry-count"`
}

// StmtSummary is the config for statement summary.
type StmtSummary struct {
	// Enable statement summary or not.
	Enable bool `toml:"enable" json:"enable"`
	// Enable summary internal query.
	EnableInternalQuery bool `toml:"enable-internal-query" json:"enable-internal-query"`
	// The maximum number of statements kept in memory.
	MaxStmtCount uint `toml:"max-stmt-count" json:"max-stmt-count"`
	// The maximum length of displayed normalized SQL and sample SQL.
	MaxSQLLength uint `toml:"max-sql-length" json:"max-sql-length"`
	// The refresh interval of statement summary.
	RefreshInterval int `toml:"refresh-interval" json:"refresh-interval"`
	// The maximum history size of statement summary.
	HistorySize int `toml:"history-size" json:"history-size"`
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
}

var defaultConf = Config{
	Host:                         DefHost,
	AdvertiseAddress:             "",
	Port:                         DefPort,
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
	NestedLoopJoinCacheCapacity:  20971520,
	EnableStreaming:              false,
	EnableBatchDML:               false,
	CheckMb4ValueInUTF8:          true,
	MaxIndexLength:               3072,
	AlterPrimaryKey:              false,
	TreatOldVersionUTF8AsUTF8MB4: true,
	EnableTableLock:              false,
	DelayCleanTableLock:          0,
	SplitRegionMaxNum:            1000,
	RepairMode:                   false,
	RepairTableList:              []string{},
	MaxServerConnections:         0,
	TxnLocalLatches: TxnLocalLatches{
		Enabled:  false,
		Capacity: 0,
	},
	LowerCaseTableNames: 2,
	ServerVersion:       "",
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
		EnableSlowLog:       logutil.DefaultTiDBEnableSlowLog,
	},
	Status: Status{
		ReportStatus:    true,
		StatusHost:      DefStatusHost,
		StatusPort:      DefStatusPort,
		MetricsInterval: 15,
		RecordQPSbyDB:   false,
	},
	Performance: Performance{
		MaxMemory:            0,
		ServerMemoryQuota:    0,
		TCPKeepAlive:         true,
		CrossJoin:            true,
		StatsLease:           "3s",
		RunAutoAnalyze:       true,
		StmtCountLimit:       5000,
		FeedbackProbability:  0.05,
		QueryFeedbackLimit:   512,
		PseudoEstimateRatio:  0.8,
		ForcePriority:        "NO_PRIORITY",
		BindInfoLease:        "3s",
		TxnEntrySizeLimit:    DefTxnEntrySizeLimit,
		TxnTotalSizeLimit:    DefTxnTotalSizeLimit,
		DistinctAggPushDown:  false,
		CommitterConcurrency: 16,
		MaxTxnTTL:            10 * 60 * 1000, // 10min
		MemProfileInterval:   "1m",
	},
	ProxyProtocol: ProxyProtocol{
		Networks:      "",
		HeaderTimeout: 5,
	},
	PreparedPlanCache: PreparedPlanCache{
		Enabled:          false,
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
	TiKVClient: TiKVClient{
		GrpcConnectionCount:  4,
		GrpcKeepAliveTime:    10,
		GrpcKeepAliveTimeout: 3,
		CommitTimeout:        "41s",
		EnableAsyncCommit:    false,

		MaxBatchSize:      128,
		OverloadThreshold: 200,
		MaxBatchWaitTime:  0,
		BatchWaitSize:     8,

		EnableChunkRPC: true,

		RegionCacheTTL:       600,
		StoreLimit:           0,
		StoreLivenessTimeout: DefStoreLivenessTimeout,

		TTLRefreshedTxnSize: 32 * 1024 * 1024,

		CoprCache: CoprocessorCache{
			Enable:                true,
			CapacityMB:            1000,
			AdmissionMaxResultMB:  10,
			AdmissionMinProcessMs: 5,
		},
	},
	Binlog: Binlog{
		WriteTimeout: "15s",
		Strategy:     "range",
	},
	PessimisticTxn: PessimisticTxn{
		Enable:        true,
		MaxRetryCount: 256,
	},
	StmtSummary: StmtSummary{
		Enable:              true,
		EnableInternalQuery: false,
		MaxStmtCount:        200,
		MaxSQLLength:        4096,
		RefreshInterval:     1800,
		HistorySize:         24,
	},
	IsolationRead: IsolationRead{
		Engines: []string{"tikv", "tiflash", "tidb"},
	},
	Experimental: Experimental{
		AllowsExpressionIndex: false,
	},
	EnableCollectExecutionInfo: true,
	EnableTelemetry:            true,
	Labels:                     make(map[string]string),
	EnableGlobalIndex:          false,
	Security: Security{
		SpilledFileEncryptionMethod: SpilledFileEncryptionMethodPlaintext,
	},
	DeprecateIntegerDisplayWidth: false,
	EnableRedactLog:              DefTiDBRedactLog,
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
}

var deprecatedConfig = map[string]struct{}{
	"pessimistic-txn.ttl":            {},
	"log.file.log-rotate":            {},
	"log.log-slow-query":             {},
	"txn-local-latches":              {},
	"txn-local-latches.enabled":      {},
	"txn-local-latches.capacity":     {},
	"performance.max-memory":         {},
	"max-txn-time-use":               {},
	"experimental.allow-auto-random": {},
}

func isAllDeprecatedConfigItems(items []string) bool {
	for _, item := range items {
		if _, ok := deprecatedConfig[item]; !ok {
			return false
		}
	}
	return true
}

// InitializeConfig initialize the global config handler.
// The function enforceCmdArgs is used to merge the config file with command arguments:
// For example, if you start TiDB by the command "./tidb-server --port=3000", the port number should be
// overwritten to 3000 and ignore the port number in the config file.
func InitializeConfig(confPath string, configCheck, configStrict bool, reloadFunc ConfReloadFunc, enforceCmdArgs func(*Config)) {
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
	if len(c.ServerVersion) > 0 {
		mysql.ServerVersion = c.ServerVersion
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
	if _, ok := ValidStorage[c.Store]; !ok {
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
	if c.Log.File.MaxSize > MaxLogFileSize {
		return fmt.Errorf("invalid max log file size=%v which is larger than max=%v", c.Log.File.MaxSize, MaxLogFileSize)
	}
	c.OOMAction = strings.ToLower(c.OOMAction)
	if c.OOMAction != OOMActionLog && c.OOMAction != OOMActionCancel {
		return fmt.Errorf("unsupported OOMAction %v, TiDB only supports [%v, %v]", c.OOMAction, OOMActionLog, OOMActionCancel)
	}

	// lower_case_table_names is allowed to be 0, 1, 2
	if c.LowerCaseTableNames < 0 || c.LowerCaseTableNames > 2 {
		return fmt.Errorf("lower-case-table-names should be 0 or 1 or 2")
	}

	if c.TxnLocalLatches.Enabled && c.TxnLocalLatches.Capacity == 0 {
		return fmt.Errorf("txn-local-latches.capacity can not be 0")
	}

	// For tikvclient.
	if c.TiKVClient.GrpcConnectionCount == 0 {
		return fmt.Errorf("grpc-connection-count should be greater than 0")
	}

	if c.Performance.TxnTotalSizeLimit > 10<<30 {
		return fmt.Errorf("txn-total-size-limit should be less than %d", 10<<30)
	}

	if c.StmtSummary.MaxStmtCount <= 0 {
		return fmt.Errorf("max-stmt-count in [stmt-summary] should be greater than 0")
	}
	if c.StmtSummary.HistorySize < 0 {
		return fmt.Errorf("history-size in [stmt-summary] should be greater than or equal to 0")
	}
	if c.StmtSummary.RefreshInterval <= 0 {
		return fmt.Errorf("refresh-interval in [stmt-summary] should be greater than 0")
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

// ParsePath parses this path.
func ParsePath(path string) (etcdAddrs []string, disableGC bool, err error) {
	var u *url.URL
	u, err = url.Parse(path)
	if err != nil {
		err = errors.Trace(err)
		return
	}
	if strings.ToLower(u.Scheme) != "tikv" {
		err = errors.Errorf("Uri scheme expected [tikv] but found [%s]", u.Scheme)
		logutil.BgLogger().Error("parsePath error", zap.Error(err))
		return
	}
	switch strings.ToLower(u.Query().Get("disableGC")) {
	case "true":
		disableGC = true
	case "false", "":
	default:
		err = errors.New("disableGC flag should be true/false")
		return
	}
	etcdAddrs = strings.Split(u.Host, ",")
	return
}
