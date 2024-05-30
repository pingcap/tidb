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
	"encoding/json"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	zaplog "github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
	tracing "github.com/uber/jaeger-client-go/config"
)

func TestAtomicBoolUnmarshal(t *testing.T) {
	type data struct {
		Ab AtomicBool `toml:"ab"`
	}
	var d data
	var firstBuffer bytes.Buffer
	_, err := toml.Decode("ab=true", &d)
	require.NoError(t, err)
	require.True(t, d.Ab.Load())
	err = toml.NewEncoder(&firstBuffer).Encode(d)
	require.NoError(t, err)
	require.Equal(t, "ab = \"true\"\n", firstBuffer.String())
	firstBuffer.Reset()

	_, err = toml.Decode("ab=false", &d)
	require.NoError(t, err)
	require.False(t, d.Ab.Load())
	err = toml.NewEncoder(&firstBuffer).Encode(d)
	require.NoError(t, err)
	require.Equal(t, "ab = \"false\"\n", firstBuffer.String())

	_, err = toml.Decode("ab = 1", &d)
	require.EqualError(t, err, "Invalid value for bool type: 1")
}

func TestNullableBoolUnmarshal(t *testing.T) {
	var nb = nullableBool{false, false}
	data, err := json.Marshal(nb)
	require.NoError(t, err)
	err = json.Unmarshal(data, &nb)
	require.NoError(t, err)
	require.Equal(t, nbUnset, nb)

	nb = nullableBool{true, false}
	data, err = json.Marshal(nb)
	require.NoError(t, err)
	err = json.Unmarshal(data, &nb)
	require.NoError(t, err)
	require.Equal(t, nbFalse, nb)

	nb = nullableBool{true, true}
	data, err = json.Marshal(nb)
	require.NoError(t, err)
	err = json.Unmarshal(data, &nb)
	require.NoError(t, err)
	require.Equal(t, nbTrue, nb)

	// Test for UnmarshalText
	var log Log
	_, err = toml.Decode("enable-error-stack = true", &log)
	require.NoError(t, err)
	require.Equal(t, nbTrue, log.EnableErrorStack)

	_, err = toml.Decode("enable-error-stack = \"\"", &log)
	require.NoError(t, err)
	require.Equal(t, nbUnset, log.EnableErrorStack)

	_, err = toml.Decode("enable-error-stack = 1", &log)
	require.EqualError(t, err, "Invalid value for bool type: 1")
	require.Equal(t, nbUnset, log.EnableErrorStack)

	// Test for UnmarshalJSON
	err = json.Unmarshal([]byte("{\"enable-timestamp\":false}"), &log)
	require.NoError(t, err)
	require.Equal(t, nbFalse, log.EnableTimestamp)

	err = json.Unmarshal([]byte("{\"disable-timestamp\":null}"), &log)
	require.NoError(t, err)
	require.Equal(t, nbUnset, log.DisableTimestamp)
}

func TestLogConfig(t *testing.T) {
	var conf Config
	configFile := "log_config.toml"
	f, err := os.Create(configFile)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, f.Close())
		require.NoError(t, os.Remove(configFile))
	}()

	var testLoad = func(confStr string, expectedEnableErrorStack, expectedDisableErrorStack, expectedEnableTimestamp, expectedDisableTimestamp nullableBool, resultedDisableTimestamp, resultedDisableErrorVerbose bool) {
		conf = defaultConf
		_, err = f.WriteString(confStr)
		require.NoError(t, err)
		require.NoError(t, conf.Load(configFile))
		require.NoError(t, conf.Valid())
		require.Equal(t, expectedEnableErrorStack, conf.Log.EnableErrorStack)
		require.Equal(t, expectedDisableErrorStack, conf.Log.DisableErrorStack)
		require.Equal(t, expectedEnableTimestamp, conf.Log.EnableTimestamp)
		require.Equal(t, expectedDisableTimestamp, conf.Log.DisableTimestamp)
		require.Equal(t, logutil.NewLogConfig("info", "text", "tidb-slow.log", "", conf.Log.File, resultedDisableTimestamp, func(config *zaplog.Config) { config.DisableErrorVerbose = resultedDisableErrorVerbose }), conf.Log.ToLogConfig())
		err := f.Truncate(0)
		require.NoError(t, err)
		_, err = f.Seek(0, 0)
		require.NoError(t, err)
	}

	testLoad(`
[Log]
`, nbUnset, nbUnset, nbUnset, nbUnset, false, true)

	testLoad(`
[Log]
enable-timestamp = false
`, nbUnset, nbUnset, nbFalse, nbUnset, true, true)

	testLoad(`
[Log]
enable-timestamp = true
disable-timestamp = false
`, nbUnset, nbUnset, nbTrue, nbFalse, false, true)

	testLoad(`
[Log]
enable-timestamp = false
disable-timestamp = true
`, nbUnset, nbUnset, nbFalse, nbTrue, true, true)

	testLoad(`
[Log]
enable-timestamp = true
disable-timestamp = true
`, nbUnset, nbUnset, nbTrue, nbUnset, false, true)

	testLoad(`
[Log]
enable-error-stack = false
disable-error-stack = false
`, nbFalse, nbUnset, nbUnset, nbUnset, false, true)
}

func TestRemovedVariableCheck(t *testing.T) {
	configTest := []struct {
		options string
		err     error
	}{
		// Invalid is not removed = no error
		{`
		unrecognized-option-test = true
		`, nil},
		// The config file from TiDB 6.0
		{`# TiDB Configuration.
# TiDB server host.
host = "0.0.0.0"

# tidb server advertise IP.
advertise-address = ""

# TiDB server port.
port = 4000

# Registered store name, [tikv, mocktikv, unistore]
store = "unistore"

# TiDB storage path.
path = "/tmp/tidb"

# The socket file to use for connection.
socket = "/tmp/tidb-{Port}.sock"

# Schema lease duration, very dangerous to change only if you know what you do.
lease = "45s"

# When create table, split a separated region for it. It is recommended to
# turn off this option if there will be a large number of tables created.
split-table = true

# The limit of concurrent executed sessions.
token-limit = 1000

# The maximum memory available for a single SQL statement. Default: 1GB
mem-quota-query = 1073741824

# Specifies the temporary storage path for some operators when a single SQL statement exceeds the memory quota specified by mem-quota-query.
# <snip>
# tmp-storage-path = "/tmp/<os/user.Current().Uid>_tidb/MC4wLjAuMDo0MDAwLzAuMC4wLjA6MTAwODA=/tmp-storage"

# Specifies the maximum use of temporary storage (bytes) for all active queries when tidb_enable_tmp_storage_on_oom is enabled.
# If the tmp-storage-quota exceeds the capacity of the temporary storage directory, tidb-server would return an error and exit.
# The default value of tmp-storage-quota is under 0 which means tidb-server wouldn't check the capacity.
tmp-storage-quota = -1

# Specifies what operation TiDB performs when a single SQL statement exceeds the memory quota specified by mem-quota-query and cannot be spilled over to disk.
# Valid options: ["log", "cancel"]
oom-action = "cancel"

# Enable batch commit for the DMLs.
enable-batch-dml = false

# Set system variable 'lower_case_table_names'
lower-case-table-names = 2

# Make "kill query" behavior compatible with MySQL. It's not recommend to
# turn on this option when TiDB server is behind a proxy.
compatible-kill-query = false

# Make SIGTERM wait N seconds before starting the shutdown procedure. This is designed for when TiDB is behind a proxy/load balancer.
# The health check will fail immediately but the server will not start shutting down until the time has elapsed.
graceful-wait-before-shutdown = 0

# check mb4 value in utf8 is used to control whether to check the mb4 characters when the charset is utf8.
check-mb4-value-in-utf8 = true

# treat-old-version-utf8-as-utf8mb4 use for upgrade compatibility. Set to true will treat old version table/column UTF8 charset as UTF8MB4.
treat-old-version-utf8-as-utf8mb4 = true

# max-index-length is used to deal with compatibility issues from v3.0.7 and previous version upgrades. It can only be in [3072, 3072*4].
max-index-length = 3072

# index-limit is used to deal with compatibility issues. It can only be in [64, 64*8].
index-limit = 64

# enable-table-lock is used to control table lock feature. Default is false, indicate the table lock feature is disabled.
enable-table-lock = false

# delay-clean-table-lock is used to control the time (Milliseconds) of delay before unlock the table in the abnormal situation.
delay-clean-table-lock = 0

# Maximum number of the splitting region, which is used by the split region statement.
split-region-max-num = 1000

# alter-primary-key is used to control whether the primary keys are clustered.
# Note that this config is deprecated. Only valid when @@global.tidb_enable_clustered_index = 'int_only'.
# Default is false, only the integer primary keys are clustered.
# If it is true, all types of primary keys are nonclustered.
alter-primary-key = false

# server-version is used to change the version string of TiDB in the following scenarios:
# 1. the server version returned by builtin-function VERSION().
# 2. the server version filled in handshake packets of MySQL Connection Protocol, see https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::Handshake for more details.
# if server-version = "", the default value(original TiDB version string) is used.
server-version = ""

# repair mode is used to repair the broken table meta in TiKV in extreme cases.
repair-mode = false

# Repair table list is used to list the tables in repair mode with the format like ["db.table",].
# In repair mode, repairing table which is not in repair list will get wrong database or wrong table error.
repair-table-list = []

# Whether new collations are enabled, as indicated by its name, this configuration entry take effect ONLY when a TiDB cluster bootstraps for the first time.
new_collations_enabled_on_first_bootstrap = true

# Don't register information of this TiDB to etcd, so this instance of TiDB won't appear in the services like dashboard.
# This option is useful when you want to embed TiDB into your service(i.e. use TiDB as a library).
# *If you want to start a TiDB service, NEVER enable this.*
skip-register-to-dashboard = false

# When enabled, usage data (for example, instance versions) will be reported to PingCAP periodically for user experience analytics.
# If this config is set to false on all TiDB servers, telemetry will be always disabled regardless of the value of the global variable tidb_enable_telemetry.
# See PingCAP privacy policy for details: https://pingcap.com/en/privacy-policy/
enable-telemetry = true

# deprecate-integer-display-length is used to be compatible with MySQL 8.0 in which the integer declared with display length will be returned with
# <snip>
deprecate-integer-display-length = false

# enable-enum-length-limit is used to deal with compatibility issues. When true, the enum/set element length is limited.
# According to MySQL 8.0 Refman:
# The maximum supported length of an individual SET element is M <= 255 and (M x w) <= 1020,
# where M is the element literal length and w is the number of bytes required for the maximum-length character in the character set.
# See https://dev.mysql.com/doc/refman/8.0/en/string-type-syntax.html for more details.
enable-enum-length-limit = true

[instance]
tidb_memory_usage_alarm_ratio = 0.7

# The maximum permitted number of simultaneous client connections. When the value is 0, the number of connections is unlimited.
max_connections = 0

# Run ddl worker on this tidb-server.
tidb_enable_ddl = true

[log]
# Log level: debug, info, warn, error, fatal.
level = "info"

# Log format, one of json or text.
format = "text"

# Enable automatic timestamps in log output, if not set, it will be defaulted to true.
# enable-timestamp = true

# Enable annotating logs with the full stack error message, if not set, it will be defaulted to false.
# enable-error-stack = false

# Whether to enable slow query log.
enable-slow-log = true

# Stores slow query log into separated files.
slow-query-file = "tidb-slow.log"

# Queries with execution time greater than this value will be logged. (Milliseconds)
slow-threshold = 300

# record-plan-in-slow-log is used to enable record query plan in slow log.
# 0 is disable. 1 is enable.
record-plan-in-slow-log = 1

# Queries with internal result greater than this value will be logged.
expensive-threshold = 10000

# Maximum query length recorded in log.
query-log-max-len = 4096

# File logging.
[log.file]
# Log file name.
filename = ""

# Max log file size in MB (upper limit to 4096MB).
max-size = 300

# Max log file keep days. No clean up by default.
max-days = 0

# Maximum number of old log files to retain. No clean up by default.
max-backups = 0

[security]
# Path of file that contains list of trusted SSL CAs for connection with mysql client.
ssl-ca = ""

# Path of file that contains X509 certificate in PEM format for connection with mysql client.
ssl-cert = ""

# Path of file that contains X509 key in PEM format for connection with mysql client.
ssl-key = ""

# Path of file that contains list of trusted SSL CAs for connection with cluster components.
cluster-ssl-ca = ""

# Path of file that contains X509 certificate in PEM format for connection with cluster components.
cluster-ssl-cert = ""

# Path of file that contains X509 key in PEM format for connection with cluster components.
cluster-ssl-key = ""

# Configurations of the encryption method to use for encrypting the spilled data files.
# Possible values are "plaintext", "aes128-ctr", if not set, it will be "plaintext" by default.
# "plaintext" means encryption is disabled.
spilled-file-encryption-method = "plaintext"

# Security Enhanced Mode (SEM) restricts the "SUPER" privilege and requires fine-grained privileges instead.
enable-sem = false

# Automatic creation of TLS certificates.
# Setting it to 'true' is recommended because it is safer and tie with the default configuration of MySQL.
# If this config is commented/missed, the value would be 'false' for the compatibility with TiDB versions that does not support it.
auto-tls = true

# Minium TLS version to use, e.g. "TLSv1.2"
tls-version = ""

# The RSA Key size for automatic generated RSA keys
rsa-key-size = 4096

[status]
# If enable status report HTTP service.
report-status = true

# TiDB status host.
status-host = "0.0.0.0"

## status-host is the HTTP address for reporting the internal status of a TiDB server, for example:
## API for prometheus: http://${status-host}:${status_port}/metrics
## API for pprof:      http://${status-host}:${status_port}/debug/pprof
# TiDB status port.
status-port = 10080

# Prometheus pushgateway address, leaves it empty will disable push to pushgateway.
metrics-addr = ""

# Prometheus client push interval in second, set \"0\" to disable push to pushgateway.
metrics-interval = 15

# Record statements qps by database name if it is enabled.
record-db-qps = false

# Record database name label if it is enabled.
record-db-label = false

[performance]
# Max CPUs to use, 0 use number of CPUs in the machine.
max-procs = 0

# Memory size quota for tidb server, 0 means unlimited
server-memory-quota = 0

# The alarm threshold when memory usage of the tidb-server exceeds. The valid value range is greater than or equal to 0
# and less than or equal to 1. The default value is 0.7.
# If this configuration is set to 0 or 1, it'll disable the alarm.
# <snip>
memory-usage-alarm-ratio = 0.7

# StmtCountLimit limits the max count of statement inside a transaction.
stmt-count-limit = 5000

# Set keep alive option for tcp connection.
tcp-keep-alive = true

# Whether support cartesian product.
cross-join = true

# Stats lease duration, which influences the time of analyze and stats load.
stats-lease = "3s"

# Run auto analyze worker on this tidb-server.
run-auto-analyze = true

# Probability to use the query feedback to update stats, 0.0 or 1.0 for always false/true.
feedback-probability = 0.0

# The max number of query feedback that cache in memory.
query-feedback-limit = 512

# Pseudo stats will be used if the ratio between the modify count and
# row count in statistics of a table is greater than it.
pseudo-estimate-ratio = 0.8

# Force the priority of all statements in a specified priority.
# The value could be "NO_PRIORITY", "LOW_PRIORITY", "HIGH_PRIORITY" or "DELAYED".
force-priority = "NO_PRIORITY"

# Bind info lease duration, which influences the duration of loading bind info and handling invalid bind.
bind-info-lease = "3s"

# Whether support pushing down aggregation with distinct to cop task
distinct-agg-push-down = false

# The limitation of the size in byte for the entries in one transaction.
# If using TiKV as the storage, the entry represents a key/value pair.
# NOTE: If binlog is enabled with Kafka (e.g. arbiter cluster),
# this value should be less than 1073741824(1G) because this is the maximum size that can be handled by Kafka.
# If binlog is disabled or binlog is enabled without Kafka, this value should be less than 1099511627776(1T).
txn-total-size-limit = 104857600

# The limitation of the size in byte for each entry in one transaction.
# NOTE: Increasing this limit may cause performance problems.
txn-entry-size-limit = 6291456

# The max number of running concurrency two phase committer request for an SQL.
committer-concurrency = 128

# max lifetime of transaction ttl manager.
max-txn-ttl = 3600000

# The Go GC trigger factor, you can get more information about it at https://golang.org/pkg/runtime.
# If you encounter OOM when executing large query, you can decrease this value to trigger GC earlier.
# If you find the CPU used by GC is too high or GC is too frequent and impact your business you can increase this value.
gogc = 100

[proxy-protocol]
# PROXY protocol acceptable client networks.
# Empty string means disable PROXY protocol, * means all networks.
networks = ""

# PROXY protocol header read timeout, unit is second
header-timeout = 5

[prepared-plan-cache]
enabled = false
capacity = 1000
memory-guard-ratio = 0.1

[opentracing]
# Enable opentracing.
enable = false

# Whether to enable the rpc metrics.
rpc-metrics = false

[opentracing.sampler]
# Type specifies the type of the sampler: const, probabilistic, rateLimiting, or remote
type = "const"

# Param is a value passed to the sampler.
# Valid values for Param field are:
# - for "const" sampler, 0 or 1 for always false/true respectively
# - for "probabilistic" sampler, a probability between 0 and 1
# - for "rateLimiting" sampler, the number of spans per second
# - for "remote" sampler, param is the same as for "probabilistic"
# and indicates the initial sampling rate before the actual one
# is received from the mothership
param = 1.0

# SamplingServerURL is the address of jaeger-agent's HTTP sampling server
sampling-server-url = ""

# MaxOperations is the maximum number of operations that the sampler
# will keep track of. If an operation is not tracked, a default probabilistic
# sampler will be used rather than the per operation specific sampler.
max-operations = 0

# SamplingRefreshInterval controls how often the remotely controlled sampler will poll
# jaeger-agent for the appropriate sampling strategy.
sampling-refresh-interval = 0

[opentracing.reporter]
# QueueSize controls how many spans the reporter can keep in memory before it starts dropping
# new spans. The queue is continuously drained by a background go-routine, as fast as spans
# can be sent out of process.
queue-size = 0

# BufferFlushInterval controls how often the buffer is force-flushed, even if it's not full.
# It is generally not useful, as it only matters for very low traffic services.
buffer-flush-interval = 0

# LogSpans, when true, enables LoggingReporter that runs in parallel with the main reporter
# and logs all submitted spans. Main Configuration.Logger must be initialized in the code
# for this option to have any effect.
log-spans = false

#  LocalAgentHostPort instructs reporter to send spans to jaeger-agent at this address
local-agent-host-port = ""

[pd-client]
# Max time which PD client will wait for the PD server in seconds.
pd-server-timeout = 3

[tikv-client]
# Max gRPC connections that will be established with each tikv-server.
grpc-connection-count = 4

# After a duration of this time in seconds if the client doesn't see any activity it pings
# the server to see if the transport is still alive.
grpc-keepalive-time = 10

# After having pinged for keepalive check, the client waits for a duration of Timeout in seconds
# and if no activity is seen even after that the connection is closed.
grpc-keepalive-timeout = 3

# The compression type for gRPC channel: none or gzip.
grpc-compression-type = "none"

# Max time for commit command, must be twice bigger than raft election timeout.
commit-timeout = "41s"

# Max batch size in gRPC.
max-batch-size = 128
# Overload threshold of TiKV.
overload-threshold = 200
# Max batch wait time in nanosecond to avoid waiting too long. 0 means disable this feature.
max-batch-wait-time = 0
# Batch wait size, to avoid waiting too long.
batch-wait-size = 8

# Enable chunk encoded data for coprocessor requests.
enable-chunk-rpc = true

# If a Region has not been accessed for more than the given duration (in seconds), it
# will be reloaded from the PD.
region-cache-ttl = 600

# store-limit is used to restrain TiDB from sending request to some stores which is up to the limit.
# If a store has been up to the limit, it will return error for the successive request in same store.
# default 0 means shutting off store limit.
store-limit = 0

# store-liveness-timeout is used to control timeout for store liveness after sending request failed.
store-liveness-timeout = "1s"

# ttl-refreshed-txn-size decides whether a transaction should update its lock TTL.
# If the size(in byte) of a transaction is large than ttl-refreshed-txn-size, it update the lock TTL during the 2PC.
ttl-refreshed-txn-size = 33554432

# If the number of keys that one prewrite request of one region involves exceed this threshold, it will use ResolveLock instead of ResolveLockLite.
resolve-lock-lite-threshold = 16

[tikv-client.copr-cache]
# The capacity in MB of the cache. Zero means disable coprocessor cache.
capacity-mb = 1000.0

[binlog]
# enable to write binlog.
# NOTE: If binlog is enabled with Kafka (e.g. arbiter cluster),
# txn-total-size-limit should be less than 1073741824(1G) because this is the maximum size that can be handled by Kafka.
enable = false

# WriteTimeout specifies how long it will wait for writing binlog to pump.
write-timeout = "15s"

# If IgnoreError is true, when writing binlog meets error, TiDB would stop writing binlog,
# but still provide service.
ignore-error = false

# use socket file to write binlog, for compatible with kafka version tidb-binlog.
binlog-socket = ""

# the strategy for sending binlog to pump, value can be "range" or "hash" now.
strategy = "range"

[pessimistic-txn]
# max retry count for a statement in a pessimistic transaction.
max-retry-count = 256

# The max count of deadlock events that will be recorded in the information_schema.deadlocks table.
deadlock-history-capacity = 10

# Whether retryable deadlocks (in-statement deadlocks) are collected to the information_schema.deadlocks table.
deadlock-history-collect-retryable = false

# If true it means the auto-commit transactions will be in pessimistic mode.
pessimistic-auto-commit = false

# experimental section controls the features that are still experimental: their semantics,
# interfaces are subject to change, using these features in the production environment is not recommended.
[experimental]
# enable creating expression index.
allow-expression-index = false

# server level isolation read by engines and labels
[isolation-read]
# engines means allow the tidb server read data from which types of engines. options: "tikv", "tiflash", "tidb".
engines = ["tikv", "tiflash", "tidb"]
		`, errors.New("The following configuration options are no longer supported in this version of TiDB. Check the release notes for more information: check-mb4-value-in-utf8, enable-batch-dml, instance.tidb_memory_usage_alarm_ratio, log.enable-slow-log, log.expensive-threshold, log.query-log-max-len, log.record-plan-in-slow-log, log.slow-threshold, lower-case-table-names, mem-quota-query, oom-action, performance.committer-concurrency, performance.feedback-probability, performance.force-priority, performance.memory-usage-alarm-ratio, performance.query-feedback-limit, performance.run-auto-analyze, prepared-plan-cache.capacity, prepared-plan-cache.enabled, prepared-plan-cache.memory-guard-ratio")},
	}

	for _, test := range configTest {
		conf := new(Config)
		storeDir := t.TempDir()
		configFile := filepath.Join(storeDir, "config.toml")
		f, err := os.Create(configFile)
		require.NoError(t, err)
		// Write the sample config file
		_, err = f.WriteString(test.options)
		require.NoError(t, err)
		require.NoError(t, f.Sync())
		// Check the error matches our expectations.
		err = conf.RemovedVariableCheck(configFile)
		if test.err != nil {
			require.Equal(t, test.err.Error(), err.Error())
		} else {
			require.NoError(t, err)
		}
		// Delete the file so we can start again
		require.NoError(t, os.Remove(configFile))
	}
	// Make sure the current config file has no removed items
	// since this is a bad user experience.
	conf := new(Config)
	configFile := "config.toml.example"
	err := conf.RemovedVariableCheck(configFile)
	require.NoError(t, err)
}

func TestConfig(t *testing.T) {
	conf := new(Config)
	conf.TempStoragePath = tempStorageDirName
	conf.Binlog.Enable = true
	conf.Binlog.IgnoreError = true
	conf.Binlog.Strategy = "hash"
	conf.Performance.TxnTotalSizeLimit = 1000
	conf.TiKVClient.CommitTimeout = "10s"
	conf.TiKVClient.RegionCacheTTL = 600
	conf.Instance.EnableSlowLog.Store(logutil.DefaultTiDBEnableSlowLog)
	storeDir := t.TempDir()
	configFile := filepath.Join(storeDir, "config.toml")
	f, err := os.Create(configFile)
	require.NoError(t, err)
	defer func(configFile string) {
		require.NoError(t, os.Remove(configFile))
	}(configFile)

	// Make sure the server refuses to start if there's an unrecognized configuration option
	_, err = f.WriteString(`
unrecognized-option-test = true
`)
	require.NoError(t, err)
	require.NoError(t, f.Sync())

	err = conf.Load(configFile)
	require.Error(t, err)
	match, err := regexp.Match("(?:.|\n)*invalid configuration option(?:.|\n)*", []byte(err.Error()))
	require.NoError(t, err)
	require.True(t, match)
	require.Equal(t, uint32(0), conf.Instance.MaxConnections)

	err = f.Truncate(0)
	require.NoError(t, err)
	_, err = f.Seek(0, 0)
	require.NoError(t, err)

	_, err = f.WriteString(`
token-limit = 0
enable-table-lock = true
alter-primary-key = true
delay-clean-table-lock = 5
split-region-max-num=10000
server-version = "test_version"
repair-mode = true
max-index-length = 3080
index-limit = 70
table-column-count-limit = 4000
skip-register-to-dashboard = true
deprecate-integer-display-length = true
enable-enum-length-limit = false
stores-refresh-interval = 30
enable-forwarding = true
enable-global-kill = true
tidb-max-reuse-chunk = 10
tidb-max-reuse-column = 20
tidb-enable-exit-check = false
[performance]
txn-total-size-limit=2000
tcp-no-delay = false
enable-load-fmsketch = true
plan-replayer-dump-worker-concurrency = 1
lite-init-stats = false
force-init-stats = false
[tikv-client]
commit-timeout="41s"
max-batch-size=128
region-cache-ttl=6000
store-limit=0
ttl-refreshed-txn-size=8192
resolve-lock-lite-threshold = 16
copr-req-timeout = "120s"
enable-replica-selector-v2 = false
[tikv-client.async-commit]
keys-limit=123
total-key-size-limit=1024
[experimental]
allow-expression-index = true
[isolation-read]
engines = ["tiflash"]
[labels]
foo= "bar"
group= "abc"
zone= "dc-1"
[security]
spilled-file-encryption-method = "plaintext"
[pessimistic-txn]
deadlock-history-capacity = 123
deadlock-history-collect-retryable = true
pessimistic-auto-commit = true
[top-sql]
receiver-address = "127.0.0.1:10100"
[status]
grpc-keepalive-time = 20
grpc-keepalive-timeout = 10
grpc-concurrent-streams = 2048
grpc-initial-window-size = 10240
grpc-max-send-msg-size = 40960
[instance]
max_connections = 200
`)

	require.NoError(t, err)
	require.NoError(t, f.Sync())

	require.NoError(t, conf.Load(configFile))

	// Test that the original value will not be clear by load the config file that does not contain the option.
	require.True(t, conf.Binlog.Enable)
	require.Equal(t, "hash", conf.Binlog.Strategy)

	// Test that the value will be overwritten by the config file.
	require.Equal(t, uint64(2000), conf.Performance.TxnTotalSizeLimit)
	require.True(t, conf.AlterPrimaryKey)
	require.False(t, conf.Performance.TCPNoDelay)

	require.Equal(t, "41s", conf.TiKVClient.CommitTimeout)
	require.Equal(t, uint(123), conf.TiKVClient.AsyncCommit.KeysLimit)
	require.Equal(t, uint64(1024), conf.TiKVClient.AsyncCommit.TotalKeySizeLimit)
	require.Equal(t, uint(128), conf.TiKVClient.MaxBatchSize)
	require.Equal(t, uint(6000), conf.TiKVClient.RegionCacheTTL)
	require.Equal(t, int64(0), conf.TiKVClient.StoreLimit)
	require.Equal(t, int64(8192), conf.TiKVClient.TTLRefreshedTxnSize)
	require.Equal(t, false, conf.TiKVClient.EnableReplicaSelectorV2)
	require.Equal(t, true, defaultConf.TiKVClient.EnableReplicaSelectorV2)
	require.Equal(t, uint(1000), conf.TokenLimit)
	require.True(t, conf.EnableTableLock)
	require.Equal(t, uint64(5), conf.DelayCleanTableLock)
	require.Equal(t, uint64(10000), conf.SplitRegionMaxNum)
	require.True(t, conf.RepairMode)
	require.Equal(t, uint64(16), conf.TiKVClient.ResolveLockLiteThreshold)
	require.Equal(t, 120*time.Second, conf.TiKVClient.CoprReqTimeout)
	require.Equal(t, uint32(200), conf.Instance.MaxConnections)
	require.Equal(t, uint32(10), conf.TiDBMaxReuseChunk)
	require.Equal(t, uint32(20), conf.TiDBMaxReuseColumn)
	require.Equal(t, []string{"tiflash"}, conf.IsolationRead.Engines)
	require.Equal(t, 3080, conf.MaxIndexLength)
	require.Equal(t, 70, conf.IndexLimit)
	require.Equal(t, uint32(4000), conf.TableColumnCountLimit)
	require.True(t, conf.SkipRegisterToDashboard)
	require.Equal(t, 3, len(conf.Labels))
	require.Equal(t, "bar", conf.Labels["foo"])
	require.Equal(t, "abc", conf.Labels["group"])
	require.Equal(t, "dc-1", conf.Labels["zone"])
	require.Equal(t, SpilledFileEncryptionMethodPlaintext, conf.Security.SpilledFileEncryptionMethod)
	require.True(t, conf.DeprecateIntegerDisplayWidth)
	require.False(t, conf.EnableEnumLengthLimit)
	require.True(t, conf.EnableForwarding)
	require.Equal(t, uint64(30), conf.StoresRefreshInterval)
	require.Equal(t, uint(123), conf.PessimisticTxn.DeadlockHistoryCapacity)
	require.True(t, conf.PessimisticTxn.DeadlockHistoryCollectRetryable)
	require.True(t, conf.PessimisticTxn.PessimisticAutoCommit.Load())
	require.Equal(t, "127.0.0.1:10100", conf.TopSQL.ReceiverAddress)
	require.True(t, conf.Experimental.AllowsExpressionIndex)
	require.Equal(t, uint(20), conf.Status.GRPCKeepAliveTime)
	require.Equal(t, uint(10), conf.Status.GRPCKeepAliveTimeout)
	require.Equal(t, uint(2048), conf.Status.GRPCConcurrentStreams)
	require.Equal(t, 10240, conf.Status.GRPCInitialWindowSize)
	require.Equal(t, 40960, conf.Status.GRPCMaxSendMsgSize)
	require.True(t, conf.Performance.EnableLoadFMSketch)
	require.False(t, conf.Performance.LiteInitStats)
	require.False(t, conf.Performance.ForceInitStats)

	err = f.Truncate(0)
	require.NoError(t, err)
	_, err = f.Seek(0, 0)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	_, err = f.WriteString(`
[log.file]
log-rotate = true
[performance]
mem-profile-interval="1m"
[stmt-summary]
enable=false
enable-internal-query=true
max-stmt-count=1000
max-sql-length=1024
refresh-interval=100
history-size=100`)
	require.NoError(t, err)
	err = conf.Load(configFile)
	tmp := err.(*ErrConfigValidationFailed)
	require.True(t, isAllRemovedConfigItems(tmp.UndecodedItems), fmt.Sprintf("some config items were not in the removed list: %#v", tmp.UndecodedItems))

	// Test telemetry config default value and whether it will be overwritten.
	conf = NewConfig()
	err = f.Truncate(0)
	require.NoError(t, err)
	_, err = f.Seek(0, 0)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, conf.Load(configFile))
	require.False(t, conf.EnableTelemetry)

	_, err = f.WriteString(`
enable-table-lock = true
`)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, conf.Load(configFile))
	require.False(t, conf.EnableTelemetry)

	_, err = f.WriteString(`
enable-telemetry = true
`)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, conf.Load(configFile))
	require.True(t, conf.EnableTelemetry)

	_, err = f.WriteString(`
[security]
spilled-file-encryption-method = "aes128-ctr"
`)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, conf.Load(configFile))
	require.Equal(t, SpilledFileEncryptionMethodAES128CTR, conf.Security.SpilledFileEncryptionMethod)

	require.NoError(t, f.Sync())
	require.NoError(t, conf.Load(configFile))

	configFile = "config.toml.example"
	require.NoError(t, conf.Load(configFile))

	// Make sure the example config is the same as default config except `auto_tls`.
	conf.Security.AutoTLS = false
	require.Equal(t, GetGlobalConfig(), conf)

	// Test for log config.
	require.Equal(t, logutil.NewLogConfig("info", "text", "tidb-slow.log", "", conf.Log.File, false, func(config *zaplog.Config) { config.DisableErrorVerbose = conf.Log.getDisableErrorStack() }), conf.Log.ToLogConfig())

	// Test for tracing config.
	tracingConf := &tracing.Configuration{
		Disabled: true,
		Reporter: &tracing.ReporterConfig{},
		Sampler:  &tracing.SamplerConfig{Type: "const", Param: 1.0},
	}
	require.Equal(t, conf.OpenTracing.ToTracingConfig(), tracingConf)

	// Test for TLS config.
	certFile := "cert.pem"
	f, err = os.Create(certFile)
	require.NoError(t, err)
	_, err = f.WriteString(`-----BEGIN CERTIFICATE-----
MIIC+jCCAeKgAwIBAgIRALsvlisKJzXtiwKcv7toreswDQYJKoZIhvcNAQELBQAw
EjEQMA4GA1UEChMHQWNtZSBDbzAeFw0xOTAzMTMwNzExNDhaFw0yMDAzMTIwNzEx
NDhaMBIxEDAOBgNVBAoTB0FjbWUgQ28wggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAw
ggEKAoIBAQDECyY5cZ4SccQdk4XCgENwOLsE92uZvutBcYHk8ndIpxuxQnmS/2af
JxWlduKgauuLlwRYrzwvmUQumzB0LIJIwZN37KMeepTv+cf1Iv0U1Tw2PyXa7jD1
VxccI7lHxqObYrnLdZ1AOG2SyWoJp/g6jZqbdGnYAbBxbZXYv9FyA6h0FksDysEP
62zu5YwtRcmhob7L5Wezq0/eV/2U1WdbGGWMCUs2LKQav4TP7Kaopk+MAl9UpSoc
arl+NGxs39TsvrxQvT7k/W6g7mo0rOc5PEc6Zho2+E8JjnEYCdGKmMW/Bea6V1yc
ShMe79lwN7ISCw3e7GZhZGM2XFTjvhH/AgMBAAGjSzBJMA4GA1UdDwEB/wQEAwIF
oDATBgNVHSUEDDAKBggrBgEFBQcDATAMBgNVHRMBAf8EAjAAMBQGA1UdEQQNMAuC
CWxvY2FsaG9zdDANBgkqhkiG9w0BAQsFAAOCAQEAK+pS76DxAbQBdbpyqt0Xi1QY
SnWxFEFepP3pHC28oy8fzHiys9fwMvJwgMgLcwyB9GUhMZ/xsO2ehutWbzYCCRmV
4einEx9Ipr26i2txzZPphqXNkV+ZhPeQK54fWrzAkRq4qKNyoYfvhldZ+uTuKNiS
If0KbvbS6qDfimA+m0m6n5yDzc5tPl+kgKyeivSyqeG7T9m40gvCLAMgI7iTFhIZ
BvUPi88z3wGa8rmhn9dOvkwauLFU5i5dqoz6m9HXmaEKzAAigGzgU8vPDt/Dxxgu
c933WW1E0hCtvuGxWFIFtoJMQoyH0Pl4ACmY/6CokCCZKDInrPdhhf3MGRjkkw==
-----END CERTIFICATE-----
`)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	keyFile := "key.pem"
	f, err = os.Create(keyFile)
	require.NoError(t, err)
	_, err = f.WriteString(`-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAxAsmOXGeEnHEHZOFwoBDcDi7BPdrmb7rQXGB5PJ3SKcbsUJ5
kv9mnycVpXbioGrri5cEWK88L5lELpswdCyCSMGTd+yjHnqU7/nH9SL9FNU8Nj8l
2u4w9VcXHCO5R8ajm2K5y3WdQDhtkslqCaf4Oo2am3Rp2AGwcW2V2L/RcgOodBZL
A8rBD+ts7uWMLUXJoaG+y+Vns6tP3lf9lNVnWxhljAlLNiykGr+Ez+ymqKZPjAJf
VKUqHGq5fjRsbN/U7L68UL0+5P1uoO5qNKznOTxHOmYaNvhPCY5xGAnRipjFvwXm
uldcnEoTHu/ZcDeyEgsN3uxmYWRjNlxU474R/wIDAQABAoIBAGyZAIOxvK7a9pir
r90e0DzKME9//8sbR5bpGduJtSo558051b7oXCCttgAC62eR0wlwjqfR6rUzYeGv
dhfk0AcdtGMqYvHvVbHZ3DqfNzLjLIegU4gDintd0x9zap+oGdlpxyI99O4uVASM
LoFK2ucUqiCTTE6sIOG0ot1+5LcS9xlygmmBfl8Q+6dG1D+vtPlU4J1kQ1MZV/JI
01Mbea4iiUKD9qrbxfsMiu52u/J3MMoWJHsvAA/LpOp2Ua6pUECluZECslxYSnJJ
IyjeGYxAIfXj81bqBk3RpemlX7YAxMbn6noZPQ6KUzS4IT2clrG10boCBdUNK1pN
WjVOEoECgYEA0/aP1wvrC3sZtHmURHv1El0vmaZocmH3sdwUfrW5cMqqhOosax6d
5iKAJQ1cAL6ZivIB4WJ3X8mlfMOaHPOQxqdudPui0sMHQehT2NBl/gwX9wXMwxXl
t+ebqK5DSSbVuJQS45sSdYPQvrMVDB/owHHjfdeOk1EwmqxHv1r338UCgYEA7MXk
IIF+LETxkw4QqbEPzwJ8kVRjkU3jmlEClOatTe+RQJxanErgMiGi9NZMM+Vm5GjC
5kzAuNgMDuD/NAWyzPzWd+mbeG/2IHYf44OiK1TmnFHkTc0JW7s4tUQgDMQccheR
EgA3UDGU9aevUoUDUhpeXxBdtnf66qw0e1cSovMCgYBLJdg7UsNjT6J+ZLhXS2dI
unb8z42qN+d8TF2LytvTDFdGRku3MqSiicrK2CCtNuXy5/gYszNFZ5VfVW3XI9dJ
RuUXXnuMo454JGlNrhzq49i/QHQnGiVWfSunsxix363YAc9smHcD6NbiNVWZ9dos
GHSiEgE/Y4KK49eQFS1aTQKBgQC+xzznTC+j7/FOcjjO4hJA1FoWp46Kl93ai4eu
/qeJcozxKIqCAHrhKeUprjo8Xo0nYZoZAqMOzVX57yTyf9zv+pG8kQhqZJxGz6cm
JPxYOdKPBhUU8y6lMReiRsAkSSg6be7AOFhZT3oc7f4AWZixYPnFU2SPD+GnkRXA
hApKLQKBgHUG+SjrxQjiFipE52YNGFLzbMR6Uga4baACW05uGPpao/+MkCGRAidL
d/8eU66iPNt/23iVAbqkF8mRpCxC0+O5HRqTEzgrlWKabXfmhYqIVjq+tkonJ0NU
xkNuJ2BlEGkwWLiRbKy1lNBBFUXKuhh3L/EIY10WTnr3TQzeL6H1
-----END RSA PRIVATE KEY-----
`)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	conf.Security.ClusterSSLCA = certFile
	conf.Security.ClusterSSLCert = certFile
	conf.Security.ClusterSSLKey = keyFile
	clusterSecurity := conf.Security.ClusterSecurity()
	tlsConfig, err := clusterSecurity.ToTLSConfig()
	require.NoError(t, err)
	require.NotNil(t, tlsConfig)

	// Note that on windows, we can't Remove a file if the file is not closed.
	// The behavior is different on linux, we can always Remove a file even
	// if it's open. The OS maintains a reference count for open/close, the file
	// is recycled when the reference count drops to 0.
	require.NoError(t, os.Remove(certFile))
	require.NoError(t, os.Remove(keyFile))

	// test for config `toml` and `json` tag names
	c1 := Config{}
	st := reflect.TypeOf(c1)
	for i := 0; i < st.NumField(); i++ {
		field := st.Field(i)
		require.Equal(t, field.Tag.Get("json"), field.Tag.Get("toml"))
	}
}

func TestTxnTotalSizeLimitValid(t *testing.T) {
	conf := NewConfig()
	tests := []struct {
		limit uint64
		valid bool
	}{
		{4 << 10, true},
		{10 << 30, true},
		{10<<30 + 1, true},
		{1 << 40, true},
		{1<<40 + 1, false},
	}

	for _, tt := range tests {
		conf.Performance.TxnTotalSizeLimit = tt.limit
		require.Equal(t, tt.valid, conf.Valid() == nil)
	}
}

func TestConflictInstanceConfig(t *testing.T) {
	var expectedNewName string
	conf := new(Config)
	storeDir := t.TempDir()
	configFile := filepath.Join(storeDir, "config.toml")

	f, err := os.Create(configFile)
	require.NoError(t, err)
	defer func(configFile string) {
		require.NoError(t, os.Remove(configFile))
	}(configFile)

	// ConflictOptions indicates the options existing in both [instance] and some other sessions.
	// Just receive a warning and keep their respective values.
	expectedConflictOptions := map[string]InstanceConfigSection{
		"": {
			"", map[string]string{
				"check-mb4-value-in-utf8": "tidb_check_mb4_value_in_utf8",
				"run-ddl":                 "tidb_enable_ddl",
			},
		},
		"log": {
			"log", map[string]string{"enable-slow-log": "tidb_enable_slow_log"},
		},
		"performance": {
			"performance", map[string]string{"force-priority": "tidb_force_priority"},
		},
	}
	_, err = f.WriteString("check-mb4-value-in-utf8 = true \nrun-ddl = true \n" +
		"[log] \nenable-slow-log = true \n" +
		"[performance] \nforce-priority = \"NO_PRIORITY\"\n" +
		"[instance] \ntidb_check_mb4_value_in_utf8 = false \ntidb_enable_slow_log = false \ntidb_force_priority = \"LOW_PRIORITY\"\ntidb_enable_ddl = false")
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	err = conf.Load(configFile)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "Conflict configuration options exists on both [instance] section and some other sections."))
	require.False(t, conf.Instance.CheckMb4ValueInUTF8.Load())
	require.True(t, conf.CheckMb4ValueInUTF8.Load())
	require.Equal(t, true, conf.Log.EnableSlowLog.Load())
	require.Equal(t, false, conf.Instance.EnableSlowLog.Load())
	require.Equal(t, "NO_PRIORITY", conf.Performance.ForcePriority)
	require.Equal(t, "LOW_PRIORITY", conf.Instance.ForcePriority)
	require.Equal(t, true, conf.RunDDL)
	require.Equal(t, false, conf.Instance.TiDBEnableDDL.Load())
	require.Equal(t, 0, len(DeprecatedOptions))
	for _, conflictOption := range ConflictOptions {
		expectedConflictOption, ok := expectedConflictOptions[conflictOption.SectionName]
		require.True(t, ok)
		for oldName, newName := range conflictOption.NameMappings {
			expectedNewName, ok = expectedConflictOption.NameMappings[oldName]
			require.True(t, ok)
			require.Equal(t, expectedNewName, newName)
		}
	}
}

func TestDeprecatedConfig(t *testing.T) {
	var expectedNewName string
	conf := new(Config)
	storeDir := t.TempDir()
	configFile := filepath.Join(storeDir, "config.toml")

	f, err := os.Create(configFile)
	require.NoError(t, err)
	defer func(configFile string) {
		require.NoError(t, os.Remove(configFile))
	}(configFile)

	// DeprecatedOptions indicates the options that should be moved to [instance] section.
	// The value in conf.Instance.* would be overwritten by the other sections.
	expectedDeprecatedOptions := map[string]InstanceConfigSection{
		"": {
			"", map[string]string{
				"enable-collect-execution-info": "tidb_enable_collect_execution_info",
				"run-ddl":                       "tidb_enable_ddl",
			},
		},
		"log": {
			"log", map[string]string{"slow-threshold": "tidb_slow_log_threshold"},
		},
		"performance": {
			"performance", map[string]string{"memory-usage-alarm-ratio": "tidb_memory_usage_alarm_ratio"},
		},
		"plugin": {
			"plugin", map[string]string{
				"load": "plugin_load",
				"dir":  "plugin_dir",
			},
		},
	}
	_, err = f.WriteString("enable-collect-execution-info = false \nrun-ddl = false \n" +
		"[plugin] \ndir=\"/plugin-path\" \nload=\"audit-1,whitelist-1\" \n" +
		"[log] \nslow-threshold = 100 \n" +
		"[performance] \nmemory-usage-alarm-ratio = 0.5")
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	err = conf.Load(configFile)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "Some configuration options should be moved to [instance] section."))
	for _, deprecatedOption := range DeprecatedOptions {
		expectedDeprecatedOption, ok := expectedDeprecatedOptions[deprecatedOption.SectionName]
		require.True(t, ok)
		for oldName, newName := range deprecatedOption.NameMappings {
			expectedNewName, ok = expectedDeprecatedOption.NameMappings[oldName]
			require.True(t, ok, fmt.Sprintf("Get unexpected %s.", oldName))
			require.Equal(t, expectedNewName, newName)
		}
	}
}

func TestMaxIndexLength(t *testing.T) {
	conf := NewConfig()
	checkValid := func(indexLen int, shouldBeValid bool) {
		conf.MaxIndexLength = indexLen
		require.Equal(t, shouldBeValid, conf.Valid() == nil)
	}
	checkValid(DefMaxIndexLength, true)
	checkValid(DefMaxIndexLength-1, false)
	checkValid(DefMaxOfMaxIndexLength, true)
	checkValid(DefMaxOfMaxIndexLength+1, false)
}

func TestIndexLimit(t *testing.T) {
	conf := NewConfig()
	checkValid := func(indexLimit int, shouldBeValid bool) {
		conf.IndexLimit = indexLimit
		require.Equal(t, shouldBeValid, conf.Valid() == nil)
	}
	checkValid(DefIndexLimit, true)
	checkValid(DefIndexLimit-1, false)
	checkValid(DefMaxOfIndexLimit, true)
	checkValid(DefMaxOfIndexLimit+1, false)
}

func TestTableColumnCountLimit(t *testing.T) {
	conf := NewConfig()
	checkValid := func(tableColumnLimit int, shouldBeValid bool) {
		conf.TableColumnCountLimit = uint32(tableColumnLimit)
		require.Equal(t, shouldBeValid, conf.Valid() == nil)
	}
	checkValid(DefTableColumnCountLimit, true)
	checkValid(DefTableColumnCountLimit-1, false)
	checkValid(DefMaxOfTableColumnCountLimit, true)
	checkValid(DefMaxOfTableColumnCountLimit+1, false)
}

func TestTokenLimit(t *testing.T) {
	storeDir := t.TempDir()
	configFile := filepath.Join(storeDir, "config.toml")
	f, err := os.Create(configFile)
	require.NoError(t, err)
	defer func(configFile string) {
		require.NoError(t, os.Remove(configFile))
	}(configFile)

	tests := []struct {
		tokenLimit         uint
		expectedTokenLimit uint
	}{
		{
			0,
			1000,
		},
		{
			99999999999,
			MaxTokenLimit,
		},
	}

	for _, test := range tests {
		require.NoError(t, f.Truncate(0))
		_, err = f.Seek(0, 0)
		require.NoError(t, err)
		_, err = f.WriteString(fmt.Sprintf(`
token-limit = %d
`, test.tokenLimit))
		require.NoError(t, err)
		require.NoError(t, f.Sync())
		conf := NewConfig()
		require.NoError(t, conf.Load(configFile))
		require.Equal(t, test.expectedTokenLimit, conf.TokenLimit)
	}
}

func TestEncodeDefTempStorageDir(t *testing.T) {
	tests := []struct {
		host       string
		statusHost string
		port       uint
		statusPort uint
		expect     string
	}{
		{"0.0.0.0", "0.0.0.0", 4000, 10080, "MC4wLjAuMDo0MDAwLzAuMC4wLjA6MTAwODA="},
		{"127.0.0.1", "127.16.5.1", 4000, 10080, "MTI3LjAuMC4xOjQwMDAvMTI3LjE2LjUuMToxMDA4MA=="},
		{"127.0.0.1", "127.16.5.1", 4000, 15532, "MTI3LjAuMC4xOjQwMDAvMTI3LjE2LjUuMToxNTUzMg=="},
	}

	var osUID string
	currentUser, err := user.Current()
	if err != nil {
		osUID = ""
	} else {
		osUID = currentUser.Uid
	}

	dirPrefix := filepath.Join(os.TempDir(), osUID+"_tidb")
	for _, test := range tests {
		tempStorageDir := encodeDefTempStorageDir(os.TempDir(), test.host, test.statusHost, test.port, test.statusPort)
		require.Equal(t, filepath.Join(dirPrefix, test.expect, "tmp-storage"), tempStorageDir)
	}
}

func TestModifyThroughLDFlags(t *testing.T) {
	tests := []struct {
		Edition               string
		CheckBeforeDropLDFlag string
		EnableTelemetry       bool
		CheckTableBeforeDrop  bool
	}{
		{"Community", "None", false, false},
		{"Community", "1", false, true},
		{"Enterprise", "None", false, false},
		{"Enterprise", "1", false, true},
	}

	originalEnableTelemetry := defaultConf.EnableTelemetry
	originalCheckTableBeforeDrop := CheckTableBeforeDrop
	originalGlobalConfig := GetGlobalConfig()

	for _, test := range tests {
		defaultConf.EnableTelemetry = false
		CheckTableBeforeDrop = false

		initByLDFlags(test.Edition, test.CheckBeforeDropLDFlag)

		conf := GetGlobalConfig()
		require.Equal(t, test.EnableTelemetry, conf.EnableTelemetry)
		require.Equal(t, test.EnableTelemetry, defaultConf.EnableTelemetry)
		require.Equal(t, test.CheckTableBeforeDrop, CheckTableBeforeDrop)
	}

	defaultConf.EnableTelemetry = originalEnableTelemetry
	CheckTableBeforeDrop = originalCheckTableBeforeDrop
	StoreGlobalConfig(originalGlobalConfig)
}

func TestSecurityValid(t *testing.T) {
	c1 := NewConfig()
	tests := []struct {
		spilledFileEncryptionMethod string
		valid                       bool
	}{
		{"", false},
		{"Plaintext", true},
		{"plaintext123", false},
		{"aes256-ctr", false},
		{"aes128-ctr", true},
	}
	for _, tt := range tests {
		c1.Security.SpilledFileEncryptionMethod = tt.spilledFileEncryptionMethod
		require.Equal(t, tt.valid, c1.Valid() == nil)
	}
}

func TestTcpNoDelay(t *testing.T) {
	c1 := NewConfig()
	// check default value
	require.True(t, c1.Performance.TCPNoDelay)
}

func TestGetJSONConfig(t *testing.T) {
	conf, err := GetJSONConfig()
	require.NoError(t, err)

	// Make sure that hidden and deprecated items are not listed in the conf
	require.NotContains(t, conf, "index-usage-sync-lease")
	require.NotContains(t, conf, "enable-batch-dml")
	require.NotContains(t, conf, "mem-quota-query")
	require.NotContains(t, conf, "query-log-max-len")
	require.NotContains(t, conf, "oom-action")

	require.Contains(t, conf, "stmt-count-limit")
	require.Contains(t, conf, "rpc-metrics")
}

func TestConfigExample(t *testing.T) {
	conf := NewConfig()
	configFile := "config.toml.example"
	metaData, err := toml.DecodeFile(configFile, conf)
	require.NoError(t, err)
	keys := metaData.Keys()
	for _, key := range keys {
		for _, s := range key {
			require.False(t, ContainHiddenConfig(s), fmt.Sprintf("%s should be hidden", s))
		}
	}
}

func TestStatsLoadLimit(t *testing.T) {
	conf := NewConfig()
	checkConcurrencyValid := func(concurrency int, shouldBeValid bool) {
		conf.Performance.StatsLoadConcurrency = concurrency
		require.Equal(t, shouldBeValid, conf.Valid() == nil)
	}
	checkConcurrencyValid(DefStatsLoadConcurrencyLimit, true)
	checkConcurrencyValid(DefStatsLoadConcurrencyLimit-1, false)
	checkConcurrencyValid(DefMaxOfStatsLoadConcurrencyLimit, true)
	checkConcurrencyValid(DefMaxOfStatsLoadConcurrencyLimit+1, false)
	conf = NewConfig()
	checkQueueSizeValid := func(queueSize int, shouldBeValid bool) {
		conf.Performance.StatsLoadQueueSize = uint(queueSize)
		require.Equal(t, shouldBeValid, conf.Valid() == nil)
	}
	checkQueueSizeValid(DefStatsLoadQueueSizeLimit, true)
	checkQueueSizeValid(DefStatsLoadQueueSizeLimit-1, false)
	checkQueueSizeValid(DefMaxOfStatsLoadQueueSizeLimit, true)
	checkQueueSizeValid(DefMaxOfStatsLoadQueueSizeLimit+1, false)
}

func TestGetGlobalKeyspaceName(t *testing.T) {
	conf := NewConfig()
	require.Empty(t, conf.KeyspaceName)

	UpdateGlobal(func(conf *Config) {
		conf.KeyspaceName = "test"
	})

	require.Equal(t, "test", GetGlobalKeyspaceName())

	UpdateGlobal(func(conf *Config) {
		conf.KeyspaceName = ""
	})
}

func TestAutoScalerConfig(t *testing.T) {
	conf := NewConfig()
	require.False(t, conf.UseAutoScaler)

	conf = GetGlobalConfig()
	require.False(t, conf.UseAutoScaler)

	UpdateGlobal(func(conf *Config) {
		conf.UseAutoScaler = true
	})
	require.True(t, GetGlobalConfig().UseAutoScaler)

	UpdateGlobal(func(conf *Config) {
		conf.UseAutoScaler = false
	})
}

func TestInvalidConfigWithDeprecatedConfig(t *testing.T) {
	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "config.toml")

	f, err := os.Create(configFile)
	require.NoError(t, err)

	_, err = f.WriteString(`
[log]
slow-threshold = 1000
[performance]
enforce-mpp = 1
	`)
	require.NoError(t, err)
	require.NoError(t, f.Sync())

	var conf Config
	err = conf.Load(configFile)
	require.Error(t, err)
	require.Equal(t, err.Error(), "toml: line 5 (last key \"performance.enforce-mpp\"): incompatible types: TOML value has type int64; destination has type boolean")
}
