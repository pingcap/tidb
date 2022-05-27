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
	"runtime"
	"strings"
	"testing"

	"github.com/BurntSushi/toml"
	zaplog "github.com/pingcap/log"
	"github.com/pingcap/tidb/util/logutil"
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
		require.Equal(t, logutil.NewLogConfig("info", "text", "tidb-slow.log", conf.Log.File, resultedDisableTimestamp, func(config *zaplog.Config) { config.DisableErrorVerbose = resultedDisableErrorVerbose }), conf.Log.ToLogConfig())
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
	configFile := "config.toml"
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
	require.Equal(t, uint32(0), conf.MaxServerConnections)

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
max-server-connections = 200
max-index-length = 3080
index-limit = 70
table-column-count-limit = 4000
skip-register-to-dashboard = true
deprecate-integer-display-length = true
enable-enum-length-limit = false
stores-refresh-interval = 30
enable-forwarding = true
enable-global-kill = true
[performance]
txn-total-size-limit=2000
tcp-no-delay = false
[tikv-client]
commit-timeout="41s"
max-batch-size=128
region-cache-ttl=6000
store-limit=0
ttl-refreshed-txn-size=8192
resolve-lock-lite-threshold = 16
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
	require.Equal(t, uint(1000), conf.TokenLimit)
	require.True(t, conf.EnableTableLock)
	require.Equal(t, uint64(5), conf.DelayCleanTableLock)
	require.Equal(t, uint64(10000), conf.SplitRegionMaxNum)
	require.True(t, conf.RepairMode)
	require.Equal(t, uint64(16), conf.TiKVClient.ResolveLockLiteThreshold)
	require.Equal(t, uint32(200), conf.MaxServerConnections)
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
	require.True(t, isAllDeprecatedConfigItems(tmp.UndecodedItems))

	// Test telemetry config default value and whether it will be overwritten.
	conf = NewConfig()
	err = f.Truncate(0)
	require.NoError(t, err)
	_, err = f.Seek(0, 0)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, conf.Load(configFile))
	require.True(t, conf.EnableTelemetry)

	_, err = f.WriteString(`
enable-table-lock = true
`)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, conf.Load(configFile))
	require.True(t, conf.EnableTelemetry)

	_, err = f.WriteString(`
enable-telemetry = false
`)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, conf.Load(configFile))
	require.False(t, conf.EnableTelemetry)

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
	require.Equal(t, logutil.NewLogConfig("info", "text", "tidb-slow.log", conf.Log.File, false, func(config *zaplog.Config) { config.DisableErrorVerbose = conf.Log.getDisableErrorStack() }), conf.Log.ToLogConfig())

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
	configFile := "config.toml"
	_, localFile, _, _ := runtime.Caller(0)
	configFile = filepath.Join(filepath.Dir(localFile), configFile)

	f, err := os.Create(configFile)
	require.NoError(t, err)
	defer func(configFile string) {
		require.NoError(t, os.Remove(configFile))
	}(configFile)

	// ConflictOptions indicates the options existing in both [instance] and some other sessions.
	// Just receive a warning and keep their respective values.
	expectedConflictOptions := map[string]InstanceConfigSection{
		"": {
			"", map[string]string{"check-mb4-value-in-utf8": "tidb_check_mb4_value_in_utf8"},
		},
		"log": {
			"log", map[string]string{"enable-slow-log": "tidb_enable_slow_log"},
		},
		"performance": {
			"performance", map[string]string{"force-priority": "tidb_force_priority"},
		},
	}
	_, err = f.WriteString("check-mb4-value-in-utf8 = true \n" +
		"[log] \nenable-slow-log = true \n" +
		"[performance] \nforce-priority = \"NO_PRIORITY\"\n" +
		"[instance] \ntidb_check_mb4_value_in_utf8 = false \ntidb_enable_slow_log = false \ntidb_force_priority = \"LOW_PRIORITY\"")
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
	configFile := "config.toml"
	_, localFile, _, _ := runtime.Caller(0)
	configFile = filepath.Join(filepath.Dir(localFile), configFile)

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
	_, err = f.WriteString("enable-collect-execution-info = false \n" +
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
		{"Community", "None", true, false},
		{"Community", "1", true, true},
		{"Enterprise", "None", false, false},
		{"Enterprise", "1", false, true},
	}

	originalEnableTelemetry := defaultConf.EnableTelemetry
	originalCheckTableBeforeDrop := CheckTableBeforeDrop
	originalGlobalConfig := GetGlobalConfig()

	for _, test := range tests {
		defaultConf.EnableTelemetry = true
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

func TestConfigExample(t *testing.T) {
	conf := NewConfig()
	configFile := "config.toml.example"
	metaData, err := toml.DecodeFile(configFile, conf)
	require.NoError(t, err)
	keys := metaData.Keys()
	for _, key := range keys {
		for _, s := range key {
			require.False(t, ContainHiddenConfig(s))
		}
	}
}

func TestStatsLoadLimit(t *testing.T) {
	conf := NewConfig()
	checkConcurrencyValid := func(concurrency int, shouldBeValid bool) {
		conf.Performance.StatsLoadConcurrency = uint(concurrency)
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
