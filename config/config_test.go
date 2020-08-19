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
	"encoding/json"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/BurntSushi/toml"
	. "github.com/pingcap/check"
	zaplog "github.com/pingcap/log"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/util/logutil"
	tracing "github.com/uber/jaeger-client-go/config"
)

var _ = SerialSuites(&testConfigSuite{})

type testConfigSuite struct{}

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

func (s *testConfigSuite) TestNullableBoolUnmarshal(c *C) {
	var nb = nullableBool{false, false}
	data, err := json.Marshal(nb)
	c.Assert(err, IsNil)
	err = json.Unmarshal(data, &nb)
	c.Assert(err, IsNil)
	c.Assert(nb, Equals, nbUnset)

	nb = nullableBool{true, false}
	data, err = json.Marshal(nb)
	c.Assert(err, IsNil)
	err = json.Unmarshal(data, &nb)
	c.Assert(err, IsNil)
	c.Assert(nb, Equals, nbFalse)

	nb = nullableBool{true, true}
	data, err = json.Marshal(nb)
	c.Assert(err, IsNil)
	err = json.Unmarshal(data, &nb)
	c.Assert(err, IsNil)
	c.Assert(nb, Equals, nbTrue)

	// Test for UnmarshalText
	var log Log
	_, err = toml.Decode("enable-error-stack = true", &log)
	c.Assert(err, IsNil)
	c.Assert(log.EnableErrorStack, Equals, nbTrue)

	_, err = toml.Decode("enable-error-stack = \"\"", &log)
	c.Assert(err, IsNil)
	c.Assert(log.EnableErrorStack, Equals, nbUnset)

	_, err = toml.Decode("enable-error-stack = 1", &log)
	c.Assert(err, ErrorMatches, "Invalid value for bool type: 1")
	c.Assert(log.EnableErrorStack, Equals, nbUnset)

	// Test for UnmarshalJSON
	err = json.Unmarshal([]byte("{\"enable-timestamp\":false}"), &log)
	c.Assert(err, IsNil)
	c.Assert(log.EnableTimestamp, Equals, nbFalse)

	err = json.Unmarshal([]byte("{\"disable-timestamp\":null}"), &log)
	c.Assert(err, IsNil)
	c.Assert(log.DisableTimestamp, Equals, nbUnset)
}

func (s *testConfigSuite) TestLogConfig(c *C) {
	var conf Config
	configFile := "log_config.toml"
	_, localFile, _, _ := runtime.Caller(0)
	configFile = filepath.Join(filepath.Dir(localFile), configFile)

	f, err := os.Create(configFile)
	c.Assert(err, IsNil)
	defer func() {
		c.Assert(f.Close(), IsNil)
		c.Assert(os.Remove(configFile), IsNil)
	}()

	var testLoad = func(confStr string, expectedEnableErrorStack, expectedDisableErrorStack, expectedEnableTimestamp, expectedDisableTimestamp nullableBool, resultedDisableTimestamp, resultedDisableErrorVerbose bool, valid Checker) {
		conf = defaultConf
		_, err = f.WriteString(confStr)
		c.Assert(err, IsNil)
		c.Assert(conf.Load(configFile), IsNil)
		c.Assert(conf.Valid(), valid)
		c.Assert(conf.Log.EnableErrorStack, Equals, expectedEnableErrorStack)
		c.Assert(conf.Log.DisableErrorStack, Equals, expectedDisableErrorStack)
		c.Assert(conf.Log.EnableTimestamp, Equals, expectedEnableTimestamp)
		c.Assert(conf.Log.DisableTimestamp, Equals, expectedDisableTimestamp)
		c.Assert(conf.Log.ToLogConfig(), DeepEquals, logutil.NewLogConfig("info", "text", "tidb-slow.log", conf.Log.File, resultedDisableTimestamp, func(config *zaplog.Config) { config.DisableErrorVerbose = resultedDisableErrorVerbose }))
		f.Truncate(0)
		f.Seek(0, 0)
	}

	testLoad(`
[Log]
`, nbUnset, nbUnset, nbUnset, nbUnset, false, true, IsNil)

	testLoad(`
[Log]
enable-timestamp = false
`, nbUnset, nbUnset, nbFalse, nbUnset, true, true, IsNil)

	testLoad(`
[Log]
enable-timestamp = true
disable-timestamp = false
`, nbUnset, nbUnset, nbTrue, nbFalse, false, true, IsNil)

	testLoad(`
[Log]
enable-timestamp = false
disable-timestamp = true
`, nbUnset, nbUnset, nbFalse, nbTrue, true, true, IsNil)

	testLoad(`
[Log]
enable-timestamp = true
disable-timestamp = true
`, nbUnset, nbUnset, nbTrue, nbUnset, false, true, IsNil)

	testLoad(`
[Log]
enable-error-stack = false
disable-error-stack = false
`, nbFalse, nbUnset, nbUnset, nbUnset, false, true, IsNil)

}

func (s *testConfigSuite) TestConfig(c *C) {
	conf := new(Config)
	conf.TempStoragePath = tempStorageDirName
	conf.Binlog.Enable = true
	conf.Binlog.IgnoreError = true
	conf.Binlog.Strategy = "hash"
	conf.Performance.TxnTotalSizeLimit = 1000
	conf.TiKVClient.CommitTimeout = "10s"
	conf.TiKVClient.RegionCacheTTL = 600
	conf.Log.EnableSlowLog = logutil.DefaultTiDBEnableSlowLog
	configFile := "config.toml"
	_, localFile, _, _ := runtime.Caller(0)
	configFile = filepath.Join(filepath.Dir(localFile), configFile)

	f, err := os.Create(configFile)
	c.Assert(err, IsNil)

	// Make sure the server refuses to start if there's an unrecognized configuration option
	_, err = f.WriteString(`
unrecognized-option-test = true
`)
	c.Assert(err, IsNil)
	c.Assert(f.Sync(), IsNil)

	c.Assert(conf.Load(configFile), ErrorMatches, "(?:.|\n)*unknown configuration option(?:.|\n)*")
	c.Assert(conf.MaxServerConnections, Equals, uint32(0))

	f.Truncate(0)
	f.Seek(0, 0)

	_, err = f.WriteString(`
token-limit = 0
enable-table-lock = true
alter-primary-key = true
delay-clean-table-lock = 5
split-region-max-num=10000
enable-batch-dml = true
server-version = "test_version"
repair-mode = true
max-server-connections = 200
mem-quota-query = 10000
mem-quota-statistic = 10000
nested-loop-join-cache-capacity = 100
max-index-length = 3080
skip-register-to-dashboard = true
[performance]
txn-total-size-limit=2000
[tikv-client]
commit-timeout="41s"
enable-async-commit=true
max-batch-size=128
region-cache-ttl=6000
store-limit=0
ttl-refreshed-txn-size=8192
[stmt-summary]
enable=false
enable-internal-query=true
max-stmt-count=1000
max-sql-length=1024
refresh-interval=100
history-size=100
[experimental]
allow-expression-index = true
[isolation-read]
engines = ["tiflash"]
`)

	c.Assert(err, IsNil)
	c.Assert(f.Sync(), IsNil)

	c.Assert(conf.Load(configFile), IsNil)

	c.Assert(conf.ServerVersion, Equals, "test_version")
	c.Assert(mysql.ServerVersion, Equals, conf.ServerVersion)
	// Test that the original value will not be clear by load the config file that does not contain the option.
	c.Assert(conf.Binlog.Enable, Equals, true)
	c.Assert(conf.Binlog.Strategy, Equals, "hash")

	// Test that the value will be overwritten by the config file.
	c.Assert(conf.Performance.TxnTotalSizeLimit, Equals, uint64(2000))
	c.Assert(conf.AlterPrimaryKey, Equals, true)

	c.Assert(conf.TiKVClient.CommitTimeout, Equals, "41s")
	c.Assert(conf.TiKVClient.EnableAsyncCommit, Equals, true)
	c.Assert(conf.TiKVClient.MaxBatchSize, Equals, uint(128))
	c.Assert(conf.TiKVClient.RegionCacheTTL, Equals, uint(6000))
	c.Assert(conf.TiKVClient.StoreLimit, Equals, int64(0))
	c.Assert(conf.TiKVClient.TTLRefreshedTxnSize, Equals, int64(8192))
	c.Assert(conf.TokenLimit, Equals, uint(1000))
	c.Assert(conf.EnableTableLock, IsTrue)
	c.Assert(conf.DelayCleanTableLock, Equals, uint64(5))
	c.Assert(conf.SplitRegionMaxNum, Equals, uint64(10000))
	c.Assert(conf.StmtSummary.Enable, Equals, false)
	c.Assert(conf.StmtSummary.EnableInternalQuery, Equals, true)
	c.Assert(conf.StmtSummary.MaxStmtCount, Equals, uint(1000))
	c.Assert(conf.StmtSummary.MaxSQLLength, Equals, uint(1024))
	c.Assert(conf.StmtSummary.RefreshInterval, Equals, 100)
	c.Assert(conf.StmtSummary.HistorySize, Equals, 100)
	c.Assert(conf.EnableBatchDML, Equals, true)
	c.Assert(conf.RepairMode, Equals, true)
	c.Assert(conf.MaxServerConnections, Equals, uint32(200))
	c.Assert(conf.MemQuotaQuery, Equals, int64(10000))
	c.Assert(conf.MemQuotaStatistic, Equals, int64(10000))
	c.Assert(conf.NestedLoopJoinCacheCapacity, Equals, int64(100))
	c.Assert(conf.Experimental.AllowsExpressionIndex, IsTrue)
	c.Assert(conf.IsolationRead.Engines, DeepEquals, []string{"tiflash"})
	c.Assert(conf.MaxIndexLength, Equals, 3080)
	c.Assert(conf.SkipRegisterToDashboard, Equals, true)

	_, err = f.WriteString(`
[log.file]
log-rotate = true`)
	c.Assert(err, IsNil)
	err = conf.Load(configFile)
	tmp := err.(*ErrConfigValidationFailed)
	c.Assert(isAllDeprecatedConfigItems(tmp.UndecodedItems), IsTrue)

	// Test telemetry config default value and whether it will be overwritten.
	conf = NewConfig()
	f.Truncate(0)
	f.Seek(0, 0)
	c.Assert(f.Sync(), IsNil)
	c.Assert(conf.Load(configFile), IsNil)
	c.Assert(conf.EnableTelemetry, Equals, true)

	_, err = f.WriteString(`
enable-table-lock = true
`)
	c.Assert(err, IsNil)
	c.Assert(f.Sync(), IsNil)
	c.Assert(conf.Load(configFile), IsNil)
	c.Assert(conf.EnableTelemetry, Equals, true)

	_, err = f.WriteString(`
enable-telemetry = false
`)
	c.Assert(err, IsNil)
	c.Assert(f.Sync(), IsNil)
	c.Assert(conf.Load(configFile), IsNil)
	c.Assert(conf.EnableTelemetry, Equals, false)

	c.Assert(f.Close(), IsNil)
	c.Assert(os.Remove(configFile), IsNil)

	configFile = filepath.Join(filepath.Dir(localFile), "config.toml.example")
	c.Assert(conf.Load(configFile), IsNil)

	// Make sure the example config is the same as default config.
	c.Assert(conf, DeepEquals, GetGlobalConfig())

	// Test for log config.
	c.Assert(conf.Log.ToLogConfig(), DeepEquals, logutil.NewLogConfig("info", "text", "tidb-slow.log", conf.Log.File, false, func(config *zaplog.Config) { config.DisableErrorVerbose = conf.Log.getDisableErrorStack() }))

	// Test for tracing config.
	tracingConf := &tracing.Configuration{
		Disabled: true,
		Reporter: &tracing.ReporterConfig{},
		Sampler:  &tracing.SamplerConfig{Type: "const", Param: 1.0},
	}
	c.Assert(tracingConf, DeepEquals, conf.OpenTracing.ToTracingConfig())

	// Test for TLS config.
	certFile := "cert.pem"
	certFile = filepath.Join(filepath.Dir(localFile), certFile)
	f, err = os.Create(certFile)
	c.Assert(err, IsNil)
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
	c.Assert(err, IsNil)
	c.Assert(f.Close(), IsNil)

	keyFile := "key.pem"
	keyFile = filepath.Join(filepath.Dir(localFile), keyFile)
	f, err = os.Create(keyFile)
	c.Assert(err, IsNil)
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
	c.Assert(err, IsNil)
	c.Assert(f.Close(), IsNil)

	conf.Security.ClusterSSLCA = certFile
	conf.Security.ClusterSSLCert = certFile
	conf.Security.ClusterSSLKey = keyFile
	tlsConfig, err := conf.Security.ToTLSConfig()
	c.Assert(err, IsNil)
	c.Assert(tlsConfig, NotNil)

	// Note that on windows, we can't Remove a file if the file is not closed.
	// The behavior is different on linux, we can always Remove a file even
	// if it's open. The OS maintains a reference count for open/close, the file
	// is recycled when the reference count drops to 0.
	c.Assert(os.Remove(certFile), IsNil)
	c.Assert(os.Remove(keyFile), IsNil)
}

func (s *testConfigSuite) TestOOMActionValid(c *C) {
	c1 := NewConfig()
	tests := []struct {
		oomAction string
		valid     bool
	}{
		{"log", true},
		{"Log", true},
		{"Cancel", true},
		{"cANceL", true},
		{"quit", false},
	}
	for _, tt := range tests {
		c1.OOMAction = tt.oomAction
		c.Assert(c1.Valid() == nil, Equals, tt.valid)
	}
}

func (s *testConfigSuite) TestTxnTotalSizeLimitValid(c *C) {
	conf := NewConfig()
	tests := []struct {
		limit uint64
		valid bool
	}{
		{4 << 10, true},
		{10 << 30, true},
		{10<<30 + 1, false},
	}

	for _, tt := range tests {
		conf.Performance.TxnTotalSizeLimit = tt.limit
		c.Assert(conf.Valid() == nil, Equals, tt.valid)
	}
}

func (s *testConfigSuite) TestPreparePlanCacheValid(c *C) {
	conf := NewConfig()
	tests := map[PreparedPlanCache]bool{
		{Enabled: true, Capacity: 0}:                        false,
		{Enabled: true, Capacity: 2}:                        true,
		{Enabled: true, MemoryGuardRatio: -0.1}:             false,
		{Enabled: true, MemoryGuardRatio: 2.2}:              false,
		{Enabled: true, Capacity: 2, MemoryGuardRatio: 0.5}: true,
	}
	for testCase, res := range tests {
		conf.PreparedPlanCache = testCase
		c.Assert(conf.Valid() == nil, Equals, res)
	}
}

func (s *testConfigSuite) TestMaxIndexLength(c *C) {
	conf := NewConfig()
	checkValid := func(indexLen int, shouldBeValid bool) {
		conf.MaxIndexLength = indexLen
		c.Assert(conf.Valid() == nil, Equals, shouldBeValid)
	}
	checkValid(DefMaxIndexLength, true)
	checkValid(DefMaxIndexLength-1, false)
	checkValid(DefMaxOfMaxIndexLength, true)
	checkValid(DefMaxOfMaxIndexLength+1, false)
}

func (s *testConfigSuite) TestParsePath(c *C) {
	etcdAddrs, disableGC, err := ParsePath("tikv://node1:2379,node2:2379")
	c.Assert(err, IsNil)
	c.Assert(etcdAddrs, DeepEquals, []string{"node1:2379", "node2:2379"})
	c.Assert(disableGC, IsFalse)

	_, _, err = ParsePath("tikv://node1:2379")
	c.Assert(err, IsNil)
	_, disableGC, err = ParsePath("tikv://node1:2379?disableGC=true")
	c.Assert(err, IsNil)
	c.Assert(disableGC, IsTrue)
}

func (s *testConfigSuite) TestEncodeDefTempStorageDir(c *C) {
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
		c.Assert(tempStorageDir, Equals, filepath.Join(dirPrefix, test.expect, "tmp-storage"))
	}
}

func (s *testConfigSuite) TestModifyThroughLDFlags(c *C) {
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
		c.Assert(conf.EnableTelemetry, Equals, test.EnableTelemetry)
		c.Assert(defaultConf.EnableTelemetry, Equals, test.EnableTelemetry)
		c.Assert(CheckTableBeforeDrop, Equals, test.CheckTableBeforeDrop)
	}

	defaultConf.EnableTelemetry = originalEnableTelemetry
	CheckTableBeforeDrop = originalCheckTableBeforeDrop
	StoreGlobalConfig(originalGlobalConfig)
}
