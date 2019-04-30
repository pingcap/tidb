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
	"os"
	"path"
	"runtime"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/logutil"
	tracing "github.com/uber/jaeger-client-go/config"
)

var _ = Suite(&testConfigSuite{})

type testConfigSuite struct{}

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

func (s *testConfigSuite) TestConfig(c *C) {
	conf := new(Config)
	conf.Binlog.Enable = true
	conf.Binlog.IgnoreError = true
	conf.Binlog.Strategy = "hash"
	conf.TiKVClient.CommitTimeout = "10s"
	configFile := "config.toml"
	_, localFile, _, _ := runtime.Caller(0)
	configFile = path.Join(path.Dir(localFile), configFile)

	f, err := os.Create(configFile)
	c.Assert(err, IsNil)

	// Make sure the server refuses to start if there's an unrecognized configuration option
	_, err = f.WriteString(`
unrecognized-option-test = true
`)
	c.Assert(err, IsNil)
	c.Assert(f.Sync(), IsNil)

	c.Assert(conf.Load(configFile), ErrorMatches, "(?:.|\n)*unknown configuration option(?:.|\n)*")

	f.Truncate(0)
	f.Seek(0, 0)

	_, err = f.WriteString(`
token-limit = 0
[performance]
[tikv-client]
commit-timeout="41s"
max-batch-size=128
`)

	c.Assert(err, IsNil)
	c.Assert(f.Sync(), IsNil)

	c.Assert(conf.Load(configFile), IsNil)

	// Test that the original value will not be clear by load the config file that does not contain the option.
	c.Assert(conf.Binlog.Enable, Equals, true)
	c.Assert(conf.Binlog.Strategy, Equals, "hash")

	c.Assert(conf.TiKVClient.CommitTimeout, Equals, "41s")
	c.Assert(conf.TiKVClient.MaxBatchSize, Equals, uint(128))
	c.Assert(conf.TokenLimit, Equals, uint(1000))
	c.Assert(f.Close(), IsNil)
	c.Assert(os.Remove(configFile), IsNil)

	configFile = path.Join(path.Dir(localFile), "config.toml.example")
	c.Assert(conf.Load(configFile), IsNil)

	// Make sure the example config is the same as default config.
	c.Assert(conf, DeepEquals, GetGlobalConfig())

	// Test for lof config.
	c.Assert(conf.Log.ToLogConfig(), DeepEquals, logutil.NewLogConfig("info", "text", "tidb-slow.log", conf.Log.File, false))

	// Test for tracing config.
	tracingConf := &tracing.Configuration{
		Disabled: true,
		Reporter: &tracing.ReporterConfig{},
		Sampler:  &tracing.SamplerConfig{Type: "const", Param: 1.0},
	}
	c.Assert(tracingConf, DeepEquals, conf.OpenTracing.ToTracingConfig())

	// Test for TLS config.
	certFile := "cert.pem"
	certFile = path.Join(path.Dir(localFile), certFile)
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
	c.Assert(f.Sync(), IsNil)

	keyFile := "key.pem"
	keyFile = path.Join(path.Dir(localFile), keyFile)
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
	c.Assert(f.Sync(), IsNil)

	conf.Security.ClusterSSLCA = certFile
	conf.Security.ClusterSSLCert = certFile
	conf.Security.ClusterSSLKey = keyFile
	tlsConfig, err := conf.Security.ToTLSConfig()
	c.Assert(err, IsNil)
	c.Assert(tlsConfig, NotNil)
	c.Assert(os.Remove(certFile), IsNil)
	c.Assert(os.Remove(keyFile), IsNil)
}

func (s *testConfigSuite) TestConfigDiff(c *C) {
	c1 := NewConfig()
	c2 := &Config{}
	*c2 = *c1
	c1.OOMAction = "c1"
	c2.OOMAction = "c2"
	c1.MemQuotaQuery = 2333
	c2.MemQuotaQuery = 3222
	c1.Performance.CrossJoin = true
	c2.Performance.CrossJoin = false
	c1.Performance.FeedbackProbability = 2333
	c2.Performance.FeedbackProbability = 23.33

	diffs := collectsDiff(*c1, *c2, "")
	c.Assert(len(diffs), Equals, 4)
	c.Assert(diffs["OOMAction"][0], Equals, "c1")
	c.Assert(diffs["OOMAction"][1], Equals, "c2")
	c.Assert(diffs["MemQuotaQuery"][0], Equals, int64(2333))
	c.Assert(diffs["MemQuotaQuery"][1], Equals, int64(3222))
	c.Assert(diffs["Performance.CrossJoin"][0], Equals, true)
	c.Assert(diffs["Performance.CrossJoin"][1], Equals, false)
	c.Assert(diffs["Performance.FeedbackProbability"][0], Equals, float64(2333))
	c.Assert(diffs["Performance.FeedbackProbability"][1], Equals, float64(23.33))
}
