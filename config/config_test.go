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
	conf.Performance.RetryLimit = 20
	conf.TiKVClient.CommitTimeout = "10s"

	configFile := "config.toml"
	_, localFile, _, _ := runtime.Caller(0)
	configFile = path.Join(path.Dir(localFile), configFile)

	f, err := os.Create(configFile)
	c.Assert(err, IsNil)
	_, err = f.WriteString(`[performance]
retry-limit=10
[tikv-client]
commit-timeout="41s"`)
	c.Assert(err, IsNil)
	c.Assert(f.Sync(), IsNil)

	c.Assert(conf.Load(configFile), IsNil)

	// Test that the original value will not be clear by load the config file that does not contain the option.
	c.Assert(conf.Binlog.Enable, Equals, true)

	// Test that the value will be overwritten by the config file.
	c.Assert(conf.Performance.RetryLimit, Equals, uint(10))

	c.Assert(conf.TiKVClient.CommitTimeout, Equals, "41s")
	c.Assert(f.Close(), IsNil)
	c.Assert(os.Remove(configFile), IsNil)

	configFile = path.Join(path.Dir(localFile), "config.toml.example")
	c.Assert(conf.Load(configFile), IsNil)

	// Make sure the example config is the same as default config.
	c.Assert(conf, DeepEquals, GetGlobalConfig())
}
