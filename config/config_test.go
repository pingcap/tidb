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
	"github.com/pingcap/parser/mysql"
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
	conf.Binlog.BinlogSocket = "/tmp/socket"
	conf.Performance.TxnEntryCountLimit = 1000
	conf.Performance.TxnTotalSizeLimit = 1000
	conf.TiKVClient.CommitTimeout = "10s"
	conf.CheckMb4ValueInUTF8 = true
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
split-region-max-num=10000
server-version = "test_version"
[performance]
txn-entry-count-limit=2000
txn-total-size-limit=2000
[tikv-client]
commit-timeout="41s"
`)

	c.Assert(err, IsNil)
	c.Assert(f.Sync(), IsNil)

	c.Assert(conf.Load(configFile), IsNil)

	c.Assert(conf.ServerVersion, Equals, "test_version")
	c.Assert(mysql.ServerVersion, Equals, conf.ServerVersion)
	// Test that the original value will not be clear by load the config file that does not contain the option.
	c.Assert(conf.Binlog.Enable, Equals, true)
	c.Assert(conf.Binlog.BinlogSocket, Equals, "/tmp/socket")

	// Test that the value will be overwritten by the config file.
	c.Assert(conf.Performance.TxnEntryCountLimit, Equals, uint64(2000))
	c.Assert(conf.Performance.TxnTotalSizeLimit, Equals, uint64(2000))

	c.Assert(conf.TiKVClient.CommitTimeout, Equals, "41s")
	c.Assert(conf.SplitRegionMaxNum, Equals, uint64(10000))
	c.Assert(f.Close(), IsNil)
	c.Assert(os.Remove(configFile), IsNil)

	configFile = path.Join(path.Dir(localFile), "config.toml.example")
	c.Assert(conf.Load(configFile), IsNil)

	// Make sure the example config is the same as default config.
	c.Assert(conf, DeepEquals, GetGlobalConfig())
}
