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
	conf.BinlogSocket = "/tmp/socket"
	conf.Performance.RetryLimit = 20

	_, filename, _, _ := runtime.Caller(0)
	configFile := path.Join(path.Dir(filename), "config.toml.example")
	err := conf.Load(configFile)
	c.Assert(err, IsNil)

	// Test that the original value will not be clear by load the config file that does not contain the option.
	c.Assert(conf.BinlogSocket, Equals, "/tmp/socket")

	// Test that the value will be overwritten by the config file.
	c.Assert(conf.Performance.RetryLimit, Equals, uint(10))

	// Reset
	conf.BinlogSocket = ""

	// Make sure the example config is the same as default config.
	c.Assert(conf, DeepEquals, GetGlobalConfig())
}
