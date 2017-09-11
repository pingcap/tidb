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
	c.Assert(conf.Performance.RetryLimit, Equals, 10)

	// Reset
	conf.BinlogSocket = ""

	// Make sure the example config is the same as default config.
	c.Assert(conf, DeepEquals, GetGlobalConfig())
}
