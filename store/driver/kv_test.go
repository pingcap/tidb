package driver

import (
	. "github.com/pingcap/check"
	"github.com/tikv/client-go/v2/config"
)

type testTiKVDriverSuite struct {
}

var _ = SerialSuites(&testTiKVDriverSuite{})

func (s *testTiKVDriverSuite) TestSetDefaultAndOptions(c *C) {
	globalConfig := config.GetGlobalConfig()
	originSec := globalConfig.Security

	d := TiKVDriver{}
	security := config.Security{ClusterSSLCA: "test"}
	d.setDefaultAndOptions(WithSecurity(security))

	c.Assert(d.security, DeepEquals, security)
	c.Assert(d.tikvConfig, DeepEquals, globalConfig.TiKVClient)
	c.Assert(d.txnLocalLatches, DeepEquals, globalConfig.TxnLocalLatches)
	c.Assert(d.pdConfig, DeepEquals, globalConfig.PDClient)
	c.Assert(config.GetGlobalConfig().Security, DeepEquals, originSec)
}
