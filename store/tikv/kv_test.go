package tikv

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/config"
)

type testTiKVDriverSuite struct {
	OneByOneSuite
	store *tikvStore
}

var _ = Suite(&testTiKVDriverSuite{})

func (s *testTiKVDriverSuite) TestSetDefaultAndOptions(c *C) {
	d := Driver{}
	security := config.Security{ClusterSSLCA: "test"}
	d.setDefaultAndOptions(WithSecurity(security))

	defaultCfg := config.NewConfig()
	c.Assert(d.security, DeepEquals, security)
	c.Assert(d.tikvConfig, DeepEquals, defaultCfg.TiKVClient)
	c.Assert(d.txnLocalLatches, DeepEquals, defaultCfg.TxnLocalLatches)
}
