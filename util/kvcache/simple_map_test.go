package kvcache

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testBindCacheSuite{})

type testBindCacheSuite struct {
}

func (s *testBindCacheSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
}

func (s *testBindCacheSuite) TearDownSuite(c *C) {
	testleak.AfterTest(c, TestLeakCheckCnt)()
}

func (s *testBindCacheSuite) TestPut(c *C) {
	//b := NewSimpleMapCache()
	//element := bindInfo{
	//	ast:      nil,
	//	database: []string{"testdb"},
	//}
	//b.Put("aa", element)
	//value, ok := b.Get("aa")
	//c.Assert(ok, IsTrue)
	//for i:= 0; i < len(value.database); i++ {
	//	c.Assert(value.database[i], Equals, element.database[i])
	//}

}
