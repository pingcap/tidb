package testdata

import (
	"testing"
	
	"github.com/pingcap/failpoint"
)

type TestSuite struct {
}

var _ = SerialSuite(&TestSuite{})

func (ts *TestSuite) Test1(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/xxxxxxxxxxxx", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/xxxxxxxxxxxx"), IsNil)
	}()
}

func (ts *TestSuite) Test2(c *C) {
	failpoint.Enable("github.com/pingcap/tidb/xxxxxxxxxxxx", `return(true)`)
}

func test3()  {
	failpoint.Enable("github.com/pingcap/tidb/xxxxxxxxxxxx", `return(true)`)
}

func (ts *TestSuite) Test3(c *C) {
	test3()
}

func (ts *TestSuite) Test4(c *C) {
	go func() {
		failpoint.Enable("github.com/pingcap/tidb/xxxxxxxxxxxx", `return(true)`)
	}()
}

func TestT(t *testing.T) {
	TestingT(t)
}
func Test5(t *testing.T)  {
	failpoint.Enable("github.com/pingcap/tidb/xxxxxxxxxxxx", `return(true)`)
}
