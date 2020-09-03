package testdata

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
)

type TestSuite2 struct {
}

var _ = SerialSuites(&TestSuite2{})

func (ts *TestSuite2) Test1(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/xxxxxxxxxxxx", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/xxxxxxxxxxxx"), IsNil)
	}()
}

func (ts *TestSuite2) Test2(c *C) {
	failpoint.Enable("github.com/pingcap/tidb/xxxxxxxxxxxx", `return(true)`)
}

func test33()  {
	failpoint.Enable("github.com/pingcap/tidb/xxxxxxxxxxxx", `return(true)`)
}

func (ts *TestSuite2) Test3(c *C) {
	test33()
}

func (ts *TestSuite2) Test4(c *C) {
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
