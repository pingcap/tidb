package failpoint_test

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
)

func TestNewRestorer(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&runtimeSuite{})

type runtimeSuite struct{}

func (s *runtimeSuite) TestRuntime(c *C) {
	err := failpoint.Enable("runtime-test-1", "return(1)")
	c.Assert(err, IsNil)
	val, ok := failpoint.Eval("runtime-test-1")
	c.Assert(ok, IsTrue)
	c.Assert(val.(int), Equals, 1)

	err = failpoint.Enable("runtime-test-2", "invalid")
	c.Assert(err, ErrorMatches, `failpoint: could not parse terms`)

	val, ok = failpoint.Eval("runtime-test-2")
	c.Assert(ok, IsFalse)
	c.Assert(val, IsNil)

	err = failpoint.Disable("runtime-test-1")
	c.Assert(err, IsNil)

	val, ok = failpoint.Eval("runtime-test-1")
	c.Assert(ok, IsFalse)
	c.Assert(val, IsNil)

	err = failpoint.Disable("runtime-test-1")
	c.Assert(err, ErrorMatches, `failpoint: failpoint is disabled`)

	err = failpoint.Enable("runtime-test-1", "return(1)")
	c.Assert(err, IsNil)

	status, err := failpoint.Status("runtime-test-1")
	c.Assert(err, IsNil)
	c.Assert(status, Equals, "return(1)")

	err = failpoint.Enable("runtime-test-3", "return(2)")
	c.Assert(err, IsNil)

	ch := make(chan struct{})
	go func() {
		time.Sleep(time.Second)
		err := failpoint.Disable("gofail/testPause")
		c.Assert(err, IsNil)
		close(ch)
	}()
	err = failpoint.Enable("gofail/testPause", "pause")
	c.Assert(err, IsNil)
	start := time.Now()
	v, ok := failpoint.Eval("gofail/testPause")
	c.Assert(ok, IsFalse)
	c.Assert(v, IsNil)
	c.Assert(time.Since(start), GreaterEqual, 100*time.Millisecond, Commentf("not paused"))
	<-ch

	err = failpoint.Enable("runtime-test-4", "50.0%return(5)")
	c.Assert(err, IsNil)
	var succ int
	for i := 0; i < 1000; i++ {
		val, ok = failpoint.Eval("runtime-test-4")
		if ok {
			succ++
			c.Assert(val.(int), Equals, 5)
		}
	}
	if succ < 450 || succ > 550 {
		c.Fatalf("prop failure: %v", succ)
	}

	err = failpoint.Enable("runtime-test-5", "50*return(5)")
	c.Assert(err, IsNil)
	for i := 0; i < 50; i++ {
		val, ok = failpoint.Eval("runtime-test-5")
		c.Assert(ok, Equals, true)
		c.Assert(val.(int), Equals, 5)
	}
	val, ok = failpoint.Eval("runtime-test-5")
	c.Assert(ok, IsFalse)
	c.Assert(val, IsNil)

	fps := map[string]struct{}{}
	for _, fp := range failpoint.List() {
		fps[fp] = struct{}{}
	}
	c.Assert(fps, HasKey, "runtime-test-1")
	c.Assert(fps, HasKey, "runtime-test-2")
	c.Assert(fps, HasKey, "runtime-test-3")
	c.Assert(fps, HasKey, "runtime-test-4")
	c.Assert(fps, HasKey, "runtime-test-5")

	err = failpoint.Enable("runtime-test-6", "50*return(5)->1*return(true)->1*return(false)->10*return(20)")
	c.Assert(err, IsNil)
	// 50*return(5)
	for i := 0; i < 50; i++ {
		val, ok = failpoint.Eval("runtime-test-6")
		c.Assert(ok, IsTrue)
		c.Assert(val.(int), Equals, 5)
	}
	// 1*return(true)
	val, ok = failpoint.Eval("runtime-test-6")
	c.Assert(ok, IsTrue)
	c.Assert(val.(bool), IsTrue)
	// 1*return(false)
	val, ok = failpoint.Eval("runtime-test-6")
	c.Assert(ok, IsTrue)
	c.Assert(val.(bool), IsFalse)
	// 10*return(20)
	for i := 0; i < 10; i++ {
		val, ok = failpoint.Eval("runtime-test-6")
		c.Assert(ok, IsTrue)
		c.Assert(val.(int), Equals, 20)
	}
	val, ok = failpoint.Eval("runtime-test-6")
	c.Assert(ok, IsFalse)
	c.Assert(val, IsNil)

	val, ok = failpoint.Eval("failpoint-env1")
	c.Assert(ok, IsTrue)
	c.Assert(val.(int), Equals, 10)
	val, ok = failpoint.Eval("failpoint-env2")
	c.Assert(ok, IsTrue)
	c.Assert(val.(bool), IsTrue)

	// Tests for sleep
	ch = make(chan struct{})
	go func() {
		defer close(ch)
		time.Sleep(time.Second)
		err := failpoint.Disable("gofail/test-sleep")
		c.Assert(err, IsNil)
	}()
	err = failpoint.Enable("gofail/test-sleep", "sleep(100)")
	c.Assert(err, IsNil)
	start = time.Now()
	v, ok = failpoint.Eval("gofail/test-sleep")
	c.Assert(ok, IsFalse)
	c.Assert(v, IsNil)
	c.Assert(time.Since(start), GreaterEqual, 90*time.Millisecond, Commentf("not sleep"))
	<-ch

	// Tests for sleep duration
	ch = make(chan struct{})
	go func() {
		defer close(ch)
		time.Sleep(time.Second)
		err := failpoint.Disable("gofail/test-sleep2")
		c.Assert(err, IsNil)
	}()
	err = failpoint.Enable("gofail/test-sleep2", `sleep("100ms")`)
	c.Assert(err, IsNil)
	start = time.Now()
	v, ok = failpoint.Eval("gofail/test-sleep2")
	c.Assert(ok, IsFalse)
	c.Assert(v, IsNil)
	c.Assert(time.Since(start), GreaterEqual, 90*time.Millisecond, Commentf("not sleep"))
	<-ch

	// Tests for print
	oldStdio := os.Stdout
	r, w, err := os.Pipe()
	c.Assert(err, IsNil)
	os.Stdout = w
	err = failpoint.Enable("test-print", `print`)
	c.Assert(err, IsNil)
	val, ok = failpoint.Eval("test-print")
	c.Assert(ok, IsFalse)
	c.Assert(val, IsNil)
	outC := make(chan string)
	// copy the output in a separate goroutine so printing can't block indefinitely
	go func() {
		defer close(outC)
		s, err := ioutil.ReadAll(r)
		c.Assert(err, IsNil)
		outC <- string(s)
	}()
	w.Close()
	os.Stdout = oldStdio
	out := <-outC
	c.Assert(out, Equals, "failpoint print: test-print\n")

	// Tests for panic
	c.Assert(testPanic, PanicMatches, "failpoint panic.*")

	err = failpoint.Enable("runtime-test-7", `return`)
	c.Assert(err, IsNil)
	val, ok = failpoint.Eval("runtime-test-7")
	c.Assert(ok, IsTrue)
	c.Assert(val, Equals, struct{}{})

	err = failpoint.Enable("runtime-test-8", `return()`)
	c.Assert(err, IsNil)
	val, ok = failpoint.Eval("runtime-test-8")
	c.Assert(ok, IsTrue)
	c.Assert(val, Equals, struct{}{})
}

func testPanic() {
	_ = failpoint.Enable("test-panic", `panic`)
	failpoint.Eval("test-panic")
}
