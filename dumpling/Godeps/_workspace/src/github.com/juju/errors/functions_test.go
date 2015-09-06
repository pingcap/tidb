// Copyright 2014 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package errors_test

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"

	"github.com/juju/errors"
)

type functionSuite struct {
}

var _ = gc.Suite(&functionSuite{})

func (*functionSuite) TestNew(c *gc.C) {
	err := errors.New("testing") //err newTest
	c.Assert(err.Error(), gc.Equals, "testing")
	c.Assert(errors.Cause(err), gc.Equals, err)
	c.Assert(errors.Details(err), jc.Contains, tagToLocation["newTest"].String())
}

func (*functionSuite) TestErrorf(c *gc.C) {
	err := errors.Errorf("testing %d", 42) //err errorfTest
	c.Assert(err.Error(), gc.Equals, "testing 42")
	c.Assert(errors.Cause(err), gc.Equals, err)
	c.Assert(errors.Details(err), jc.Contains, tagToLocation["errorfTest"].String())
}

func (*functionSuite) TestTrace(c *gc.C) {
	first := errors.New("first")
	err := errors.Trace(first) //err traceTest
	c.Assert(err.Error(), gc.Equals, "first")
	c.Assert(errors.Cause(err), gc.Equals, first)
	c.Assert(errors.Details(err), jc.Contains, tagToLocation["traceTest"].String())

	c.Assert(errors.Trace(nil), gc.IsNil)
}

func (*functionSuite) TestAnnotate(c *gc.C) {
	first := errors.New("first")
	err := errors.Annotate(first, "annotation") //err annotateTest
	c.Assert(err.Error(), gc.Equals, "annotation: first")
	c.Assert(errors.Cause(err), gc.Equals, first)
	c.Assert(errors.Details(err), jc.Contains, tagToLocation["annotateTest"].String())

	c.Assert(errors.Annotate(nil, "annotate"), gc.IsNil)
}

func (*functionSuite) TestAnnotatef(c *gc.C) {
	first := errors.New("first")
	err := errors.Annotatef(first, "annotation %d", 2) //err annotatefTest
	c.Assert(err.Error(), gc.Equals, "annotation 2: first")
	c.Assert(errors.Cause(err), gc.Equals, first)
	c.Assert(errors.Details(err), jc.Contains, tagToLocation["annotatefTest"].String())

	c.Assert(errors.Annotatef(nil, "annotate"), gc.IsNil)
}

func (*functionSuite) TestDeferredAnnotatef(c *gc.C) {
	// NOTE: this test fails with gccgo
	if runtime.Compiler == "gccgo" {
		c.Skip("gccgo can't determine the location")
	}
	first := errors.New("first")
	test := func() (err error) {
		defer errors.DeferredAnnotatef(&err, "deferred %s", "annotate")
		return first
	} //err deferredAnnotate
	err := test()
	c.Assert(err.Error(), gc.Equals, "deferred annotate: first")
	c.Assert(errors.Cause(err), gc.Equals, first)
	c.Assert(errors.Details(err), jc.Contains, tagToLocation["deferredAnnotate"].String())

	err = nil
	errors.DeferredAnnotatef(&err, "deferred %s", "annotate")
	c.Assert(err, gc.IsNil)
}

func (*functionSuite) TestWrap(c *gc.C) {
	first := errors.New("first") //err wrapFirst
	detailed := errors.New("detailed")
	err := errors.Wrap(first, detailed) //err wrapTest
	c.Assert(err.Error(), gc.Equals, "detailed")
	c.Assert(errors.Cause(err), gc.Equals, detailed)
	c.Assert(errors.Details(err), jc.Contains, tagToLocation["wrapFirst"].String())
	c.Assert(errors.Details(err), jc.Contains, tagToLocation["wrapTest"].String())
}

func (*functionSuite) TestWrapOfNil(c *gc.C) {
	detailed := errors.New("detailed")
	err := errors.Wrap(nil, detailed) //err nilWrapTest
	c.Assert(err.Error(), gc.Equals, "detailed")
	c.Assert(errors.Cause(err), gc.Equals, detailed)
	c.Assert(errors.Details(err), jc.Contains, tagToLocation["nilWrapTest"].String())
}

func (*functionSuite) TestWrapf(c *gc.C) {
	first := errors.New("first") //err wrapfFirst
	detailed := errors.New("detailed")
	err := errors.Wrapf(first, detailed, "value %d", 42) //err wrapfTest
	c.Assert(err.Error(), gc.Equals, "value 42: detailed")
	c.Assert(errors.Cause(err), gc.Equals, detailed)
	c.Assert(errors.Details(err), jc.Contains, tagToLocation["wrapfFirst"].String())
	c.Assert(errors.Details(err), jc.Contains, tagToLocation["wrapfTest"].String())
}

func (*functionSuite) TestWrapfOfNil(c *gc.C) {
	detailed := errors.New("detailed")
	err := errors.Wrapf(nil, detailed, "value %d", 42) //err nilWrapfTest
	c.Assert(err.Error(), gc.Equals, "value 42: detailed")
	c.Assert(errors.Cause(err), gc.Equals, detailed)
	c.Assert(errors.Details(err), jc.Contains, tagToLocation["nilWrapfTest"].String())
}

func (*functionSuite) TestMask(c *gc.C) {
	first := errors.New("first")
	err := errors.Mask(first) //err maskTest
	c.Assert(err.Error(), gc.Equals, "first")
	c.Assert(errors.Cause(err), gc.Equals, err)
	c.Assert(errors.Details(err), jc.Contains, tagToLocation["maskTest"].String())

	c.Assert(errors.Mask(nil), gc.IsNil)
}

func (*functionSuite) TestMaskf(c *gc.C) {
	first := errors.New("first")
	err := errors.Maskf(first, "masked %d", 42) //err maskfTest
	c.Assert(err.Error(), gc.Equals, "masked 42: first")
	c.Assert(errors.Cause(err), gc.Equals, err)
	c.Assert(errors.Details(err), jc.Contains, tagToLocation["maskfTest"].String())

	c.Assert(errors.Maskf(nil, "mask"), gc.IsNil)
}

func (*functionSuite) TestCause(c *gc.C) {
	c.Assert(errors.Cause(nil), gc.IsNil)
	c.Assert(errors.Cause(someErr), gc.Equals, someErr)

	fmtErr := fmt.Errorf("simple")
	c.Assert(errors.Cause(fmtErr), gc.Equals, fmtErr)

	err := errors.Wrap(someErr, fmtErr)
	c.Assert(errors.Cause(err), gc.Equals, fmtErr)

	err = errors.Annotate(err, "annotated")
	c.Assert(errors.Cause(err), gc.Equals, fmtErr)

	err = errors.Maskf(err, "maksed")
	c.Assert(errors.Cause(err), gc.Equals, err)

	// Look for a file that we know isn't there.
	dir := c.MkDir()
	_, err = os.Stat(filepath.Join(dir, "not-there"))
	c.Assert(os.IsNotExist(err), jc.IsTrue)

	err = errors.Annotatef(err, "wrap it")
	// Now the error itself isn't a 'IsNotExist'.
	c.Assert(os.IsNotExist(err), jc.IsFalse)
	// However if we use the Check method, it is.
	c.Assert(os.IsNotExist(errors.Cause(err)), jc.IsTrue)
}

func (s *functionSuite) TestDetails(c *gc.C) {
	if runtime.Compiler == "gccgo" {
		c.Skip("gccgo can't determine the location")
	}
	c.Assert(errors.Details(nil), gc.Equals, "[]")

	otherErr := fmt.Errorf("other")
	checkDetails(c, otherErr, "[{other}]")

	err0 := newEmbed("foo") //err TestStack#0
	checkDetails(c, err0, "[{$TestStack#0$: foo}]")

	err1 := errors.Annotate(err0, "bar") //err TestStack#1
	checkDetails(c, err1, "[{$TestStack#1$: bar} {$TestStack#0$: foo}]")

	err2 := errors.Trace(err1) //err TestStack#2
	checkDetails(c, err2, "[{$TestStack#2$: } {$TestStack#1$: bar} {$TestStack#0$: foo}]")
}

type tracer interface {
	StackTrace() []string
}

func (*functionSuite) TestErrorStack(c *gc.C) {
	for i, test := range []struct {
		message   string
		generator func() error
		expected  string
		tracer    bool
	}{
		{
			message: "nil",
			generator: func() error {
				return nil
			},
		}, {
			message: "raw error",
			generator: func() error {
				return fmt.Errorf("raw")
			},
			expected: "raw",
		}, {
			message: "single error stack",
			generator: func() error {
				return errors.New("first error") //err single
			},
			expected: "$single$: first error",
			tracer:   true,
		}, {
			message: "annotated error",
			generator: func() error {
				err := errors.New("first error")          //err annotated-0
				return errors.Annotate(err, "annotation") //err annotated-1
			},
			expected: "" +
				"$annotated-0$: first error\n" +
				"$annotated-1$: annotation",
			tracer: true,
		}, {
			message: "wrapped error",
			generator: func() error {
				err := errors.New("first error")                    //err wrapped-0
				return errors.Wrap(err, newError("detailed error")) //err wrapped-1
			},
			expected: "" +
				"$wrapped-0$: first error\n" +
				"$wrapped-1$: detailed error",
			tracer: true,
		}, {
			message: "annotated wrapped error",
			generator: func() error {
				err := errors.Errorf("first error")                  //err ann-wrap-0
				err = errors.Wrap(err, fmt.Errorf("detailed error")) //err ann-wrap-1
				return errors.Annotatef(err, "annotated")            //err ann-wrap-2
			},
			expected: "" +
				"$ann-wrap-0$: first error\n" +
				"$ann-wrap-1$: detailed error\n" +
				"$ann-wrap-2$: annotated",
			tracer: true,
		}, {
			message: "traced, and annotated",
			generator: func() error {
				err := errors.New("first error")           //err stack-0
				err = errors.Trace(err)                    //err stack-1
				err = errors.Annotate(err, "some context") //err stack-2
				err = errors.Trace(err)                    //err stack-3
				err = errors.Annotate(err, "more context") //err stack-4
				return errors.Trace(err)                   //err stack-5
			},
			expected: "" +
				"$stack-0$: first error\n" +
				"$stack-1$: \n" +
				"$stack-2$: some context\n" +
				"$stack-3$: \n" +
				"$stack-4$: more context\n" +
				"$stack-5$: ",
			tracer: true,
		}, {
			message: "uncomparable, wrapped with a value error",
			generator: func() error {
				err := newNonComparableError("first error")     //err mixed-0
				err = errors.Trace(err)                         //err mixed-1
				err = errors.Wrap(err, newError("value error")) //err mixed-2
				err = errors.Maskf(err, "masked")               //err mixed-3
				err = errors.Annotate(err, "more context")      //err mixed-4
				return errors.Trace(err)                        //err mixed-5
			},
			expected: "" +
				"first error\n" +
				"$mixed-1$: \n" +
				"$mixed-2$: value error\n" +
				"$mixed-3$: masked\n" +
				"$mixed-4$: more context\n" +
				"$mixed-5$: ",
			tracer: true,
		},
	} {
		c.Logf("%v: %s", i, test.message)
		err := test.generator()
		expected := replaceLocations(test.expected)
		stack := errors.ErrorStack(err)
		ok := c.Check(stack, gc.Equals, expected)
		if !ok {
			c.Logf("%#v", err)
		}
		tracer, ok := err.(tracer)
		c.Check(ok, gc.Equals, test.tracer)
		if ok {
			stackTrace := tracer.StackTrace()
			c.Check(stackTrace, gc.DeepEquals, strings.Split(stack, "\n"))
		}
	}
}
