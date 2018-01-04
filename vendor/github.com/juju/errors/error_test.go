// Copyright 2014 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package errors_test

import (
	"fmt"
	"runtime"

	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"

	"github.com/juju/errors"
)

type errorsSuite struct{}

var _ = gc.Suite(&errorsSuite{})

var someErr = errors.New("some error") //err varSomeErr

func (*errorsSuite) TestErrorString(c *gc.C) {
	for i, test := range []struct {
		message   string
		generator func() error
		expected  string
	}{
		{
			message: "uncomparable errors",
			generator: func() error {
				err := errors.Annotatef(newNonComparableError("uncomparable"), "annotation")
				return errors.Annotatef(err, "another")
			},
			expected: "another: annotation: uncomparable",
		}, {
			message: "Errorf",
			generator: func() error {
				return errors.Errorf("first error")
			},
			expected: "first error",
		}, {
			message: "annotated error",
			generator: func() error {
				err := errors.Errorf("first error")
				return errors.Annotatef(err, "annotation")
			},
			expected: "annotation: first error",
		}, {
			message: "test annotation format",
			generator: func() error {
				err := errors.Errorf("first %s", "error")
				return errors.Annotatef(err, "%s", "annotation")
			},
			expected: "annotation: first error",
		}, {
			message: "wrapped error",
			generator: func() error {
				err := newError("first error")
				return errors.Wrap(err, newError("detailed error"))
			},
			expected: "detailed error",
		}, {
			message: "wrapped annotated error",
			generator: func() error {
				err := errors.Errorf("first error")
				err = errors.Annotatef(err, "annotated")
				return errors.Wrap(err, fmt.Errorf("detailed error"))
			},
			expected: "detailed error",
		}, {
			message: "annotated wrapped error",
			generator: func() error {
				err := errors.Errorf("first error")
				err = errors.Wrap(err, fmt.Errorf("detailed error"))
				return errors.Annotatef(err, "annotated")
			},
			expected: "annotated: detailed error",
		}, {
			message: "traced, and annotated",
			generator: func() error {
				err := errors.New("first error")
				err = errors.Trace(err)
				err = errors.Annotate(err, "some context")
				err = errors.Trace(err)
				err = errors.Annotate(err, "more context")
				return errors.Trace(err)
			},
			expected: "more context: some context: first error",
		}, {
			message: "traced, and annotated, masked and annotated",
			generator: func() error {
				err := errors.New("first error")
				err = errors.Trace(err)
				err = errors.Annotate(err, "some context")
				err = errors.Maskf(err, "masked")
				err = errors.Annotate(err, "more context")
				return errors.Trace(err)
			},
			expected: "more context: masked: some context: first error",
		},
	} {
		c.Logf("%v: %s", i, test.message)
		err := test.generator()
		ok := c.Check(err.Error(), gc.Equals, test.expected)
		if !ok {
			c.Logf("%#v", test.generator())
		}
	}
}

type embed struct {
	errors.Err
}

func newEmbed(format string, args ...interface{}) *embed {
	err := &embed{errors.NewErr(format, args...)}
	err.SetLocation(1)
	return err
}

func (*errorsSuite) TestNewErr(c *gc.C) {
	if runtime.Compiler == "gccgo" {
		c.Skip("gccgo can't determine the location")
	}
	err := newEmbed("testing %d", 42) //err embedErr
	c.Assert(err.Error(), gc.Equals, "testing 42")
	c.Assert(errors.Cause(err), gc.Equals, err)
	c.Assert(errors.Details(err), jc.Contains, tagToLocation["embedErr"].String())
}

func newEmbedWithCause(other error, format string, args ...interface{}) *embed {
	err := &embed{errors.NewErrWithCause(other, format, args...)}
	err.SetLocation(1)
	return err
}

func (*errorsSuite) TestNewErrWithCause(c *gc.C) {
	if runtime.Compiler == "gccgo" {
		c.Skip("gccgo can't determine the location")
	}
	causeErr := fmt.Errorf("external error")
	err := newEmbedWithCause(causeErr, "testing %d", 43) //err embedCause
	c.Assert(err.Error(), gc.Equals, "testing 43: external error")
	c.Assert(errors.Cause(err), gc.Equals, causeErr)
	c.Assert(errors.Details(err), jc.Contains, tagToLocation["embedCause"].String())
}

var _ error = (*embed)(nil)

// This is an uncomparable error type, as it is a struct that supports the
// error interface (as opposed to a pointer type).
type error_ struct {
	info  string
	slice []string
}

// Create a non-comparable error
func newNonComparableError(message string) error {
	return error_{info: message}
}

func (e error_) Error() string {
	return e.info
}

func newError(message string) error {
	return testError{message}
}

// The testError is a value type error for ease of seeing results
// when the test fails.
type testError struct {
	message string
}

func (e testError) Error() string {
	return e.message
}
