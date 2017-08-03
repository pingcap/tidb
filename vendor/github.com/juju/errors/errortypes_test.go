// Copyright 2013, 2014 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package errors_test

import (
	stderrors "errors"
	"fmt"
	"reflect"
	"runtime"

	"github.com/juju/errors"
	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"
)

// errorInfo holds information about a single error type: a satisfier
// function, wrapping and variable arguments constructors and message
// suffix.
type errorInfo struct {
	satisfier       func(error) bool
	argsConstructor func(string, ...interface{}) error
	wrapConstructor func(error, string) error
	suffix          string
}

// allErrors holds information for all defined errors. When adding new
// errors, add them here as well to include them in tests.
var allErrors = []*errorInfo{
	&errorInfo{errors.IsNotFound, errors.NotFoundf, errors.NewNotFound, " not found"},
	&errorInfo{errors.IsUserNotFound, errors.UserNotFoundf, errors.NewUserNotFound, " user not found"},
	&errorInfo{errors.IsUnauthorized, errors.Unauthorizedf, errors.NewUnauthorized, ""},
	&errorInfo{errors.IsNotImplemented, errors.NotImplementedf, errors.NewNotImplemented, " not implemented"},
	&errorInfo{errors.IsAlreadyExists, errors.AlreadyExistsf, errors.NewAlreadyExists, " already exists"},
	&errorInfo{errors.IsNotSupported, errors.NotSupportedf, errors.NewNotSupported, " not supported"},
	&errorInfo{errors.IsNotValid, errors.NotValidf, errors.NewNotValid, " not valid"},
	&errorInfo{errors.IsNotProvisioned, errors.NotProvisionedf, errors.NewNotProvisioned, " not provisioned"},
	&errorInfo{errors.IsNotAssigned, errors.NotAssignedf, errors.NewNotAssigned, " not assigned"},
	&errorInfo{errors.IsMethodNotAllowed, errors.MethodNotAllowedf, errors.NewMethodNotAllowed, ""},
	&errorInfo{errors.IsBadRequest, errors.BadRequestf, errors.NewBadRequest, ""},
}

type errorTypeSuite struct{}

var _ = gc.Suite(&errorTypeSuite{})

func (t *errorInfo) satisfierName() string {
	value := reflect.ValueOf(t.satisfier)
	f := runtime.FuncForPC(value.Pointer())
	return f.Name()
}

func (t *errorInfo) equal(t0 *errorInfo) bool {
	if t0 == nil {
		return false
	}
	return t.satisfierName() == t0.satisfierName()
}

type errorTest struct {
	err     error
	message string
	errInfo *errorInfo
}

func deferredAnnotatef(err error, format string, args ...interface{}) error {
	errors.DeferredAnnotatef(&err, format, args...)
	return err
}

func mustSatisfy(c *gc.C, err error, errInfo *errorInfo) {
	if errInfo != nil {
		msg := fmt.Sprintf("%#v must satisfy %v", err, errInfo.satisfierName())
		c.Check(err, jc.Satisfies, errInfo.satisfier, gc.Commentf(msg))
	}
}

func mustNotSatisfy(c *gc.C, err error, errInfo *errorInfo) {
	if errInfo != nil {
		msg := fmt.Sprintf("%#v must not satisfy %v", err, errInfo.satisfierName())
		c.Check(err, gc.Not(jc.Satisfies), errInfo.satisfier, gc.Commentf(msg))
	}
}

func checkErrorMatches(c *gc.C, err error, message string, errInfo *errorInfo) {
	if message == "<nil>" {
		c.Check(err, gc.IsNil)
		c.Check(errInfo, gc.IsNil)
	} else {
		c.Check(err, gc.ErrorMatches, message)
	}
}

func runErrorTests(c *gc.C, errorTests []errorTest, checkMustSatisfy bool) {
	for i, t := range errorTests {
		c.Logf("test %d: %T: %v", i, t.err, t.err)
		checkErrorMatches(c, t.err, t.message, t.errInfo)
		if checkMustSatisfy {
			mustSatisfy(c, t.err, t.errInfo)
		}

		// Check all other satisfiers to make sure none match.
		for _, otherErrInfo := range allErrors {
			if checkMustSatisfy && otherErrInfo.equal(t.errInfo) {
				continue
			}
			mustNotSatisfy(c, t.err, otherErrInfo)
		}
	}
}

func (*errorTypeSuite) TestDeferredAnnotatef(c *gc.C) {
	// Ensure DeferredAnnotatef annotates the errors.
	errorTests := []errorTest{}
	for _, errInfo := range allErrors {
		errorTests = append(errorTests, []errorTest{{
			deferredAnnotatef(nil, "comment"),
			"<nil>",
			nil,
		}, {
			deferredAnnotatef(stderrors.New("blast"), "comment"),
			"comment: blast",
			nil,
		}, {
			deferredAnnotatef(errInfo.argsConstructor("foo %d", 42), "comment %d", 69),
			"comment 69: foo 42" + errInfo.suffix,
			errInfo,
		}, {
			deferredAnnotatef(errInfo.argsConstructor(""), "comment"),
			"comment: " + errInfo.suffix,
			errInfo,
		}, {
			deferredAnnotatef(errInfo.wrapConstructor(stderrors.New("pow!"), "woo"), "comment"),
			"comment: woo: pow!",
			errInfo,
		}}...)
	}

	runErrorTests(c, errorTests, true)
}

func (*errorTypeSuite) TestAllErrors(c *gc.C) {
	errorTests := []errorTest{}
	for _, errInfo := range allErrors {
		errorTests = append(errorTests, []errorTest{{
			nil,
			"<nil>",
			nil,
		}, {
			errInfo.argsConstructor("foo %d", 42),
			"foo 42" + errInfo.suffix,
			errInfo,
		}, {
			errInfo.argsConstructor(""),
			errInfo.suffix,
			errInfo,
		}, {
			errInfo.wrapConstructor(stderrors.New("pow!"), "prefix"),
			"prefix: pow!",
			errInfo,
		}, {
			errInfo.wrapConstructor(stderrors.New("pow!"), ""),
			"pow!",
			errInfo,
		}, {
			errInfo.wrapConstructor(nil, "prefix"),
			"prefix",
			errInfo,
		}}...)
	}

	runErrorTests(c, errorTests, true)
}
