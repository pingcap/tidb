// Copyright 2013, 2014 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package errors_test

import (
	"path"

	gc "gopkg.in/check.v1"

	"github.com/juju/errors"
)

type pathSuite struct{}

var _ = gc.Suite(&pathSuite{})

func (*pathSuite) TestGoPathSet(c *gc.C) {
	c.Assert(errors.GoPath(), gc.Not(gc.Equals), "")
}

func (*pathSuite) TestTrimGoPath(c *gc.C) {
	relativeImport := "github.com/foo/bar/baz.go"
	filename := path.Join(errors.GoPath(), relativeImport)
	c.Assert(errors.TrimGoPath(filename), gc.Equals, relativeImport)

	absoluteImport := "/usr/share/foo/bar/baz.go"
	c.Assert(errors.TrimGoPath(absoluteImport), gc.Equals, absoluteImport)
}
