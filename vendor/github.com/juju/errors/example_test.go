// Copyright 2013, 2014 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package errors_test

import (
	"fmt"

	"github.com/juju/errors"
)

func ExampleTrace() {
	var err1 error = fmt.Errorf("something wicked this way comes")
	var err2 error = nil

	// Tracing a non nil error will return an error
	fmt.Println(errors.Trace(err1))
	// Tracing nil will return nil
	fmt.Println(errors.Trace(err2))

	// Output: something wicked this way comes
	// <nil>
}
