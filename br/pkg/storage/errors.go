// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"fmt"

	"github.com/pingcap/errors"
)

// wrappedError is a wrapper of BR specified errors.wrappedError. When this package is used
// generally, wrappedError can provide a clear error message. When this package is used
// in BR, wrappedError can be converted to BR's error.
type wrappedError struct {
	text    string
	brError error
}

func newWrappedError(err error, format string, args ...interface{}) error {
	return &wrappedError{
		text:    fmt.Sprintf(format, args...),
		brError: err,
	}
}

func (e *wrappedError) Error() string {
	return e.text
}

// TryConvertToBRError converts the error to BR's error if it is a *wrappedError.
func TryConvertToBRError(err error) error {
	if err == nil {
		return nil
	}
	err2 := errors.Cause(err)
	if e, ok := err2.(*wrappedError); ok {
		return errors.Annotatef(e.brError, e.text)
	}
	return err
}
