// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/errno"
)

// HandleTruncate ignores or returns the error based on the Context state.
func (c *Context) HandleTruncate(err error) error {
	// TODO: At present we have not checked whether the error can be ignored or treated as warning.
	// We will do that later, and then append WarnDataTruncated instead of the error itself.
	if err == nil {
		return nil
	}

	err = errors.Cause(err)
	if e, ok := err.(*errors.Error); !ok ||
		(e.Code() != errno.ErrTruncatedWrongValue &&
			e.Code() != errno.ErrDataTooLong &&
			e.Code() != errno.ErrTruncatedWrongValueForField &&
			e.Code() != errno.ErrWarnDataOutOfRange &&
			e.Code() != errno.ErrDataOutOfRange &&
			e.Code() != errno.ErrBadNumber &&
			e.Code() != errno.ErrWrongValueForType &&
			e.Code() != errno.ErrDatetimeFunctionOverflow &&
			e.Code() != errno.WarnDataTruncated &&
			e.Code() != errno.ErrIncorrectDatetimeValue) {
		return err
	}

	if c.Flags().IgnoreTruncateErr() {
		return nil
	}
	if c.Flags().TruncateAsWarning() {
		c.AppendWarning(err)
		return nil
	}
	return err
}
