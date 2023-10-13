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

package context

import (
	"time"

	"github.com/pingcap/tidb/pkg/util/intest"
)

// StrictFlags is a flags with a fields unset and has the most strict behavior.
const StrictFlags Flags = 0

// Flags indicates how to handle the conversion of a value.
type Flags uint16

const (
	// FlagIgnoreTruncateErr indicates to ignore the truncate error.
	// If this flag is set, `FlagTruncateAsWarning` will be ignored.
	FlagIgnoreTruncateErr Flags = 1 << iota
	// FlagTruncateAsWarning indicates to append the truncate error to warnings instead of returning it to user.
	FlagTruncateAsWarning
	// FlagClipNegativeToZero indicates to clip the value to zero when casting a negative value to an unsigned integer.
	// When this flag is set and the clip happens, an overflow error occurs and how to handle it will be determined by flags
	// `FlagIgnoreOverflowError` and `FlagOverflowAsWarning`.
	FlagClipNegativeToZero
	// FlagIgnoreOverflowError indicates to ignore the overflow error.
	// If this flag is set, `FlagOverflowAsWarning` will be ignored.
	FlagIgnoreOverflowError
	// FlagOverflowAsWarning indicates to append the overflow error to warnings instead of returning it to user.
	FlagOverflowAsWarning
	// FlagIgnoreZeroDateErr indicates to ignore the zero-date error.
	// See: https://dev.mysql.com/doc/refman/8.0/en/sql-mode.html#sqlmode_no_zero_date for details about the "zero-date" error.
	// If this flag is set, `FlagZeroDateAsWarning` will be ignored.
	FlagIgnoreZeroDateErr
	// FlagZeroDateAsWarning indicates to append the zero-date error to warnings instead of returning it to user.
	FlagZeroDateAsWarning
	// FlagIgnoreZeroInDateErr indicates to ignore the zero-in-date error.
	// See: https://dev.mysql.com/doc/refman/8.0/en/sql-mode.html#sqlmode_no_zero_in_date for details about the "zero-in-date" error.
	FlagIgnoreZeroInDateErr
	// FlagZeroInDateAsWarning indicates to append the zero-in-date error to warnings instead of returning it to user.
	FlagZeroInDateAsWarning
	// FlagIgnoreInvalidDateErr indicates to ignore the invalid-date error.
	// See: https://dev.mysql.com/doc/refman/8.0/en/sql-mode.html#sqlmode_allow_invalid_dates for details about the "invalid-date" error.
	FlagIgnoreInvalidDateErr
	// FlagInvalidDateAsWarning indicates to append the invalid-date error to warnings instead of returning it to user.
	FlagInvalidDateAsWarning
	// FlagSkipASCIICheck indicates to skip the ASCII check when converting the value to an ASCII string.
	FlagSkipASCIICheck
	// FlagSkipUTF8Check indicates to skip the UTF8 check when converting the value to an UTF8MB3 string.
	FlagSkipUTF8Check
	// FlagSkipUTF8MB4Check indicates to skip the UTF8MB4 check when converting the value to an UTF8 string.
	FlagSkipUTF8MB4Check
)

// SkipASCIICheck indicates whether the flag `FlagSkipASCIICheck` is set
func (f Flags) SkipASCIICheck() bool {
	return f&FlagSkipASCIICheck != 0
}

// WithSkipSACIICheck returns a new flags with `FlagSkipASCIICheck` set/unset according to the skip parameter
func (f Flags) WithSkipSACIICheck(skip bool) Flags {
	if skip {
		return f | FlagSkipASCIICheck
	}
	return f &^ FlagSkipASCIICheck
}

// SkipUTF8Check indicates whether the flag `FlagSkipUTF8Check` is set
func (f Flags) SkipUTF8Check() bool {
	return f&FlagSkipUTF8Check != 0
}

// WithSkipUTF8Check returns a new flags with `FlagSkipUTF8Check` set/unset according to the skip parameter
func (f Flags) WithSkipUTF8Check(skip bool) Flags {
	if skip {
		return f | FlagSkipUTF8Check
	}
	return f &^ FlagSkipUTF8Check
}

// SkipUTF8MB4Check indicates whether the flag `FlagSkipUTF8MB4Check` is set
func (f Flags) SkipUTF8MB4Check() bool {
	return f&FlagSkipUTF8MB4Check != 0
}

// WithSkipUTF8MB4Check returns a new flags with `FlagSkipUTF8MB4Check` set/unset according to the skip parameter
func (f Flags) WithSkipUTF8MB4Check(skip bool) Flags {
	if skip {
		return f | FlagSkipUTF8MB4Check
	}
	return f &^ FlagSkipUTF8MB4Check
}

// Context provides the information when converting between different types.
type Context struct {
	flags           Flags
	loc             *time.Location
	appendWarningFn func(err error)
}

// NewContext creates a new `Context`
func NewContext(flags Flags, loc *time.Location, appendWarningFn func(err error)) Context {
	intest.Assert(loc != nil && appendWarningFn != nil)
	return Context{
		flags:           flags,
		loc:             loc,
		appendWarningFn: appendWarningFn,
	}
}

// Flags returns the flags of the context
func (c *Context) Flags() Flags {
	return c.flags
}

// WithFlags returns a new context with the flags set to the given value
func (c *Context) WithFlags(f Flags) Context {
	ctx := *c
	ctx.flags = f
	return ctx
}

// WithLocation returns a new context with the given location
func (c *Context) WithLocation(loc *time.Location) Context {
	intest.Assert(loc)
	ctx := *c
	ctx.loc = loc
	return ctx
}

// Location returns the location of the context
func (c *Context) Location() *time.Location {
	intest.Assert(c.loc)
	if c.loc == nil {
		// c.loc should always not be nil, just make the code safe here.
		return time.UTC
	}
	return c.loc
}

// AppendWarning appends the error to warning. If the inner `appendWarningFn` is nil, do nothing.
func (c *Context) AppendWarning(err error) {
	intest.Assert(c.appendWarningFn != nil)
	if fn := c.appendWarningFn; fn != nil {
		// appendWarningFn should always not be nil, check fn != nil here to just make code safe.
		fn(err)
	}
}

// AppendWarningFunc returns the inner `appendWarningFn`
func (c *Context) AppendWarningFunc() func(err error) {
	return c.appendWarningFn
}
