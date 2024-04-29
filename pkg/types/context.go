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
	"time"

	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// StrictFlags is a flags with a fields unset and has the most strict behavior.
const StrictFlags Flags = 0

// Flags indicate how to handle the conversion of a value.
type Flags uint16

const (
	// FlagIgnoreTruncateErr indicates to ignore the truncate error.
	// If this flag is set, `FlagTruncateAsWarning` will be ignored.
	FlagIgnoreTruncateErr Flags = 1 << iota
	// FlagTruncateAsWarning indicates to append the truncate error to warnings instead of returning it to user.
	FlagTruncateAsWarning
	// FlagAllowNegativeToUnsigned indicates to allow the casting from negative to unsigned int.
	// When this flag is not set by default, casting a negative value to unsigned results an overflow error.
	// Otherwise, a negative value will be cast to the corresponding unsigned value without any error.
	// For example, when casting -1 to an unsigned bigint with `FlagAllowNegativeToUnsigned` set,
	// we will get `18446744073709551615` which is the biggest unsigned value.
	FlagAllowNegativeToUnsigned
	// FlagIgnoreZeroDateErr indicates to ignore the zero-date error.
	// See: https://dev.mysql.com/doc/refman/8.0/en/sql-mode.html#sqlmode_no_zero_date for details about the "zero-date" error.
	// If this flag is set, `FlagZeroDateAsWarning` will be ignored.
	//
	// TODO: `FlagIgnoreZeroDateErr` and `FlagZeroDateAsWarning` don't represent the comments right now, because the
	// errors related with `time` and `duration` are handled directly according to SQL mode in many places (expression,
	// ddl ...). These error handling will be refined in the future. Currently, the `FlagZeroDateAsWarning` is not used,
	// and the `FlagIgnoreZeroDateErr` is used to allow or disallow casting zero to date in `alter` statement. See #25728
	// This flag is the reverse of `NoZeroDate` in #30507. It's set to `true` for most context, and is only set to
	// `false` for `alter` (and `create`) statements.
	FlagIgnoreZeroDateErr
	// FlagIgnoreZeroInDateErr indicates to ignore the zero-in-date error.
	// See: https://dev.mysql.com/doc/refman/8.0/en/sql-mode.html#sqlmode_no_zero_in_date for details about the "zero-in-date" error.
	FlagIgnoreZeroInDateErr
	// FlagIgnoreInvalidDateErr indicates to ignore the invalid-date error.
	// See: https://dev.mysql.com/doc/refman/8.0/en/sql-mode.html#sqlmode_allow_invalid_dates for details about the "invalid-date" error.
	FlagIgnoreInvalidDateErr
	// FlagSkipASCIICheck indicates to skip the ASCII check when converting the value to an ASCII string.
	FlagSkipASCIICheck
	// FlagSkipUTF8Check indicates to skip the UTF8 check when converting the value to an UTF8MB3 string.
	FlagSkipUTF8Check
	// FlagSkipUTF8MB4Check indicates to skip the UTF8MB4 check when converting the value to an UTF8 string.
	FlagSkipUTF8MB4Check
	// FlagCastTimeToYearThroughConcat indicates to cast time to year through concatenation. For example, `00:19:59` will be converted to '1959'
	FlagCastTimeToYearThroughConcat
)

// AllowNegativeToUnsigned indicates whether the flag `FlagAllowNegativeToUnsigned` is set
func (f Flags) AllowNegativeToUnsigned() bool {
	return f&FlagAllowNegativeToUnsigned != 0
}

// WithAllowNegativeToUnsigned returns a new flags with `FlagAllowNegativeToUnsigned` set/unset according to the clip parameter
func (f Flags) WithAllowNegativeToUnsigned(clip bool) Flags {
	if clip {
		return f | FlagAllowNegativeToUnsigned
	}
	return f &^ FlagAllowNegativeToUnsigned
}

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

// IgnoreTruncateErr indicates whether the flag `FlagIgnoreTruncateErr` is set
func (f Flags) IgnoreTruncateErr() bool {
	return f&FlagIgnoreTruncateErr != 0
}

// WithIgnoreTruncateErr returns a new flags with `FlagIgnoreTruncateErr` set/unset according to the skip parameter
func (f Flags) WithIgnoreTruncateErr(ignore bool) Flags {
	if ignore {
		return f | FlagIgnoreTruncateErr
	}
	return f &^ FlagIgnoreTruncateErr
}

// TruncateAsWarning indicates whether the flag `FlagTruncateAsWarning` is set
func (f Flags) TruncateAsWarning() bool {
	return f&FlagTruncateAsWarning != 0
}

// WithTruncateAsWarning returns a new flags with `FlagTruncateAsWarning` set/unset according to the skip parameter
func (f Flags) WithTruncateAsWarning(warn bool) Flags {
	if warn {
		return f | FlagTruncateAsWarning
	}
	return f &^ FlagTruncateAsWarning
}

// IgnoreZeroInDate indicates whether the flag `FlagIgnoreZeroInData` is set
func (f Flags) IgnoreZeroInDate() bool {
	return f&FlagIgnoreZeroInDateErr != 0
}

// WithIgnoreZeroInDate returns a new flags with `FlagIgnoreZeroInDateErr` set/unset according to the ignore parameter
func (f Flags) WithIgnoreZeroInDate(ignore bool) Flags {
	if ignore {
		return f | FlagIgnoreZeroInDateErr
	}
	return f &^ FlagIgnoreZeroInDateErr
}

// IgnoreInvalidDateErr indicates whether the flag `FlagIgnoreInvalidDateErr` is set
func (f Flags) IgnoreInvalidDateErr() bool {
	return f&FlagIgnoreInvalidDateErr != 0
}

// WithIgnoreInvalidDateErr returns a new flags with `FlagIgnoreInvalidDateErr` set/unset according to the ignore parameter
func (f Flags) WithIgnoreInvalidDateErr(ignore bool) Flags {
	if ignore {
		return f | FlagIgnoreInvalidDateErr
	}
	return f &^ FlagIgnoreInvalidDateErr
}

// IgnoreZeroDateErr indicates whether the flag `FlagIgnoreZeroDateErr` is set
func (f Flags) IgnoreZeroDateErr() bool {
	return f&FlagIgnoreZeroDateErr != 0
}

// WithIgnoreZeroDateErr returns a new flags with `FlagIgnoreZeroDateErr` set/unset according to the ignore parameter
func (f Flags) WithIgnoreZeroDateErr(ignore bool) Flags {
	if ignore {
		return f | FlagIgnoreZeroDateErr
	}
	return f &^ FlagIgnoreZeroDateErr
}

// CastTimeToYearThroughConcat whether `FlagCastTimeToYearThroughConcat` is set
func (f Flags) CastTimeToYearThroughConcat() bool {
	return f&FlagCastTimeToYearThroughConcat != 0
}

// WithCastTimeToYearThroughConcat returns a new flags with `FlagCastTimeToYearThroughConcat` set/unset according to the flag parameter
func (f Flags) WithCastTimeToYearThroughConcat(flag bool) Flags {
	if flag {
		return f | FlagCastTimeToYearThroughConcat
	}
	return f &^ FlagCastTimeToYearThroughConcat
}

// Context provides the information when converting between different types.
type Context struct {
	flags       Flags
	loc         *time.Location
	warnHandler contextutil.WarnAppender
}

// NewContext creates a new `Context`
func NewContext(flags Flags, loc *time.Location, handler contextutil.WarnAppender) Context {
	intest.Assert(loc != nil && handler != nil)
	return Context{
		flags:       flags,
		loc:         loc,
		warnHandler: handler,
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
	intest.AssertNotNil(loc)
	ctx := *c
	ctx.loc = loc
	return ctx
}

// Location returns the location of the context
func (c *Context) Location() *time.Location {
	intest.AssertNotNil(c.loc)
	if c.loc == nil {
		// c.loc should always not be nil, just make the code safe here.
		return time.UTC
	}
	return c.loc
}

// AppendWarning appends the error to warning. If the inner `warnHandler` is nil, do nothing.
func (c *Context) AppendWarning(err error) {
	intest.Assert(c.warnHandler != nil)
	if w := c.warnHandler; w != nil {
		// warnHandler should always not be nil, check fn != nil here to just make code safe.
		w.AppendWarning(err)
	}
}

// DefaultStmtFlags is the default flags for statement context with the flag `FlagAllowNegativeToUnsigned` set.
// TODO: make DefaultStmtFlags to be equal with StrictFlags, and setting flag `FlagAllowNegativeToUnsigned`
// is only for make the code to be equivalent with the old implement during refactoring.
const DefaultStmtFlags = StrictFlags | FlagAllowNegativeToUnsigned | FlagIgnoreZeroDateErr

// DefaultStmtNoWarningContext is the context with default statement flags without any other special configuration
var DefaultStmtNoWarningContext = NewContext(DefaultStmtFlags, time.UTC, contextutil.IgnoreWarn)

// StrictContext is the most strict context which returns every error it meets
//
// this context should never append warnings
// However, the implementation of `types` may still append some warnings. TODO: remove them in the future.
var StrictContext = NewContext(StrictFlags, time.UTC, contextutil.IgnoreWarn)
