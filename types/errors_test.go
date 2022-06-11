// Copyright 2019 PingCAP, Inc.
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
	"testing"

	"github.com/pingcap/tidb/parser/terror"
	"github.com/stretchr/testify/require"
)

func TestError(t *testing.T) {
	kvErrs := []*terror.Error{
		ErrInvalidDefault,
		ErrDataTooLong,
		ErrIllegalValueForType,
		ErrTruncated,
		ErrOverflow,
		ErrDivByZero,
		ErrTooBigDisplayWidth,
		ErrTooBigFieldLength,
		ErrTooBigSet,
		ErrTooBigScale,
		ErrTooBigPrecision,
		ErrBadNumber,
		ErrInvalidFieldSize,
		ErrMBiggerThanD,
		ErrWarnDataOutOfRange,
		ErrDuplicatedValueInType,
		ErrDatetimeFunctionOverflow,
		ErrCastAsSignedOverflow,
		ErrCastNegIntAsUnsigned,
		ErrInvalidYearFormat,
		ErrTruncatedWrongVal,
		ErrInvalidWeekModeFormat,
		ErrWrongValue,
	}

	for _, err := range kvErrs {
		code := terror.ToSQLError(err).Code
		require.Equalf(t, code, uint16(err.Code()), "err: %v", err)
	}
}
