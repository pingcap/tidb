// Copyright 2026 PingCAP, Inc.
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

package exeerrors

import (
	"testing"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
)

func TestRedactTruncateWrongInsertValue(t *testing.T) {
	original := errors.RedactLogEnabled.Load()
	defer func() { errors.RedactLogEnabled.Store(original) }()

	const sensitiveValue = "2099-99-99-secret"
	for _, mode := range []string{errors.RedactLogEnable, errors.RedactLogMarker} {
		errors.RedactLogEnabled.Store(mode)
		err := ErrTruncateWrongInsertValue.FastGenByArgs("datetime", sensitiveValue, "d", 1)

		if mode == errors.RedactLogEnable {
			require.NotContains(t, err.Error(), sensitiveValue)
			require.Contains(t, err.Error(), "Incorrect datetime value: '?' for column 'd' at row 1")
		} else {
			require.Contains(t, err.Error(), "Incorrect datetime value: '‹"+sensitiveValue+"›' for column 'd' at row 1")
		}
	}
}
