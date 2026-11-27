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

func TestTruncateWrongInsertValueRedaction(t *testing.T) {
	original := errors.RedactLogEnabled.Load()
	t.Cleanup(func() { errors.RedactLogEnabled.Store(original) })
	for _, tc := range []struct {
		mode      string
		wantValue string
	}{
		{errors.RedactLogDisable, "test-value"},
		{errors.RedactLogEnable, "?"},
		{errors.RedactLogMarker, "‹test-value›"},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			errors.RedactLogEnabled.Store(tc.mode)
			err := ErrTruncateWrongInsertValue.FastGenByArgs("datetime", "test-value", "d", 2)
			require.EqualError(t, err, "[table:1292]Incorrect datetime value: '"+tc.wantValue+"' for column 'd' at row 2")
		})
	}
}
