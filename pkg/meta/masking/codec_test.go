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

package masking

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestDecodeMaskingPolicyAuditTimeAcrossTimeZones(t *testing.T) {
	previous := time.Local
	t.Cleanup(func() { time.Local = previous })
	for _, zone := range []string{"UTC", "America/New_York", "Asia/Shanghai"} {
		t.Run(zone, func(t *testing.T) {
			var err error
			time.Local, err = time.LoadLocation(zone)
			require.NoError(t, err)
			// This legal DATETIME is inside New York's spring-forward gap.
			audit := types.NewTime(types.FromDate(2026, 3, 8, 2, 30, 0, 123456), mysql.TypeDatetime, 6)
			row := chunk.MutRowFromValues(int64(1), "p", "test", "t", int64(2), "c", int64(1), "MASK_NULL(c)", "ENABLED", "MASK_NULL", "NONE", audit, audit, "root").ToRow()
			policy, err := DecodeMaskingPolicyRow(row)
			require.NoError(t, err)
			expected := time.Date(2026, 3, 8, 2, 30, 0, 123456000, time.UTC)
			require.Equal(t, expected, policy.CreatedAt)
			require.Equal(t, expected, policy.UpdatedAt)
		})
	}
}
