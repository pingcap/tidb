// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

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
