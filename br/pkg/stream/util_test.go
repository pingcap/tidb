// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package stream

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestDateFormat(t *testing.T) {
	cases := []struct {
		ts     uint64
		target string
	}{
		{
			434604259287760897,
			"2022-07-15 19:14:39.534 +0800",
		},
		{
			434605479096221697,
			"2022-07-15 20:32:12.734 +0800",
		},
		{
			434605478903808000,
			"2022-07-15 20:32:12 +0800",
		},
	}

	timeZone, _ := time.LoadLocation("Asia/Shanghai")
	for _, ca := range cases {
		ts := oracle.GetTimeFromTS(ca.ts).In(timeZone)
		date := FormatDate(ts)
		require.Equal(t, ca.target, date)
		ts2, err := ParseDate(date)
		require.NoError(t, err)
		require.Equal(t, ts, ts2.In(timeZone))
	}
}

func TestPrefix(t *testing.T) {
	require.True(t, IsMetaDBKey([]byte("mDBs")))
	require.False(t, IsMetaDBKey([]byte("mDDL")))
	require.True(t, IsMetaDDLJobHistoryKey([]byte("mDDLJobHistory")))
	require.False(t, IsMetaDDLJobHistoryKey([]byte("mDDL")))
	require.True(t, MaybeDBOrDDLJobHistoryKey([]byte("mDL")))
	require.True(t, MaybeDBOrDDLJobHistoryKey([]byte("mDB:")))
	require.True(t, MaybeDBOrDDLJobHistoryKey([]byte("mDDLHistory")))
	require.False(t, MaybeDBOrDDLJobHistoryKey([]byte("DDL")))
}
