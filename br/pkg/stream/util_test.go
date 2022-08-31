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
	}

	timeZone, _ := time.LoadLocation("Asia/Shanghai")
	for _, ca := range cases {
		date := FormatDate(oracle.GetTimeFromTS(ca.ts).In(timeZone))
		require.Equal(t, ca.target, date)
	}
}
