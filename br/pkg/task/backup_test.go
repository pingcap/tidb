// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"testing"
	"time"

	backup "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/stretchr/testify/require"
)

func TestParseTSString(t *testing.T) {
	var (
		ts  uint64
		err error
	)

	ts, err = ParseTSString("", false)
	require.NoError(t, err)
	require.Zero(t, ts)

	ts, err = ParseTSString("400036290571534337", false)
	require.NoError(t, err)
	require.Equal(t, uint64(400036290571534337), ts)

	ts, err = ParseTSString("2021-01-01 01:42:23", false)
	require.NoError(t, err)
	localTime := time.Date(2021, time.Month(1), 1, 1, 42, 23, 0, time.Local)
	localTimestamp := localTime.Unix()
	localTSO := uint64((localTimestamp << 18) * 1000)
	require.Equal(t, localTSO, ts)

	_, err = ParseTSString("2021-01-01 01:42:23", true)
	require.Error(t, err)
	require.Regexp(t, "must set timezone*", err.Error())

	ts, err = ParseTSString("2021-01-01 01:42:23+00:00", true)
	require.NoError(t, err)
	localTime = time.Date(2021, time.Month(1), 1, 1, 42, 23, 0, time.UTC)
	localTimestamp = localTime.Unix()
	localTSO = uint64((localTimestamp << 18) * 1000)
	require.Equal(t, localTSO, ts)

	ts, err = ParseTSString("2021-01-01 01:42:23+08:00", true)
	require.NoError(t, err)
	secondsEastOfUTC := int((8 * time.Hour).Seconds())
	beijing := time.FixedZone("Beijing Time", secondsEastOfUTC)
	localTime = time.Date(2021, time.Month(1), 1, 1, 42, 23, 0, beijing)
	localTimestamp = localTime.Unix()
	localTSO = uint64((localTimestamp << 18) * 1000)
	require.Equal(t, localTSO, ts)
}

func TestParseCompressionType(t *testing.T) {
	var (
		ct  backup.CompressionType
		err error
	)
	ct, err = parseCompressionType("lz4")
	require.NoError(t, err)
	require.Equal(t, 1, int(ct))

	ct, err = parseCompressionType("snappy")
	require.NoError(t, err)
	require.Equal(t, 2, int(ct))

	ct, err = parseCompressionType("zstd")
	require.NoError(t, err)
	require.Equal(t, 3, int(ct))

	ct, err = parseCompressionType("Other Compression (strings)")
	require.Error(t, err)
	require.Regexp(t, "invalid compression.*", err.Error())
	require.Zero(t, ct)
}
