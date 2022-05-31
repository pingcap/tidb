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

	ts, err = ParseTSString("")
	require.NoError(t, err)
	require.Zero(t, ts)

	ts, err = ParseTSString("400036290571534337")
	require.NoError(t, err)
	require.Equal(t, uint64(400036290571534337), ts)

	ts, err = ParseTSString("2021-01-01 01:42:23")
	require.NoError(t, err)
	localTime := time.Date(2021, time.Month(1), 1, 1, 42, 23, 0, time.Local)

	localTimestamp := localTime.Unix()
	localTSO := uint64((localTimestamp << 18) * 1000)
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
