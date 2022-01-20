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

	ts, err = parseTSString("")
	require.NoError(t, err)
	require.Zero(t, ts)

	ts, err = parseTSString("400036290571534337")
	require.NoError(t, err)
	require.Equal(t, uint64(400036290571534337), ts)

	_, offset := time.Now().Local().Zone()
	ts, err = parseTSString("2018-05-11 01:42:23")
	require.NoError(t, err)
	require.Equal(t, uint64(400032515489792000-(offset*1000)<<18), ts)
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
