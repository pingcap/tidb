// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"testing"
	"time"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/stretchr/testify/require"
)

func TestParseTSString(t *testing.T) {
	var (
		ts  uint64
		err error
	)

	ts, err = parseTSString("")
	require.NoError(t, err)
	require.Equal(t, 0, int(ts))

	ts, err = parseTSString("400036290571534337")
	require.NoError(t, err)
	require.Equal(t, 400036290571534337, int(ts))

	_, offset := time.Now().Local().Zone()
	ts, err = parseTSString("2018-05-11 01:42:23")
	require.NoError(t, err)
	require.Equal(t, 400032515489792000-(offset*1000)<<18, int(ts))
}

func TestParseCompressionType(t *testing.T) {
	var (
		ct  backuppb.CompressionType
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
	require.EqualError(t, err, "invalid compression type 'Other Compression (strings)': [BR:Common:ErrInvalidArgument]invalid argument")
	//c.Assert(err, ErrorMatches, "invalid compression.*")
	require.Equal(t, 0, int(ct))
}
