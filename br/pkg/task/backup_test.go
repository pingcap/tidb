// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"testing"
	"time"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/stretchr/testify/require"
)

func TestParseTSString(t *testing.T) {
	t.Parallel()
	var (
		ts  uint64
		err error
	)

	ts, err = parseTSString("")
	require.NoError(t, err)
	require.Equal(t, int(ts), 0)

	ts, err = parseTSString("400036290571534337")
	require.NoError(t, err)
	require.Equal(t, int(ts), 400036290571534337)

	_, offset := time.Now().Local().Zone()
	ts, err = parseTSString("2018-05-11 01:42:23")
	require.NoError(t, err)
	require.Equal(t, int(ts), 400032515489792000-(offset*1000)<<18)
}

func TestParseCompressionType(t *testing.T) {
	t.Parallel()
	cases := []struct {
		str      string
		algo     backuppb.CompressionType
		errMatch func(error)
	}{
		{"lz4", backuppb.CompressionType_LZ4, nil},
		{"snappy", backuppb.CompressionType_SNAPPY, nil},
		{"zstd", backuppb.CompressionType_ZSTD, nil},
		{"Other Compression (strings)", backuppb.CompressionType_UNKNOWN, func(e error) {
			require.Error(t, e)
			require.Regexp(t, "invalid compression.*", e.Error())
		}},
	}

	for _, c := range cases {
		ct, err := parseCompressionType(c.str)
		if c.errMatch == nil {
			require.NoError(t, err)
		} else {
			c.errMatch(err)
		}
		require.Equal(t, c.algo, ct)
	}
}
