// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNextPowerOfTwo(t *testing.T) {
	require.Equal(t, int64(1), NextPowerOfTwo(1))
	require.Equal(t, int64(4), NextPowerOfTwo(3))
	require.Equal(t, int64(256), NextPowerOfTwo(255))
	require.Equal(t, int64(1024), NextPowerOfTwo(1024))
	require.Equal(t, int64(0x100000000), NextPowerOfTwo(0xabcd1234))
}
