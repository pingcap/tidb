// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package build

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInfo(t *testing.T) {
	info := Info()
	lines := strings.Split(info, "\n")
	require.Regexp(t, "^Release Version", lines[0])
	require.Regexp(t, "^Git Commit Hash", lines[1])
	require.Regexp(t, "^Git Branch", lines[2])
	require.Regexp(t, "^Go Version", lines[3])
	require.Regexp(t, "^UTC Build Time", lines[4])
}

func TestLogInfo(t *testing.T) {
	LogInfo(BR)
	LogInfo(Lightning)
}
