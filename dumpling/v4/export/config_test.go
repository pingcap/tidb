// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"testing"

	tcontext "github.com/pingcap/dumpling/v4/context"
	"github.com/stretchr/testify/require"
)

func TestCreateExternalStorage(t *testing.T) {
	t.Parallel()
	mockConfig := defaultConfigForTest(t)
	loc, err := mockConfig.createExternalStorage(tcontext.Background())
	require.NoError(t, err)
	require.Regexp(t, "file:.*", loc.URI())
}

func TestMatchMysqlBugVersion(t *testing.T) {
	t.Parallel()
	cases := []struct {
		serverInfo ServerInfo
		expected   bool
	}{
		{ParseServerInfo(tcontext.Background(), "5.7.25-TiDB-3.0.6"), false},
		{ParseServerInfo(tcontext.Background(), "8.0.2"), false},
		{ParseServerInfo(tcontext.Background(), "8.0.3"), true},
		{ParseServerInfo(tcontext.Background(), "8.0.22"), true},
		{ParseServerInfo(tcontext.Background(), "8.0.23"), false},
	}
	for _, x := range cases {
		require.Equalf(t, x.expected, matchMysqlBugversion(x.serverInfo), "server info: %s", x.serverInfo)
	}
}
