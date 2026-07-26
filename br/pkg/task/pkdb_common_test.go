// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
)

func TestTaskStatusConfig(t *testing.T) {
	fs := pflag.NewFlagSet("dummy", pflag.ContinueOnError)
	DefineCommonFlags(fs)
	require.NoError(t, fs.Set(flagStatusTextfileDir, "/tmp/br-status"))
	cfg := Config{}
	require.NoError(t, cfg.ParseFromFlags(fs))
	require.Equal(t, "/tmp/br-status", cfg.StatusTextfileDir)
}

func TestTaskStatusCommand(t *testing.T) {
	cases := []struct {
		cmdName string
		want    string
	}{
		{FullBackupCmd, "full"},
		{DBBackupCmd, "db"},
		{TableBackupCmd, "table"},
		{FullRestoreCmd, "full"},
		{DBRestoreCmd, "db"},
		{TableRestoreCmd, "table"},
		{"Raw Backup", "raw_backup"},
	}
	for _, tc := range cases {
		require.Equal(t, tc.want, taskStatusCommand(tc.cmdName))
	}
}

func TestTaskStatusScope(t *testing.T) {
	cases := []struct {
		command        string
		explicitFilter bool
		filter         []string
		want           string
	}{
		{"full", false, []string{"*.*"}, "cluster"},
		{"full", false, []string{"*.*", "!mysql.*"}, "cluster"},
		{"full", true, []string{"*.*"}, "cluster"},
		{"full", true, []string{"*.*", "!mysql.*"}, "filter=*.*,!mysql.*"},
		{"db", false, []string{"`test`.*"}, "db=`test`"},
		{"table", false, []string{"`test`.`t1`"}, "table=`test`.`t1`"},
		{"unknown", false, []string{"*.*"}, "unknown"},
		{"full", false, nil, "unknown"},
	}
	for _, tc := range cases {
		require.Equal(t, tc.want, taskStatusScope(tc.command, tc.explicitFilter, tc.filter))
	}
}
