package core

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/stretchr/testify/require"
)

func TestGetLogReplOptsMap(t *testing.T) {
	ret, err := getLogReplOptsMap([]*ast.LogReplicationOpt{})
	require.NoError(t, err)
	require.Len(t, ret, 0)

	ret, err = getLogReplOptsMap([]*ast.LogReplicationOpt{
		{
			Tp:    ast.LogReplicationOptSourceHost,
			Value: "host",
		},
		{
			Tp:        ast.LogReplicationOptSourcePort,
			UintValue: 1234,
		},
	})
	require.NoError(t, err)
	require.Len(t, ret, 2)

	ret, err = getLogReplOptsMap([]*ast.LogReplicationOpt{
		{
			Tp:    ast.LogReplicationOptSourceHost,
			Value: "host",
		},
		{
			Tp:    ast.LogReplicationOptSourceHost,
			Value: "host-2",
		},
	})
	require.ErrorContains(t, err, "duplicate option type: SOURCE_HOST")
}

func TestIllegalAlterLogRepl(t *testing.T) {
	b := &PlanBuilder{}
	ctx := context.Background()

	sourceID := uint64(1234)

	cases := []struct {
		name   string
		a      *ast.AlterLogReplication
		errMsg string
	}{
		{
			name: "change-source-with-options",
			a: &ast.AlterLogReplication{
				Name:               model.NewCIStr("repl"),
				NewSourceClusterID: &sourceID,
				Opts: []*ast.LogReplicationOpt{
					{Tp: ast.LogReplicationOptProtectionMode, UintValue: uint64(ast.ProtectionModeMaximumProtection)},
				},
			},
			errMsg: "ALTER LOG REPLICATION CHANGE SOURCE cannot be combined with option list",
		},
		{
			name: "unsupported-option",
			a: &ast.AlterLogReplication{
				Name: model.NewCIStr("repl"),
				Opts: []*ast.LogReplicationOpt{
					{Tp: ast.LogReplicationOptSourceHost, Value: "host"},
				},
			},
			errMsg: "SOURCE_HOST is not supported in ALTER LOG REPLICATION",
		},
		{
			name: "degrade-timeout-without-max-availability",
			a: &ast.AlterLogReplication{
				Name: model.NewCIStr("repl"),
				Opts: []*ast.LogReplicationOpt{
					{Tp: ast.LogReplicationOptDegradeTimeout, Value: "10s"},
				},
			},
			errMsg: "DEGRADE_TIMEOUT is only allowed when PROTECTION_MODE is MAXIMUM_AVAILABILITY",
		},
		{
			name: "max-availability-without-degrade-timeout",
			a: &ast.AlterLogReplication{
				Name: model.NewCIStr("repl"),
				Opts: []*ast.LogReplicationOpt{
					{Tp: ast.LogReplicationOptProtectionMode, UintValue: uint64(ast.ProtectionModeMaximumAvailability)},
				},
			},
			errMsg: "DEGRADE_TIMEOUT is required when PROTECTION_MODE is MAXIMUM_AVAILABILITY",
		},
		{
			name: "invalid-degrade-timeout-value",
			a: &ast.AlterLogReplication{
				Name: model.NewCIStr("repl"),
				Opts: []*ast.LogReplicationOpt{
					{Tp: ast.LogReplicationOptProtectionMode, UintValue: uint64(ast.ProtectionModeMaximumAvailability)},
					{Tp: ast.LogReplicationOptDegradeTimeout, Value: "bad-duration"},
				},
			},
			errMsg: "invalid DEGRADE_TIMEOUT value: bad-duration",
		},
		{
			name: "non-positive-degrade-timeout",
			a: &ast.AlterLogReplication{
				Name: model.NewCIStr("repl"),
				Opts: []*ast.LogReplicationOpt{
					{Tp: ast.LogReplicationOptProtectionMode, UintValue: uint64(ast.ProtectionModeMaximumAvailability)},
					{Tp: ast.LogReplicationOptDegradeTimeout, Value: "0s"},
				},
			},
			errMsg: "DEGRADE_TIMEOUT must be greater than 0",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, err := b.buildAdminAlterLogReplication(ctx, tc.a)
			require.Nil(t, p)
			require.ErrorContains(t, err, tc.errMsg)
		})
	}
}

func TestParseDegradeTimeoutOption(t *testing.T) {
	cases := []struct {
		name           string
		mode           ast.ProtectionMode
		optsMap        map[ast.LogReplicationOptType]*ast.LogReplicationOpt
		wantTimeoutSec uint64
		errMsg         string
	}{
		{
			name:    "no-timeout-no-max-availability",
			mode:    ast.ProtectionModeMaximumProtection,
			optsMap: nil,
		},
		{
			name:    "no-timeout-but-max-availability",
			mode:    ast.ProtectionModeMaximumAvailability,
			optsMap: nil,
			errMsg:  "DEGRADE_TIMEOUT is required when PROTECTION_MODE is MAXIMUM_AVAILABILITY",
		},
		{
			name: "timeout-without-max-availability",
			mode: ast.ProtectionModeMaximumProtection,
			optsMap: map[ast.LogReplicationOptType]*ast.LogReplicationOpt{
				ast.LogReplicationOptDegradeTimeout: {Tp: ast.LogReplicationOptDegradeTimeout, Value: "10s"},
			},
			errMsg: "DEGRADE_TIMEOUT is only allowed when PROTECTION_MODE is MAXIMUM_AVAILABILITY",
		},
		{
			name: "invalid-duration-missing-unit",
			mode: ast.ProtectionModeMaximumAvailability,
			optsMap: map[ast.LogReplicationOptType]*ast.LogReplicationOpt{
				ast.LogReplicationOptDegradeTimeout: {Tp: ast.LogReplicationOptDegradeTimeout, Value: "10"},
			},
			errMsg: "invalid DEGRADE_TIMEOUT value: 10",
		},
		{
			name: "invalid-duration-bad-string",
			mode: ast.ProtectionModeMaximumAvailability,
			optsMap: map[ast.LogReplicationOptType]*ast.LogReplicationOpt{
				ast.LogReplicationOptDegradeTimeout: {Tp: ast.LogReplicationOptDegradeTimeout, Value: "bad-duration"},
			},
			errMsg: "invalid DEGRADE_TIMEOUT value: bad-duration",
		},
		{
			name: "non-positive-duration-zero",
			mode: ast.ProtectionModeMaximumAvailability,
			optsMap: map[ast.LogReplicationOptType]*ast.LogReplicationOpt{
				ast.LogReplicationOptDegradeTimeout: {Tp: ast.LogReplicationOptDegradeTimeout, Value: "0s"},
			},
			errMsg: "DEGRADE_TIMEOUT must be greater than 0",
		},
		{
			name: "non-positive-duration-negative",
			mode: ast.ProtectionModeMaximumAvailability,
			optsMap: map[ast.LogReplicationOptType]*ast.LogReplicationOpt{
				ast.LogReplicationOptDegradeTimeout: {Tp: ast.LogReplicationOptDegradeTimeout, Value: "-1s"},
			},
			errMsg: "DEGRADE_TIMEOUT must be greater than 0",
		},
		{
			name: "positive-duration-less-than-one-second",
			mode: ast.ProtectionModeMaximumAvailability,
			optsMap: map[ast.LogReplicationOptType]*ast.LogReplicationOpt{
				ast.LogReplicationOptDegradeTimeout: {Tp: ast.LogReplicationOptDegradeTimeout, Value: "500ms"},
			},
			errMsg: "the minimal value of DEGRADE_TIMEOUT is 1 second, got 500ms",
		},
		{
			name: "valid-duration-mixed-units",
			mode: ast.ProtectionModeMaximumAvailability,
			optsMap: map[ast.LogReplicationOptType]*ast.LogReplicationOpt{
				ast.LogReplicationOptDegradeTimeout: {Tp: ast.LogReplicationOptDegradeTimeout, Value: "1m30s"},
			},
			wantTimeoutSec: 90,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var timeoutSec uint64
			err := parseDegradeTimeoutOption(&timeoutSec, tc.mode, tc.optsMap)
			if tc.errMsg == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.errMsg)
			}
			if tc.wantTimeoutSec > 0 {
				require.Equal(t, tc.wantTimeoutSec, timeoutSec)
			}
		})
	}
}
