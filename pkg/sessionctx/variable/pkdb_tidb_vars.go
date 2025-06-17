package variable

import "go.uber.org/atomic"

// TiDB system variable names that only in session scope.
const (
	// TiDBXEnableScheduleLeaderRule indicates whether to enable region leader in one store.
	TiDBXEnableScheduleLeaderRule = "tidbx_enable_schedule_leader_rule"
)

// TiDB system variable names that both in session and global scope.
const (
	// TiDBXEnableTiKVLocalCall indicates whether to enable TiKV local calls.
	TiDBXEnableTiKVLocalCall = "tidbx_enable_tikv_local_call"
)

// Default TiDB system variable values.
const (
	DefTiDBXEnableLocalRPCOpt        = false
	DefTiDBXEnableScheduleLeaderRule = false
)

// Process global variables.
var (
	EnableScheduleLeaderRule                = atomic.NewBool(DefTiDBXEnableScheduleLeaderRule)
	EnableScheduleLeaderRuleFn func(v bool) = nil
)
