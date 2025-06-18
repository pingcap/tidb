package variable

import "go.uber.org/atomic"

const (
	// TiDBXEnableScheduleLeaderRule indicates whether to enable region leader in one store.
	TiDBXEnableScheduleLeaderRule = "tidbx_enable_schedule_leader_rule"
	// TiDBXEnableTiKVLocalCall indicates whether to enable TiKV local calls.
	TiDBXEnableTiKVLocalCall = "tidbx_enable_tikv_local_call"
	// TiDBXEnablePDFusion indicates whether to use Inter-Process Call for PD.
	TiDBXEnablePDFusion = "tidbx_enable_pd_fusion"
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
