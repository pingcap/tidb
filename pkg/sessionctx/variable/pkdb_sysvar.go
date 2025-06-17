//go:build fusion
// +build fusion

package variable

import (
	"context"

	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/tikvrpc"
	"go.uber.org/zap"
)

// FFI-dependent system variables
var ffiSysVars = []*SysVar{
	{Scope: ScopeGlobal, Name: TiDBXEnableTiKVLocalCall, Value: BoolToOnOff(DefTiDBXEnableLocalRPCOpt), Type: TypeBool,
		SetGlobal: func(_ context.Context, vars *SessionVars, s string) error {
			if TiDBOptOn(s) != tikvrpc.EnableTiKVLocalCall.Load() {
				tikvrpc.EnableTiKVLocalCall.Store(TiDBOptOn(s))
				logutil.BgLogger().Info("set enable local rpc opt", zap.Bool("enable", TiDBOptOn(s)))
			}
			return nil
		}, GetGlobal: func(_ context.Context, vars *SessionVars) (string, error) {
			return BoolToOnOff(tikvrpc.EnableTiKVLocalCall.Load()), nil
		}},
	{Scope: ScopeGlobal, Name: TiDBXEnableScheduleLeaderRule, Value: BoolToOnOff(DefTiDBXEnableScheduleLeaderRule), Type: TypeBool,
		SetGlobal: func(_ context.Context, vars *SessionVars, s string) error {
			v := TiDBOptOn(s)
			if v != EnableScheduleLeaderRule.Load() {
				EnableScheduleLeaderRule.Store(v)
				if EnableScheduleLeaderRuleFn != nil {
					EnableScheduleLeaderRuleFn(v)
				}
			}
			return nil
		}, GetGlobal: func(_ context.Context, vars *SessionVars) (string, error) {
			return BoolToOnOff(EnableScheduleLeaderRule.Load()), nil
		}},
}

func init() {
	defaultSysVars = append(defaultSysVars, ffiSysVars...)
}
