//go:build fusion
// +build fusion

package variable

import (
	"context"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/tikvrpc"
	"go.uber.org/zap"
)

// FFI-dependent system variables
var ffiSysVars = []*SysVar{
	{Scope: ScopeGlobal, Name: TiDBXEnableTiKVLocalCall, Value: BoolToOnOff(DefTiDBXEnableLocalRPCOpt), Type: TypeBool,
		SetGlobal: func(_ context.Context, _ *SessionVars, s string) error {
			if TiDBOptOn(s) != tikvrpc.EnableTiKVLocalCall.Load() {
				tikvrpc.EnableTiKVLocalCall.Store(TiDBOptOn(s))
				logutil.BgLogger().Info("set enable local rpc opt",
					zap.String("variable", TiDBXEnableTiKVLocalCall),
					zap.Bool("enable", TiDBOptOn(s)))
			}
			return nil
		}, GetGlobal: func(_ context.Context, _ *SessionVars) (string, error) {
			return BoolToOnOff(tikvrpc.EnableTiKVLocalCall.Load()), nil
		}},
	{Scope: ScopeGlobal, Name: TiDBXEnableScheduleLeaderRule, Value: BoolToOnOff(DefTiDBXEnableScheduleLeaderRule), Type: TypeBool,
		SetGlobal: func(_ context.Context, _ *SessionVars, s string) error {
			v := TiDBOptOn(s)
			if v != EnableScheduleLeaderRule.Load() {
				EnableScheduleLeaderRule.Store(v)
				if EnableScheduleLeaderRuleFn != nil {
					EnableScheduleLeaderRuleFn(v)
				}
			}
			return nil
		}, GetGlobal: func(context.Context, *SessionVars) (string, error) {
			return BoolToOnOff(EnableScheduleLeaderRule.Load()), nil
		}},
	{Scope: ScopeGlobal, Name: TiDBXEnablePDLocalCall, Value: BoolToOnOff(DefTiDBXEnableLocalRPCOpt), Type: TypeBool,
		SetGlobal: func(_ context.Context, _ *SessionVars, s string) error {
			if PDLocalCallVar == nil {
				return errors.Errorf("%s is not initialized. Please check if this is fusion mode", TiDBXEnablePDLocalCall)
			}
			if TiDBOptOn(s) != (*PDLocalCallVar).Load() {
				(*PDLocalCallVar).Store(TiDBOptOn(s))
				logutil.BgLogger().Info("set enable local rpc opt",
					zap.String("variable", TiDBXEnablePDLocalCall),
					zap.Bool("enable", TiDBOptOn(s)))
			}
			return nil
		}, GetGlobal: func(context.Context, *SessionVars) (string, error) {
			if PDLocalCallVar == nil {
				return "", errors.Errorf("%s is not initialized. Please check if this is fusion mode", TiDBXEnablePDLocalCall)
			}
			return BoolToOnOff((*PDLocalCallVar).Load()), nil
		}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBCreateFromSelectUsingImport, Value: BoolToOnOff(vardef.DefTiDBCreateFromSelectUsingImport), Type: vardef.TypeBool,
		SetSession: func(s *SessionVars, val string) error {
			s.CreateFromSelectUsingImport = TiDBOptOn(val)
			return nil
		},
		IsHintUpdatableVerified: true,
	},
}

// PDLocalCallVar will be set by the upper package tidbx-server to point to pd-server's
// EnableIPC. This is to break the dependency cycle between tidbx-server and
// pd-server.
var PDLocalCallVar *atomic.Bool

func init() {
	defaultSysVars = append(defaultSysVars, ffiSysVars...)
}
