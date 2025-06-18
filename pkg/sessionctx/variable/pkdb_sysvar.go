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
	{Scope: ScopeGlobal, Name: TiDBXEnablePDFusion, Value: BoolToOnOff(DefTiDBXEnableLocalRPCOpt), Type: TypeBool,
		SetGlobal: func(_ context.Context, _ *SessionVars, s string) error {
			if PDFusionVar == nil {
				return errors.Errorf("%s is not initialized. Please check if this is fusion mode", TiDBXEnablePDFusion)
			}
			if TiDBOptOn(s) != (*PDFusionVar).Load() {
				(*PDFusionVar).Store(TiDBOptOn(s))
				logutil.BgLogger().Info("set enable local rpc opt",
					zap.String("variable", TiDBXEnablePDFusion),
					zap.Bool("enable", TiDBOptOn(s)))
			}
			return nil
		}, GetGlobal: func(context.Context, *SessionVars) (string, error) {
		if PDFusionVar == nil {
			return "", errors.Errorf("%s is not initialized. Please check if this is fusion mode", TiDBXEnablePDFusion)
		}
		return BoolToOnOff((*PDFusionVar).Load()), nil
	}},
}

// PDFusionVar will be set by the upper package tidbx-server to point to pd-server's
// EnableIPC. This is to break the dependency cycle between tidbx-server and
// pd-server.
var PDFusionVar *atomic.Bool

func init() {
	defaultSysVars = append(defaultSysVars, ffiSysVars...)
}
