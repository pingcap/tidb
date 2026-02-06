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
	{Scope: ScopeGlobal, Name: TiDBXShmLocalCall, Value: BoolToOnOff(DefTiDBXShmLocalCallOpt), Type: TypeBool,
		SetGlobal: func(_ context.Context, _ *SessionVars, s string) error {
			if TiDBOptOn(s) != tikvrpc.TiDBXShmLocalCall.Load() {
				tikvrpc.TiDBXShmLocalCall.Store(TiDBOptOn(s))
				logutil.BgLogger().Info("set enable shm_local_call opt",
					zap.String("variable", TiDBXShmLocalCall),
					zap.Bool("enable", TiDBOptOn(s)))
			}
			return nil
		}, GetGlobal: func(_ context.Context, _ *SessionVars) (string, error) {
			return BoolToOnOff(tikvrpc.TiDBXShmLocalCall.Load()), nil
		}},
	{Scope: ScopeGlobal, Name: TiDBXLocalCallNoMarshall, Value: BoolToOnOff(DefTiDBXLocalCallNoMarshallOpt), Type: TypeBool,
		SetGlobal: func(_ context.Context, _ *SessionVars, s string) error {
			if TiDBOptOn(s) != tikvrpc.TiDBXLocalCallNoMarshall.Load() {
				tikvrpc.TiDBXLocalCallNoMarshall.Store(TiDBOptOn(s))
				logutil.BgLogger().Info("set enable local_call_no_marshall opt",
					zap.String("variable", TiDBXLocalCallNoMarshall),
					zap.Bool("enable", TiDBOptOn(s)))
			}
			return nil
		}, GetGlobal: func(_ context.Context, _ *SessionVars) (string, error) {
			return BoolToOnOff(tikvrpc.TiDBXLocalCallNoMarshall.Load()), nil
		}},
	{Scope: ScopeGlobal, Name: TiDBXStoreBatchGet, Value: BoolToOnOff(DefTiDBXStoreBatchGetOpt), Type: TypeBool,
		SetGlobal: func(_ context.Context, _ *SessionVars, s string) error {
			if TiDBOptOn(s) != tikvrpc.TiDBXStoreBatchGet.Load() {
				tikvrpc.TiDBXStoreBatchGet.Store(TiDBOptOn(s))
				logutil.BgLogger().Info("set enable local rpc with store_batch_get opt",
					zap.String("variable", TiDBXStoreBatchGet),
					zap.Bool("enable", TiDBOptOn(s)))
			}
			return nil
		}, GetGlobal: func(_ context.Context, _ *SessionVars) (string, error) {
			return BoolToOnOff(tikvrpc.TiDBXStoreBatchGet.Load()), nil
		}},

	{Scope: ScopeGlobal, Name: TiDBXEnableFastPath, Value: BoolToOnOff(DefTiDBXFastPath), Type: TypeBool,
		SetGlobal: func(ctx context.Context, vars *SessionVars, val string) error {
			EnableFastPath.Store(TiDBOptOn(val))
			tikvrpc.EnableFastPath.Store(TiDBOptOn(val))
			return nil
		},
		GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
			return BoolToOnOff(EnableFastPath.Load()), nil
		},
	},
}

// PDLocalCallVar will be set by the upper package tidbx-server to point to pd-server's
// EnableIPC. This is to break the dependency cycle between tidbx-server and
// pd-server.
var PDLocalCallVar *atomic.Bool

func init() {
	defaultSysVars = append(defaultSysVars, ffiSysVars...)
}
