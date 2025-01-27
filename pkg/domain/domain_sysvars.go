// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package domain

import (
	"context"
	"strconv"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/circuitbreaker"
)

// initDomainSysVars() is called when a domain is initialized.
// These are special system variables which require the current domain.
// They can not be SetGlobal functions in sessionctx/variable directly
// because the domain is not available. Instead a noop func is specified,
// which is overwritten here.
func (do *Domain) initDomainSysVars() {
	setStatsCacheCapacityFunc := do.setStatsCacheCapacity
	variable.SetStatsCacheCapacity.Store(&setStatsCacheCapacityFunc)
	pdClientDynamicOptionFunc := do.setPDClientDynamicOption
	variable.SetPDClientDynamicOption.Store(&pdClientDynamicOptionFunc)

	variable.SetExternalTimestamp = do.setExternalTimestamp
	variable.GetExternalTimestamp = do.getExternalTimestamp

	setGlobalResourceControlFunc := do.setGlobalResourceControl
	variable.SetGlobalResourceControl.Store(&setGlobalResourceControlFunc)
	variable.SetLowResolutionTSOUpdateInterval = do.setLowResolutionTSOUpdateInterval

	variable.ChangeSchemaCacheSize = do.changeSchemaCacheSize

	variable.ChangePDMetadataCircuitBreakerErrorRateThresholdPct = changePDMetadataCircuitBreakerErrorRateThresholdPct
}

// setStatsCacheCapacity sets statsCache cap
func (do *Domain) setStatsCacheCapacity(c int64) {
	statsHandle := do.StatsHandle()
	if statsHandle == nil { // from test
		return
	}
	do.StatsHandle().SetStatsCacheCapacity(c)
}

func (do *Domain) setPDClientDynamicOption(name, sVal string) error {
	switch name {
	case vardef.TiDBTSOClientBatchMaxWaitTime:
		val, err := strconv.ParseFloat(sVal, 64)
		if err != nil {
			return err
		}
		err = do.updatePDClient(opt.MaxTSOBatchWaitInterval, time.Duration(float64(time.Millisecond)*val))
		if err != nil {
			return err
		}
		vardef.MaxTSOBatchWaitInterval.Store(val)
	case vardef.TiDBEnableTSOFollowerProxy:
		val := variable.TiDBOptOn(sVal)
		err := do.updatePDClient(opt.EnableTSOFollowerProxy, val)
		if err != nil {
			return err
		}
		vardef.EnableTSOFollowerProxy.Store(val)
	case vardef.PDEnableFollowerHandleRegion:
		val := variable.TiDBOptOn(sVal)
		// Note: EnableFollowerHandle is only used for region API now.
		// If pd support more APIs in follower, the pd option may be changed.
		err := do.updatePDClient(opt.EnableFollowerHandle, val)
		if err != nil {
			return err
		}
		vardef.EnablePDFollowerHandleRegion.Store(val)
	case vardef.TiDBTSOClientRPCMode:
		var concurrency int

		switch sVal {
		case vardef.TSOClientRPCModeDefault:
			concurrency = 1
		case vardef.TSOClientRPCModeParallel:
			concurrency = 2
		case vardef.TSOClientRPCModeParallelFast:
			concurrency = 4
		default:
			return variable.ErrWrongValueForVar.GenWithStackByArgs(name, sVal)
		}

		err := do.updatePDClient(opt.TSOClientRPCConcurrency, concurrency)
		if err != nil {
			return err
		}
	}
	return nil
}

func (*Domain) setGlobalResourceControl(enable bool) {
	if enable {
		variable.EnableGlobalResourceControlFunc()
	} else {
		variable.DisableGlobalResourceControlFunc()
	}
}

func (do *Domain) setLowResolutionTSOUpdateInterval(interval time.Duration) error {
	return do.store.GetOracle().SetLowResolutionTimestampUpdateInterval(interval)
}

// updatePDClient is used to set the dynamic option into the PD client.
func (do *Domain) updatePDClient(option opt.DynamicOption, val any) error {
	store, ok := do.store.(interface{ GetPDClient() pd.Client })
	if !ok {
		return nil
	}
	pdClient := store.GetPDClient()
	if pdClient == nil {
		return nil
	}
	return pdClient.UpdateOption(option, val)
}

func (do *Domain) setExternalTimestamp(ctx context.Context, ts uint64) error {
	return do.store.GetOracle().SetExternalTimestamp(ctx, ts)
}

func (do *Domain) getExternalTimestamp(ctx context.Context) (uint64, error) {
	return do.store.GetOracle().GetExternalTimestamp(ctx)
}

func (do *Domain) changeSchemaCacheSize(ctx context.Context, size uint64) error {
	err := kv.RunInNewTxn(kv.WithInternalSourceType(ctx, kv.InternalTxnDDL), do.store, true, func(_ context.Context, txn kv.Transaction) error {
		t := meta.NewMutator(txn)
		return t.SetSchemaCacheSize(size)
	})
	if err != nil {
		return err
	}
	do.infoCache.Data.SetCacheCapacity(size)
	return nil
}

func changePDMetadataCircuitBreakerErrorRateThresholdPct(errorRatePct uint32) {
	tikv.ChangePDRegionMetaCircuitBreakerSettings(func(config *circuitbreaker.Settings) {
		config.ErrorRateThresholdPct = errorRatePct
	})
}
