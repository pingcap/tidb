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

	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/logutil"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

// initDomainSysVars() is called when a domain is initialized.
// These are special system variables which require the current domain.
// They can not be SetGlobal functions in sessionctx/variable directly
// because the domain is not available. Instead a noop func is specified,
// which is overwritten here.
func (do *Domain) initDomainSysVars() {
	variable.SetStatsCacheCapacity.Store(do.setStatsCacheCapacity)
	pdClientDynamicOptionFunc := do.setPDClientDynamicOption
	variable.SetPDClientDynamicOption.Store(&pdClientDynamicOptionFunc)

	variable.SetExternalTimestamp = do.setExternalTimestamp
	variable.GetExternalTimestamp = do.getExternalTimestamp

	setGlobalResourceControlFunc := do.setGlobalResourceControl
	variable.SetGlobalResourceControl.Store(&setGlobalResourceControlFunc)
}

// setStatsCacheCapacity sets statsCache cap
func (do *Domain) setStatsCacheCapacity(c int64) {
	do.StatsHandle().SetStatsCacheCapacity(c)
	logutil.BgLogger().Info("update stats cache capacity successfully", zap.Int64("capacity", c))
}

func (do *Domain) setPDClientDynamicOption(name, sVal string) {
	switch name {
	case variable.TiDBTSOClientBatchMaxWaitTime:
		val, err := strconv.ParseFloat(sVal, 64)
		if err != nil {
			break
		}
		err = do.updatePDClient(pd.MaxTSOBatchWaitInterval, time.Duration(float64(time.Millisecond)*val))
		if err != nil {
			break
		}
		variable.MaxTSOBatchWaitInterval.Store(val)
	case variable.TiDBEnableTSOFollowerProxy:
		val := variable.TiDBOptOn(sVal)
		err := do.updatePDClient(pd.EnableTSOFollowerProxy, val)
		if err != nil {
			break
		}
		variable.EnableTSOFollowerProxy.Store(val)
	}
}

func (do *Domain) setGlobalResourceControl(enable bool) {
	if enable {
		variable.EnableGlobalResourceControlFunc()
	} else {
		variable.DisableGlobalResourceControlFunc()
	}
}

// updatePDClient is used to set the dynamic option into the PD client.
func (do *Domain) updatePDClient(option pd.DynamicOption, val interface{}) error {
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
