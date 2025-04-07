// Copyright 2017 PingCAP, Inc.
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

package variable

import (
	"context"
	"time"

	"go.uber.org/atomic"
)

var (
	// SetMemQuotaAnalyze is the func registered by global/subglobal tracker to set memory quota.
	SetMemQuotaAnalyze func(quota int64) = nil
	// GetMemQuotaAnalyze is the func registered by global/subglobal tracker to get memory quota.
	GetMemQuotaAnalyze func() int64 = nil
	// SetStatsCacheCapacity is the func registered by domain to set statsCache memory quota.
	SetStatsCacheCapacity atomic.Pointer[func(int64)]
	// SetPDClientDynamicOption is the func registered by domain
	SetPDClientDynamicOption atomic.Pointer[func(string, string) error]
	// SwitchMDL is the func registered by DDL to switch MDL.
	SwitchMDL func(bool2 bool) error = nil
	// EnableDDL is the func registered by ddl to enable running ddl in this instance.
	EnableDDL func() error = nil
	// DisableDDL is the func registered by ddl to disable running ddl in this instance.
	DisableDDL func() error = nil
	// SwitchFastCreateTable is the func registered by DDL to switch fast create table.
	SwitchFastCreateTable func(val bool) error
	// SetExternalTimestamp is the func registered by staleread to set externaltimestamp in pd
	SetExternalTimestamp func(ctx context.Context, ts uint64) error
	// GetExternalTimestamp is the func registered by staleread to get externaltimestamp from pd
	GetExternalTimestamp func(ctx context.Context) (uint64, error)
	// SetGlobalResourceControl is the func registered by domain to set cluster resource control.
	SetGlobalResourceControl atomic.Pointer[func(bool)]
	// ValidateCloudStorageURI validates the cloud storage URI.
	ValidateCloudStorageURI func(ctx context.Context, uri string) error
	// SetLowResolutionTSOUpdateInterval is the func registered by domain to set slow resolution tso update interval.
	SetLowResolutionTSOUpdateInterval func(interval time.Duration) error = nil
	// ChangeSchemaCacheSize is called when tidb_schema_cache_size is changed.
	ChangeSchemaCacheSize func(ctx context.Context, size uint64) error
	// EnableStatsOwner is the func registered by stats to enable running stats in this instance.
	EnableStatsOwner func() error = nil
	// DisableStatsOwner is the func registered by stats to disable running stats in this instance.
	DisableStatsOwner func() error = nil
	// ChangePDMetadataCircuitBreakerErrorRateThresholdPct changes the error rate threshold of the PD metadata circuit breaker.
	ChangePDMetadataCircuitBreakerErrorRateThresholdPct func(uint32) = nil
)

// Hooks functions for Cluster Resource Control.
var (
	// EnableGlobalResourceControlFunc is the function registered by tikv_driver to set cluster resource control.
	EnableGlobalResourceControlFunc = func() {}
	// DisableGlobalResourceControlFunc is the function registered by tikv_driver to unset cluster resource control.
	DisableGlobalResourceControlFunc = func() {}
)
