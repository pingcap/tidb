// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/store/tikv/client"
	"github.com/pingcap/tidb/store/tikv/locate"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	pd "github.com/tikv/pd/client"
)

// RPCContext contains data that is needed to send RPC to a region.
type RPCContext = locate.RPCContext

// RPCCanceller is rpc send cancelFunc collector.
type RPCCanceller = locate.RPCCanceller

// RegionVerID is a unique ID that can identify a Region at a specific version.
type RegionVerID = locate.RegionVerID

// RegionCache caches Regions loaded from PD.
type RegionCache = locate.RegionCache

// KeyLocation is the region and range that a key is located.
type KeyLocation = locate.KeyLocation

// RPCCancellerCtxKey is context key attach rpc send cancelFunc collector to ctx.
type RPCCancellerCtxKey = locate.RPCCancellerCtxKey

// RegionRequestSender sends KV/Cop requests to tikv server. It handles network
// errors and some region errors internally.
//
// Typically, a KV/Cop request is bind to a region, all keys that are involved
// in the request should be located in the region.
// The sending process begins with looking for the address of leader store's
// address of the target region from cache, and the request is then sent to the
// destination tikv server over TCP connection.
// If region is updated, can be caused by leader transfer, region split, region
// merge, or region balance, tikv server may not able to process request and
// send back a RegionError.
// RegionRequestSender takes care of errors that does not relevant to region
// range, such as 'I/O timeout', 'NotLeader', and 'ServerIsBusy'. For other
// errors, since region range have changed, the request may need to split, so we
// simply return the error to caller.
type RegionRequestSender = locate.RegionRequestSender

// StoreSelectorOption configures storeSelectorOp.
type StoreSelectorOption = locate.StoreSelectorOption

// RegionRequestRuntimeStats records the runtime stats of send region requests.
type RegionRequestRuntimeStats = locate.RegionRequestRuntimeStats

// RPCRuntimeStats indicates the RPC request count and consume time.
type RPCRuntimeStats = locate.RPCRuntimeStats

// CodecPDClient wraps a PD Client to decode the encoded keys in region meta.
type CodecPDClient = locate.CodecPDClient

// RecordRegionRequestRuntimeStats records request runtime stats.
func RecordRegionRequestRuntimeStats(stats map[tikvrpc.CmdType]*locate.RPCRuntimeStats, cmd tikvrpc.CmdType, d time.Duration) {
	locate.RecordRegionRequestRuntimeStats(stats, cmd, d)
}

// Store contains a kv process's address.
type Store = locate.Store

// Region presents kv region
type Region = locate.Region

// EpochNotMatch indicates it's invalidated due to epoch not match
const EpochNotMatch = locate.EpochNotMatch

// NewRPCanceller creates RPCCanceller with init state.
func NewRPCanceller() *RPCCanceller {
	return locate.NewRPCanceller()
}

// NewRegionVerID creates a region ver id, which used for invalidating regions.
func NewRegionVerID(id, confVer, ver uint64) RegionVerID {
	return locate.NewRegionVerID(id, confVer, ver)
}

// GetStoreTypeByMeta gets store type by store meta pb.
func GetStoreTypeByMeta(store *metapb.Store) tikvrpc.EndpointType {
	return tikvrpc.GetStoreTypeByMeta(store)
}

// NewRegionRequestSender creates a new sender.
func NewRegionRequestSender(regionCache *RegionCache, client client.Client) *RegionRequestSender {
	return locate.NewRegionRequestSender(regionCache, client)
}

// LoadShuttingDown atomically loads ShuttingDown.
func LoadShuttingDown() uint32 {
	return locate.LoadShuttingDown()
}

// StoreShuttingDown atomically stores ShuttingDown into v.
func StoreShuttingDown(v uint32) {
	locate.StoreShuttingDown(v)
}

// WithMatchLabels indicates selecting stores with matched labels
func WithMatchLabels(labels []*metapb.StoreLabel) StoreSelectorOption {
	return locate.WithMatchLabels(labels)
}

// NewRegionRequestRuntimeStats returns a new RegionRequestRuntimeStats.
func NewRegionRequestRuntimeStats() RegionRequestRuntimeStats {
	return locate.NewRegionRequestRuntimeStats()
}

// SetRegionCacheTTLSec sets regionCacheTTLSec to t.
func SetRegionCacheTTLSec(t int64) {
	locate.SetRegionCacheTTLSec(t)
}

// SetStoreLivenessTimeout sets storeLivenessTimeout to t.
func SetStoreLivenessTimeout(t time.Duration) {
	locate.SetStoreLivenessTimeout(t)
}

// NewRegionCache creates a RegionCache.
func NewRegionCache(pdClient pd.Client) *locate.RegionCache {
	return locate.NewRegionCache(pdClient)
}
