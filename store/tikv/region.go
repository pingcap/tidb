package tikv

import (
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/store/tikv/client"
	"github.com/pingcap/tidb/store/tikv/region"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	pd "github.com/tikv/pd/client"
)

// RPCContext contains data that is needed to send RPC to a region.
type RPCContext = region.RPCContext

// RPCCanceller is rpc send cancelFunc collector.
type RPCCanceller = region.RPCCanceller

// RegionVerID is a unique ID that can identify a Region at a specific version.
type RegionVerID = region.VerID

// RegionCache caches Regions loaded from PD.
type RegionCache = region.Cache

// KeyLocation is the region and range that a key is located.
type KeyLocation = region.KeyLocation

// RPCCancellerCtxKey is context key attach rpc send cancelFunc collector to ctx.
type RPCCancellerCtxKey = region.RPCCancellerCtxKey

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
type RegionRequestSender = region.RequestSender

// StoreSelectorOption configures storeSelectorOp.
type StoreSelectorOption = region.StoreSelectorOption

// RegionRequestRuntimeStats records the runtime stats of send region requests.
type RegionRequestRuntimeStats = region.RequestRuntimeStats

// RPCRuntimeStats indicates the RPC request count and consume time.
type RPCRuntimeStats = region.RPCRuntimeStats

// CodecPDClient wraps a PD Client to decode the encoded keys in region meta.
type CodecPDClient = region.CodecPDClient

// RecordRegionRequestRuntimeStats records request runtime stats.
func RecordRegionRequestRuntimeStats(stats map[tikvrpc.CmdType]*region.RPCRuntimeStats, cmd tikvrpc.CmdType, d time.Duration) {
	region.RecordRegionRequestRuntimeStats(stats, cmd, d)
}

// Store contains a kv process's address.
type Store = region.Store

// Region presents kv region
type Region = region.Region

// EpochNotMatch indicates it's invalidated due to epoch not match
const EpochNotMatch = region.EpochNotMatch

// NewRPCanceller creates RPCCanceller with init state.
func NewRPCanceller() *RPCCanceller {
	return region.NewRPCanceller()
}

// NewRegionVerID creates a region ver id, which used for invalidating regions.
func NewRegionVerID(id, confVer, ver uint64) RegionVerID {
	return region.NewRegionVerID(id, confVer, ver)
}

// GetStoreTypeByMeta gets store type by store meta pb.
func GetStoreTypeByMeta(store *metapb.Store) tikvrpc.EndpointType {
	return tikvrpc.GetStoreTypeByMeta(store)
}

// NewRegionRequestSender creates a new sender.
func NewRegionRequestSender(regionCache *RegionCache, client client.Client) *RegionRequestSender {
	return region.NewRegionRequestSender(regionCache, client)
}

// LoadShuttingDown atomically loads ShuttingDown.
func LoadShuttingDown() uint32 {
	return region.LoadShuttingDown()
}

// StoreShuttingDown atomically stores ShuttingDown into v.
func StoreShuttingDown(v uint32) {
	region.StoreShuttingDown(v)
}

// WithMatchLabels indicates selecting stores with matched labels
func WithMatchLabels(labels []*metapb.StoreLabel) StoreSelectorOption {
	return region.WithMatchLabels(labels)
}

// NewRegionRequestRuntimeStats returns a new RegionRequestRuntimeStats.
func NewRegionRequestRuntimeStats() RegionRequestRuntimeStats {
	return region.NewRegionRequestRuntimeStats()
}

// SetRegionCacheTTLSec sets regionCacheTTLSec to t.
func SetRegionCacheTTLSec(t int64) {
	region.SetRegionCacheTTLSec(t)
}

// SetStoreLivenessTimeout sets storeLivenessTimeout to t.
func SetStoreLivenessTimeout(t time.Duration) {
	region.SetStoreLivenessTimeout(t)
}

// NewRegionCache creates a RegionCache.
func NewRegionCache(pdClient pd.Client) *region.Cache {
	return region.NewRegionCache(pdClient)
}
