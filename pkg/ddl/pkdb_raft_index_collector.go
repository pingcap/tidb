package ddl

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/kvproto/pkg/debugpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/redact"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

const (
	pkdbDefaultScanPageSize         = 256
	pkdbDefaultReadStoreConcurrency = 16
	pkdbDefaultWritePDConcurrency   = 16
	pkdbDefaultRetryWait            = time.Second
)

type pkdbErrEmptyStoreAddr struct {
	storeID uint64
}

func (e *pkdbErrEmptyStoreAddr) Error() string {
	return fmt.Sprintf("store address is empty, store id: %d", e.storeID)
}

type pkdbErrRegionEpochMismatch struct {
	regionID uint64

	storeAddr string

	expected *metapb.RegionEpoch
	actual   *metapb.RegionEpoch
}

func (e *pkdbErrRegionEpochMismatch) Error() string {
	var (
		expectedConfVer uint64
		expectedVersion uint64
		actualConfVer   uint64
		actualVersion   uint64
	)
	if e.expected != nil {
		expectedConfVer = e.expected.ConfVer
		expectedVersion = e.expected.Version
	}
	if e.actual != nil {
		actualConfVer = e.actual.ConfVer
		actualVersion = e.actual.Version
	}
	return fmt.Sprintf("region epoch mismatch: region_id=%d store=%s expected(conf_ver=%d version=%d) actual(conf_ver=%d version=%d)",
		e.regionID, e.storeAddr,
		expectedConfVer, expectedVersion,
		actualConfVer, actualVersion,
	)
}

type pkdbErrMissingRegionMeta struct {
	regionID  uint64
	storeAddr string
}

func (e *pkdbErrMissingRegionMeta) Error() string {
	return fmt.Sprintf(
		"missing region meta: region_id=%d store=%s",
		e.regionID, e.storeAddr,
	)
}

type pkdbRaftIndexCollector struct {
	pdCli        pd.Client
	debugCliPool *pkdbDebugClientPool
}

func newPkdbRaftIndexCollector(pdCli pd.Client, tlsCfg *tls.Config) *pkdbRaftIndexCollector {
	transportCreds := grpc.WithTransportCredentials(insecure.NewCredentials())
	if tlsCfg != nil {
		transportCreds = grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg))
	}
	cfg := config.GetGlobalConfig()

	dialOpts := []grpc.DialOption{
		transportCreds,
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    time.Duration(cfg.TiKVClient.GrpcKeepAliveTime) * time.Second,
			Timeout: time.Duration(cfg.TiKVClient.GrpcKeepAliveTimeout) * time.Second,
		}),
	}
	return &pkdbRaftIndexCollector{
		pdCli:        pdCli,
		debugCliPool: newPkdbDebugClientPool(dialOpts),
	}
}

func (c *pkdbRaftIndexCollector) Close() {
	if c == nil {
		return
	}
	if c.debugCliPool != nil {
		c.debugCliPool.Close()
	}
}

type pkdbRaftIndexCollectOptions struct {
	startKey []byte

	pageSize int

	readStoreConcurrency int
	writePDConcurrency   int

	retryWait time.Duration
}

// pkdbOnResultFunc should internally retry for transient errors. The error
// returned from it means unretryable error (like context canceled by user).
type pkdbOnResultFunc func(context.Context, *debugpb.RegionInfoResponse) error

func (c *pkdbRaftIndexCollector) collectRaftLogIndexes(
	ctx context.Context,
	options pkdbRaftIndexCollectOptions,
	onResult pkdbOnResultFunc,
) error {
	if onResult == nil {
		return errors.New("onResult must not be nil")
	}
	if options.pageSize <= 0 {
		return errors.New("pageSize must be greater than 0")
	}
	if options.readStoreConcurrency <= 0 {
		return errors.New("readStoreConcurrency must be greater than 0")
	}
	if options.writePDConcurrency <= 0 {
		return errors.New("writePDConcurrency must be greater than 0")
	}

	for round := 0; ; round++ {
		logutil.SampleLogger().Info("pkdb raft index collector starting round",
			zap.Int("round", round),
			zap.String("startKey", redact.Key(options.startKey)),
		)

		retryStartKey, err := c.collectRound(ctx, options, onResult)
		if err == nil {
			return nil
		}
		if retryStartKey != nil {
			options.startKey = retryStartKey
			sleep(ctx, options.retryWait)
			continue
		}
		return err
	}
}

type pkdbFetchTask struct {
	region    pkdbScannedRegion
	storeAddr string
	// scanKey is the breakpoint key we should resume from if debug RPC detects changes.
	// It might be inside the region if regions merged.
	scanKey []byte
}

type pkdbScannedRegion struct {
	id            uint64
	startKey      []byte
	endKey        []byte
	epoch         *metapb.RegionEpoch
	leaderStoreID uint64
}

type pkdbFetchResult struct {
	scanKey  []byte
	response *debugpb.RegionInfoResponse
}

// collectRound scans once from options.startKey and returns one of:
//  1. (nil, nil): all regions are processed in this round.
//  2. (retryStartKey != nil, err): caller should retry from retryStartKey.
//     err is for logging/observability only.
//  3. (nil, err): unretryable failure happened (like context cancel).
//
// retryStartKey is always non-nil when a retry is needed, and vice versa. An
// empty key (len == 0) means retry from the beginning of the key space.
func (c *pkdbRaftIndexCollector) collectRound(
	ctx context.Context,
	options pkdbRaftIndexCollectOptions,
	onResult pkdbOnResultFunc,
) ([]byte, error) {
	roundCtx, cancelRound := context.WithCancelCause(ctx)
	defer cancelRound(nil)

	// scanCtx can be cancelled to stop scanning new regions, while still letting
	// in-flight debug RPCs before the breakpoint finish.
	// TODO(lance6716): check this cancel will not return a fatal error
	scanCtx, cancelScan := context.WithCancelCause(roundCtx)
	defer cancelScan(nil)

	taskBuf := options.pageSize * 2
	tasks := make(chan pkdbFetchTask, taskBuf)
	results := make(chan pkdbFetchResult, taskBuf)

	var fatalErr error
	var fatalOnce sync.Once
	setFatal := func(err error) {
		if err == nil {
			return
		}
		fatalOnce.Do(func() {
			fatalErr = err
			cancelRound(err)
		})
	}

	var (
		retryMu      sync.Mutex
		retryErr     error
		retryFromKey atomic.Pointer[[]byte]
		stopScanOnce sync.Once
	)
	getRetryFromKey := func() []byte {
		t := retryFromKey.Load()
		if t == nil {
			return nil
		}
		return *t
	}
	recordRetry := func(fromKey []byte, err error) {
		if err == nil {
			return
		}

		retryMu.Lock()
		currKey := getRetryFromKey()
		// the retry-from key should be the smallest.
		if currKey == nil || bytes.Compare(fromKey, currKey) < 0 {
			retryFromKey.Store(&fromKey)
			retryErr = err
		}
		retryMu.Unlock()

		stopScanOnce.Do(func() {
			cancelScan(err)
		})
	}
	// Once we find a retry breakpoint, everything at/after that key will be scanned
	// again in the next round, so this round can skip it.
	//
	// TODO(lance6716): use new API to write replication states, so staled states
	// won't cause trouble.
	shouldSkip := func(scanKey []byte) bool {
		breakpoint := getRetryFromKey()
		if breakpoint == nil {
			return false
		}
		return bytes.Compare(scanKey, breakpoint) >= 0
	}

	var writePDWG util.WaitGroupWrapper
	for i := 0; i < options.writePDConcurrency; i++ {
		writePDWG.RunWithLog(func() {
			for r := range results {
				if shouldSkip(r.scanKey) {
					continue
				}
				if err := onResult(roundCtx, r.response); err != nil {
					setFatal(err)
					return
				}
			}
		})
	}

	var readerWG util.WaitGroupWrapper
	for i := 0; i < options.readStoreConcurrency; i++ {
		readerWG.RunWithLog(func() {
			for t := range tasks {
				// TODO(lance6716): tasks has no reader, does the sender use right context so
				// will not be blocked?
				if err := roundCtx.Err(); err != nil {
					return
				}
				if shouldSkip(t.scanKey) {
					continue
				}
				res, retryStartKey, err := c.fetchOne(roundCtx, t)
				if retryStartKey != nil {
					recordRetry(retryStartKey, err)
					continue
				}
				if err != nil {
					setFatal(err)
					return
				}
				select {
				case results <- pkdbFetchResult{scanKey: t.scanKey, response: res}:
				case <-roundCtx.Done():
					return
				}
			}
		})
	}

	scanErr := c.scanAndEnqueue(scanCtx, options, func(t pkdbFetchTask) error {
		select {
		case tasks <- t:
			return nil
		case <-scanCtx.Done():
			return context.Cause(scanCtx)
		}
	})
	if scanErr != nil {
		// TODO(lance6716): manually check this if-branch. Maybe it means derived context
		// cancel is not elegantly processed
		if getRetryFromKey() != nil {
			// scanAndEnqueue returns the cancellation cause when a retry breakpoint
			// is found; the key was already recorded by recordRetry in reader workers.
		} else if roundCtx.Err() == nil {
			setFatal(scanErr)
		}
	}
	close(tasks)
	readerWG.Wait()
	close(results)
	writePDWG.Wait()

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if fatalErr != nil {
		return nil, fatalErr
	}

	breakpoint := getRetryFromKey()
	if breakpoint != nil {
		retryMu.Lock()
		cause := retryErr
		retryMu.Unlock()
		if cause == nil {
			return nil, errors.New("retry breakpoint is set but retry cause is nil")
		}
		return breakpoint, cause
	}
	return nil, nil
}

func pkdbGetAddStoreAddrs(ctx context.Context, pdCli pd.Client) (map[uint64]string, error) {
	stores, err := pdCli.GetAllStores(ctx, pd.WithExcludeTombstone())
	if err != nil {
		return nil, err
	}
	storeAddrByID := make(map[uint64]string, len(stores))
	for _, store := range stores {
		storeAddrByID[store.GetId()] = store.GetAddress()
	}
	return storeAddrByID, nil
}

// TODO(lance6716): enqueue return error means what? should add comment.
func (c *pkdbRaftIndexCollector) scanAndEnqueue(
	ctx context.Context,
	options pkdbRaftIndexCollectOptions,
	enqueue func(pkdbFetchTask) error,
) error {
	storeAddrByID, err := pkdbGetAddStoreAddrs(ctx, c.pdCli)
	if err != nil {
		return err
	}

	startKey := options.startKey
	nextKey := startKey

	for {
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}

		batch, err := c.pdCli.BatchScanRegions(
			ctx,
			[]pd.KeyRange{{StartKey: nextKey}},
			options.pageSize,
			pd.WithAllowFollowerHandle(),
		)
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			// even we are scanning the last region, BatchScanRegions should still return the
			// last region. So zero length regions means there's a gap.
			sleep(ctx, options.retryWait)
			continue
		}

		needRetry := false
		for _, r := range batch {
			if r == nil || r.Meta == nil {
				needRetry = true
				break
			}
			meta := r.Meta
			leader := r.Leader
			if leader == nil || leader.GetStoreId() == 0 {
				needRetry = true
				break
			}

			regionStart := meta.GetStartKey()
			if bytes.Compare(regionStart, nextKey) > 0 {
				// Gap found: retry from the breakpoint key.
				needRetry = true
				break
			}
			regionEnd := meta.GetEndKey()
			if len(regionEnd) != 0 && bytes.Compare(regionEnd, nextKey) <= 0 {
				// nextKey should always progress.
				needRetry = true
				break
			}

			storeAddr, err := pkdbResolveStoreAddr(ctx, storeAddrByID, leader.GetStoreId(), c.pdCli)
			if err != nil {
				needRetry = true
				break
			}

			region := pkdbScannedRegion{
				id:            meta.GetId(),
				startKey:      regionStart,
				endKey:        regionEnd,
				epoch:         meta.GetRegionEpoch(),
				leaderStoreID: leader.GetStoreId(),
			}
			task := pkdbFetchTask{
				region:    region,
				storeAddr: storeAddr,
				scanKey:   nextKey,
			}
			if err := enqueue(task); err != nil {
				return err
			}

			nextKey = regionEnd
			if len(nextKey) == 0 {
				// Reached the end.
				return nil
			}
		}

		if needRetry {
			sleep(ctx, options.retryWait)
			continue
		}
	}
}

// fetchOne returns one of:
//  1. (response != nil, nil, nil): success.
//  2. (nil, retryStartKey != nil, err): retry from retryStartKey.
//  3. (nil, nil, err): unretryable error.
//
// retryStartKey is always non-nil in case (2). An empty key (len == 0) means
// retry from the beginning.
func (c *pkdbRaftIndexCollector) fetchOne(
	ctx context.Context,
	t pkdbFetchTask,
) (*debugpb.RegionInfoResponse, []byte, error) {
	debugCli, err := c.debugCliPool.Get(ctx, t.storeAddr)
	if err != nil {
		return nil, nil, err
	}

	resp, err := debugCli.RegionInfo(ctx, &debugpb.RegionInfoRequest{RegionId: t.region.id})
	if err != nil {
		if pkdbIsRetryableRegionInfoError(err) {
			// Force re-dial next time for transient connection issues.
			c.debugCliPool.Delete(t.storeAddr)
			return nil, adjustRetryStartKey(t.scanKey), err
		}
		return nil, nil, err
	}

	regionMeta := resp.GetRegionLocalState().GetRegion()
	if regionMeta == nil {
		return nil, adjustRetryStartKey(t.scanKey), &pkdbErrMissingRegionMeta{
			regionID:  t.region.id,
			storeAddr: t.storeAddr,
		}
	}

	actualEpoch := regionMeta.GetRegionEpoch()
	if actualEpoch.GetConfVer() != t.region.epoch.GetConfVer() ||
		actualEpoch.GetVersion() != t.region.epoch.GetVersion() {
		return nil, adjustRetryStartKey(t.scanKey), &pkdbErrRegionEpochMismatch{
			regionID:  t.region.id,
			storeAddr: t.storeAddr,
			expected:  t.region.epoch,
			actual:    actualEpoch,
		}
	}

	return resp, nil, nil
}

func saveRaftAndRegionState2PD(pdCli pd.Client) pkdbOnResultFunc {
	return func(ctx context.Context, response *debugpb.RegionInfoResponse) error {
		raftLocalState := response.GetRaftLocalState()
		regionLocalState := response.GetRegionLocalState()
		raftApplyState := response.GetRaftApplyState()
		err := pdCli.SaveRaftAndRegionState(ctx, raftLocalState, raftApplyState, regionLocalState)
		for {
			if err == nil {
				return nil
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			logutil.SampleLogger().Warn("failed to save raft and region state to PD, retrying...",
				zap.Uint64("regionID", regionLocalState.GetRegion().GetId()),
				zap.Error(err),
			)
			sleep(ctx, time.Second)
			err = pdCli.SaveRaftAndRegionState(ctx, raftLocalState, raftApplyState, regionLocalState)
		}
	}
}

func pkdbResolveStoreAddr(ctx context.Context, storeAddrByID map[uint64]string, storeID uint64, pdCli pd.Client) (string, error) {
	if addr, ok := storeAddrByID[storeID]; ok && addr != "" {
		return addr, nil
	}
	store, err := pdCli.GetStore(ctx, storeID)
	if err != nil {
		return "", err
	}
	addr := store.GetAddress()
	if addr == "" {
		return "", &pkdbErrEmptyStoreAddr{storeID: storeID}
	}
	storeAddrByID[storeID] = addr
	return addr, nil
}

func pkdbIsRetryableRegionInfoError(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch st.Code() {
	case codes.NotFound, codes.Unavailable, codes.DeadlineExceeded, codes.Aborted:
		return true
	default:
		return false
	}
}

func adjustRetryStartKey(key []byte) []byte {
	if key == nil {
		// retry start key should not be nil. Minimum start key should use zero-length
		// slice. See the comment of collectRound.
		return []byte{}
	}
	return slices.Clone(key)
}
