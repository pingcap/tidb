// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package backup

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/google/btree"
	"github.com/pingcap/errors"
	brpb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/common"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/util/mathutil"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

const (
	waitAllScheduleStoppedInterval = 15 * time.Second
)

// todo: need a better name
var errHasPendingAdmin = errors.New("has pending admin")

// checkDupFiles checks if there are any files are duplicated.
func checkDupFiles(rangeTree *rtree.RangeTree) {
	// Name -> SHA256
	files := make(map[string][]byte)
	rangeTree.Ascend(func(i btree.Item) bool {
		rg := i.(*rtree.Range)
		for _, f := range rg.Files {
			old, ok := files[f.Name]
			if ok {
				log.Error("dup file",
					zap.String("Name", f.Name),
					zap.String("SHA256_1", hex.EncodeToString(old)),
					zap.String("SHA256_2", hex.EncodeToString(f.Sha256)),
				)
			} else {
				files[f.Name] = f.Sha256
			}
		}
		return true
	})
}

func WaitAllScheduleStoppedAndNoRegionHole(ctx context.Context, allStores []*metapb.Store,
	newBackupClientFn func(context.Context, string) (brpb.BackupClient, *grpc.ClientConn, error)) error {
	// we wait for nearly 15*40 = 600s = 10m
	backoffer := utils.InitialRetryState(40, 5*time.Second, waitAllScheduleStoppedInterval)
	for backoffer.Attempt() > 0 {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		allRegions, err2 := waitUntilAllScheduleStopped(ctx, allStores, newBackupClientFn)
		if err2 != nil {
			if causeErr := errors.Cause(err2); causeErr == errHasPendingAdmin {
				log.Info("schedule ongoing on tikv, will retry later", zap.Error(err2))
			} else {
				log.Warn("failed to wait schedule, will retry later", zap.Error(err2))
			}
			continue
		}

		log.Info("all leader regions got, start checking hole", zap.Int("len", len(allRegions)))

		if !isRegionsHasHole(allRegions) {
			return nil
		}
		time.Sleep(backoffer.ExponentialBackoff())
	}
	return errors.New("failed to wait all schedule stopped")
}

func isRegionsHasHole(allRegions []*metapb.Region) bool {
	// sort by start key
	sort.Slice(allRegions, func(i, j int) bool {
		left, right := allRegions[i], allRegions[j]
		return bytes.Compare(left.StartKey, right.StartKey) < 0
	})

	for j := 0; j < len(allRegions)-1; j++ {
		left, right := allRegions[j], allRegions[j+1]
		// we don't need to handle the empty end key specially, since
		// we sort by start key of region, and the end key of the last region is not checked
		if !bytes.Equal(left.EndKey, right.StartKey) {
			log.Info("region hole found", zap.Reflect("left-region", left), zap.Reflect("right-region", right))
			return true
		}
	}
	return false
}

func waitUntilAllScheduleStopped(ctx context.Context, allStores []*metapb.Store,
	newBackupClientFn func(context.Context, string) (brpb.BackupClient, *grpc.ClientConn, error)) ([]*metapb.Region, error) {
	concurrency := mathutil.Min(len(allStores), common.MaxStoreConcurrency)
	workerPool := utils.NewWorkerPool(uint(concurrency), "collect schedule info")
	eg, ectx := errgroup.WithContext(ctx)

	// init this slice with guess that there are 100 leaders on each store
	var mutex sync.Mutex
	allRegions := make([]*metapb.Region, 0, len(allStores)*100)
	addRegionsFunc := func(regions []*metapb.Region) {
		mutex.Lock()
		defer mutex.Unlock()
		allRegions = append(allRegions, regions...)
	}
	for i := range allStores {
		store := allStores[i]
		if ectx.Err() != nil {
			break
		}
		workerPool.ApplyOnErrorGroup(eg, func() error {
			backupClient, connection, err := newBackupClientFn(ctx, store.Address)
			if err != nil {
				return errors.Trace(err)
			}
			defer func() {
				_ = connection.Close()
			}()
			checkAdminClient, err := backupClient.CheckPendingAdminOp(ectx, &brpb.CheckAdminRequest{})
			if err != nil {
				return errors.Trace(err)
			}

			storeLeaderRegions := make([]*metapb.Region, 0, 100)
			for {
				response, err2 := checkAdminClient.Recv()
				if err2 != nil {
					causeErr := errors.Cause(err2)
					// other check routines may have HasPendingAdmin=true, so Recv() may receive canceled.
					// skip it to avoid error overriding
					if causeErr == io.EOF || causeErr == context.Canceled {
						break
					}
					return errors.Trace(err2)
				}
				if response.Error != nil {
					return errors.New(response.Error.String())
				}
				if response.Region == nil {
					return errors.New("region is nil")
				}
				if response.HasPendingAdmin {
					return errors.WithMessage(errHasPendingAdmin, fmt.Sprintf("store-id=%d", store.Id))
				}
				storeLeaderRegions = append(storeLeaderRegions, response.Region)
			}
			addRegionsFunc(storeLeaderRegions)
			return nil
		})
	}
	return allRegions, eg.Wait()
}
