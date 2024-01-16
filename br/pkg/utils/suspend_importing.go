// Copyright 2023 PingCAP, Inc. Licensed under Apache-2.0.
package utils

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/pkg/util/engine"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	DenyLightningUpdateFrequency = 5
)

func (mgr *StoreManager) GetAllStores(ctx context.Context) ([]*metapb.Store, error) {
	return mgr.PDClient().GetAllStores(ctx, pd.WithExcludeTombstone())
}

func (mgr *StoreManager) GetDenyLightningClient(ctx context.Context, storeID uint64) (SuspendImportingClient, error) {
	var cli import_sstpb.ImportSSTClient
	err := mgr.WithConn(ctx, storeID, func(cc *grpc.ClientConn) {
		cli = import_sstpb.NewImportSSTClient(cc)
	})
	if err != nil {
		return nil, err
	}
	return cli, nil
}

type SuspendImportingEnv interface {
	GetAllStores(ctx context.Context) ([]*metapb.Store, error)
	GetDenyLightningClient(ctx context.Context, storeID uint64) (SuspendImportingClient, error)
}

type SuspendImportingClient interface {
	// Temporarily disable ingest / download / write for data listeners don't support catching import data.
	SuspendImportRPC(ctx context.Context, in *import_sstpb.SuspendImportRPCRequest, opts ...grpc.CallOption) (*import_sstpb.SuspendImportRPCResponse, error)
}

type SuspendImporting struct {
	env  SuspendImportingEnv
	name string
}

func NewSuspendImporting(name string, env SuspendImportingEnv) *SuspendImporting {
	return &SuspendImporting{
		env:  env,
		name: name,
	}
}

// DenyAllStores tries to deny all current stores' lightning execution for the period of time.
// Returns a map mapping store ID to whether they are already denied to import tasks.
func (d *SuspendImporting) DenyAllStores(ctx context.Context, dur time.Duration) (map[uint64]bool, error) {
	return d.forEachStores(ctx, func() *import_sstpb.SuspendImportRPCRequest {
		return &import_sstpb.SuspendImportRPCRequest{
			ShouldSuspendImports: true,
			DurationInSecs:       uint64(dur.Seconds()),
			Caller:               d.name,
		}
	})
}

func (d *SuspendImporting) AllowAllStores(ctx context.Context) (map[uint64]bool, error) {
	return d.forEachStores(ctx, func() *import_sstpb.SuspendImportRPCRequest {
		return &import_sstpb.SuspendImportRPCRequest{
			ShouldSuspendImports: false,
			Caller:               d.name,
		}
	})
}

// forEachStores send the request to each stores reachable.
// Returns a map mapping store ID to whether they are already denied to import tasks.
func (d *SuspendImporting) forEachStores(ctx context.Context, makeReq func() *import_sstpb.SuspendImportRPCRequest) (map[uint64]bool, error) {
	stores, err := d.env.GetAllStores(ctx)
	if err != nil {
		return nil, errors.Annotate(err, "failed to get all stores")
	}

	result := map[uint64]bool{}
	log.Info("SuspendImporting/forEachStores: hint of current store.", zap.Stringers("stores", stores))
	for _, store := range stores {
		logutil.CL(ctx).Info("Handling store.", zap.Stringer("store", store))
		if engine.IsTiFlash(store) {
			logutil.CL(ctx).Info("Store is tiflash, skipping.", zap.Stringer("store", store))
			continue
		}
		cli, err := d.env.GetDenyLightningClient(ctx, store.Id)
		if err != nil {
			return nil, errors.Annotatef(err, "failed to get client for store %d", store.Id)
		}
		req := makeReq()
		resp, err := cli.SuspendImportRPC(ctx, req)
		if err != nil {
			return nil, errors.Annotatef(err, "failed to deny lightning rpc for store %d", store.Id)
		}
		result[store.Id] = resp.AlreadySuspended
	}
	return result, nil
}

// HasKeptDenying checks whether a result returned by `DenyAllStores` is able to keep the consistency with last request.
// i.e. Whether the store has some holes of pausing the import requests.
func (d *SuspendImporting) ConsistentWithPrev(result map[uint64]bool) error {
	for storeId, denied := range result {
		if !denied {
			return errors.Annotatef(berrors.ErrPossibleInconsistency, "failed to keep importing to store %d being denied, the state might be inconsistency", storeId)
		}
	}
	return nil
}

func (d *SuspendImporting) Keeper(ctx context.Context, ttl time.Duration) error {
	lastSuccess := time.Now()
	t := time.NewTicker(ttl / DenyLightningUpdateFrequency)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			res, err := d.DenyAllStores(ctx, ttl)
			if err != nil {
				if time.Since(lastSuccess) < ttl {
					logutil.CL(ctx).Warn("Failed to send deny one of the stores.", logutil.ShortError(err))
					continue
				}
				return err
			}
			if err := d.ConsistentWithPrev(res); err != nil {
				return err
			}

			lastSuccess = time.Now()
		}
	}
}
