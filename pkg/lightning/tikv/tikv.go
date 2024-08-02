// Copyright 2019 PingCAP, Inc.
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

package tikv

import (
	"context"
	"crypto/tls"
	"fmt"
	"regexp"
	"strings"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/debugpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/br/pkg/version"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/tikv/client-go/v2/util"
	pdhttp "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// StoreState is the state of a TiKV store. The numerical value is sorted by
// the store's accessibility (Tombstone < Down < Disconnected < Offline < Up).
//
// The meaning of each state can be found from PingCAP's documentation at
// https://pingcap.com/docs/v3.0/how-to/scale/horizontally/#delete-a-node-dynamically-1
type StoreState int

const (
	// StoreStateUp means the TiKV store is in service.
	StoreStateUp StoreState = -iota
	// StoreStateOffline means the TiKV store is in the process of being taken
	// offline (but is still accessible).
	StoreStateOffline
	// StoreStateDisconnected means the TiKV store does not respond to PD.
	StoreStateDisconnected
	// StoreStateDown means the TiKV store does not respond to PD for a long
	// time (> 30 minutes).
	StoreStateDown
	// StoreStateTombstone means the TiKV store is shut down and the data has
	// been evacuated. Lightning should never interact with stores in this
	// state.
	StoreStateTombstone
)

func withTiKVConnection(
	ctx context.Context,
	tls *tls.Config,
	tikvAddr string,
	action func(import_sstpb.ImportSSTClient) error,
) error {
	// Connect to the ImportSST service on the given TiKV node.
	// The connection is needed for executing `action` and will be tear down
	// when this function exits.
	conn, err := grpc.DialContext(ctx, tikvAddr, common.ToGRPCDialOption(tls), config.DefaultGrpcKeepaliveParams)
	if err != nil {
		return errors.Trace(err)
	}
	defer conn.Close()

	client := import_sstpb.NewImportSSTClient(conn)
	return action(client)
}

// ForAllStores executes `action` in parallel for all TiKV stores connected to
// a PD server.
//
// Returns the first non-nil error returned in all `action` calls. If all
// `action` returns nil, this method would return nil as well.
//
// The `maxState` argument defines the maximum store state (inclusive) to be
// included in the result (Up < Offline < Tombstone).
func ForAllStores(
	ctx context.Context,
	pdHTTPCli pdhttp.Client,
	maxState metapb.StoreState,
	action func(c context.Context, store *pdhttp.MetaStore) error,
) error {
	storesInfo, err := pdHTTPCli.GetStores(ctx)
	if err != nil {
		return err
	}

	eg, c := errgroup.WithContext(ctx)
	for _, store := range storesInfo.Stores {
		if store.Store.State <= int64(maxState) {
			s := store.Store
			eg.Go(func() error { return action(c, &s) })
		}
	}
	return eg.Wait()
}

func ignoreUnimplementedError(err error, logger log.Logger) error {
	if status.Code(err) == codes.Unimplemented {
		logger.Debug("skipping potentially TiFlash store")
		return nil
	}
	return errors.Trace(err)
}

// SwitchMode changes the TiKV node at the given address to a particular mode.
func SwitchMode(
	ctx context.Context,
	tls *tls.Config,
	tikvAddr string,
	mode import_sstpb.SwitchMode,
	ranges ...*import_sstpb.Range,
) error {
	task := log.With(zap.Stringer("mode", mode),
		zap.String("tikv", tikvAddr)).Begin(zap.DebugLevel, "switch mode")
	err := withTiKVConnection(ctx, tls, tikvAddr, func(client import_sstpb.ImportSSTClient) error {
		_, err := client.SwitchMode(ctx, &import_sstpb.SwitchModeRequest{
			Mode:   mode,
			Ranges: ranges,
		})
		return ignoreUnimplementedError(err, task.Logger)
	})
	task.End(zap.InfoLevel, err)
	return err
}

// Compact performs a leveled compaction with the given minimum level.
func Compact(ctx context.Context, tls *common.TLS, tikvAddr string, level int32, resourceGroupName string) error {
	task := log.With(zap.Int32("level", level), zap.String("tikv", tikvAddr)).Begin(zap.InfoLevel, "compact cluster")
	err := withTiKVConnection(ctx, tls.TLSConfig(), tikvAddr, func(client import_sstpb.ImportSSTClient) error {
		_, err := client.Compact(ctx, &import_sstpb.CompactRequest{
			OutputLevel: level,
			Context: &kvrpcpb.Context{
				ResourceControlContext: &kvrpcpb.ResourceControlContext{
					ResourceGroupName: resourceGroupName,
				},
				RequestSource: util.BuildRequestSource(true, kv.InternalTxnLightning, util.ExplicitTypeLightning),
			},
		})
		return ignoreUnimplementedError(err, task.Logger)
	})
	task.End(zap.ErrorLevel, err)
	return err
}

var fetchModeRegexp = regexp.MustCompile(
	`\btikv_config_rocksdb\{cf="default",name="hard_pending_compaction_bytes_limit"\} ([^\n]+)`)

// FetchMode obtains the import mode status of the TiKV node.
func FetchMode(ctx context.Context, tls *common.TLS, tikvAddr string) (import_sstpb.SwitchMode, error) {
	conn, err := grpc.DialContext(ctx, tikvAddr, tls.ToGRPCDialOption(),
		config.DefaultGrpcKeepaliveParams)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	client := debugpb.NewDebugClient(conn)
	resp, err := client.GetMetrics(ctx, &debugpb.GetMetricsRequest{All: false})
	if err != nil {
		return 0, errors.Trace(err)
	}
	return FetchModeFromMetrics(resp.Prometheus)
}

// FetchModeFromMetrics obtains the import mode status from the Prometheus metrics of a TiKV node.
func FetchModeFromMetrics(metrics string) (import_sstpb.SwitchMode, error) {
	m := fetchModeRegexp.FindStringSubmatch(metrics)
	switch {
	case len(m) < 2:
		return 0, errors.New("import mode status is not exposed")
	case m[1] == "0":
		return import_sstpb.SwitchMode_Import, nil
	default:
		return import_sstpb.SwitchMode_Normal, nil
	}
}

// FetchRemoteDBModelsFromTLS obtains the remote DB models from the given TLS.
func FetchRemoteDBModelsFromTLS(ctx context.Context, tls *common.TLS) ([]*model.DBInfo, error) {
	var dbs []*model.DBInfo
	err := tls.GetJSON(ctx, "/schema", &dbs)
	if err != nil {
		return nil, errors.Annotatef(err, "cannot read db schemas from remote")
	}
	return dbs, nil
}

// FetchRemoteTableModelsFromTLS obtains the remote table models from the given TLS.
func FetchRemoteTableModelsFromTLS(ctx context.Context, tls *common.TLS, schema string) ([]*model.TableInfo, error) {
	var tables []*model.TableInfo
	err := tls.GetJSON(ctx, "/schema/"+schema, &tables)
	if err != nil {
		return nil, errors.Annotatef(err, "cannot read schema '%s' from remote", schema)
	}
	return tables, nil
}

// CheckPDVersion checks the version of PD.
func CheckPDVersion(
	ctx context.Context,
	pdHTTPCli pdhttp.Client,
	requiredMinVersion, requiredMaxVersion semver.Version,
) error {
	ver, err := pdutil.FetchPDVersion(ctx, pdHTTPCli)
	if err != nil {
		return errors.Trace(err)
	}

	return version.CheckVersion("PD", *ver, requiredMinVersion, requiredMaxVersion)
}

// CheckTiKVVersion checks the version of TiKV.
func CheckTiKVVersion(
	ctx context.Context,
	pdHTTPCli pdhttp.Client,
	requiredMinVersion, requiredMaxVersion semver.Version,
) error {
	return ForTiKVVersions(
		ctx,
		pdHTTPCli,
		func(ver *semver.Version, addrMsg string) error {
			return version.CheckVersion(addrMsg, *ver, requiredMinVersion, requiredMaxVersion)
		},
	)
}

// ForTiKVVersions runs the given action for all versions of TiKV nodes.
func ForTiKVVersions(
	ctx context.Context,
	pdHTTPCli pdhttp.Client,
	action func(ver *semver.Version, addrMsg string) error,
) error {
	return ForAllStores(
		ctx,
		pdHTTPCli,
		metapb.StoreState_Offline,
		func(_ context.Context, store *pdhttp.MetaStore) error {
			component := fmt.Sprintf("TiKV (at %s)", store.Address)
			ver, err := semver.NewVersion(strings.TrimPrefix(store.Version, "v"))
			if err != nil {
				return errors.Annotate(err, component)
			}
			return action(ver, component)
		},
	)
}
