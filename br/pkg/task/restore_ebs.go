// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"context"
	"encoding/json"

	"github.com/coreos/go-semver/semver"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/config"
	"github.com/pingcap/tidb/br/pkg/conn"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const (
	EBSRestoreCmd = "EBS Restore"
)

const (
	flagRegionInfo = "region-info"
	flagLeaderInfo = "leader-info"
)

// DefineRestoreEBSMetaFlags defines common flags for the backup command.
func DefineRestoreDataFlags(command *cobra.Command) {
	command.Flags().Bool(flagDryRun, false, "don't access to aws environment if set to true")
	command.Flags().String(flagRegionInfo, "regionInfo", "print all region infos")
	command.Flags().String(flagLeaderInfo, "leader", "pring all region leaders")
}

type RestoreDataConfig struct {
	Config
	RegionInfo string `json:"region-info"`
	DryRun     bool   `json:"dry-run"`
	LeaderInfo string `json:"leader-info"`
}

// ParseFromFlags parses the restore-related flags from the flag set.
func (cfg *RestoreDataConfig) ParseFromFlags(flags *pflag.FlagSet) error {
	var err error
	cfg.DryRun, err = flags.GetBool(flagDryRun)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.RegionInfo, err = flags.GetString(flagRegionInfo)
	if err != nil {
		return errors.Trace(err)
	}

	cfg.LeaderInfo, err = flags.GetString(flagLeaderInfo)
	if err != nil {
		return errors.Trace(err)
	}

	return cfg.Config.ParseFromFlags(flags)
}

func checkEBSBRMeta(meta *config.EBSBasedBRMeta) error {
	if meta.ClusterInfo == nil {
		return errors.New("no cluster info")
	}
	if _, err := semver.NewVersion(meta.ClusterInfo.Version); err != nil {
		return errors.Annotatef(err, "invalid cluster version")
	}
	if meta.ClusterInfo.ResolvedTS == 0 {
		return errors.New("invalid resolved ts")
	}
	if meta.GetStoreCount() == 0 {
		return errors.New("tikv info is empty")
	}
	return nil
}

func ReadBackupMetaData(ctx context.Context, s storage.ExternalStorage) (uint64, int, error) {
	metaBytes, err := s.ReadFile(ctx, metautil.MetaFile)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}
	metaInfo := &config.EBSBasedBRMeta{}
	err = json.Unmarshal(metaBytes, metaInfo)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}
	if err = checkEBSBRMeta(metaInfo); err != nil {
		return 0, 0, errors.Trace(err)
	}

	return metaInfo.ClusterInfo.ResolvedTS, metaInfo.TiKVComponent.Replicas, nil
}

// RunRestore starts a restore task inside the current goroutine.
func RunRestoreData(c context.Context, g glue.Glue, cmdName string, cfg *RestoreDataConfig) error {
	var finished bool
	defer func() {
		if finished {
			summary.Log("EBS restore success")
		} else {
			summary.Log("EBS restore failed, please check the log for details.")
		}
	}()

	ctx, cancel := context.WithCancel(c)
	defer cancel()

	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("task.RunRestoreData", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	keepaliveCfg := GetKeepalive(&cfg.Config)
	mgr, err := NewMgr(ctx, g, cfg.PD, cfg.TLS, keepaliveCfg, cfg.CheckRequirements, false)
	if err != nil {
		return errors.Trace(err)
	}
	defer mgr.Close()

	keepaliveCfg.PermitWithoutStream = true
	client := restore.NewRestoreClient(mgr.GetPDClient(), mgr.GetTLSConfig(), keepaliveCfg, false)

	var resolveTs uint64
	var numOfStores int
	if cfg.DryRun {
		resolveTs = 46464574745
		numOfStores = 3
	} else {

		_, externStorage, err := GetStorage(ctx, cfg.Config.Storage, &cfg.Config)
		if err != nil {
			return errors.Trace(err)
		}

		resolveTs, numOfStores, err = ReadBackupMetaData(ctx, externStorage)
		if err != nil {
			return errors.Trace(err)
		}
	}

	restoreTS, err := client.GetTS(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	sp := utils.BRServiceSafePoint{
		BackupTS: restoreTS,
		TTL:      utils.DefaultBRGCSafePointTTL,
		ID:       utils.MakeSafePointID(),
	}

	err = utils.StartServiceSafePointKeeper(ctx, mgr.GetPDClient(), sp)
	if err != nil {
		return errors.Trace(err)
	}

	var allStores []*metapb.Store
	allStores, err = conn.GetAllTiKVStoresWithRetry(ctx, mgr.GetPDClient(), conn.SkipTiFlash)
	if err != nil {
		return errors.Trace(err)
	}

	if len(allStores) != numOfStores {
		log.Error("the restore meta contains the number of tikvs inconsist with the resore cluster")
		return errors.Annotatef(berrors.ErrRestoreTotalKVMismatch,
			"number of tikvs mismatch")
	}

	err = restore.RecoverCluster(ctx, resolveTs, allStores, mgr.GetTLSConfig())
	if err != nil {
		return errors.Trace(err)
	}

	log.Info("unmark recovering")
	if err := mgr.UnmarkRecovering(ctx); err != nil {
		return errors.Trace(err)
	}

	finished = true
	return nil

}
