// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package pdutil

import (
	"context"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/docker/go-units"
	"github.com/google/uuid"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/pkg/util/codec"
	pd "github.com/tikv/pd/client"
	pdhttp "github.com/tikv/pd/client/http"
	"github.com/tikv/pd/client/retry"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	maxMsgSize   = int(128 * units.MiB) // pd.ScanRegion may return a large response
	pauseTimeout = 5 * time.Minute
	// pd request retry time when connection fail
	PDRequestRetryTime = 120
	// set max-pending-peer-count to a large value to avoid scatter region failed.
	maxPendingPeerUnlimited uint64 = math.MaxInt32
)

// pauseConfigGenerator generate a config value according to store count and current value.
type pauseConfigGenerator func(int, any) any

// zeroPauseConfig sets the config to 0.
func zeroPauseConfig(int, any) any {
	return 0
}

// pauseConfigMulStores multiplies the existing value by
// number of stores. The value is limited to 40, as larger value
// may make the cluster unstable.
func pauseConfigMulStores(stores int, raw any) any {
	rawCfg := raw.(float64)
	return math.Min(40, rawCfg*float64(stores))
}

// pauseConfigFalse sets the config to "false".
func pauseConfigFalse(int, any) any {
	return "false"
}

// constConfigGeneratorBuilder build a pauseConfigGenerator based on a given const value.
func constConfigGeneratorBuilder(val any) pauseConfigGenerator {
	return func(int, any) any {
		return val
	}
}

// ClusterConfig represents a set of scheduler whose config have been modified
// along with their original config.
type ClusterConfig struct {
	// Enable PD schedulers before restore
	Schedulers []string `json:"schedulers"`
	// Original scheudle configuration
	ScheduleCfg map[string]any `json:"schedule_cfg"`
}

type pauseSchedulerBody struct {
	Delay int64 `json:"delay"`
}

var (
	// in v4.0.8 version we can use pause configs
	// see https://github.com/tikv/pd/pull/3088
	pauseConfigVersion = semver.Version{Major: 4, Minor: 0, Patch: 8}

	// After v6.1.0 version, we can pause schedulers by key range with TTL.
	minVersionForRegionLabelTTL = semver.Version{Major: 6, Minor: 1, Patch: 0}

	// Schedulers represent region/leader schedulers which can impact on performance.
	Schedulers = map[string]struct{}{
		"balance-leader-scheduler":     {},
		"balance-hot-region-scheduler": {},
		"balance-region-scheduler":     {},

		"shuffle-leader-scheduler":     {},
		"shuffle-region-scheduler":     {},
		"shuffle-hot-region-scheduler": {},

		"evict-slow-store-scheduler": {},
	}
	expectPDCfgGenerators = map[string]pauseConfigGenerator{
		"merge-schedule-limit": zeroPauseConfig,
		// TODO "leader-schedule-limit" and "region-schedule-limit" don't support ttl for now,
		// but we still need set these config for compatible with old version.
		// we need wait for https://github.com/tikv/pd/pull/3131 merged.
		// see details https://github.com/pingcap/br/pull/592#discussion_r522684325
		"leader-schedule-limit":       pauseConfigMulStores,
		"region-schedule-limit":       pauseConfigMulStores,
		"max-snapshot-count":          pauseConfigMulStores,
		"enable-location-replacement": pauseConfigFalse,
		"max-pending-peer-count":      constConfigGeneratorBuilder(maxPendingPeerUnlimited),
	}

	// defaultPDCfg find by https://github.com/tikv/pd/blob/master/conf/config.toml.
	// only use for debug command.
	defaultPDCfg = map[string]any{
		"merge-schedule-limit":        8,
		"leader-schedule-limit":       4,
		"region-schedule-limit":       2048,
		"enable-location-replacement": "true",
	}
)

// DefaultExpectPDCfgGenerators returns default pd config generators
func DefaultExpectPDCfgGenerators() map[string]pauseConfigGenerator {
	clone := make(map[string]pauseConfigGenerator, len(expectPDCfgGenerators))
	for k := range expectPDCfgGenerators {
		clone[k] = expectPDCfgGenerators[k]
	}
	return clone
}

// PdController manage get/update config from pd.
type PdController struct {
	pdClient  pd.Client
	pdHTTPCli pdhttp.Client
	version   *semver.Version

	// control the pause schedulers goroutine
	schedulerPauseCh chan struct{}
	// control the ttl of pausing schedulers
	SchedulerPauseTTL time.Duration
}

// NewPdController creates a new PdController.
func NewPdController(
	ctx context.Context,
	pdAddrs []string,
	tlsConf *tls.Config,
	securityOption pd.SecurityOption,
) (*PdController, error) {
	maxCallMsgSize := []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSize)),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(maxMsgSize)),
	}
	pdClient, err := pd.NewClientWithContext(
		ctx, pdAddrs, securityOption,
		pd.WithGRPCDialOptions(maxCallMsgSize...),
		// If the time too short, we may scatter a region many times, because
		// the interface `ScatterRegions` may time out.
		pd.WithCustomTimeoutOption(60*time.Second),
	)
	if err != nil {
		log.Error("fail to create pd client", zap.Error(err))
		return nil, errors.Trace(err)
	}

	pdHTTPCliConfig := make([]pdhttp.ClientOption, 0, 1)
	if tlsConf != nil {
		pdHTTPCliConfig = append(pdHTTPCliConfig, pdhttp.WithTLSConfig(tlsConf))
	}
	pdHTTPCli := pdhttp.NewClientWithServiceDiscovery(
		"br/lightning PD controller",
		pdClient.GetServiceDiscovery(),
		pdHTTPCliConfig...,
	).WithBackoffer(retry.InitialBackoffer(time.Second, time.Second, PDRequestRetryTime*time.Second))
	versionStr, err := pdHTTPCli.GetPDVersion(ctx)
	if err != nil {
		pdHTTPCli.Close()
		pdClient.Close()
		return nil, errors.Trace(err)
	}
	version := parseVersion(versionStr)

	return &PdController{
		pdClient:  pdClient,
		pdHTTPCli: pdHTTPCli,
		version:   version,
		// We should make a buffered channel here otherwise when context canceled,
		// gracefully shutdown will stick at resuming schedulers.
		schedulerPauseCh: make(chan struct{}, 1),
	}, nil
}

func parseVersion(versionStr string) *semver.Version {
	// we need trim space or semver will parse failed
	v := strings.TrimSpace(versionStr)
	v = strings.Trim(v, "\"")
	v = strings.TrimPrefix(v, "v")
	version, err := semver.NewVersion(v)
	if err != nil {
		log.Warn("fail back to v0.0.0 version",
			zap.String("version", versionStr), zap.Error(err))
		version = &semver.Version{Major: 0, Minor: 0, Patch: 0}
	}
	failpoint.Inject("PDEnabledPauseConfig", func(val failpoint.Value) {
		if val.(bool) {
			// test pause config is enable
			version = &semver.Version{Major: 5, Minor: 0, Patch: 0}
		}
	})
	return version
}

func (p *PdController) isPauseConfigEnabled() bool {
	return p.version.Compare(pauseConfigVersion) >= 0
}

// SetPDClient set pd addrs and cli for test.
func (p *PdController) SetPDClient(pdClient pd.Client) {
	p.pdClient = pdClient
}

// GetPDClient set pd addrs and cli for test.
func (p *PdController) GetPDClient() pd.Client {
	return p.pdClient
}

// GetPDHTTPClient returns the pd http client.
func (p *PdController) GetPDHTTPClient() pdhttp.Client {
	return p.pdHTTPCli
}

// GetClusterVersion returns the current cluster version.
func (p *PdController) GetClusterVersion(ctx context.Context) (string, error) {
	v, err := p.pdHTTPCli.GetClusterVersion(ctx)
	return v, errors.Trace(err)
}

// GetRegionCount returns the region count in the specified range.
func (p *PdController) GetRegionCount(ctx context.Context, startKey, endKey []byte) (int, error) {
	// TiKV reports region start/end keys to PD in memcomparable-format.
	var start, end []byte
	start = codec.EncodeBytes(nil, startKey)
	if len(endKey) != 0 { // Empty end key means the max.
		end = codec.EncodeBytes(nil, endKey)
	}
	status, err := p.pdHTTPCli.GetRegionStatusByKeyRange(ctx, pdhttp.NewKeyRange(start, end), true)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return status.Count, nil
}

// GetStoreInfo returns the info of store with the specified id.
func (p *PdController) GetStoreInfo(ctx context.Context, storeID uint64) (*pdhttp.StoreInfo, error) {
	info, err := p.pdHTTPCli.GetStore(ctx, storeID)
	return info, errors.Trace(err)
}

func (p *PdController) doPauseSchedulers(
	ctx context.Context,
	schedulers []string,
) ([]string, error) {
	// pause this scheduler with 300 seconds
	delay := int64(p.ttlOfPausing().Seconds())
	removedSchedulers := make([]string, 0, len(schedulers))
	for _, scheduler := range schedulers {
		err := p.pdHTTPCli.SetSchedulerDelay(ctx, scheduler, delay)
		if err != nil {
			return removedSchedulers, errors.Trace(err)
		}
		removedSchedulers = append(removedSchedulers, scheduler)
	}
	return removedSchedulers, nil
}

func (p *PdController) pauseSchedulersAndConfigWith(
	ctx context.Context, schedulers []string,
	schedulerCfg map[string]any,
) ([]string, error) {
	// first pause this scheduler, if the first time failed. we should return the error
	// so put first time out of for loop. and in for loop we could ignore other failed pause.
	removedSchedulers, err := p.doPauseSchedulers(ctx, schedulers)
	if err != nil {
		log.Error("failed to pause scheduler at beginning",
			zap.Strings("name", schedulers), zap.Error(err))
		return nil, errors.Trace(err)
	}
	log.Info("pause scheduler successful at beginning", zap.Strings("name", schedulers))
	if schedulerCfg != nil {
		err = p.doPauseConfigs(ctx, schedulerCfg)
		if err != nil {
			log.Error("failed to pause config at beginning",
				zap.Any("cfg", schedulerCfg), zap.Error(err))
			return nil, errors.Trace(err)
		}
		log.Info("pause configs successful at beginning", zap.Any("cfg", schedulerCfg))
	}

	go func() {
		tick := time.NewTicker(p.ttlOfPausing() / 3)
		defer tick.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				_, err := p.doPauseSchedulers(ctx, schedulers)
				if err != nil {
					log.Warn("pause scheduler failed, ignore it and wait next time pause", zap.Error(err))
				}
				if schedulerCfg != nil {
					err = p.doPauseConfigs(ctx, schedulerCfg)
					if err != nil {
						log.Warn("pause configs failed, ignore it and wait next time pause", zap.Error(err))
					}
				}
				log.Info("pause scheduler(configs)", zap.Strings("name", removedSchedulers),
					zap.Any("cfg", schedulerCfg))
			case <-p.schedulerPauseCh:
				log.Info("exit pause scheduler and configs successful")
				return
			}
		}
	}()
	return removedSchedulers, nil
}

// ResumeSchedulers resume pd scheduler.
func (p *PdController) ResumeSchedulers(ctx context.Context, schedulers []string) error {
	return errors.Trace(p.resumeSchedulerWith(ctx, schedulers))
}

func (p *PdController) resumeSchedulerWith(ctx context.Context, schedulers []string) (err error) {
	log.Info("resume scheduler", zap.Strings("schedulers", schedulers))
	p.schedulerPauseCh <- struct{}{}

	// 0 means stop pause.
	delay := int64(0)
	for _, scheduler := range schedulers {
		err = p.pdHTTPCli.SetSchedulerDelay(ctx, scheduler, delay)
		if err != nil {
			log.Error("failed to resume scheduler after retry, you may reset this scheduler manually"+
				"or just wait this scheduler pause timeout", zap.String("scheduler", scheduler))
		} else {
			log.Info("resume scheduler successful", zap.String("scheduler", scheduler))
		}
	}
	// no need to return error, because the pause will timeout.
	return nil
}

// ListSchedulers list all pd scheduler.
func (p *PdController) ListSchedulers(ctx context.Context) ([]string, error) {
	s, err := p.pdHTTPCli.GetSchedulers(ctx)
	return s, errors.Trace(err)
}

// GetPDScheduleConfig returns PD schedule config value associated with the key.
// It returns nil if there is no such config item.
func (p *PdController) GetPDScheduleConfig(ctx context.Context) (map[string]any, error) {
	cfg, err := p.pdHTTPCli.GetScheduleConfig(ctx)
	return cfg, errors.Trace(err)
}

// UpdatePDScheduleConfig updates PD schedule config value associated with the key.
func (p *PdController) UpdatePDScheduleConfig(ctx context.Context) error {
	log.Info("update pd with default config", zap.Any("cfg", defaultPDCfg))
	return errors.Trace(p.doUpdatePDScheduleConfig(ctx, defaultPDCfg))
}

func (p *PdController) doUpdatePDScheduleConfig(
	ctx context.Context, cfg map[string]any, ttlSeconds ...float64,
) error {
	newCfg := make(map[string]any)
	for k, v := range cfg {
		// if we want use ttl, we need use config prefix first.
		// which means cfg should transfer from "max-merge-region-keys" to "schedule.max-merge-region-keys".
		sc := fmt.Sprintf("schedule.%s", k)
		newCfg[sc] = v
	}

	if err := p.pdHTTPCli.SetConfig(ctx, newCfg, ttlSeconds...); err != nil {
		return errors.Annotatef(
			berrors.ErrPDUpdateFailed,
			"failed to update PD schedule config: %s",
			err.Error(),
		)
	}
	return nil
}

func (p *PdController) doPauseConfigs(ctx context.Context, cfg map[string]any) error {
	// pause this scheduler with 300 seconds
	return errors.Trace(p.doUpdatePDScheduleConfig(ctx, cfg, p.ttlOfPausing().Seconds()))
}

func restoreSchedulers(ctx context.Context, pd *PdController, clusterCfg ClusterConfig,
	configsNeedRestore map[string]pauseConfigGenerator) error {
	if err := pd.ResumeSchedulers(ctx, clusterCfg.Schedulers); err != nil {
		return errors.Annotate(err, "fail to add PD schedulers")
	}
	log.Info("restoring config", zap.Any("config", clusterCfg.ScheduleCfg))
	mergeCfg := make(map[string]any)
	for cfgKey := range configsNeedRestore {
		value := clusterCfg.ScheduleCfg[cfgKey]
		if value == nil {
			// Ignore non-exist config.
			continue
		}
		mergeCfg[cfgKey] = value
	}

	prefix := make([]float64, 0, 1)
	if pd.isPauseConfigEnabled() {
		// set config's ttl to zero, make temporary config invalid immediately.
		prefix = append(prefix, 0)
	}
	// reset config with previous value.
	if err := pd.doUpdatePDScheduleConfig(ctx, mergeCfg, prefix...); err != nil {
		return errors.Annotate(err, "fail to update PD merge config")
	}
	return nil
}

// MakeUndoFunctionByConfig return an UndoFunc based on specified ClusterConfig
func (p *PdController) MakeUndoFunctionByConfig(config ClusterConfig) UndoFunc {
	return p.GenRestoreSchedulerFunc(config, expectPDCfgGenerators)
}

// GenRestoreSchedulerFunc gen restore func
func (p *PdController) GenRestoreSchedulerFunc(config ClusterConfig,
	configsNeedRestore map[string]pauseConfigGenerator) UndoFunc {
	// todo: we only need config names, not a map[string]pauseConfigGenerator
	restore := func(ctx context.Context) error {
		return restoreSchedulers(ctx, p, config, configsNeedRestore)
	}
	return restore
}

// RemoveSchedulers removes the schedulers that may slow down BR speed.
func (p *PdController) RemoveSchedulers(ctx context.Context) (undo UndoFunc, err error) {
	undo = Nop

	origin, _, err1 := p.RemoveSchedulersWithOrigin(ctx)
	if err1 != nil {
		err = err1
		return
	}

	undo = p.MakeUndoFunctionByConfig(ClusterConfig{Schedulers: origin.Schedulers, ScheduleCfg: origin.ScheduleCfg})
	return undo, errors.Trace(err)
}

// RemoveSchedulersWithConfig removes the schedulers that may slow down BR speed.
func (p *PdController) RemoveSchedulersWithConfig(
	ctx context.Context,
) (undo UndoFunc, config *ClusterConfig, err error) {
	undo = Nop

	origin, _, err1 := p.RemoveSchedulersWithOrigin(ctx)
	if err1 != nil {
		err = err1
		return
	}

	undo = p.MakeUndoFunctionByConfig(ClusterConfig{Schedulers: origin.Schedulers, ScheduleCfg: origin.ScheduleCfg})
	return undo, &origin, errors.Trace(err)
}

// RemoveAllPDSchedulers pause pd scheduler during the snapshot backup and restore
func (p *PdController) RemoveAllPDSchedulers(ctx context.Context) (undo UndoFunc, err error) {
	undo = Nop

	// during the backup, we shall stop all scheduler so that restore easy to implement
	// during phase-2, pd is fresh and in recovering-mode(recovering-mark=true), there's no leader
	// so there's no leader or region schedule initially. when phase-2 start force setting leaders, schedule may begin.
	// we don't want pd do any leader or region schedule during this time, so we set those params to 0
	// before we force setting leaders
	const enableTiKVSplitRegion = "enable-tikv-split-region"
	scheduleLimitParams := []string{
		"hot-region-schedule-limit",
		"leader-schedule-limit",
		"merge-schedule-limit",
		"region-schedule-limit",
		"replica-schedule-limit",
		enableTiKVSplitRegion,
	}
	pdConfigGenerators := DefaultExpectPDCfgGenerators()
	for _, param := range scheduleLimitParams {
		if param == enableTiKVSplitRegion {
			pdConfigGenerators[param] = func(int, any) any { return false }
		} else {
			pdConfigGenerators[param] = func(int, any) any { return 0 }
		}
	}

	oldPDConfig, _, err1 := p.RemoveSchedulersWithConfigGenerator(ctx, pdConfigGenerators)
	if err1 != nil {
		err = err1
		return
	}

	undo = p.GenRestoreSchedulerFunc(oldPDConfig, pdConfigGenerators)
	return undo, errors.Trace(err)
}

// RemoveSchedulersWithOrigin pause and remove br related schedule configs and return the origin and modified configs
func (p *PdController) RemoveSchedulersWithOrigin(ctx context.Context) (
	origin ClusterConfig,
	modified ClusterConfig,
	err error,
) {
	origin, modified, err = p.RemoveSchedulersWithConfigGenerator(ctx, expectPDCfgGenerators)
	err = errors.Trace(err)
	return
}

// RemoveSchedulersWithConfigGenerator pause scheduler with custom config generator
func (p *PdController) RemoveSchedulersWithConfigGenerator(
	ctx context.Context,
	pdConfigGenerators map[string]pauseConfigGenerator,
) (origin ClusterConfig, modified ClusterConfig, err error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("PdController.RemoveSchedulers",
			opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	originCfg := ClusterConfig{}
	removedCfg := ClusterConfig{}
	stores, err := p.pdClient.GetAllStores(ctx)
	if err != nil {
		return originCfg, removedCfg, errors.Trace(err)
	}
	scheduleCfg, err := p.GetPDScheduleConfig(ctx)
	if err != nil {
		return originCfg, removedCfg, errors.Trace(err)
	}
	disablePDCfg := make(map[string]any, len(pdConfigGenerators))
	originPDCfg := make(map[string]any, len(pdConfigGenerators))
	for cfgKey, cfgValFunc := range pdConfigGenerators {
		value, ok := scheduleCfg[cfgKey]
		if !ok {
			// Ignore non-exist config.
			continue
		}
		disablePDCfg[cfgKey] = cfgValFunc(len(stores), value)
		originPDCfg[cfgKey] = value
	}
	originCfg.ScheduleCfg = originPDCfg
	removedCfg.ScheduleCfg = disablePDCfg

	log.Debug("saved PD config", zap.Any("config", scheduleCfg))

	// Remove default PD scheduler that may affect restore process.
	existSchedulers, err := p.ListSchedulers(ctx)
	if err != nil {
		return originCfg, removedCfg, errors.Trace(err)
	}
	needRemoveSchedulers := make([]string, 0, len(existSchedulers))
	for _, s := range existSchedulers {
		if _, ok := Schedulers[s]; ok {
			needRemoveSchedulers = append(needRemoveSchedulers, s)
		}
	}

	removedSchedulers, err := p.doRemoveSchedulersWith(ctx, needRemoveSchedulers, disablePDCfg)
	if err != nil {
		return originCfg, removedCfg, errors.Trace(err)
	}

	originCfg.Schedulers = removedSchedulers
	removedCfg.Schedulers = removedSchedulers

	return originCfg, removedCfg, nil
}

// RemoveSchedulersWithCfg removes pd schedulers and configs with specified ClusterConfig
func (p *PdController) RemoveSchedulersWithCfg(ctx context.Context, removeCfg ClusterConfig) error {
	_, err := p.doRemoveSchedulersWith(ctx, removeCfg.Schedulers, removeCfg.ScheduleCfg)
	return errors.Trace(err)
}

func (p *PdController) doRemoveSchedulersWith(
	ctx context.Context,
	needRemoveSchedulers []string,
	disablePDCfg map[string]any,
) ([]string, error) {
	if !p.isPauseConfigEnabled() {
		return nil, errors.Errorf("pd version %s not support pause config, please upgrade", p.version.String())
	}
	// after 4.0.8 we can set these config with TTL
	s, err := p.pauseSchedulersAndConfigWith(ctx, needRemoveSchedulers, disablePDCfg)
	return s, errors.Trace(err)
}

// GetMinResolvedTS get min-resolved-ts from pd
func (p *PdController) GetMinResolvedTS(ctx context.Context) (uint64, error) {
	ts, _, err := p.pdHTTPCli.GetMinResolvedTSByStoresIDs(ctx, nil)
	return ts, errors.Trace(err)
}

// RecoverBaseAllocID recover base alloc id
func (p *PdController) RecoverBaseAllocID(ctx context.Context, id uint64) error {
	return errors.Trace(p.pdHTTPCli.ResetBaseAllocID(ctx, id))
}

// ResetTS reset current ts of pd
func (p *PdController) ResetTS(ctx context.Context, ts uint64) error {
	// reset-ts of PD will never set ts < current pd ts
	// we set force-use-larger=true to allow ts > current pd ts + 24h(on default)
	err := p.pdHTTPCli.ResetTS(ctx, ts, true)
	if err == nil {
		return nil
	}
	if strings.Contains(err.Error(), http.StatusText(http.StatusForbidden)) {
		log.Info("reset-ts returns with status forbidden, ignore")
		return nil
	}
	return errors.Trace(err)
}

// MarkRecovering mark pd into recovering
func (p *PdController) MarkRecovering(ctx context.Context) error {
	return errors.Trace(p.pdHTTPCli.SetSnapshotRecoveringMark(ctx))
}

// UnmarkRecovering unmark pd recovering
func (p *PdController) UnmarkRecovering(ctx context.Context) error {
	return errors.Trace(p.pdHTTPCli.DeleteSnapshotRecoveringMark(ctx))
}

// RegionLabel is the label of a region. This struct is partially copied from
// https://github.com/tikv/pd/blob/783d060861cef37c38cbdcab9777fe95c17907fe/server/schedule/labeler/rules.go#L31.
type RegionLabel struct {
	Key     string `json:"key"`
	Value   string `json:"value"`
	TTL     string `json:"ttl,omitempty"`
	StartAt string `json:"start_at,omitempty"`
}

// LabelRule is the rule to assign labels to a region. This struct is partially copied from
// https://github.com/tikv/pd/blob/783d060861cef37c38cbdcab9777fe95c17907fe/server/schedule/labeler/rules.go#L41.
type LabelRule struct {
	ID       string        `json:"id"`
	Labels   []RegionLabel `json:"labels"`
	RuleType string        `json:"rule_type"`
	Data     any           `json:"data"`
}

// KeyRangeRule contains the start key and end key of the LabelRule. This struct is partially copied from
// https://github.com/tikv/pd/blob/783d060861cef37c38cbdcab9777fe95c17907fe/server/schedule/labeler/rules.go#L62.
type KeyRangeRule struct {
	StartKeyHex string `json:"start_key"` // hex format start key, for marshal/unmarshal
	EndKeyHex   string `json:"end_key"`   // hex format end key, for marshal/unmarshal
}

// PauseSchedulersByKeyRange will pause schedulers for regions in the specific key range.
// This function will spawn a goroutine to keep pausing schedulers periodically until the context is done.
// The return done channel is used to notify the caller that the background goroutine is exited.
func PauseSchedulersByKeyRange(
	ctx context.Context,
	pdHTTPCli pdhttp.Client,
	startKey, endKey []byte,
) (done <-chan struct{}, err error) {
	done, err = pauseSchedulerByKeyRangeWithTTL(ctx, pdHTTPCli, startKey, endKey, pauseTimeout)
	// Wait for the rule to take effect because the PD operator is processed asynchronously.
	// To synchronize this, checking the operator status may not be enough. For details, see
	// https://github.com/pingcap/tidb/issues/49477.
	// Let's use two times default value of `patrol-region-interval` from PD configuration.
	<-time.After(20 * time.Millisecond)
	return done, errors.Trace(err)
}

func pauseSchedulerByKeyRangeWithTTL(
	ctx context.Context,
	pdHTTPCli pdhttp.Client,
	startKey, endKey []byte,
	ttl time.Duration,
) (<-chan struct{}, error) {
	rule := &pdhttp.LabelRule{
		ID: uuid.New().String(),
		Labels: []pdhttp.RegionLabel{{
			Key:   "schedule",
			Value: "deny",
			TTL:   ttl.String(),
		}},
		RuleType: "key-range",
		// Data should be a list of KeyRangeRule when rule type is key-range.
		// See https://github.com/tikv/pd/blob/783d060861cef37c38cbdcab9777fe95c17907fe/server/schedule/labeler/rules.go#L169.
		Data: []KeyRangeRule{{
			StartKeyHex: hex.EncodeToString(startKey),
			EndKeyHex:   hex.EncodeToString(endKey),
		}},
	}
	done := make(chan struct{})

	if err := pdHTTPCli.SetRegionLabelRule(ctx, rule); err != nil {
		close(done)
		return nil, errors.Trace(err)
	}

	go func() {
		defer close(done)
		ticker := time.NewTicker(ttl / 3)
		defer ticker.Stop()
	loop:
		for {
			select {
			case <-ticker.C:
				if err := pdHTTPCli.SetRegionLabelRule(ctx, rule); err != nil {
					if berrors.IsContextCanceled(err) {
						break loop
					}
					log.Warn("pause scheduler by key range failed, ignore it and wait next time pause",
						zap.Error(err))
				}
			case <-ctx.Done():
				break loop
			}
		}
		// Use a new context to avoid the context is canceled by the caller.
		recoverCtx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()
		// Set ttl to 0 to remove the rule.
		rule.Labels[0].TTL = time.Duration(0).String()
		deleteRule := &pdhttp.LabelRulePatch{DeleteRules: []string{rule.ID}}
		if err := pdHTTPCli.PatchRegionLabelRules(recoverCtx, deleteRule); err != nil {
			log.Warn("failed to delete region label rule, the rule will be removed after ttl expires",
				zap.String("rule-id", rule.ID), zap.Duration("ttl", ttl), zap.Error(err))
		}
	}()
	return done, nil
}

// CanPauseSchedulerByKeyRange returns whether the scheduler can be paused by key range.
func (p *PdController) CanPauseSchedulerByKeyRange() bool {
	// We need ttl feature to ensure scheduler can recover from pause automatically.
	return p.version.Compare(minVersionForRegionLabelTTL) >= 0
}

// Close closes the connection to pd.
func (p *PdController) Close() {
	p.pdClient.Close()
	if p.pdHTTPCli != nil {
		// nil in some unit tests
		p.pdHTTPCli.Close()
	}
	if p.schedulerPauseCh != nil {
		close(p.schedulerPauseCh)
	}
}

func (p *PdController) ttlOfPausing() time.Duration {
	if p.SchedulerPauseTTL > 0 {
		return p.SchedulerPauseTTL
	}
	return pauseTimeout
}

// FetchPDVersion get pd version
func FetchPDVersion(ctx context.Context, pdHTTPCli pdhttp.Client) (*semver.Version, error) {
	ver, err := pdHTTPCli.GetPDVersion(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return parseVersion(ver), nil
}
