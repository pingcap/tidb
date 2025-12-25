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

package task

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"net/http"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/docker/go-units"
	"github.com/fatih/color"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/backup"
	"github.com/pingcap/tidb/br/pkg/checkpoint"
	"github.com/pingcap/tidb/br/pkg/conn"
	"github.com/pingcap/tidb/br/pkg/encryption"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/httputil"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/br/pkg/registry"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/restore/ingestrec"
	logclient "github.com/pingcap/tidb/br/pkg/restore/log_client"
	"github.com/pingcap/tidb/br/pkg/restore/tiflashrec"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	advancercfg "github.com/pingcap/tidb/br/pkg/streamhelper/config"
	"github.com/pingcap/tidb/br/pkg/streamhelper/daemon"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/utils/iter"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/cdcutil"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/spf13/pflag"
	"github.com/tikv/client-go/v2/oracle"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	flagYes                = "yes"
	flagCleanUpCompactions = "clean-up-compactions"
	flagUntil              = "until"
	flagStreamJSONOutput   = "json"
	flagStreamTaskName     = "task-name"
	flagStreamStartTS      = "start-ts"
	flagStreamEndTS        = "end-ts"
	flagGCSafePointTTS     = "gc-ttl"
	flagMessage            = "message"

	truncateLockPath   = "truncating.lock"
	hintOnTruncateLock = "There might be another truncate task running, or a truncate task that didn't exit properly. " +
		"You may check the metadata and continue by wait other task finish or manually delete the lock file " + truncateLockPath + " at the external storage."
)

const (
	waitInfoSchemaReloadCheckInterval = 1 * time.Second
	// a million tables should take a few minutes to load all DDL change, making 15 to make sure we don't exit early
	waitInfoSchemaReloadTimeout = 15 * time.Minute
)

var (
	StreamStart    = "log start"
	StreamStop     = "log stop"
	StreamPause    = "log pause"
	StreamResume   = "log resume"
	StreamStatus   = "log status"
	StreamTruncate = "log truncate"
	StreamMetadata = "log metadata"
	StreamCtl      = "log advancer"

	skipSummaryCommandList = map[string]struct{}{
		StreamStatus:   {},
		StreamTruncate: {},
	}

	streamShiftDuration = time.Hour
)

var StreamCommandMap = map[string]func(c context.Context, g glue.Glue, cmdName string, cfg *StreamConfig) error{
	StreamStart:    RunStreamStart,
	StreamStop:     RunStreamStop,
	StreamPause:    RunStreamPause,
	StreamResume:   RunStreamResume,
	StreamStatus:   RunStreamStatus,
	StreamTruncate: RunStreamTruncate,
	StreamMetadata: RunStreamMetadata,
	StreamCtl:      RunStreamAdvancer,
}

// StreamConfig specifies the configure about backup stream
type StreamConfig struct {
	Config

	TaskName string `json:"task-name" toml:"task-name"`

	// StartTS usually equals the tso of full-backup, but user can reset it
	StartTS uint64 `json:"start-ts" toml:"start-ts"`
	EndTS   uint64 `json:"end-ts" toml:"end-ts"`
	// SafePointTTL ensures TiKV can scan entries not being GC at [startTS, currentTS]
	SafePointTTL int64 `json:"safe-point-ttl" toml:"safe-point-ttl"`

	// Spec for the command `truncate`, we should truncate the until when?
	Until              uint64 `json:"until" toml:"until"`
	DryRun             bool   `json:"dry-run" toml:"dry-run"`
	SkipPrompt         bool   `json:"skip-prompt" toml:"skip-prompt"`
	CleanUpCompactions bool   `json:"clean-up-compactions" toml:"clean-up-compactions"`

	// Spec for the command `status`.
	JSONOutput bool `json:"json-output" toml:"json-output"`
	// DumpStatusTo here if not `nil`. Used for test cases.
	DumpStatusTo *[]stream.TaskStatus `json:"-" toml:"-"`

	// Spec for the command `advancer`.
	AdvancerCfg advancercfg.CommandConfig `json:"advancer-config" toml:"advancer-config"`

	// Spec for the command `pause`.
	Message string `json:"message" toml:"message"`
	AsError bool   `json:"as-error" toml:"as-error"`
}

func DefaultStreamConfig(flagsDef func(*pflag.FlagSet)) StreamConfig {
	fs := pflag.NewFlagSet("dummy", pflag.ContinueOnError)
	flagsDef(fs)
	DefineCommonFlags(fs)
	cfg := StreamConfig{}
	err := cfg.ParseFromFlags(fs)
	if err != nil {
		log.Panic("failed to parse backup flags to config", zap.Error(err))
	}
	return cfg
}

func (cfg *StreamConfig) makeStorage(ctx context.Context) (storage.ExternalStorage, error) {
	u, err := storage.ParseBackend(cfg.Storage, &cfg.BackendOptions)
	if err != nil {
		return nil, errors.Trace(err)
	}
	opts := getExternalStorageOptions(&cfg.Config, u)
	storage, err := storage.New(ctx, u, &opts)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return storage, nil
}

// DefineStreamStartFlags defines flags used for `stream start`
func DefineStreamStartFlags(flags *pflag.FlagSet) {
	DefineStreamCommonFlags(flags)

	flags.String(flagStreamStartTS, "",
		"usually equals last full backupTS, used for backup log. Default value is current ts.\n"+
			"support TSO or datetime, e.g. '400036290571534337' or '2018-05-11 01:42:23+0800'.")
	// 999999999999999999 means 2090-11-18 22:07:45
	flags.String(flagStreamEndTS, "999999999999999999", "end ts, indicate stopping observe after endTS"+
		"support TSO or datetime")
	_ = flags.MarkHidden(flagStreamEndTS)
	flags.Int64(flagGCSafePointTTS, utils.DefaultStreamStartSafePointTTL,
		"the TTL (in seconds) that PD holds for BR's GC safepoint")
	_ = flags.MarkHidden(flagGCSafePointTTS)
}

func DefineStreamPauseFlags(flags *pflag.FlagSet) {
	DefineStreamCommonFlags(flags)
	flags.Int64(flagGCSafePointTTS, utils.DefaultStreamPauseSafePointTTL,
		"the TTL (in seconds) that PD holds for BR's GC safepoint")
	flags.String(flagMessage, "", "The message for the pause task.")
}

// DefineStreamCommonFlags define common flags for `stream task`
func DefineStreamCommonFlags(flags *pflag.FlagSet) {
	flags.String(flagStreamTaskName, "", "The task name for the backup log task.")
}

func DefineStreamStatusCommonFlags(flags *pflag.FlagSet) {
	flags.String(flagStreamTaskName, stream.WildCard,
		"The task name for backup stream log. If default, get status of all of tasks",
	)
	flags.Bool(flagStreamJSONOutput, false,
		"Print JSON as the output.",
	)
}

func DefineStreamTruncateLogFlags(flags *pflag.FlagSet) {
	flags.String(flagUntil, "", "Remove all backup data until this TS."+
		"(support TSO or datetime, e.g. '400036290571534337' or '2018-05-11 01:42:23+0800'.)")
	flags.Bool(flagDryRun, false, "Run the command but don't really delete the files.")
	flags.BoolP(flagYes, "y", false, "Skip all prompts and always execute the command.")
	flags.Bool(flagCleanUpCompactions, false, "Clean up compaction files. Including the compacted log files and expired SST files.")
}

func (cfg *StreamConfig) ParseStreamStatusFromFlags(flags *pflag.FlagSet) error {
	var err error
	cfg.JSONOutput, err = flags.GetBool(flagStreamJSONOutput)
	if err != nil {
		return errors.Trace(err)
	}

	if err = cfg.ParseStreamCommonFromFlags(flags); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (cfg *StreamConfig) ParseStreamTruncateFromFlags(flags *pflag.FlagSet) error {
	tsString, err := flags.GetString(flagUntil)
	if err != nil {
		return errors.Trace(err)
	}
	if cfg.Until, err = ParseTSString(tsString, true); err != nil {
		return errors.Trace(err)
	}
	if cfg.SkipPrompt, err = flags.GetBool(flagYes); err != nil {
		return errors.Trace(err)
	}
	if cfg.DryRun, err = flags.GetBool(flagDryRun); err != nil {
		return errors.Trace(err)
	}
	if cfg.CleanUpCompactions, err = flags.GetBool(flagCleanUpCompactions); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// ParseStreamStartFromFlags parse parameters for `stream start`
func (cfg *StreamConfig) ParseStreamStartFromFlags(flags *pflag.FlagSet) error {
	err := cfg.ParseStreamCommonFromFlags(flags)
	if err != nil {
		return errors.Trace(err)
	}

	tsString, err := flags.GetString(flagStreamStartTS)
	if err != nil {
		return errors.Trace(err)
	}

	if cfg.StartTS, err = ParseTSString(tsString, true); err != nil {
		return errors.Trace(err)
	}

	tsString, err = flags.GetString(flagStreamEndTS)
	if err != nil {
		return errors.Trace(err)
	}

	if cfg.EndTS, err = ParseTSString(tsString, true); err != nil {
		return errors.Trace(err)
	}

	if cfg.SafePointTTL, err = flags.GetInt64(flagGCSafePointTTS); err != nil {
		return errors.Trace(err)
	}

	if cfg.SafePointTTL <= 0 {
		cfg.SafePointTTL = utils.DefaultStreamStartSafePointTTL
	}

	return nil
}

// ParseStreamPauseFromFlags parse parameters for `stream pause`
func (cfg *StreamConfig) ParseStreamPauseFromFlags(flags *pflag.FlagSet) error {
	err := cfg.ParseStreamCommonFromFlags(flags)
	if err != nil {
		return errors.Trace(err)
	}

	if cfg.Message, err = flags.GetString(flagMessage); err != nil {
		return errors.Trace(err)
	}

	if cfg.SafePointTTL, err = flags.GetInt64(flagGCSafePointTTS); err != nil {
		return errors.Trace(err)
	}
	if cfg.SafePointTTL <= 0 {
		cfg.SafePointTTL = utils.DefaultStreamPauseSafePointTTL
	}
	return nil
}

// ParseStreamCommonFromFlags parse parameters for `stream task`
func (cfg *StreamConfig) ParseStreamCommonFromFlags(flags *pflag.FlagSet) error {
	var err error

	cfg.TaskName, err = flags.GetString(flagStreamTaskName)
	if err != nil {
		return errors.Trace(err)
	}

	if len(cfg.TaskName) <= 0 {
		return errors.Annotate(berrors.ErrInvalidArgument, "Miss parameters task-name")
	}
	return nil
}

type streamMgr struct {
	cfg     *StreamConfig
	mgr     *conn.Mgr
	bc      *backup.Client
	httpCli *http.Client
}

func NewStreamMgr(ctx context.Context, cfg *StreamConfig, g glue.Glue, isStreamStart bool) (*streamMgr, error) {
	mgr, err := NewMgr(ctx, g, cfg.PD, cfg.TLS, GetKeepalive(&cfg.Config),
		cfg.CheckRequirements, false, conn.StreamVersionChecker)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer func() {
		if err != nil {
			mgr.Close()
		}
	}()

	// only stream start command needs Storage
	streamManager := &streamMgr{
		cfg: cfg,
		mgr: mgr,
	}
	if isStreamStart {
		client := backup.NewBackupClient(ctx, mgr)

		backend, err := storage.ParseBackend(cfg.Storage, &cfg.BackendOptions)
		if err != nil {
			return nil, errors.Trace(err)
		}

		opts := storage.ExternalStorageOptions{
			NoCredentials:            cfg.NoCreds,
			SendCredentials:          cfg.SendCreds,
			CheckS3ObjectLockOptions: true,
		}
		if err = client.SetStorage(ctx, backend, &opts); err != nil {
			return nil, errors.Trace(err)
		}
		streamManager.bc = client

		// create http client to do some requirements check.
		streamManager.httpCli = httputil.NewClient(mgr.GetTLSConfig())
	}
	return streamManager, nil
}

func (s *streamMgr) close() {
	s.mgr.Close()
}

func (s *streamMgr) checkLock(ctx context.Context) (bool, error) {
	return s.bc.GetStorage().FileExists(ctx, metautil.LockFile)
}

func (s *streamMgr) setLock(ctx context.Context) error {
	return s.bc.SetLockFile(ctx)
}

// adjustAndCheckStartTS checks that startTS should be smaller than currentTS,
// and endTS is larger than currentTS.
func (s *streamMgr) adjustAndCheckStartTS(ctx context.Context) error {
	currentTS, err := s.mgr.GetCurrentTsFromPD(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	// set currentTS to startTS as a default value
	if s.cfg.StartTS == 0 {
		s.cfg.StartTS = currentTS
	}

	if currentTS < s.cfg.StartTS {
		return errors.Annotatef(berrors.ErrInvalidArgument,
			"invalid timestamps, startTS %d should be smaller than currentTS %d",
			s.cfg.StartTS, currentTS)
	}
	if s.cfg.EndTS <= currentTS {
		return errors.Annotatef(berrors.ErrInvalidArgument,
			"invalid timestamps, endTS %d should be larger than currentTS %d",
			s.cfg.EndTS, currentTS)
	}

	return nil
}

// checkImportTaskRunning checks whether there is any import task running.
func (s *streamMgr) checkImportTaskRunning(ctx context.Context, etcdCLI *clientv3.Client) error {
	list, err := utils.GetImportTasksFrom(ctx, etcdCLI)
	if err != nil {
		return errors.Trace(err)
	}
	if !list.Empty() {
		return errors.Errorf("There are some lightning/restore tasks running: %s"+
			"please stop or wait finishing at first. "+
			"If the lightning/restore task is forced to terminate by system, "+
			"please wait for ttl to decrease to 0.", list.MessageToUser())
	}
	return nil
}

// setGCSafePoint sets the server safe point to PD.
func (s *streamMgr) setGCSafePoint(ctx context.Context, sp utils.BRServiceSafePoint) error {
	err := utils.CheckGCSafePoint(ctx, s.mgr.GetPDClient(), sp.BackupTS)
	if err != nil {
		return errors.Annotatef(err,
			"failed to check gc safePoint, ts %v", sp.BackupTS)
	}

	err = utils.UpdateServiceSafePoint(ctx, s.mgr.GetPDClient(), sp)
	if err != nil {
		return errors.Trace(err)
	}

	log.Info("set stream safePoint", zap.Object("safePoint", sp))
	return nil
}

func (s *streamMgr) buildObserveRanges() ([]kv.KeyRange, error) {
	dRanges, err := stream.BuildObserveDataRanges(
		s.mgr.GetStorage(),
		s.cfg.FilterStr,
		s.cfg.TableFilter,
		s.cfg.StartTS,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}

	mRange := stream.BuildObserveMetaRange()
	rs := append([]kv.KeyRange{*mRange}, dRanges...)
	slices.SortFunc(rs, func(i, j kv.KeyRange) int {
		return bytes.Compare(i.StartKey, j.StartKey)
	})

	return rs, nil
}

func (s *streamMgr) backupFullSchemas(ctx context.Context) error {
	clusterVersion, err := s.mgr.GetClusterVersion(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	metaWriter := metautil.NewMetaWriter(s.bc.GetStorage(), metautil.MetaFileSize, true, metautil.MetaFile, nil)
	metaWriter.Update(func(m *backuppb.BackupMeta) {
		// save log startTS to backupmeta file
		m.StartVersion = s.cfg.StartTS
		m.ClusterId = s.bc.GetClusterID()
		m.ClusterVersion = clusterVersion
	})

	if err = metaWriter.FlushBackupMeta(ctx); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (s *streamMgr) checkStreamStartEnable(ctx context.Context) error {
	supportStream, err := s.mgr.IsLogBackupEnabled(ctx, s.httpCli)
	if err != nil {
		return errors.Trace(err)
	}
	if !supportStream {
		return errors.New("Unable to create task about log-backup. " +
			"please set TiKV config `log-backup.enable` to true and restart TiKVs.")
	}

	return nil
}

type RestoreGCFunc func(string) error

// DisableGC disables and returns a function that can enable gc back.
// gc.ratio-threshold = "-1.0", which represents disable gc in TiKV.
func DisableGC(g glue.Glue, store kv.Storage) (RestoreGCFunc, string, error) {
	se, err := g.CreateSession(store)
	if err != nil {
		return nil, "", errors.Trace(err)
	}

	execCtx := se.GetSessionCtx().GetRestrictedSQLExecutor()
	oldRatio, err := utils.GetGcRatio(execCtx)
	if err != nil {
		return nil, "", errors.Trace(err)
	}

	err = utils.SetGcRatio(execCtx, utils.DisabledGcRatioVal)
	if err != nil {
		return nil, "", errors.Trace(err)
	}

	return func(ratio string) error {
		return utils.SetGcRatio(execCtx, ratio)
	}, oldRatio, nil
}

// RunStreamCommand run all kinds of `stream task`
func RunStreamCommand(
	ctx context.Context,
	g glue.Glue,
	cmdName string,
	cfg *StreamConfig,
) error {
	cfg.Config.adjust()
	defer func() {
		if _, ok := skipSummaryCommandList[cmdName]; !ok {
			summary.Summary(cmdName)
		}
	}()
	commandFn, exist := StreamCommandMap[cmdName]
	if !exist {
		return errors.Annotatef(berrors.ErrInvalidArgument, "invalid command %s", cmdName)
	}

	if err := commandFn(ctx, g, cmdName, cfg); err != nil {
		log.Error("failed to run stream command", zap.String("command", cmdName), zap.Error(err))
		summary.SetSuccessStatus(false)
		summary.CollectFailureUnit(cmdName, err)
		return err
	}
	summary.SetSuccessStatus(true)
	return nil
}

// RunStreamStart specifies starting a stream task
func RunStreamStart(
	c context.Context,
	g glue.Glue,
	cmdName string,
	cfg *StreamConfig,
) error {
	ctx, cancelFn := context.WithCancel(c)
	defer cancelFn()

	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("task.RunStreamStart", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	streamMgr, err := NewStreamMgr(ctx, cfg, g, true)
	if err != nil {
		return errors.Trace(err)
	}
	defer streamMgr.close()

	if err = streamMgr.checkStreamStartEnable(ctx); err != nil {
		return errors.Trace(err)
	}
	if err = streamMgr.adjustAndCheckStartTS(ctx); err != nil {
		return errors.Trace(err)
	}

	etcdCLI, err := dialEtcdWithCfg(ctx, cfg.Config)
	if err != nil {
		return errors.Trace(err)
	}
	cli := streamhelper.NewMetaDataClient(etcdCLI)
	defer func() {
		if closeErr := cli.Close(); closeErr != nil {
			log.Warn("failed to close etcd client", zap.Error(closeErr))
		}
	}()

	// check if any import/restore task is running, it's not allowed to start log backup
	// while restore is ongoing.
	if err = streamMgr.checkImportTaskRunning(ctx, cli.Client); err != nil {
		return errors.Trace(err)
	}

	// It supports single stream log task currently.
	if count, err := cli.GetTaskCount(ctx); err != nil {
		return errors.Trace(err)
	} else if count > 0 {
		return errors.Annotate(berrors.ErrStreamLogTaskExist, "failed to start the log backup, allow only one running task")
	}

	// make sure external file lock is available
	locked, err := streamMgr.checkLock(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	// locked means this is a stream task restart. Or create a new stream task.
	if locked {
		logInfo, err := getLogInfo(ctx, &cfg.Config)
		if err != nil {
			return errors.Trace(err)
		}
		if logInfo.clusterID > 0 && logInfo.clusterID != streamMgr.bc.GetClusterID() {
			return errors.Annotatef(berrors.ErrInvalidArgument,
				"the stream log files from cluster ID:%v and current cluster ID:%v ",
				logInfo.clusterID, streamMgr.bc.GetClusterID())
		}

		cfg.StartTS = logInfo.logMaxTS
		if err = streamMgr.setGCSafePoint(
			ctx,
			utils.BRServiceSafePoint{
				ID:       utils.MakeSafePointID(),
				TTL:      cfg.SafePointTTL,
				BackupTS: cfg.StartTS,
			},
		); err != nil {
			return errors.Trace(err)
		}
	} else {
		if err = streamMgr.setGCSafePoint(
			ctx,
			utils.BRServiceSafePoint{
				ID:       utils.MakeSafePointID(),
				TTL:      cfg.SafePointTTL,
				BackupTS: cfg.StartTS,
			},
		); err != nil {
			return errors.Trace(err)
		}
		if err = streamMgr.setLock(ctx); err != nil {
			return errors.Trace(err)
		}
		if err = streamMgr.backupFullSchemas(ctx); err != nil {
			return errors.Trace(err)
		}
	}

	ranges, err := streamMgr.buildObserveRanges()
	if err != nil {
		return errors.Trace(err)
	} else if len(ranges) == 0 {
		// nothing to backup
		pdAddress := strings.Join(cfg.PD, ",")
		log.Warn("Nothing to observe, maybe connected to cluster for restoring",
			zap.String("PD address", pdAddress))
		return errors.Annotate(berrors.ErrInvalidArgument, "nothing need to observe")
	}

	securityConfig := generateSecurityConfig(cfg)
	ti := streamhelper.TaskInfo{
		PBInfo: backuppb.StreamBackupTaskInfo{
			Storage:         streamMgr.bc.GetStorageBackend(),
			StartTs:         cfg.StartTS,
			EndTs:           cfg.EndTS,
			Name:            cfg.TaskName,
			TableFilter:     cfg.FilterStr,
			CompressionType: backuppb.CompressionType_ZSTD,
			SecurityConfig:  &securityConfig,
		},
		Ranges:  ranges,
		Pausing: false,
	}
	if err = cli.PutTask(ctx, ti); err != nil {
		return errors.Trace(err)
	}
	summary.Log(cmdName, ti.ZapTaskInfo()...)
	return nil
}

func generateSecurityConfig(cfg *StreamConfig) backuppb.StreamBackupTaskSecurityConfig {
	if len(cfg.LogBackupCipherInfo.CipherKey) > 0 && utils.IsEffectiveEncryptionMethod(cfg.LogBackupCipherInfo.CipherType) {
		return backuppb.StreamBackupTaskSecurityConfig{
			Encryption: &backuppb.StreamBackupTaskSecurityConfig_PlaintextDataKey{
				PlaintextDataKey: &backuppb.CipherInfo{
					CipherType: cfg.LogBackupCipherInfo.CipherType,
					CipherKey:  cfg.LogBackupCipherInfo.CipherKey,
				},
			},
		}
	}
	if len(cfg.MasterKeyConfig.MasterKeys) > 0 && utils.IsEffectiveEncryptionMethod(cfg.MasterKeyConfig.EncryptionType) {
		return backuppb.StreamBackupTaskSecurityConfig{
			Encryption: &backuppb.StreamBackupTaskSecurityConfig_MasterKeyConfig{
				MasterKeyConfig: &backuppb.MasterKeyConfig{
					EncryptionType: cfg.MasterKeyConfig.EncryptionType,
					MasterKeys:     cfg.MasterKeyConfig.MasterKeys,
				},
			},
		}
	}
	return backuppb.StreamBackupTaskSecurityConfig{}
}

func RunStreamMetadata(
	c context.Context,
	g glue.Glue,
	cmdName string,
	cfg *StreamConfig,
) error {
	ctx, cancelFn := context.WithCancel(c)
	defer cancelFn()

	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan(
			"task.RunStreamCheckLog",
			opentracing.ChildOf(span.Context()),
		)
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	logInfo, err := getLogInfo(ctx, &cfg.Config)
	if err != nil {
		return errors.Trace(err)
	}

	logMinDate := utils.FormatDate(oracle.GetTimeFromTS(logInfo.logMinTS))
	logMaxDate := utils.FormatDate(oracle.GetTimeFromTS(logInfo.logMaxTS))
	summary.Log(cmdName, zap.Uint64("log-min-ts", logInfo.logMinTS),
		zap.String("log-min-date", logMinDate),
		zap.Uint64("log-max-ts", logInfo.logMaxTS),
		zap.String("log-max-date", logMaxDate),
	)
	return nil
}

// RunStreamStop specifies stoping a stream task
func RunStreamStop(
	c context.Context,
	g glue.Glue,
	cmdName string,
	cfg *StreamConfig,
) error {
	ctx, cancelFn := context.WithCancel(c)
	defer cancelFn()

	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan(
			"task.RunStreamStop",
			opentracing.ChildOf(span.Context()),
		)
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	streamMgr, err := NewStreamMgr(ctx, cfg, g, false)
	if err != nil {
		return errors.Trace(err)
	}
	defer streamMgr.close()

	etcdCLI, err := dialEtcdWithCfg(ctx, cfg.Config)
	if err != nil {
		return errors.Trace(err)
	}
	cli := streamhelper.NewMetaDataClient(etcdCLI)
	defer func() {
		if closeErr := cli.Close(); closeErr != nil {
			log.Warn("failed to close etcd client", zap.Error(closeErr))
		}
	}()
	// to add backoff
	ti, err := cli.GetTask(ctx, cfg.TaskName)
	if err != nil {
		return errors.Trace(err)
	}

	if err = cli.DeleteTask(ctx, cfg.TaskName); err != nil {
		return errors.Trace(err)
	}

	if err := streamMgr.setGCSafePoint(ctx,
		utils.BRServiceSafePoint{
			ID:       buildPauseSafePointName(ti.Info.Name),
			TTL:      0, // 0 means remove this service safe point.
			BackupTS: math.MaxUint64,
		},
	); err != nil {
		log.Warn("failed to remove safe point", zap.String("error", err.Error()))
	}

	summary.Log(cmdName, logutil.StreamBackupTaskInfo(&ti.Info))
	return nil
}

// RunStreamPause specifies pausing a stream task.
func RunStreamPause(
	c context.Context,
	g glue.Glue,
	cmdName string,
	cfg *StreamConfig,
) error {
	ctx, cancelFn := context.WithCancel(c)
	defer cancelFn()

	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan(
			"task.RunStreamPause",
			opentracing.ChildOf(span.Context()),
		)
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	streamMgr, err := NewStreamMgr(ctx, cfg, g, false)
	if err != nil {
		return errors.Trace(err)
	}
	defer streamMgr.close()

	etcdCLI, err := dialEtcdWithCfg(ctx, cfg.Config)
	if err != nil {
		return errors.Trace(err)
	}
	cli := streamhelper.NewMetaDataClient(etcdCLI)
	defer func() {
		if closeErr := cli.Close(); closeErr != nil {
			log.Warn("failed to close etcd client", zap.Error(closeErr))
		}
	}()
	// to add backoff
	ti, isPaused, err := cli.GetTaskWithPauseStatus(ctx, cfg.TaskName)
	if err != nil {
		return errors.Trace(err)
	} else if isPaused {
		return errors.Annotatef(berrors.ErrKVUnknown, "The task %s is paused already.", cfg.TaskName)
	}

	globalCheckPointTS, err := ti.GetGlobalCheckPointTS(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	if err = streamMgr.setGCSafePoint(
		ctx,
		utils.BRServiceSafePoint{
			ID:       buildPauseSafePointName(ti.Info.Name),
			TTL:      cfg.SafePointTTL,
			BackupTS: globalCheckPointTS,
		},
	); err != nil {
		return errors.Trace(err)
	}

	opts := []streamhelper.PauseTaskOption{}
	if len(cfg.Message) > 0 {
		opts = append(opts, streamhelper.PauseWithMessage(cfg.Message))
	}
	if cfg.AsError {
		opts = append(opts, streamhelper.PauseWithErrorSeverity)
	}
	err = cli.PauseTask(ctx, cfg.TaskName, opts...)
	if err != nil {
		return errors.Trace(err)
	}

	summary.Log(cmdName, logutil.StreamBackupTaskInfo(&ti.Info))
	return nil
}

// RunStreamResume specifies resuming a stream task.
func RunStreamResume(
	c context.Context,
	g glue.Glue,
	cmdName string,
	cfg *StreamConfig,
) error {
	ctx, cancelFn := context.WithCancel(c)
	defer cancelFn()

	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan(
			"task.RunStreamResume",
			opentracing.ChildOf(span.Context()),
		)
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	streamMgr, err := NewStreamMgr(ctx, cfg, g, false)
	if err != nil {
		return errors.Trace(err)
	}
	defer streamMgr.close()

	etcdCLI, err := dialEtcdWithCfg(ctx, cfg.Config)
	if err != nil {
		return errors.Trace(err)
	}
	cli := streamhelper.NewMetaDataClient(etcdCLI)
	defer func() {
		if closeErr := cli.Close(); closeErr != nil {
			log.Warn("failed to close etcd client", zap.Error(closeErr))
		}
	}()
	// to add backoff
	ti, isPaused, err := cli.GetTaskWithPauseStatus(ctx, cfg.TaskName)
	if err != nil {
		return errors.Trace(err)
	} else if !isPaused {
		return errors.Annotatef(berrors.ErrKVUnknown,
			"The task %s is active already.", cfg.TaskName)
	}

	globalCheckPointTS, err := ti.GetGlobalCheckPointTS(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	err = utils.CheckGCSafePoint(ctx, streamMgr.mgr.GetPDClient(), globalCheckPointTS)
	if err != nil {
		return errors.Annotatef(err, "the global checkpoint ts: %v(%s) has been gc. ",
			globalCheckPointTS, oracle.GetTimeFromTS(globalCheckPointTS))
	}

	err = cli.ResumeTask(ctx, cfg.TaskName)
	if err != nil {
		return errors.Trace(err)
	}

	err = cli.CleanLastErrorOfTask(ctx, cfg.TaskName)
	if err != nil {
		return err
	}

	if err := streamMgr.setGCSafePoint(ctx,
		utils.BRServiceSafePoint{
			ID:       buildPauseSafePointName(ti.Info.Name),
			TTL:      utils.DefaultStreamStartSafePointTTL,
			BackupTS: globalCheckPointTS,
		},
	); err != nil {
		log.Warn("failed to remove safe point",
			zap.Uint64("safe-point", globalCheckPointTS), zap.String("error", err.Error()))
	}

	summary.Log(cmdName, logutil.StreamBackupTaskInfo(&ti.Info))
	return nil
}

func RunStreamAdvancer(c context.Context, g glue.Glue, cmdName string, cfg *StreamConfig) error {
	ctx, cancel := context.WithCancel(c)
	defer cancel()
	log.Info("starting", zap.String("cmd", cmdName))

	mgr, err := NewMgr(ctx, g, cfg.PD, cfg.TLS, GetKeepalive(&cfg.Config),
		cfg.CheckRequirements, false, conn.StreamVersionChecker)
	if err != nil {
		return err
	}
	defer mgr.Close()

	etcdCLI, err := dialEtcdWithCfg(ctx, cfg.Config)
	if err != nil {
		return err
	}
	env := streamhelper.CliEnv(mgr.StoreManager, mgr.GetStore(), etcdCLI)
	advancer := streamhelper.NewCommandCheckpointAdvancer(env)
	advancer.UpdateConfig(&cfg.AdvancerCfg)
	ownerMgr := streamhelper.OwnerManagerForLogBackup(ctx, etcdCLI)
	defer func() {
		ownerMgr.Close()
	}()
	advancerd := daemon.New(advancer, ownerMgr, cfg.AdvancerCfg.TickDuration)
	loop, err := advancerd.Begin(ctx)
	if err != nil {
		return err
	}
	if cfg.AdvancerCfg.OwnershipCycleInterval > 0 {
		err = advancerd.ForceToBeOwner(ctx)
		if err != nil {
			return err
		}
		log.Info("command line advancer forced to be the owner")
		go runOwnershipCycle(ctx, advancerd, cfg.AdvancerCfg.OwnershipCycleInterval, true)
	}
	loop()
	return nil
}

// runOwnershipCycle handles the periodic cycling of ownership for the advancer
func runOwnershipCycle(ctx context.Context, advancerd *daemon.OwnerDaemon, cycleDuration time.Duration, isOwner bool) {
	ticker := time.NewTicker(cycleDuration)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !isOwner {
				// try to become owner
				if err := advancerd.ForceToBeOwner(ctx); err != nil {
					log.Error("command line advancer failed to force ownership", zap.Error(err))
					continue
				}
				log.Info("command line advancer forced to be the owner")
				isOwner = true
			} else {
				// retire from being owner
				advancerd.RetireIfOwner()
				log.Info("command line advancer retired from being owner")
				isOwner = false
			}
		}
	}
}

func checkConfigForStatus(pd []string) error {
	if len(pd) == 0 {
		return errors.Annotatef(berrors.ErrInvalidArgument,
			"the command needs access to PD, please specify `-u` or `--pd`")
	}

	return nil
}

// makeStatusController makes the status controller via some config.
// this should better be in the `stream` package but it is impossible because of cyclic requirements.
func makeStatusController(ctx context.Context, cfg *StreamConfig, g glue.Glue) (*stream.StatusController, error) {
	console := glue.GetConsole(g)
	etcdCLI, err := dialEtcdWithCfg(ctx, cfg.Config)
	if err != nil {
		return nil, err
	}
	cli := streamhelper.NewMetaDataClient(etcdCLI)
	var printer stream.TaskPrinter
	if !cfg.JSONOutput {
		printer = stream.PrintTaskByTable(console)
	} else {
		printer = stream.PrintTaskWithJSON(console)
	}
	mgr, err := NewMgr(ctx, g, cfg.PD, cfg.TLS, GetKeepalive(&cfg.Config),
		cfg.CheckRequirements, false, conn.StreamVersionChecker)
	if err != nil {
		return nil, err
	}
	if cfg.DumpStatusTo != nil {
		printer = stream.TeeTaskPrinter(printer, cfg.DumpStatusTo)
	}
	ctl := stream.NewStatusController(cli, mgr, printer)
	return ctl, nil
}

// RunStreamStatus get status for a specific stream task
func RunStreamStatus(
	c context.Context,
	g glue.Glue,
	cmdName string,
	cfg *StreamConfig,
) error {
	ctx, cancelFn := context.WithCancel(c)
	defer cancelFn()

	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan(
			"task.RunStreamStatus",
			opentracing.ChildOf(span.Context()),
		)
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	if err := checkConfigForStatus(cfg.PD); err != nil {
		return err
	}
	ctl, err := makeStatusController(ctx, cfg, g)
	if err != nil {
		return err
	}

	defer func() {
		if closeErr := ctl.Close(); closeErr != nil {
			log.Warn("failed to close etcd client", zap.Error(closeErr))
		}
	}()
	return ctl.PrintStatusOfTask(ctx, cfg.TaskName)
}

// RunStreamTruncate truncates the log that belong to (0, until-ts)
func RunStreamTruncate(c context.Context, g glue.Glue, cmdName string, cfg *StreamConfig) (err error) {
	console := glue.GetConsole(g)
	em := color.New(color.Bold).SprintFunc()
	warn := color.New(color.Bold, color.FgHiRed).SprintFunc()
	formatTS := func(ts uint64) string {
		return oracle.GetTimeFromTS(ts).Format("2006-01-02 15:04:05.0000")
	}
	if cfg.Until == 0 {
		return errors.Annotatef(berrors.ErrInvalidArgument, "please provide the `--until` ts")
	}

	ctx, cancelFn := context.WithCancel(c)
	defer cancelFn()

	extStorage, err := cfg.makeStorage(ctx)
	if err != nil {
		return err
	}
	lock, err := storage.TryLockRemote(ctx, extStorage, truncateLockPath, hintOnTruncateLock)
	if err != nil {
		return err
	}
	defer utils.WithCleanUp(&err, 10*time.Second, func(ctx context.Context) error {
		return lock.Unlock(ctx)
	})

	sp, err := stream.GetTSFromFile(ctx, extStorage, stream.TruncateSafePointFileName)
	if err != nil {
		return err
	}

	if cfg.Until < sp {
		console.Println("According to the log, you have truncated log backup data before", em(formatTS(sp)))
		if !cfg.SkipPrompt && !console.PromptBool("Continue? ") {
			return nil
		}
	}

	if cfg.CleanUpCompactions {
		est := stream.MigrationExtension(extStorage)
		est.Hooks = stream.NewProgressBarHooks(console)
		newSN := math.MaxInt
		optPrompt := stream.MMOptInteractiveCheck(func(ctx context.Context, m *backuppb.Migration) bool {
			console.Println("We are going to do the following: ")
			tbl := console.CreateTable()
			est.AddMigrationToTable(ctx, m, tbl)
			tbl.Print()
			return console.PromptBool("Continue? ")
		})
		optAppend := stream.MMOptAppendPhantomMigration(backuppb.Migration{TruncatedTo: cfg.Until})
		opts := []stream.MergeAndMigrateToOpt{optPrompt, optAppend, stream.MMOptAlwaysRunTruncate()}
		var res stream.MergeAndMigratedTo
		if cfg.DryRun {
			est.DryRun(func(me stream.MigrationExt) {
				res = me.MergeAndMigrateTo(ctx, newSN, opts...)
			})
		} else {
			res = est.MergeAndMigrateTo(ctx, newSN, opts...)
		}
		if len(res.Warnings) > 0 {
			glue.PrintList(console, "the following errors happened", res.Warnings, 10)
		}
		return nil
	}

	readMetaDone := console.ShowTask("Reading Metadata... ", glue.WithTimeCost())
	metas := stream.StreamMetadataSet{
		MetadataDownloadBatchSize: cfg.MetadataDownloadBatchSize,
		Helper:                    stream.NewMetadataHelper(),
		DryRun:                    cfg.DryRun,
	}
	shiftUntilTS, err := metas.LoadUntilAndCalculateShiftTS(ctx, extStorage, cfg.Until)
	if err != nil {
		return err
	}
	readMetaDone()

	var (
		fileCount int    = 0
		kvCount   int64  = 0
		totalSize uint64 = 0
	)

	metas.IterateFilesFullyBefore(shiftUntilTS, func(d *stream.FileGroupInfo) (shouldBreak bool) {
		fileCount++
		totalSize += d.Length
		kvCount += d.KVCount
		return
	})
	console.Printf("We are going to truncate %s files, up to TS %s.\n",
		em(fileCount),
		em(formatTS(cfg.Until)),
	)
	if !cfg.SkipPrompt && !console.PromptBool(warn("Are you sure?")) {
		return nil
	}

	if cfg.Until > sp && !cfg.DryRun {
		if err := stream.SetTSToFile(
			ctx, extStorage, cfg.Until, stream.TruncateSafePointFileName); err != nil {
			return err
		}
	}

	// begin to remove log restore table IDs blocklist files
	removeLogRestoreTableIDsMarkerFilesDone := console.ShowTask("Removing log restore table IDs blocklist files...", glue.WithTimeCost())
	defer func() {
		if removeLogRestoreTableIDsMarkerFilesDone != nil {
			removeLogRestoreTableIDsMarkerFilesDone()
		}
	}()
	if err := restore.TruncateLogRestoreTableIDsBlocklistFiles(ctx, extStorage, cfg.Until); err != nil {
		return errors.Trace(err)
	}
	removeLogRestoreTableIDsMarkerFilesDone()
	removeLogRestoreTableIDsMarkerFilesDone = nil

	// begin to remove
	p := console.StartProgressBar(
		"Truncating Data Files and Metadata", fileCount,
		glue.WithTimeCost(),
		glue.WithConstExtraField("kv-count", kvCount),
		glue.WithConstExtraField("kv-size", fmt.Sprintf("%d(%s)", totalSize, units.HumanSize(float64(totalSize)))),
	)
	defer p.Close()

	notDeleted, err := metas.RemoveDataFilesAndUpdateMetadataInBatch(ctx, shiftUntilTS, extStorage, p.IncBy)
	if err != nil {
		return err
	}

	if err := p.Wait(ctx); err != nil {
		return err
	}

	if len(notDeleted) > 0 {
		const keepFirstNFailure = 16
		console.Println("Files below are not deleted due to error, you may clear it manually, check log for detail error:")
		console.Println("- Total", em(len(notDeleted)), "items.")
		if len(notDeleted) > keepFirstNFailure {
			console.Println("-", em(len(notDeleted)-keepFirstNFailure), "items omitted.")
			// TODO: maybe don't add them at the very first.
			notDeleted = notDeleted[:keepFirstNFailure]
		}
		for _, f := range notDeleted {
			console.Println(f)
		}
	}

	return nil
}

// checkConflictingLogBackup checks whether there is a log backup task running.
// If so, return an error.
// If the execution is PITR restore, returns the external storage backend of taskInfos to record log restore table ids marker.
func checkConflictingLogBackup(ctx context.Context, cfg *RestoreConfig, streamRestore bool, etcdCLI *clientv3.Client) (*backuppb.StorageBackend, error) {
	if err := checkConfigForStatus(cfg.PD); err != nil {
		return nil, err
	}

	cli := streamhelper.NewMetaDataClient(etcdCLI)
	// check log backup task
	tasks, err := cli.GetAllTasks(ctx)
	if err != nil {
		return nil, err
	}
	if streamRestore && len(tasks) > 0 {
		if tasks[0].Info.Storage == nil {
			return nil, errors.Annotatef(berrors.ErrStreamLogTaskHasNoStorage,
				"cannot save log restore table IDs blocklist file because the external storage backend of the task[%s] is empty", tasks[0].Info.Name)
		}
		return tasks[0].Info.Storage, nil
	}
	for _, task := range tasks {
		if err := checkTaskCompat(cfg, task); err != nil {
			return nil, err
		}
	}

	return nil, nil
}

func checkTaskCompat(cfg *RestoreConfig, task streamhelper.Task) error {
	baseErr := errors.Errorf("log backup task is running: %s, and isn't compatible with your restore."+
		"You may check the extra information to get rid of this. If that doesn't work, you may "+
		"stop the task before restore, and after the restore operation finished, "+
		"create log-backup task again and create a full backup on this cluster.", task.Info.Name)
	if !cfg.UserFiltered() {
		return errors.Annotate(baseErr,
			"you want to restore a whole cluster, you may use `-f` or `restore table|database` to "+
				"specify the tables to restore to continue")
	}
	if cfg.LocalEncryptionEnabled() {
		return errors.Annotate(baseErr, "the data you want to restore is encrypted, they cannot be copied to the log storage")
	}
	if task.Info.GetSecurityConfig().GetEncryption() != nil {
		return errors.Annotate(baseErr, "the running log backup task is encrypted, the data copied to the log storage cannot work")
	}
	return nil
}

func checkIncompatibleChangefeed(ctx context.Context, backupTS uint64, etcdCLI *clientv3.Client) error {
	nameSet, err := cdcutil.GetIncompatibleChangefeedsWithSafeTS(ctx, etcdCLI, backupTS)
	if err != nil {
		return err
	}
	if !nameSet.Empty() {
		return errors.Errorf("%splease remove changefeed(s) before restore", nameSet.MessageToUser())
	}
	return nil
}

// RunStreamRestore is the entry point to do PiTR restore. It can optionally start a full/snapshot restore followed
// by the log restore.
func RunStreamRestore(
	c context.Context,
	mgr *conn.Mgr,
	g glue.Glue,
	cfg *RestoreConfig,
) (err error) {
	ctx, cancelFn := context.WithCancel(c)
	defer cancelFn()

	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("task.RunStreamRestore", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}
	_, s, err := GetStorage(ctx, cfg.Config.Storage, &cfg.Config)
	if err != nil {
		return errors.Trace(err)
	}
	logInfo, err := getLogInfoFromStorage(ctx, s)
	if err != nil {
		return errors.Trace(err)
	}

	// if not set by user, restore to the max TS available
	if cfg.RestoreTS == 0 {
		cfg.RestoreTS = logInfo.logMaxTS
		cfg.IsRestoredTSUserSpecified = false
	} else {
		cfg.IsRestoredTSUserSpecified = true
	}
	cfg.UpstreamClusterID = logInfo.clusterID

	if len(cfg.FullBackupStorage) > 0 {
		startTS, fullClusterID, err := getFullBackupTS(ctx, cfg)
		if err != nil {
			return errors.Trace(err)
		}
		if logInfo.clusterID > 0 && fullClusterID > 0 && logInfo.clusterID != fullClusterID {
			return errors.Annotatef(berrors.ErrInvalidArgument,
				"the full snapshot(from cluster ID:%v) and log(from cluster ID:%v) come from different cluster.",
				fullClusterID, logInfo.clusterID)
		}

		cfg.StartTS = startTS
		if cfg.StartTS < logInfo.logMinTS {
			return errors.Annotatef(berrors.ErrInvalidArgument,
				"it has gap between full backup ts:%d(%s) and log backup ts:%d(%s). ",
				cfg.StartTS, oracle.GetTimeFromTS(cfg.StartTS),
				logInfo.logMinTS, oracle.GetTimeFromTS(logInfo.logMinTS))
		}
	}

	log.Info("start point in time restore",
		zap.Uint64("restore-from", cfg.StartTS), zap.Uint64("restore-to", cfg.RestoreTS),
		zap.Uint64("log-min-ts", logInfo.logMinTS), zap.Uint64("log-max-ts", logInfo.logMaxTS))
	if err := checkLogRange(cfg.StartTS, cfg.RestoreTS, logInfo.logMinTS, logInfo.logMaxTS); err != nil {
		return errors.Trace(err)
	}

	// register task if needed
	// will potentially override restoredTS
	err = RegisterRestoreIfNeeded(ctx, cfg, PointRestoreCmd, mgr.GetDomain())
	if err != nil {
		return errors.Trace(err)
	}

	taskInfo, err := generatePiTRTaskInfo(ctx, mgr, g, cfg, cfg.RestoreID)
	if err != nil {
		return errors.Trace(err)
	}

	cfg.adjustRestoreConfigForStreamRestore()
	cfg.tiflashRecorder = tiflashrec.New()
	logClient, err := createLogClient(ctx, g, cfg, mgr)
	if err != nil {
		return errors.Trace(err)
	}
	defer logClient.Close(ctx)

	ddlFiles, err := logClient.LoadDDLFiles(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	// TODO: pitr filtered restore doesn't support restore system table yet
	if cfg.ExplicitFilter {
		if cfg.TableFilter.MatchSchema(mysql.SystemDB) || cfg.TableFilter.MatchSchema(mysql.SysDB) {
			return errors.Annotatef(berrors.ErrInvalidArgument,
				"PiTR doesn't support custom filter to include system db, consider to exclude system db")
		}
	}
	metaInfoProcessor := logclient.NewMetaKVInfoProcessor(logClient)
	// doesn't need to build if id map has been saved
	idMapSaved := isCurrentIdMapSaved(taskInfo.CheckpointInfo)
	if !idMapSaved {
		// we restore additional tables at full snapshot phase when it is renamed into the filter range
		// later in log backup.
		// we also ignore the tables that currently in filter range but later renamed out of the filter.
		log.Info("reading meta kv files to collect table info and id mapping information")
		err = metaInfoProcessor.ReadMetaKVFilesAndBuildInfo(ctx, ddlFiles)
		if err != nil {
			return errors.Trace(err)
		}
		dbReplace := metaInfoProcessor.GetTableMappingManager().DBReplaceMap
		stream.LogDBReplaceMap("scanned log meta kv before snapshot restore", dbReplace)
	}

	// Save PITR-related info to cfg for blocklist creation in defer function
	cfg.tableMappingManager = metaInfoProcessor.GetTableMappingManager()

	// Capture restore start timestamp before any table creation (for blocklist)
	restoreStartTS, err := restore.GetTSWithRetry(ctx, mgr.GetPDClient())
	if err != nil {
		return errors.Trace(err)
	}
	cfg.RestoreStartTS = restoreStartTS
	log.Info("captured restore start timestamp for blocklist",
		zap.Uint64("restoreStartTS", restoreStartTS))

	// restore full snapshot.
	if taskInfo.NeedFullRestore {
		logStorage := cfg.Config.Storage
		cfg.Config.Storage = cfg.FullBackupStorage

		snapshotRestoreConfig := SnapshotRestoreConfig{
			RestoreConfig:          cfg,
			piTRTaskInfo:           taskInfo,
			logRestoreStorage:      s,
			logTableHistoryManager: metaInfoProcessor.GetTableHistoryManager(),
			tableMappingManager:    metaInfoProcessor.GetTableMappingManager(),
		}
		// TiFlash replica is restored to down-stream on 'pitr' currently.
		if err = runSnapshotRestore(ctx, mgr, g, FullRestoreCmd, &snapshotRestoreConfig); err != nil {
			return errors.Trace(err)
		}
		cfg.Config.Storage = logStorage
	} else if len(cfg.FullBackupStorage) > 0 {
		if err = WriteStringToConsole(g, fmt.Sprintf("%s is skipped due to checkpoint mode for restore\n", FullRestoreCmd)); err != nil {
			return errors.Trace(err)
		}
		if taskInfo.hasTiFlashItemsInCheckpoint() {
			log.Info("load tiflash records of snapshot restore from checkpoint")
			cfg.tiflashRecorder.Load(taskInfo.CheckpointInfo.Metadata.TiFlashItems)
		}
	}
	logRestoreConfig := &LogRestoreConfig{
		RestoreConfig:       cfg,
		checkpointTaskInfo:  taskInfo.CheckpointInfo,
		tableMappingManager: metaInfoProcessor.GetTableMappingManager(),
		logClient:           logClient,
		ddlFiles:            ddlFiles,
	}
	if err := restoreStream(ctx, mgr, g, logRestoreConfig); err != nil {
		return errors.Trace(err)
	}
	return nil
}

type LogRestoreConfig struct {
	*RestoreConfig
	checkpointTaskInfo  *checkpoint.TaskInfoForLogRestore
	tableMappingManager *stream.TableMappingManager
	logClient           *logclient.LogClient
	ddlFiles            []logclient.Log
}

// restoreStream starts the log restore
func restoreStream(
	c context.Context,
	mgr *conn.Mgr,
	g glue.Glue,
	cfg *LogRestoreConfig,
) (err error) {
	var (
		totalKVCount           uint64
		totalSize              uint64
		checkpointTotalKVCount uint64
		checkpointTotalSize    uint64
		currentTS              uint64
		extraFields            []zapcore.Field
		mu                     sync.Mutex
		startTime              = time.Now()
	)
	defer func() {
		if err != nil {
			summary.Log("restore log failed summary", zap.Error(err))
		} else {
			totalDureTime := time.Since(startTime)
			summary.Log("restore log success summary",
				append([]zapcore.Field{zap.Duration("total-take", totalDureTime),
					zap.Uint64("source-start-point", cfg.StartTS),
					zap.Uint64("source-end-point", cfg.RestoreTS),
					zap.Uint64("target-end-point", currentTS),
					zap.String("source-start", utils.FormatDate(oracle.GetTimeFromTS(cfg.StartTS))),
					zap.String("source-end", utils.FormatDate(oracle.GetTimeFromTS(cfg.RestoreTS))),
					zap.String("target-end", utils.FormatDate(oracle.GetTimeFromTS(currentTS))),
					zap.Uint64("total-kv-count", totalKVCount),
					zap.Uint64("skipped-kv-count-by-checkpoint", checkpointTotalKVCount),
					zap.String("total-size", units.HumanSize(float64(totalSize))),
					zap.String("skipped-size-by-checkpoint", units.HumanSize(float64(checkpointTotalSize))),
					zap.String("average-speed (log)", units.HumanSize(float64(totalSize)/totalDureTime.Seconds())+"/s")},
					extraFields...)...,
			)
		}
	}()

	ctx, cancelFn := context.WithCancel(c)
	defer cancelFn()

	restoreCfg := tweakLocalConfForRestore()
	defer restoreCfg()

	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan(
			"restoreStream",
			opentracing.ChildOf(span.Context()),
		)
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	client := cfg.logClient
	migs, err := client.GetLockedMigrations(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	client.BuildMigrations(migs.Migs)

	skipCleanup := false
	if _, _err_ := failpoint.Eval(_curpkg_("skip-migration-read-lock-cleanup")); _err_ == nil {
		// Skip the cleanup - this keeps the read lock held
		// and will cause lock conflicts for other restore operations
		log.Info("Skipping migration read lock cleanup due to failpoint")
		skipCleanup = true
	}

	if !skipCleanup {
		defer cleanUpWithRetErr(&err, migs.ReadLock.Unlock)
	}

	defer client.RestoreSSTStatisticFields(&extraFields)

	ddlFiles := cfg.ddlFiles

	currentTS, err = getCurrentTSFromCheckpointOrPD(ctx, mgr, cfg)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.RewriteTS = currentTS

	if err := client.SetCurrentTS(currentTS); err != nil {
		return errors.Trace(err)
	}

	// It need disable GC in TiKV when PiTR.
	// because the process of PITR is concurrent and kv events isn't sorted by tso.
	var restoreGCFunc RestoreGCFunc
	var oldGCRatio string
	if err := cfg.RestoreRegistry.OperationAfterWaitIDs(ctx, func() (err error) {
		restoreGCFunc, oldGCRatio, err = DisableGC(g, mgr.GetStorage())
		return errors.Trace(err)
	}); err != nil {
		return errors.Trace(err)
	}
	gcDisabledRestorable := false
	defer func() {
		// don't restore the gc-ratio-threshold if checkpoint mode is used and restored is not finished
		if cfg.UseCheckpoint && !gcDisabledRestorable {
			log.Info("skip restore the gc-ratio-threshold for next retry")
			return
		}

		// If the oldGcRatio is negative, which is not normal status.
		// It should set default value "1.1" after PiTR finished.
		if strings.HasPrefix(oldGCRatio, "-") {
			log.Warn("the original gc-ratio is negative, reset by default value 1.1", zap.String("old-gc-ratio", oldGCRatio))
			oldGCRatio = utils.DefaultGcRatioVal
		}
		log.Info("start to restore gc", zap.String("ratio", oldGCRatio))
		err = cfg.RestoreRegistry.GlobalOperationAfterSetResettingStatus(ctx, cfg.RestoreID, func() error {
			if err := restoreGCFunc(oldGCRatio); err != nil {
				log.Error("failed to restore gc", zap.Error(err))
				return errors.Trace(err)
			}
			return nil
		})
		log.Info("finish restoring gc")
	}()

	var sstCheckpointSets map[string]struct{}
	if cfg.UseCheckpoint {
		gcRatioFromCheckpoint, err := client.LoadOrCreateCheckpointMetadataForLogRestore(
			ctx, cfg.StartTS, cfg.RestoreTS, oldGCRatio, cfg.tiflashRecorder, cfg.logCheckpointMetaManager)
		if err != nil {
			return errors.Trace(err)
		}
		oldGCRatio = gcRatioFromCheckpoint
		sstCheckpointSets, err = client.InitCheckpointMetadataForCompactedSstRestore(ctx, cfg.sstCheckpointMetaManager)
		if err != nil {
			return errors.Trace(err)
		}
	}

	// build and save id map
	if err := buildAndSaveIDMapIfNeeded(ctx, client, cfg); err != nil {
		return errors.Trace(err)
	}

	// build schema replace
	schemasReplace, err := buildSchemaReplace(client, cfg)
	if err != nil {
		return errors.Trace(err)
	}

	importModeSwitcher := restore.NewImportModeSwitcher(mgr.GetPDClient(),
		cfg.Config.SwitchModeInterval, mgr.GetTLSConfig())
	// set up scheduler pausing before meta kv restore starts
	var restoreSchedulersFunc pdutil.UndoFunc

	// use fine-grained scheduler pausing if we have specific tables to restore (not full restore)
	if cfg.ExplicitFilter {
		keyRanges := buildKeyRangesFromSchemasReplace(schemasReplace, cfg)
		if len(keyRanges) > 0 {
			log.Info("using fine-grained scheduler pausing for log restore",
				zap.Int("key-ranges-count", len(keyRanges)))
			restoreSchedulersFunc, _, err = restore.FineGrainedRestorePreWork(ctx, mgr,
				importModeSwitcher, keyRanges, cfg.Online, false)
		} else {
			log.Info("no key ranges to pause, skipping scheduler pausing")
			restoreSchedulersFunc = func(context.Context) error { return nil }
		}
	} else {
		log.Info("using full scheduler pausing for log restore (full restore)")
		restoreSchedulersFunc, _, err = restore.RestorePreWork(ctx, mgr, importModeSwitcher, cfg.Online, false)
	}

	if err != nil {
		return errors.Trace(err)
	}

	// Failpoint for testing scheduler pausing behavior
	failpoint.Call(_curpkg_("log-restore-scheduler-paused"))

	// Always run the post-work even on error, so we don't stuck in the import
	// mode or emptied schedulers
	defer restore.RestorePostWork(ctx, importModeSwitcher, restoreSchedulersFunc, cfg.Online)

	updateStats := func(kvCount uint64, size uint64) {
		mu.Lock()
		defer mu.Unlock()
		totalKVCount += kvCount
		totalSize += size
	}

	var rp *logclient.RestoreMetaKVProcessor
	if err = glue.WithProgress(ctx, g, "Restore Meta Files", int64(len(ddlFiles)), !cfg.LogProgress, func(p glue.Progress) error {
		rp = logclient.NewRestoreMetaKVProcessor(client, schemasReplace, updateStats, p.Inc)
		return rp.RestoreAndRewriteMetaKVFiles(ctx, cfg.ExplicitFilter, ddlFiles, schemasReplace)
	}); err != nil {
		return errors.Annotate(err, "failed to restore meta files")
	}
	stream.LogDBReplaceMap("built db replace map, start to build rewrite rules", schemasReplace.DbReplaceMap)
	rewriteRules := buildRewriteRules(schemasReplace)

	ingestRecorder := schemasReplace.GetIngestRecorder()
	if err := rangeFilterFromIngestRecorder(ingestRecorder, rewriteRules); err != nil {
		return errors.Trace(err)
	}

	logFilesIter, err := client.LoadDMLFiles(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	numberOfKVsInSST, err := client.LogFileManager.CountExtraSSTTotalKVs(ctx)
	if err != nil {
		return err
	}

	se, err := g.CreateSession(mgr.GetStorage())
	if err != nil {
		return errors.Trace(err)
	}
	execCtx := se.GetSessionCtx().GetRestrictedSQLExecutor()
	splitSize, splitKeys := utils.GetRegionSplitInfo(execCtx)
	log.Info("[Log Restore] get split threshold from tikv config", zap.Uint64("split-size", splitSize), zap.Int64("split-keys", splitKeys))

	// TODO: need keep the order of ssts for compatible of rewrite rules
	// compacted ssts will set ts range for filter out irrelevant data
	// ingested ssts cannot use this ts range
	addedSSTsIter := client.LogFileManager.GetIngestedSSTs(ctx)
	compactionIter := client.LogFileManager.GetCompactionIter(ctx)
	sstsIter := iter.ConcatAll(addedSSTsIter, compactionIter)

	totalWorkUnits := numberOfKVsInSST + client.Stats.NumEntries
	err = glue.WithProgress(ctx, g, "Restore Files(SST + Log)", totalWorkUnits, !cfg.LogProgress, func(p glue.Progress) (pErr error) {
		updateStatsWithCheckpoint := func(kvCount, size uint64) {
			mu.Lock()
			defer mu.Unlock()
			totalKVCount += kvCount
			totalSize += size
			checkpointTotalKVCount += kvCount
			checkpointTotalSize += size
			// increase the progress
			p.IncBy(int64(kvCount))
		}
		compactedSplitIter, err := client.WrapCompactedFilesIterWithSplitHelper(
			ctx, sstsIter, rewriteRules, sstCheckpointSets,
			updateStatsWithCheckpoint, splitSize, splitKeys,
		)
		if err != nil {
			return errors.Trace(err)
		}

		err = client.RestoreSSTFiles(ctx, compactedSplitIter, rewriteRules, importModeSwitcher, p.IncBy)
		if err != nil {
			return errors.Trace(err)
		}

		logFilesIter = iter.WithEmitSizeTrace(logFilesIter, metrics.KVLogFileEmittedMemory.WithLabelValues("0-loaded"))
		logFilesIterWithSplit, err := client.WrapLogFilesIterWithSplitHelper(ctx, logFilesIter, cfg.logCheckpointMetaManager, rewriteRules, updateStatsWithCheckpoint, splitSize, splitKeys)
		if err != nil {
			return errors.Trace(err)
		}
		logFilesIterWithSplit = iter.WithEmitSizeTrace(logFilesIterWithSplit, metrics.KVLogFileEmittedMemory.WithLabelValues("1-split"))

		if cfg.UseCheckpoint {
			// TODO make a failpoint iter inside the logclient.
			if v, _err_ := failpoint.Eval(_curpkg_("corrupt-files")); _err_ == nil {
				var retErr error
				logFilesIterWithSplit, retErr = logclient.WrapLogFilesIterWithCheckpointFailpoint(v, logFilesIterWithSplit, rewriteRules)
				defer func() { pErr = retErr }()
			}
		}

		return client.RestoreKVFiles(ctx, rewriteRules, logFilesIterWithSplit,
			cfg.PitrBatchCount, cfg.PitrBatchSize, updateStats, p.IncBy, &cfg.LogBackupCipherInfo, cfg.MasterKeyConfig.MasterKeys)
	})
	if err != nil {
		return errors.Annotate(err, "failed to restore kv files")
	}

	if cfg.ExplicitFilter {
		if _, _err_ := failpoint.Eval(_curpkg_("before-set-table-mode-to-normal")); _err_ == nil {
			return errors.New("fail before setting table mode to normal")
		}

		if err = client.SetTableModeToNormal(ctx, schemasReplace); err != nil {
			return errors.Trace(err)
		}
	}

	// failpoint to stop for a while after restoring kvs
	// this is to mimic the scenario that restore takes long time and the lease in schemaInfo has expired and needs refresh
	if val, _err_ := failpoint.Eval(_curpkg_("post-restore-kv-pending")); _err_ == nil {
		if val.(bool) {
			// not ideal to use sleep but not sure what's the better way right now
			log.Info("sleep after restoring kv")
			time.Sleep(2 * time.Second)
		}
	}

	// make sure schema reload finishes before proceeding
	if err = waitUntilSchemaReload(ctx, client); err != nil {
		return errors.Trace(err)
	}

	if err = client.CleanUpKVFiles(ctx); err != nil {
		return errors.Annotate(err, "failed to clean up")
	}

	// to delete range(table, schema) that's dropped during log backup
	if err = client.InsertGCRows(ctx); err != nil {
		return errors.Annotate(err, "failed to insert rows into gc_delete_range")
	}

	// index ingestion is not captured by regular log backup, so we need to manually ingest again
	if err = client.RepairIngestIndex(ctx, ingestRecorder, cfg.logCheckpointMetaManager, g); err != nil {
		return errors.Annotate(err, "failed to repair ingest index")
	}

	if cfg.tiflashRecorder != nil {
		sqls := cfg.tiflashRecorder.GenerateAlterTableDDLs(mgr.GetDomain().InfoSchema())
		log.Info("Generating SQLs for restoring TiFlash Replica",
			zap.Strings("sqls", sqls))
		if err := client.ResetTiflashReplicas(ctx, sqls, g); err != nil {
			return errors.Annotate(err, "failed to reset tiflash replicas")
		}
	}

	if _, _err_ := failpoint.Eval(_curpkg_("do-checksum-with-rewrite-rules")); _err_ == nil {
		if err := client.FailpointDoChecksumForLogRestore(ctx, mgr.GetStorage().GetClient(), mgr.GetPDClient(), rewriteRules); err != nil {
			return errors.Annotate(err, "failed to do checksum")
		}
	}

	gcDisabledRestorable = true

	return nil
}

func createLogClient(ctx context.Context, g glue.Glue, cfg *RestoreConfig, mgr *conn.Mgr) (*logclient.LogClient, error) {
	var err error
	keepaliveCfg := GetKeepalive(&cfg.Config)
	keepaliveCfg.PermitWithoutStream = true
	client := logclient.NewLogClient(mgr.GetPDClient(), mgr.GetPDHTTPClient(), mgr.GetTLSConfig(), keepaliveCfg)

	err = client.Init(ctx, g, mgr.GetStorage())
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer func() {
		if err != nil {
			client.Close(ctx)
		}
	}()

	u, err := storage.ParseBackend(cfg.Storage, &cfg.BackendOptions)
	if err != nil {
		return nil, errors.Trace(err)
	}

	opts := getExternalStorageOptions(&cfg.Config, u)
	if err = client.SetStorage(ctx, u, &opts); err != nil {
		return nil, errors.Trace(err)
	}
	client.SetCrypter(&cfg.CipherInfo)
	client.SetUpstreamClusterID(cfg.UpstreamClusterID)

	err = client.InitClients(ctx, u, cfg.logCheckpointMetaManager, cfg.sstCheckpointMetaManager, uint(cfg.PitrConcurrency), cfg.ConcurrencyPerStore.Value)
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = client.SetRawKVBatchClient(ctx, cfg.PD, cfg.TLS.ToKVSecurity())
	if err != nil {
		return nil, errors.Trace(err)
	}

	encryptionManager, err := encryption.NewManager(&cfg.LogBackupCipherInfo, &cfg.MasterKeyConfig)
	if err != nil {
		return nil, errors.Annotate(err, "failed to create encryption manager for log restore")
	}
	if err = client.InstallLogFileManager(ctx, cfg.StartTS, cfg.RestoreTS, cfg.MetadataDownloadBatchSize, encryptionManager); err != nil {
		return nil, errors.Trace(err)
	}

	return client, nil
}

// rangeFilterFromIngestRecorder rewrites the table id of items in the ingestRecorder
// TODO: need to implement the range filter out feature
func rangeFilterFromIngestRecorder(recorder *ingestrec.IngestRecorder, rewriteRules map[int64]*restoreutils.RewriteRules) error {
	err := recorder.RewriteTableID(func(tableID int64) (int64, bool, error) {
		rewriteRule, exists := rewriteRules[tableID]
		if !exists {
			// since the table's files will be skipped restoring, here also skips.
			return 0, true, nil
		}
		newTableID := restoreutils.GetRewriteTableID(tableID, rewriteRule)
		if newTableID == 0 {
			return 0, false, errors.Errorf("newTableID is 0, tableID: %d", tableID)
		}
		return newTableID, false, nil
	})
	return errors.Trace(err)
}

func getExternalStorageOptions(cfg *Config, u *backuppb.StorageBackend) storage.ExternalStorageOptions {
	var httpClient *http.Client
	if u.GetGcs() == nil {
		httpClient = storage.GetDefaultHttpClient(cfg.MetadataDownloadBatchSize)
	}
	return storage.ExternalStorageOptions{
		NoCredentials:   cfg.NoCreds,
		SendCredentials: cfg.SendCreds,
		HTTPClient:      httpClient,
	}
}

func checkLogRange(restoreFromTS, restoreToTS, logMinTS, logMaxTS uint64) error {
	// several ts constraint：
	// logMinTS <= restoreFromTS <= restoreToTS <= logMaxTS
	if logMinTS > restoreFromTS || restoreFromTS > restoreToTS || restoreToTS > logMaxTS {
		return errors.Annotatef(berrors.ErrInvalidArgument,
			"restore log from %d(%s) to %d(%s), "+
				" but the current existed log from %d(%s) to %d(%s)",
			restoreFromTS, oracle.GetTimeFromTS(restoreFromTS),
			restoreToTS, oracle.GetTimeFromTS(restoreToTS),
			logMinTS, oracle.GetTimeFromTS(logMinTS),
			logMaxTS, oracle.GetTimeFromTS(logMaxTS),
		)
	}
	return nil
}

type backupLogInfo struct {
	logMaxTS  uint64
	logMinTS  uint64
	clusterID uint64
}

// getLogInfo gets the log-min-ts and log-max-ts of starting log backup.
func getLogInfo(
	ctx context.Context,
	cfg *Config,
) (backupLogInfo, error) {
	_, s, err := GetStorage(ctx, cfg.Storage, cfg)
	if err != nil {
		return backupLogInfo{}, errors.Trace(err)
	}
	return getLogInfoFromStorage(ctx, s)
}

func getLogInfoFromStorage(
	ctx context.Context,
	s storage.ExternalStorage,
) (backupLogInfo, error) {
	// logStartTS: Get log start ts from backupmeta file.
	metaData, err := s.ReadFile(ctx, metautil.MetaFile)
	if err != nil {
		return backupLogInfo{}, errors.Trace(err)
	}
	backupMeta := &backuppb.BackupMeta{}
	if err = backupMeta.Unmarshal(metaData); err != nil {
		return backupLogInfo{}, errors.Trace(err)
	}
	// endVersion > 0 represents that the storage has been used for `br backup`
	if backupMeta.GetEndVersion() > 0 {
		return backupLogInfo{}, errors.Annotate(berrors.ErrStorageUnknown,
			"the storage has been used for full backup")
	}
	logStartTS := backupMeta.GetStartVersion()

	// truncateTS: get log truncate ts from TruncateSafePointFileName.
	// If truncateTS equals 0, which represents the stream log has never been truncated.
	truncateTS, err := stream.GetTSFromFile(ctx, s, stream.TruncateSafePointFileName)
	if err != nil {
		return backupLogInfo{}, errors.Trace(err)
	}
	logMinTS := max(logStartTS, truncateTS)

	// get max global resolved ts from metas.
	logMaxTS, err := getGlobalCheckpointFromStorage(ctx, s)
	if err != nil {
		return backupLogInfo{}, errors.Trace(err)
	}
	logMaxTS = max(logMinTS, logMaxTS)

	return backupLogInfo{
		logMaxTS:  logMaxTS,
		logMinTS:  logMinTS,
		clusterID: backupMeta.ClusterId,
	}, nil
}

func getGlobalCheckpointFromStorage(ctx context.Context, s storage.ExternalStorage) (uint64, error) {
	var globalCheckPointTS uint64 = 0
	opt := storage.WalkOption{SubDir: stream.GetStreamBackupGlobalCheckpointPrefix()}
	err := s.WalkDir(ctx, &opt, func(path string, size int64) error {
		if !strings.HasSuffix(path, ".ts") {
			return nil
		}

		buff, err := s.ReadFile(ctx, path)
		if err != nil {
			return errors.Trace(err)
		}
		ts := binary.LittleEndian.Uint64(buff)
		globalCheckPointTS = max(ts, globalCheckPointTS)
		return nil
	})
	return globalCheckPointTS, errors.Trace(err)
}

// getFullBackupTS gets the snapshot-ts of full backup
func getFullBackupTS(
	ctx context.Context,
	cfg *RestoreConfig,
) (uint64, uint64, error) {
	_, s, err := GetStorage(ctx, cfg.FullBackupStorage, &cfg.Config)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}

	metaData, err := s.ReadFile(ctx, metautil.MetaFile)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}

	decryptedMetaData, err := metautil.DecryptFullBackupMetaIfNeeded(metaData, &cfg.CipherInfo)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}

	backupmeta := &backuppb.BackupMeta{}
	if err = backupmeta.Unmarshal(decryptedMetaData); err != nil {
		return 0, 0, errors.Trace(err)
	}

	// start and end are identical in full backup, pick random one
	return backupmeta.GetEndVersion(), backupmeta.GetClusterId(), nil
}

func buildRewriteRules(schemasReplace *stream.SchemasReplace) map[int64]*restoreutils.RewriteRules {
	rules := make(map[int64]*restoreutils.RewriteRules)

	for _, dbReplace := range schemasReplace.DbReplaceMap {
		if dbReplace.FilteredOut || utils.IsSysOrTempSysDB(dbReplace.Name) {
			continue
		}
		for oldTableID, tableReplace := range dbReplace.TableMap {
			if tableReplace.FilteredOut {
				continue
			}
			if _, exist := rules[oldTableID]; !exist {
				log.Info("add rewrite rule",
					zap.String("tableName", dbReplace.Name+"."+tableReplace.Name),
					zap.Int64("oldID", oldTableID), zap.Int64("newID", tableReplace.TableID))
				rules[oldTableID] = restoreutils.GetRewriteRuleOfTable(
					oldTableID, tableReplace.TableID, tableReplace.IndexMap, false)
			} else {
				log.Info("skip adding table rewrite rule, already exists",
					zap.Int64("oldID", oldTableID),
					zap.Int64("newID", tableReplace.TableID))
			}

			for oldID, newID := range tableReplace.PartitionMap {
				if _, exist := rules[oldID]; !exist {
					log.Info("add rewrite rule",
						zap.String("tableName", dbReplace.Name+"."+tableReplace.Name),
						zap.Int64("oldID", oldID), zap.Int64("newID", newID))
					rules[oldID] = restoreutils.GetRewriteRuleOfTable(oldID, newID, tableReplace.IndexMap, false)
				} else {
					log.Info("skip adding partition rewrite rule, already exists",
						zap.Int64("oldID", oldID),
						zap.Int64("newID", newID))
				}
			}
		}
	}
	return rules
}

// ShiftTS gets a smaller shiftTS than startTS.
// It has a safe duration between shiftTS and startTS for transaction.
func ShiftTS(startTS uint64) uint64 {
	physical := oracle.ExtractPhysical(startTS)
	logical := oracle.ExtractLogical(startTS)

	shiftPhysical := physical - streamShiftDuration.Milliseconds()
	if shiftPhysical < 0 {
		return 0
	}
	return oracle.ComposeTS(shiftPhysical, logical)
}

func buildPauseSafePointName(taskName string) string {
	return fmt.Sprintf("%s_pause_safepoint", taskName)
}

type PiTRTaskInfo struct {
	CheckpointInfo  *checkpoint.TaskInfoForLogRestore
	RestoreTS       uint64
	NeedFullRestore bool
}

func (p *PiTRTaskInfo) hasTiFlashItemsInCheckpoint() bool {
	return p.CheckpointInfo != nil && p.CheckpointInfo.Metadata != nil && p.CheckpointInfo.Metadata.TiFlashItems != nil
}

func generatePiTRTaskInfo(
	ctx context.Context,
	mgr *conn.Mgr,
	g glue.Glue,
	cfg *RestoreConfig,
	restoreID uint64,
) (*PiTRTaskInfo, error) {
	var (
		doFullRestore = len(cfg.FullBackupStorage) > 0
		curTaskInfo   *checkpoint.TaskInfoForLogRestore
		err           error
	)
	checkInfo := &PiTRTaskInfo{}

	log.Info("generating PiTR task info",
		zap.Uint64("restoreID", restoreID),
		zap.Bool("useCheckpoint", cfg.UseCheckpoint),
		zap.Bool("doFullRestore", doFullRestore))

	if cfg.UseCheckpoint {
		if len(cfg.CheckpointStorage) > 0 {
			clusterID := mgr.GetPDClient().GetClusterID(ctx)
			log.Info("initializing storage checkpoint meta manager for PiTR",
				zap.Uint64("restoreID", restoreID),
				zap.Uint64("clusterID", clusterID))
			if err = cfg.newStorageCheckpointMetaManagerPITR(ctx, clusterID, restoreID); err != nil {
				return nil, errors.Trace(err)
			}
		} else {
			log.Info("initializing table checkpoint meta manager for PiTR",
				zap.Uint64("restoreID", restoreID))
			if err = cfg.newTableCheckpointMetaManagerPITR(g, mgr.GetDomain(), restoreID); err != nil {
				return nil, errors.Trace(err)
			}
		}
		curTaskInfo, err = checkpoint.GetCheckpointTaskInfo(ctx, cfg.snapshotCheckpointMetaManager, cfg.logCheckpointMetaManager)
		log.Info("current task checkpoint info",
			zap.Any("checkpoint", curTaskInfo),
			zap.Uint64("restoreID", restoreID))
		if err != nil {
			return checkInfo, errors.Trace(err)
		}
		// the log restore checkpoint metadata is persisted, so the PITR is in the log restore stage and id map saved
		if curTaskInfo.Metadata != nil && curTaskInfo.IdMapSaved() {
			// TODO: check whether user has manually modified the cluster(ddl). If so, regard the behavior
			//       as restore from scratch. (update `curTaskInfo.RewriteTs` to 0 as an uninitial value)

			if curTaskInfo.Metadata.UpstreamClusterID != cfg.UpstreamClusterID {
				return checkInfo, errors.Errorf(
					"The upstream cluster id[%d] of the current log restore does not match that[%d] recorded in checkpoint. "+
						"Perhaps you should specify the last log backup storage instead, "+
						"or just clean the checkpoint %s if the cluster has been cleaned up.",
					cfg.UpstreamClusterID, curTaskInfo.Metadata.UpstreamClusterID, cfg.logCheckpointMetaManager)
			}

			if curTaskInfo.Metadata.StartTS != cfg.StartTS || curTaskInfo.Metadata.RestoredTS != cfg.RestoreTS {
				return checkInfo, errors.Errorf(
					"The current log restore want to restore cluster from %d to %d, "+
						"which is different from that from %d to %d recorded in checkpoint. "+
						"Perhaps you should specify the last full backup storage to match the start-ts and "+
						"the parameter --restored-ts to match the restored-ts. "+
						"or just clean the checkpoint %s if the cluster has been cleaned up.",
					cfg.StartTS, cfg.RestoreTS, curTaskInfo.Metadata.StartTS, curTaskInfo.Metadata.RestoredTS, cfg.logCheckpointMetaManager,
				)
			}

			log.Info("detect log restore checkpoint. so skip snapshot restore and start log restore from the checkpoint")
			// the same task, skip full restore because it is already in the log restore stage.
			doFullRestore = false
		}
	}
	checkInfo.CheckpointInfo = curTaskInfo
	checkInfo.NeedFullRestore = doFullRestore
	checkInfo.RestoreTS = cfg.RestoreTS
	return checkInfo, nil
}

func cleanUpWithRetErr(errOut *error, f func(ctx context.Context) error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	err := f(ctx)
	if errOut != nil {
		*errOut = multierr.Combine(*errOut, err)
	}
}

func waitUntilSchemaReload(ctx context.Context, client *logclient.LogClient) error {
	log.Info("waiting for schema info finishes reloading")
	reloadStart := time.Now()
	conditionFunc := func() bool {
		return !client.GetDomain().IsLeaseExpired()
	}
	if err := utils.WaitUntil(ctx, conditionFunc, waitInfoSchemaReloadCheckInterval, waitInfoSchemaReloadTimeout); err != nil {
		return errors.Annotate(err, "failed to wait until schema reload")
	}
	log.Info("reloading schema finished", zap.Duration("timeTaken", time.Since(reloadStart)))
	return nil
}

func isCurrentIdMapSaved(checkpointTaskInfo *checkpoint.TaskInfoForLogRestore) bool {
	return checkpointTaskInfo != nil && checkpointTaskInfo.Progress == checkpoint.InLogRestoreAndIdMapPersisted
}

func buildSchemaReplace(client *logclient.LogClient, cfg *LogRestoreConfig) (*stream.SchemasReplace, error) {
	schemasReplace := stream.NewSchemasReplace(cfg.tableMappingManager.DBReplaceMap, cfg.tiflashRecorder,
		client.CurrentTS(), client.RecordDeleteRange, cfg.ExplicitFilter)
	schemasReplace.AfterTableRewrittenFn = func(deleted bool, tableInfo *model.TableInfo) {
		// When the table replica changed to 0, the tiflash replica might be set to `nil`.
		// We should remove the table if we meet.
		if deleted || tableInfo.TiFlashReplica == nil {
			cfg.tiflashRecorder.DelTable(tableInfo.ID)
			return
		}
		cfg.tiflashRecorder.AddTable(tableInfo.ID, *tableInfo.TiFlashReplica)
		// Remove the replica first and restore them at the end.
		tableInfo.TiFlashReplica = nil
	}
	return schemasReplace, nil
}

func buildAndSaveIDMapIfNeeded(ctx context.Context, client *logclient.LogClient, cfg *LogRestoreConfig) error {
	// get the schemas ID replace information.
	saved := isCurrentIdMapSaved(cfg.checkpointTaskInfo)
	hasFullBackupStorage := len(cfg.FullBackupStorage) != 0
	err := client.GetBaseIDMapAndMerge(ctx, hasFullBackupStorage, saved,
		cfg.logCheckpointMetaManager, cfg.tableMappingManager)
	if err != nil {
		return errors.Trace(err)
	}

	if saved {
		return nil
	}

	// either getting base id map from previous pitr or this is a new task and get base map from snapshot restore phase
	// do filter
	if cfg.PiTRTableTracker != nil {
		cfg.tableMappingManager.ApplyFilterToDBReplaceMap(cfg.PiTRTableTracker)
	}
	// replace temp id with read global id
	err = cfg.tableMappingManager.ReplaceTemporaryIDs(ctx, client.GenGlobalIDs)
	if err != nil {
		return errors.Trace(err)
	}
	if err = client.SaveIdMapWithFailPoints(ctx, cfg.tableMappingManager, cfg.logCheckpointMetaManager); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func getCurrentTSFromCheckpointOrPD(ctx context.Context, mgr *conn.Mgr, cfg *LogRestoreConfig) (uint64, error) {
	if cfg.checkpointTaskInfo != nil && cfg.checkpointTaskInfo.Metadata != nil {
		// reuse the checkpoint task's rewrite ts
		rewriteTS := cfg.checkpointTaskInfo.Metadata.RewriteTS
		log.Info("reuse the task's rewrite ts", zap.Uint64("rewrite-ts", rewriteTS))
		return rewriteTS, nil
	}
	currentTS, err := restore.GetTSWithRetry(ctx, mgr.GetPDClient())
	if err != nil {
		return 0, errors.Trace(err)
	}
	return currentTS, nil
}

func RegisterRestoreIfNeeded(ctx context.Context, cfg *RestoreConfig, cmdName string, domain *domain.Domain) error {
	// already registered previously
	if cfg.RestoreID != 0 {
		log.Info("restore task already registered, skipping re-registration",
			zap.Uint64("restoreID", cfg.RestoreID),
			zap.String("cmdName", cmdName))
		return nil
	}

	// skip registration if target TiDB version doesn't support restore registry
	if !restore.HasRestoreIDColumn(domain) {
		log.Info("skipping restore registration since target TiDB version doesn't support restore registry")
		return nil
	}

	originalRestoreTS := cfg.RestoreTS
	registrationInfo := registry.RegistrationInfo{
		StartTS:           cfg.StartTS,
		RestoredTS:        cfg.RestoreTS,
		FilterStrings:     cfg.FilterStr,
		UpstreamClusterID: cfg.UpstreamClusterID,
		WithSysTable:      cfg.WithSysTable,
		Cmd:               cmdName,
	}

	// get current registered (due to failure retry) or create new restore task id
	restoreID, resolvedRestoreTS, err := cfg.RestoreRegistry.ResumeOrCreateRegistration(
		ctx, registrationInfo, cfg.IsRestoredTSUserSpecified)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.RestoreID = restoreID

	// the registry may have resolved a different RestoreTS from existing tasks
	// if so, we need to apply it to the config for consistency
	if resolvedRestoreTS != originalRestoreTS {
		log.Info("registry resolved a different RestoreTS, updating config",
			zap.Uint64("original_restored_ts", originalRestoreTS),
			zap.Uint64("resolved_restored_ts", resolvedRestoreTS),
			zap.Uint64("restore_id", restoreID))
		cfg.RestoreTS = resolvedRestoreTS
	}

	log.Info("registered restore task",
		zap.Uint64("restoreID", restoreID),
		zap.String("cmdName", cmdName))

	cfg.RestoreRegistry.StartHeartbeatManager(ctx, restoreID)

	return nil
}

// isValidSnapshotRange checks if a snapshot range is valid for scheduler pausing
// A valid range must be non-zero, have positive start ID, and end > start
func isValidSnapshotRange(snapshotRange [2]int64) bool {
	return snapshotRange != [2]int64{} && snapshotRange[0] > 0 && snapshotRange[1] > snapshotRange[0]
}

// buildKeyRangesFromSchemasReplace creates key ranges for fine-grained scheduler pausing
// It analyzes the new table IDs from schemasReplace to determine ranges:
// 1. Snapshot restore range: preallocated range stored in table mapping manager
// 2. Log restore range: contiguous range allocated in one batch during log restore
func buildKeyRangesFromSchemasReplace(schemasReplace *stream.SchemasReplace,
	cfg *LogRestoreConfig) [][2]kv.Key {
	var ranges [][2]int64

	// get preallocated range from snapshot restore
	// try table mapping manager first (memory read), then checkpoint if needed (database read)
	var snapshotRange [2]int64

	// first try table mapping manager (fast memory read)
	snapshotRange = cfg.tableMappingManager.PreallocatedRange
	if snapshotRange != [2]int64{} {
		log.Info("using snapshot preallocated range from table mapping manager for scheduler pausing",
			zap.Int64("start", snapshotRange[0]),
			zap.Int64("end", snapshotRange[1]))
	} else if cfg.snapshotCheckpointMetaManager != nil {
		// fallback to checkpoint metadata if table mapping manager doesn't have the range
		if snapshotMeta, err := cfg.snapshotCheckpointMetaManager.LoadCheckpointMetadata(context.Background()); err == nil && snapshotMeta.PreallocIDs != nil {
			snapshotRange = [2]int64{snapshotMeta.PreallocIDs.Start, snapshotMeta.PreallocIDs.End}
			log.Info("loaded snapshot preallocated range from checkpoint for scheduler pausing",
				zap.Int64("start", snapshotRange[0]),
				zap.Int64("end", snapshotRange[1]))
		} else if err != nil {
			log.Warn("failed to load snapshot checkpoint metadata for scheduler pausing", zap.Error(err))
		}
	}

	if snapshotRange == [2]int64{} {
		log.Info("no preallocated range found for scheduler pausing")
	}

	hasValidSnapshotRange := isValidSnapshotRange(snapshotRange)
	if hasValidSnapshotRange {
		ranges = append(ranges, snapshotRange)
	}

	// helper function to check if an ID is outside the snapshot range and needs a separate pause range
	isOutsideSnapshotRange := func(id int64) bool {
		if id == 0 {
			return false
		}
		if hasValidSnapshotRange {
			// check if this ID is outside the snapshot range (needs separate pausing)
			return id < snapshotRange[0] || id >= snapshotRange[1]
		}
		return true
	}

	// collect table IDs by category
	var schedulerPauseIDs []int64

	for _, dbReplace := range schemasReplace.DbReplaceMap {
		if dbReplace.FilteredOut {
			continue
		}
		for _, tableReplace := range dbReplace.TableMap {
			if tableReplace.FilteredOut {
				continue
			}

			// collect table ID if it's outside snapshot range
			if isOutsideSnapshotRange(tableReplace.TableID) {
				schedulerPauseIDs = append(schedulerPauseIDs, tableReplace.TableID)
			}

			// also collect partition IDs for this table
			for _, partitionID := range tableReplace.PartitionMap {
				if isOutsideSnapshotRange(partitionID) {
					schedulerPauseIDs = append(schedulerPauseIDs, partitionID)
				}
			}
		}
	}

	// create range for scheduler pause IDs (should be contiguous since allocated in one batch)
	if len(schedulerPauseIDs) > 0 {
		sort.Slice(schedulerPauseIDs, func(i, j int) bool {
			return schedulerPauseIDs[i] < schedulerPauseIDs[j]
		})

		minID := schedulerPauseIDs[0]
		maxID := schedulerPauseIDs[len(schedulerPauseIDs)-1]
		logRange := [2]int64{minID, maxID + 1}
		ranges = append(ranges, logRange)

		// only verify contiguity when we have a valid snapshot range
		// (meaning schedulerPauseIDs contains only log restore tables, which should be contiguous)
		if hasValidSnapshotRange {
			contiguous := verifyContiguousIDs(schedulerPauseIDs)
			log.Info("using log restore range for scheduler pausing",
				zap.Int64("start", logRange[0]),
				zap.Int64("end", logRange[1]),
				zap.Int("table-count", len(schedulerPauseIDs)),
				zap.Bool("contiguous", contiguous))

			if !contiguous {
				log.Warn("log restore table IDs are not contiguous, using broader range",
					zap.Int64s("ids", schedulerPauseIDs))
			}
		} else {
			log.Info("no valid snapshot range found, pausing scheduler for entire ID range found in log backup",
				zap.Int64("start", logRange[0]),
				zap.Int64("end", logRange[1]),
				zap.Int("table-count", len(schedulerPauseIDs)))
		}
	}

	// convert ID ranges to key ranges
	keyRanges := make([][2]kv.Key, 0, len(ranges))
	for _, idRange := range ranges {
		startKey := codec.EncodeBytes([]byte{}, tablecodec.EncodeTablePrefix(idRange[0]))
		endKey := codec.EncodeBytes([]byte{}, tablecodec.EncodeTablePrefix(idRange[1]))
		keyRanges = append(keyRanges, [2]kv.Key{startKey, endKey})
	}

	return keyRanges
}

// verifyContiguousIDs checks if the sorted table IDs are contiguous
func verifyContiguousIDs(ids []int64) bool {
	for i := 1; i < len(ids); i++ {
		if ids[i] != ids[i-1]+1 {
			return false
		}
	}
	return true
}
