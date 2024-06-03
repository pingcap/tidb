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
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/httputil"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/metautil"
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
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/util/cdcutil"
	"github.com/spf13/pflag"
	"github.com/tikv/client-go/v2/oracle"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	flagYes              = "yes"
	flagUntil            = "until"
	flagStreamJSONOutput = "json"
	flagStreamTaskName   = "task-name"
	flagStreamStartTS    = "start-ts"
	flagStreamEndTS      = "end-ts"
	flagGCSafePointTTS   = "gc-ttl"

	truncateLockPath   = "truncating.lock"
	hintOnTruncateLock = "There might be another truncate task running, or a truncate task that didn't exit properly. " +
		"You may check the metadata and continue by wait other task finish or manually delete the lock file " + truncateLockPath + " at the external storage."
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
	Until      uint64 `json:"until" toml:"until"`
	DryRun     bool   `json:"dry-run" toml:"dry-run"`
	SkipPrompt bool   `json:"skip-prompt" toml:"skip-prompt"`

	// Spec for the command `status`.
	JSONOutput bool `json:"json-output" toml:"json-output"`

	// Spec for the command `advancer`.
	AdvancerCfg advancercfg.Config `json:"advancer-config" toml:"advancer-config"`
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

	// just stream start need Storage
	s := &streamMgr{
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
		s.bc = client

		// create http client to do some requirements check.
		s.httpCli = httputil.NewClient(mgr.GetTLSConfig())
	}
	return s, nil
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
	currentTS, err := s.mgr.GetTS(ctx)
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

	schemas := backup.NewBackupSchemas(func(storage kv.Storage, fn func(*model.DBInfo, *model.TableInfo)) error {
		return backup.BuildFullSchema(storage, s.cfg.StartTS, func(dbInfo *model.DBInfo, tableInfo *model.TableInfo) {
			fn(dbInfo, tableInfo)
		})
	}, 0)

	err = schemas.BackupSchemas(ctx, metaWriter, nil, s.mgr.GetStorage(), nil,
		s.cfg.StartTS, backup.DefaultSchemaConcurrency, 0, true, nil)
	if err != nil {
		return errors.Trace(err)
	}

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

type RestoreFunc func(string) error

// KeepGcDisabled keeps GC disabled and return a function that used to gc enabled.
// gc.ratio-threshold = "-1.0", which represents disable gc in TiKV.
func KeepGcDisabled(g glue.Glue, store kv.Storage) (RestoreFunc, string, error) {
	se, err := g.CreateSession(store)
	if err != nil {
		return nil, "", errors.Trace(err)
	}

	execCtx := se.GetSessionCtx().GetRestrictedSQLExecutor()
	oldRatio, err := utils.GetGcRatio(execCtx)
	if err != nil {
		return nil, "", errors.Trace(err)
	}

	newRatio := "-1.0"
	err = utils.SetGcRatio(execCtx, newRatio)
	if err != nil {
		return nil, "", errors.Trace(err)
	}

	// If the oldRatio is negative, which is not normal status.
	// It should set default value "1.1" after PiTR finished.
	if strings.HasPrefix(oldRatio, "-") {
		oldRatio = utils.DefaultGcRatioVal
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
		log.Error("failed to stream", zap.String("command", cmdName), zap.Error(err))
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
	if err = streamMgr.checkImportTaskRunning(ctx, cli.Client); err != nil {
		return errors.Trace(err)
	}
	// It supports single stream log task currently.
	if count, err := cli.GetTaskCount(ctx); err != nil {
		return errors.Trace(err)
	} else if count > 0 {
		return errors.Annotate(berrors.ErrStreamLogTaskExist, "It supports single stream log task currently")
	}

	exist, err := streamMgr.checkLock(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	// exist is true, which represents restart a stream task. Or create a new stream task.
	if exist {
		logInfo, err := getLogRange(ctx, &cfg.Config)
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

	ti := streamhelper.TaskInfo{
		PBInfo: backuppb.StreamBackupTaskInfo{
			Storage:         streamMgr.bc.GetStorageBackend(),
			StartTs:         cfg.StartTS,
			EndTs:           cfg.EndTS,
			Name:            cfg.TaskName,
			TableFilter:     cfg.FilterStr,
			CompressionType: backuppb.CompressionType_ZSTD,
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

	logInfo, err := getLogRange(ctx, &cfg.Config)
	if err != nil {
		return errors.Trace(err)
	}

	logMinDate := stream.FormatDate(oracle.GetTimeFromTS(logInfo.logMinTS))
	logMaxDate := stream.FormatDate(oracle.GetTimeFromTS(logInfo.logMaxTS))
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

	err = cli.PauseTask(ctx, cfg.TaskName)
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
	mgr, err := NewMgr(ctx, g, cfg.PD, cfg.TLS, GetKeepalive(&cfg.Config),
		cfg.CheckRequirements, false, conn.StreamVersionChecker)
	if err != nil {
		return err
	}

	etcdCLI, err := dialEtcdWithCfg(ctx, cfg.Config)
	if err != nil {
		return err
	}
	env := streamhelper.CliEnv(mgr.StoreManager, mgr.GetStore(), etcdCLI)
	advancer := streamhelper.NewCheckpointAdvancer(env)
	advancer.UpdateConfig(cfg.AdvancerCfg)
	advancerd := daemon.New(advancer, streamhelper.OwnerManagerForLogBackup(ctx, etcdCLI), cfg.AdvancerCfg.TickDuration)
	loop, err := advancerd.Begin(ctx)
	if err != nil {
		return err
	}
	loop()
	return nil
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
	return stream.NewStatusController(cli, mgr, printer), nil
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
	if err := storage.TryLockRemote(ctx, extStorage, truncateLockPath, hintOnTruncateLock); err != nil {
		return err
	}
	defer utils.WithCleanUp(&err, 10*time.Second, func(ctx context.Context) error {
		return storage.UnlockRemote(ctx, extStorage, truncateLockPath)
	})

	sp, err := stream.GetTSFromFile(ctx, extStorage, stream.TruncateSafePointFileName)
	if err != nil {
		return err
	}

	if cfg.Until < sp {
		console.Println("According to the log, you have truncated backup data before", em(formatTS(sp)))
		if !cfg.SkipPrompt && !console.PromptBool("Continue? ") {
			return nil
		}
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
	console.Printf("We are going to remove %s files, until %s.\n",
		em(fileCount),
		em(formatTS(cfg.Until)),
	)
	if !cfg.SkipPrompt && !console.PromptBool(warn("Sure? ")) {
		return nil
	}

	if cfg.Until > sp && !cfg.DryRun {
		if err := stream.SetTSToFile(
			ctx, extStorage, cfg.Until, stream.TruncateSafePointFileName); err != nil {
			return err
		}
	}

	// begin to remove
	p := console.StartProgressBar(
		"Clearing Data Files and Metadata", fileCount,
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

// checkTaskExists checks whether there is a log backup task running.
// If so, return an error.
func checkTaskExists(ctx context.Context, cfg *RestoreConfig, etcdCLI *clientv3.Client) error {
	if err := checkConfigForStatus(cfg.PD); err != nil {
		return err
	}

	cli := streamhelper.NewMetaDataClient(etcdCLI)
	// check log backup task
	tasks, err := cli.GetAllTasks(ctx)
	if err != nil {
		return err
	}
	if len(tasks) > 0 {
		return errors.Errorf("log backup task is running: %s, "+
			"please stop the task before restore, and after PITR operation finished, "+
			"create log-backup task again and create a full backup on this cluster", tasks[0].Info.Name)
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

// RunStreamRestore restores stream log.
func RunStreamRestore(
	c context.Context,
	g glue.Glue,
	cmdName string,
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
	logInfo, err := getLogRangeWithStorage(ctx, s)
	if err != nil {
		return errors.Trace(err)
	}
	if cfg.RestoreTS == 0 {
		cfg.RestoreTS = logInfo.logMaxTS
	}

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

	log.Info("start restore on point",
		zap.Uint64("restore-from", cfg.StartTS), zap.Uint64("restore-to", cfg.RestoreTS),
		zap.Uint64("log-min-ts", logInfo.logMinTS), zap.Uint64("log-max-ts", logInfo.logMaxTS))
	if err := checkLogRange(cfg.StartTS, cfg.RestoreTS, logInfo.logMinTS, logInfo.logMaxTS); err != nil {
		return errors.Trace(err)
	}

	curTaskInfo, doFullRestore, err := checkPiTRTaskInfo(ctx, g, s, cfg)
	if err != nil {
		return errors.Trace(err)
	}

	failpoint.Inject("failed-before-full-restore", func(_ failpoint.Value) {
		failpoint.Return(errors.New("failpoint: failed before full restore"))
	})

	recorder := tiflashrec.New()
	cfg.tiflashRecorder = recorder
	// restore full snapshot.
	if doFullRestore {
		logStorage := cfg.Config.Storage
		cfg.Config.Storage = cfg.FullBackupStorage
		// TiFlash replica is restored to down-stream on 'pitr' currently.
		if err = runRestore(ctx, g, FullRestoreCmd, cfg); err != nil {
			return errors.Trace(err)
		}
		cfg.Config.Storage = logStorage
	} else if len(cfg.FullBackupStorage) > 0 {
		skipMsg := []byte(fmt.Sprintf("%s command is skipped due to checkpoint mode for restore\n", FullRestoreCmd))
		if _, err := glue.GetConsole(g).Out().Write(skipMsg); err != nil {
			return errors.Trace(err)
		}
		if curTaskInfo != nil && curTaskInfo.TiFlashItems != nil {
			log.Info("load tiflash records of snapshot restore from checkpoint")
			if err != nil {
				return errors.Trace(err)
			}
			cfg.tiflashRecorder.Load(curTaskInfo.TiFlashItems)
		}
	}
	// restore log.
	cfg.adjustRestoreConfigForStreamRestore()
	if err := restoreStream(ctx, g, cfg, curTaskInfo); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// RunStreamRestore start restore job
func restoreStream(
	c context.Context,
	g glue.Glue,
	cfg *RestoreConfig,
	taskInfo *checkpoint.CheckpointTaskInfoForLogRestore,
) (err error) {
	var (
		totalKVCount           uint64
		totalSize              uint64
		checkpointTotalKVCount uint64
		checkpointTotalSize    uint64
		currentTS              uint64
		mu                     sync.Mutex
		startTime              = time.Now()
	)
	defer func() {
		if err != nil {
			summary.Log("restore log failed summary", zap.Error(err))
		} else {
			totalDureTime := time.Since(startTime)
			summary.Log("restore log success summary", zap.Duration("total-take", totalDureTime),
				zap.Uint64("source-start-point", cfg.StartTS),
				zap.Uint64("source-end-point", cfg.RestoreTS),
				zap.Uint64("target-end-point", currentTS),
				zap.String("source-start", stream.FormatDate(oracle.GetTimeFromTS(cfg.StartTS))),
				zap.String("source-end", stream.FormatDate(oracle.GetTimeFromTS(cfg.RestoreTS))),
				zap.String("target-end", stream.FormatDate(oracle.GetTimeFromTS(currentTS))),
				zap.Uint64("total-kv-count", totalKVCount),
				zap.Uint64("skipped-kv-count-by-checkpoint", checkpointTotalKVCount),
				zap.String("total-size", units.HumanSize(float64(totalSize))),
				zap.String("skipped-size-by-checkpoint", units.HumanSize(float64(checkpointTotalSize))),
				zap.String("average-speed", units.HumanSize(float64(totalSize)/totalDureTime.Seconds())+"/s"),
			)
		}
	}()

	ctx, cancelFn := context.WithCancel(c)
	defer cancelFn()

	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan(
			"restoreStream",
			opentracing.ChildOf(span.Context()),
		)
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	mgr, err := NewMgr(ctx, g, cfg.PD, cfg.TLS, GetKeepalive(&cfg.Config),
		cfg.CheckRequirements, true, conn.StreamVersionChecker)
	if err != nil {
		return errors.Trace(err)
	}
	defer mgr.Close()

	client, err := createRestoreClient(ctx, g, cfg, mgr)
	if err != nil {
		return errors.Annotate(err, "failed to create restore client")
	}
	defer client.Close()

	if taskInfo != nil && taskInfo.RewriteTS > 0 {
		// reuse the task's rewrite ts
		log.Info("reuse the task's rewrite ts", zap.Uint64("rewrite-ts", taskInfo.RewriteTS))
		currentTS = taskInfo.RewriteTS
	} else {
		currentTS, err = restore.GetTSWithRetry(ctx, mgr.GetPDClient())
		if err != nil {
			return errors.Trace(err)
		}
	}
	client.SetCurrentTS(currentTS)

	importModeSwitcher := restore.NewImportModeSwitcher(mgr.GetPDClient(), cfg.Config.SwitchModeInterval, mgr.GetTLSConfig())
	restoreSchedulers, _, err := restore.RestorePreWork(ctx, mgr, importModeSwitcher, cfg.Online, false)
	if err != nil {
		return errors.Trace(err)
	}
	// Always run the post-work even on error, so we don't stuck in the import
	// mode or emptied schedulers
	defer restore.RestorePostWork(ctx, importModeSwitcher, restoreSchedulers, cfg.Online)

	// It need disable GC in TiKV when PiTR.
	// because the process of PITR is concurrent and kv events isn't sorted by tso.
	restoreGc, oldRatio, err := KeepGcDisabled(g, mgr.GetStorage())
	if err != nil {
		return errors.Trace(err)
	}
	gcDisabledRestorable := false
	defer func() {
		// don't restore the gc-ratio-threshold if checkpoint mode is used and restored is not finished
		if cfg.UseCheckpoint && !gcDisabledRestorable {
			log.Info("skip restore the gc-ratio-threshold for next retry")
			return
		}

		log.Info("start to restore gc", zap.String("ratio", oldRatio))
		if err := restoreGc(oldRatio); err != nil {
			log.Error("failed to set gc enabled", zap.Error(err))
		}
		log.Info("finish restoring gc")
	}()

	var taskName string
	var checkpointRunner *checkpoint.CheckpointRunner[checkpoint.LogRestoreKeyType, checkpoint.LogRestoreValueType]
	if cfg.UseCheckpoint {
		taskName = cfg.generateLogRestoreTaskName(client.GetClusterID(ctx), cfg.StartTS, cfg.RestoreTS)
		oldRatioFromCheckpoint, err := client.InitCheckpointMetadataForLogRestore(ctx, taskName, oldRatio)
		if err != nil {
			return errors.Trace(err)
		}
		oldRatio = oldRatioFromCheckpoint

		checkpointRunner, err = client.StartCheckpointRunnerForLogRestore(ctx, taskName)
		if err != nil {
			return errors.Trace(err)
		}
		defer func() {
			log.Info("wait for flush checkpoint...")
			checkpointRunner.WaitForFinish(ctx, !gcDisabledRestorable)
		}()
	}

	err = client.InstallLogFileManager(ctx, cfg.StartTS, cfg.RestoreTS, cfg.MetadataDownloadBatchSize)
	if err != nil {
		return err
	}

	// get full backup meta storage to generate rewrite rules.
	fullBackupStorage, err := parseFullBackupTablesStorage(cfg)
	if err != nil {
		return errors.Trace(err)
	}
	// load the id maps only when the checkpoint mode is used and not the first execution
	newTask := true
	if taskInfo != nil && taskInfo.Progress == checkpoint.InLogRestoreAndIdMapPersist {
		newTask = false
	}
	// get the schemas ID replace information.
	schemasReplace, err := client.InitSchemasReplaceForDDL(ctx, &logclient.InitSchemaConfig{
		IsNewTask:         newTask,
		HasFullRestore:    len(cfg.FullBackupStorage) > 0,
		TableFilter:       cfg.TableFilter,
		TiFlashRecorder:   cfg.tiflashRecorder,
		FullBackupStorage: fullBackupStorage,
	})
	if err != nil {
		return errors.Trace(err)
	}
	schemasReplace.AfterTableRewritten = func(deleted bool, tableInfo *model.TableInfo) {
		// When the table replica changed to 0, the tiflash replica might be set to `nil`.
		// We should remove the table if we meet.
		if deleted || tableInfo.TiFlashReplica == nil {
			cfg.tiflashRecorder.DelTable(tableInfo.ID)
			return
		}
		cfg.tiflashRecorder.AddTable(tableInfo.ID, *tableInfo.TiFlashReplica)
		// Remove the replica firstly. Let's restore them at the end.
		tableInfo.TiFlashReplica = nil
	}

	updateStats := func(kvCount uint64, size uint64) {
		mu.Lock()
		defer mu.Unlock()
		totalKVCount += kvCount
		totalSize += size
	}
	dataFileCount := 0
	ddlFiles, err := client.LoadDDLFilesAndCountDMLFiles(ctx, &dataFileCount)
	if err != nil {
		return err
	}
	pm := g.StartProgress(ctx, "Restore Meta Files", int64(len(ddlFiles)), !cfg.LogProgress)
	if err = withProgress(pm, func(p glue.Progress) error {
		client.RunGCRowsLoader(ctx)
		return client.RestoreMetaKVFiles(ctx, ddlFiles, schemasReplace, updateStats, p.Inc)
	}); err != nil {
		return errors.Annotate(err, "failed to restore meta files")
	}

	rewriteRules := initRewriteRules(schemasReplace)

	ingestRecorder := schemasReplace.GetIngestRecorder()
	if err := rangeFilterFromIngestRecorder(ingestRecorder, rewriteRules); err != nil {
		return errors.Trace(err)
	}

	// generate the upstream->downstream id maps for checkpoint
	idrules := make(map[int64]int64)
	downstreamIdset := make(map[int64]struct{})
	for upstreamId, rule := range rewriteRules {
		downstreamId := restoreutils.GetRewriteTableID(upstreamId, rule)
		idrules[upstreamId] = downstreamId
		downstreamIdset[downstreamId] = struct{}{}
	}

	logFilesIter, err := client.LoadDMLFiles(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	pd := g.StartProgress(ctx, "Restore KV Files", int64(dataFileCount), !cfg.LogProgress)
	err = withProgress(pd, func(p glue.Progress) error {
		if cfg.UseCheckpoint {
			updateStatsWithCheckpoint := func(kvCount, size uint64) {
				mu.Lock()
				defer mu.Unlock()
				totalKVCount += kvCount
				totalSize += size
				checkpointTotalKVCount += kvCount
				checkpointTotalSize += size
			}
			logFilesIter, err = client.WrapLogFilesIterWithCheckpoint(ctx, logFilesIter, downstreamIdset, taskName, updateStatsWithCheckpoint, p.Inc)
			if err != nil {
				return errors.Trace(err)
			}
		}
		logFilesIterWithSplit, err := client.WrapLogFilesIterWithSplitHelper(logFilesIter, rewriteRules, g, mgr.GetStorage())
		if err != nil {
			return errors.Trace(err)
		}

		return client.RestoreKVFiles(ctx, rewriteRules, idrules, logFilesIterWithSplit, checkpointRunner, cfg.PitrBatchCount, cfg.PitrBatchSize, updateStats, p.IncBy)
	})
	if err != nil {
		return errors.Annotate(err, "failed to restore kv files")
	}

	if err = client.CleanUpKVFiles(ctx); err != nil {
		return errors.Annotate(err, "failed to clean up")
	}

	if err = client.InsertGCRows(ctx); err != nil {
		return errors.Annotate(err, "failed to insert rows into gc_delete_range")
	}

	if err = client.RepairIngestIndex(ctx, ingestRecorder, g, taskName); err != nil {
		return errors.Annotate(err, "failed to repair ingest index")
	}

	if cfg.tiflashRecorder != nil {
		sqls := cfg.tiflashRecorder.GenerateAlterTableDDLs(mgr.GetDomain().InfoSchema())
		log.Info("Generating SQLs for restoring TiFlash Replica",
			zap.Strings("sqls", sqls))
		err = g.UseOneShotSession(mgr.GetStorage(), false, func(se glue.Session) error {
			for _, sql := range sqls {
				if errExec := se.ExecuteInternal(ctx, sql); errExec != nil {
					logutil.WarnTerm("Failed to restore tiflash replica config, you may execute the sql restore it manually.",
						logutil.ShortError(errExec),
						zap.String("sql", sql),
					)
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	failpoint.Inject("do-checksum-with-rewrite-rules", func(_ failpoint.Value) {
		if err := client.FailpointDoChecksumForLogRestore(ctx, mgr.GetStorage().GetClient(), mgr.GetPDClient(), idrules, rewriteRules); err != nil {
			failpoint.Return(errors.Annotate(err, "failed to do checksum"))
		}
	})

	gcDisabledRestorable = true

	return nil
}

func createRestoreClient(ctx context.Context, g glue.Glue, cfg *RestoreConfig, mgr *conn.Mgr) (*logclient.LogClient, error) {
	var err error
	keepaliveCfg := GetKeepalive(&cfg.Config)
	keepaliveCfg.PermitWithoutStream = true
	client := logclient.NewRestoreClient(mgr.GetPDClient(), mgr.GetPDHTTPClient(), mgr.GetTLSConfig(), keepaliveCfg)
	err = client.Init(g, mgr.GetStorage())
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer func() {
		if err != nil {
			client.Close()
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
	client.SetConcurrency(uint(cfg.Concurrency))
	client.InitClients(ctx, u)

	err = client.SetRawKVBatchClient(ctx, cfg.PD, cfg.TLS.ToKVSecurity())
	if err != nil {
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

func checkLogRange(restoreFrom, restoreTo, logMinTS, logMaxTS uint64) error {
	// serveral ts constraint：
	// logMinTS <= restoreFrom <= restoreTo <= logMaxTS
	if logMinTS > restoreFrom || restoreFrom > restoreTo || restoreTo > logMaxTS {
		return errors.Annotatef(berrors.ErrInvalidArgument,
			"restore log from %d(%s) to %d(%s), "+
				" but the current existed log from %d(%s) to %d(%s)",
			restoreFrom, oracle.GetTimeFromTS(restoreFrom),
			restoreTo, oracle.GetTimeFromTS(restoreTo),
			logMinTS, oracle.GetTimeFromTS(logMinTS),
			logMaxTS, oracle.GetTimeFromTS(logMaxTS),
		)
	}
	return nil
}

// withProgress execute some logic with the progress, and close it once the execution done.
func withProgress(p glue.Progress, cc func(p glue.Progress) error) error {
	defer p.Close()
	return cc(p)
}

type backupLogInfo struct {
	logMaxTS  uint64
	logMinTS  uint64
	clusterID uint64
}

// getLogRange gets the log-min-ts and log-max-ts of starting log backup.
func getLogRange(
	ctx context.Context,
	cfg *Config,
) (backupLogInfo, error) {
	_, s, err := GetStorage(ctx, cfg.Storage, cfg)
	if err != nil {
		return backupLogInfo{}, errors.Trace(err)
	}
	return getLogRangeWithStorage(ctx, s)
}

func getLogRangeWithStorage(
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

// getFullBackupTS gets the snapshot-ts of full bakcup
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

	backupmeta := &backuppb.BackupMeta{}
	if err = backupmeta.Unmarshal(metaData); err != nil {
		return 0, 0, errors.Trace(err)
	}

	return backupmeta.GetEndVersion(), backupmeta.GetClusterId(), nil
}

func parseFullBackupTablesStorage(
	cfg *RestoreConfig,
) (*logclient.FullBackupStorageConfig, error) {
	var storageName string
	if len(cfg.FullBackupStorage) > 0 {
		storageName = cfg.FullBackupStorage
	} else {
		storageName = cfg.Storage
	}
	u, err := storage.ParseBackend(storageName, &cfg.BackendOptions)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &logclient.FullBackupStorageConfig{
		Backend: u,
		Opts:    storageOpts(&cfg.Config),
	}, nil
}

func initRewriteRules(schemasReplace *stream.SchemasReplace) map[int64]*restoreutils.RewriteRules {
	rules := make(map[int64]*restoreutils.RewriteRules)
	filter := schemasReplace.TableFilter

	for _, dbReplace := range schemasReplace.DbMap {
		if utils.IsSysDB(dbReplace.Name) || !filter.MatchSchema(dbReplace.Name) {
			continue
		}

		for oldTableID, tableReplace := range dbReplace.TableMap {
			if !filter.MatchTable(dbReplace.Name, tableReplace.Name) {
				continue
			}

			if _, exist := rules[oldTableID]; !exist {
				log.Info("add rewrite rule",
					zap.String("tableName", dbReplace.Name+"."+tableReplace.Name),
					zap.Int64("oldID", oldTableID), zap.Int64("newID", tableReplace.TableID))
				rules[oldTableID] = restoreutils.GetRewriteRuleOfTable(
					oldTableID, tableReplace.TableID, 0, tableReplace.IndexMap, false)
			}

			for oldID, newID := range tableReplace.PartitionMap {
				if _, exist := rules[oldID]; !exist {
					log.Info("add rewrite rule",
						zap.String("tableName", dbReplace.Name+"."+tableReplace.Name),
						zap.Int64("oldID", oldID), zap.Int64("newID", newID))
					rules[oldID] = restoreutils.GetRewriteRuleOfTable(oldID, newID, 0, tableReplace.IndexMap, false)
				}
			}
		}
	}
	return rules
}

// ShiftTS gets a smaller shiftTS than startTS.
// It has a safe duration between shiftTS and startTS for trasaction.
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

func checkPiTRRequirements(mgr *conn.Mgr) error {
	userDBs := restore.GetExistedUserDBs(mgr.GetDomain())
	if len(userDBs) > 0 {
		userDBNames := make([]string, 0, len(userDBs))
		for _, db := range userDBs {
			userDBNames = append(userDBNames, db.Name.O)
		}
		return errors.Annotatef(berrors.ErrDatabasesAlreadyExisted,
			"databases %s existed in restored cluster, please drop them before execute PiTR",
			strings.Join(userDBNames, ","))
	}

	return nil
}

func checkPiTRTaskInfo(
	ctx context.Context,
	g glue.Glue,
	s storage.ExternalStorage,
	cfg *RestoreConfig,
) (*checkpoint.CheckpointTaskInfoForLogRestore, bool, error) {
	var (
		doFullRestore = (len(cfg.FullBackupStorage) > 0)

		curTaskInfo *checkpoint.CheckpointTaskInfoForLogRestore

		errTaskMsg string
	)
	mgr, err := NewMgr(ctx, g, cfg.PD, cfg.TLS, GetKeepalive(&cfg.Config),
		cfg.CheckRequirements, true, conn.StreamVersionChecker)
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	defer mgr.Close()

	clusterID := mgr.GetPDClient().GetClusterID(ctx)
	if cfg.UseCheckpoint {
		exists, err := checkpoint.ExistsCheckpointTaskInfo(ctx, s, clusterID)
		if err != nil {
			return nil, false, errors.Trace(err)
		}
		if exists {
			curTaskInfo, err = checkpoint.LoadCheckpointTaskInfoForLogRestore(ctx, s, clusterID)
			if err != nil {
				return nil, false, errors.Trace(err)
			}
			// TODO: check whether user has manually modified the cluster(ddl). If so, regard the behavior
			//       as restore from scratch. (update `curTaskInfo.RewriteTs` to 0 as an uninitial value)

			// The task info is written to external storage without status `InSnapshotRestore` only when
			// id-maps is persist into external storage, so there is no need to do snapshot restore again.
			if curTaskInfo.StartTS == cfg.StartTS && curTaskInfo.RestoreTS == cfg.RestoreTS {
				// the same task, check whether skip snapshot restore
				doFullRestore = doFullRestore && (curTaskInfo.Progress == checkpoint.InSnapshotRestore)
				// update the snapshot restore task name to clean up in final
				if !doFullRestore && (len(cfg.FullBackupStorage) > 0) {
					_ = cfg.generateSnapshotRestoreTaskName(clusterID)
				}
				log.Info("the same task", zap.Bool("skip-snapshot-restore", !doFullRestore))
			} else {
				// not the same task, so overwrite the taskInfo with a new task
				log.Info("not the same task, start to restore from scratch")
				errTaskMsg = fmt.Sprintf(
					"a new task [start-ts=%d] [restored-ts=%d] while the last task info: [start-ts=%d] [restored-ts=%d] [skip-snapshot-restore=%t]",
					cfg.StartTS, cfg.RestoreTS, curTaskInfo.StartTS, curTaskInfo.RestoreTS, curTaskInfo.Progress == checkpoint.InLogRestoreAndIdMapPersist)

				curTaskInfo = nil
			}
		}
	}

	// restore full snapshot precheck.
	if doFullRestore {
		if !(cfg.UseCheckpoint && curTaskInfo != nil) {
			// Only when use checkpoint and not the first execution,
			// skip checking requirements.
			log.Info("check pitr requirements for the first execution")
			if err := checkPiTRRequirements(mgr); err != nil {
				if len(errTaskMsg) > 0 {
					err = errors.Annotatef(err, "The current restore task is regarded as %s. "+
						"If you ensure that no changes have been made to the cluster since the last execution, "+
						"you can adjust the `start-ts` or `restored-ts` to continue with the previous execution. "+
						"Otherwise, if you want to restore from scratch, please clean the cluster at first", errTaskMsg)
				}
				return nil, false, errors.Trace(err)
			}
		}
	}

	// persist the new task info
	if cfg.UseCheckpoint && curTaskInfo == nil {
		log.Info("save checkpoint task info with `InSnapshotRestore` status")
		if err := checkpoint.SaveCheckpointTaskInfoForLogRestore(ctx, s, &checkpoint.CheckpointTaskInfoForLogRestore{
			Progress:  checkpoint.InSnapshotRestore,
			StartTS:   cfg.StartTS,
			RestoreTS: cfg.RestoreTS,
			// updated in the stage of `InLogRestoreAndIdMapPersist`
			RewriteTS:    0,
			TiFlashItems: nil,
		}, clusterID); err != nil {
			return nil, false, errors.Trace(err)
		}
	}

	return curTaskInfo, doFullRestore, nil
}
