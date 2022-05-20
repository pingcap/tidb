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
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/backup"
	"github.com/pingcap/tidb/br/pkg/conn"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/httputil"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/spf13/pflag"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	flagYes              = "yes"
	flagDryRun           = "dry-run"
	flagUntil            = "until"
	flagStreamJSONOutput = "json"
	flagStreamTaskName   = "task-name"
	flagStreamStartTS    = "start-ts"
	flagStreamEndTS      = "end-ts"
	flagGCSafePointTTS   = "gc-ttl"
)

var (
	StreamStart    = "log start"
	StreamStop     = "log stop"
	StreamPause    = "log pause"
	StreamResume   = "log resume"
	StreamStatus   = "log status"
	StreamTruncate = "log truncate"
	StreamMetadata = "log metadata"

	skipSummaryCommandList = map[string]struct{}{
		StreamStatus:   {},
		StreamTruncate: {},
	}

	// rawKVBatchCount specifies the count of entries that the rawkv client puts into TiKV.
	rawKVBatchCount = 64

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
}

func (cfg *StreamConfig) makeStorage(ctx context.Context) (storage.ExternalStorage, error) {
	u, err := storage.ParseBackend(cfg.Storage, &cfg.BackendOptions)
	if err != nil {
		return nil, errors.Trace(err)
	}
	opts := storage.ExternalStorageOptions{
		NoCredentials:   cfg.NoCreds,
		SendCredentials: cfg.SendCreds,
	}
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
			"support TSO or datetime, e.g. '400036290571534337' or '2018-05-11 01:42:23'.")
	flags.String(flagStreamEndTS, "2035-1-1 00:00:00", "end ts, indicate stopping observe after endTS"+
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
		"(support TSO or datetime, e.g. '400036290571534337' or '2018-05-11 01:42:23'.)")
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
	if cfg.Until, err = ParseTSString(tsString); err != nil {
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

	if cfg.StartTS, err = ParseTSString(tsString); err != nil {
		return errors.Trace(err)
	}

	tsString, err = flags.GetString(flagStreamEndTS)
	if err != nil {
		return errors.Trace(err)
	}

	if cfg.EndTS, err = ParseTSString(tsString); err != nil {
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
		cfg.CheckRequirements, true)
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
		backend, err := storage.ParseBackend(cfg.Storage, &cfg.BackendOptions)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if backend.GetS3() == nil {
			return nil, errors.Annotate(berrors.ErrStorageInvalidConfig,
				"Only support s3 storage currently.")
		}

		opts := storage.ExternalStorageOptions{
			NoCredentials:   cfg.NoCreds,
			SendCredentials: cfg.SendCreds,
		}
		client, err := backup.NewBackupClient(ctx, mgr)
		if err != nil {
			return nil, errors.Trace(err)
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

	if currentTS < s.cfg.StartTS || s.cfg.EndTS <= currentTS {
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

func (s *streamMgr) buildObserveRanges(ctx context.Context) ([]kv.KeyRange, error) {
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
	sort.Slice(rs, func(i, j int) bool {
		return bytes.Compare(rs[i].StartKey, rs[j].StartKey) < 0
	})

	return rs, nil
}

// checkRequirements will check some requirements before stream starts.
func (s *streamMgr) checkRequirements(ctx context.Context) (bool, error) {
	type backupStream struct {
		EnableStreaming bool `json:"enable"`
	}
	type config struct {
		BackupStream backupStream `json:"log-backup"`
	}

	supportBackupStream := true
	hasTiKV := false
	err := s.mgr.GetConfigFromTiKV(ctx, s.httpCli, func(resp *http.Response) error {
		hasTiKV = true
		c := &config{}
		e := json.NewDecoder(resp.Body).Decode(c)
		if e != nil {
			return e
		}
		supportBackupStream = supportBackupStream && c.BackupStream.EnableStreaming
		return nil
	})
	if err != nil {
		return false, errors.Trace(err)
	}
	return hasTiKV && supportBackupStream, err
}

func (s *streamMgr) backupFullSchemas(ctx context.Context, g glue.Glue) error {
	metaWriter := metautil.NewMetaWriter(s.bc.GetStorage(), metautil.MetaFileSize, false, metautil.MetaFile, nil)
	metaWriter.Update(func(m *backuppb.BackupMeta) {
		// save log startTS to backupmeta file
		m.StartVersion = s.cfg.StartTS
	})

	schemas, err := backup.BuildFullSchema(s.mgr.GetStorage(), s.cfg.StartTS)
	if err != nil {
		return errors.Trace(err)
	}

	schemasConcurrency := uint(mathutil.Min(backup.DefaultSchemaConcurrency, schemas.Len()))
	err = schemas.BackupSchemas(ctx, metaWriter, s.mgr.GetStorage(), nil,
		s.cfg.StartTS, schemasConcurrency, 0, true, nil)
	if err != nil {
		return errors.Trace(err)
	}

	if err = metaWriter.FlushBackupMeta(ctx); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// RunStreamCommand run all kinds of `stream task``
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

	supportStream, err := streamMgr.checkRequirements(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	if !supportStream {
		return errors.New("Unable to create task about log-backup. " +
			"please set TiKV config `log-backup.enable` to true and restart TiKVs.")
	}

	if err = streamMgr.adjustAndCheckStartTS(ctx); err != nil {
		return errors.Trace(err)
	}

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

	cli := stream.NewMetaDataClient(streamMgr.mgr.GetDomain().GetEtcdClient())
	// It supports single stream log task currently.
	if count, err := cli.GetTaskCount(ctx); err != nil {
		return errors.Trace(err)
	} else if count > 0 {
		return errors.Annotate(berrors.ErrStreamLogTaskExist, "It supports single stream log task currently")
	}

	if err = streamMgr.setLock(ctx); err != nil {
		return errors.Trace(err)
	}

	if err = streamMgr.backupFullSchemas(ctx, g); err != nil {
		return errors.Trace(err)
	}

	ranges, err := streamMgr.buildObserveRanges(ctx)
	if err != nil {
		return errors.Trace(err)
	} else if len(ranges) == 0 {
		// nothing to backup
		pdAddress := strings.Join(cfg.PD, ",")
		log.Warn("Nothing to observe, maybe connected to cluster for restoring",
			zap.String("PD address", pdAddress))
		return errors.Annotate(berrors.ErrInvalidArgument, "nothing need to observe")
	}

	ti := stream.TaskInfo{
		PBInfo: backuppb.StreamBackupTaskInfo{
			Storage:     streamMgr.bc.GetStorageBackend(),
			StartTs:     cfg.StartTS,
			EndTs:       cfg.EndTS,
			Name:        cfg.TaskName,
			TableFilter: cfg.FilterStr,
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

	logMinTS, logMaxTS, err := getLogRange(ctx, &cfg.Config)
	if err != nil {
		return errors.Trace(err)
	}

	summary.Log(cmdName, zap.Uint64("log-min-ts", logMinTS),
		zap.String("log-min-date", oracle.GetTimeFromTS(logMinTS).String()),
		zap.Uint64("log-max-ts", logMaxTS),
		zap.String("log-max-date", oracle.GetTimeFromTS(logMaxTS).String()),
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

	cli := stream.NewMetaDataClient(streamMgr.mgr.GetDomain().GetEtcdClient())
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
			TTL:      0,
			BackupTS: 0,
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

	cli := stream.NewMetaDataClient(streamMgr.mgr.GetDomain().GetEtcdClient())
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

	cli := stream.NewMetaDataClient(streamMgr.mgr.GetDomain().GetEtcdClient())
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
			TTL:      0,
			BackupTS: globalCheckPointTS,
		},
	); err != nil {
		log.Warn("failed to remove safe point",
			zap.Uint64("safe-point", globalCheckPointTS), zap.String("error", err.Error()))
	}

	summary.Log(cmdName, logutil.StreamBackupTaskInfo(&ti.Info))
	return nil
}

func checkConfigForStatus(cfg *StreamConfig) error {
	if len(cfg.PD) == 0 {
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
	cli := stream.NewMetaDataClient(etcdCLI)
	var printer stream.TaskPrinter
	if !cfg.JSONOutput {
		printer = stream.PrintTaskByTable(console)
	} else {
		printer = stream.PrintTaskWithJSON(console)
	}
	mgr, err := NewMgr(ctx, g, cfg.PD, cfg.TLS, GetKeepalive(&cfg.Config),
		cfg.CheckRequirements, false)
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

	if err := checkConfigForStatus(cfg); err != nil {
		return err
	}
	ctl, err := makeStatusController(ctx, cfg, g)
	if err != nil {
		return err
	}
	return ctl.PrintStatusOfTask(ctx, cfg.TaskName)
}

// RunStreamTruncate truncates the log that belong to (0, until-ts)
func RunStreamTruncate(c context.Context, g glue.Glue, cmdName string, cfg *StreamConfig) error {
	console := glue.GetConsole(g)
	em := color.New(color.Bold).SprintFunc()
	done := color.New(color.FgGreen).SprintFunc()
	warn := color.New(color.Bold, color.FgHiRed).SprintFunc()
	formatTS := func(ts uint64) string {
		return oracle.GetTimeFromTS(ts).Format("2006-01-02 15:04:05.0000")
	}
	if cfg.Until == 0 {
		return errors.Annotatef(berrors.ErrInvalidArgument, "please provide the `--until` ts")
	}

	ctx, cancelFn := context.WithCancel(c)
	defer cancelFn()

	storage, err := cfg.makeStorage(ctx)
	if err != nil {
		return err
	}

	sp, err := restore.GetTSFromFile(ctx, storage, restore.TruncateSafePointFileName)
	if err != nil {
		return err
	}

	if cfg.Until < sp {
		console.Println("According to the log, you have truncated backup data before", em(formatTS(sp)))
		if !cfg.SkipPrompt && !console.PromptBool("Continue? ") {
			return nil
		}
	}
	if cfg.Until > sp && !cfg.DryRun {
		if err := restore.SetTSToFile(
			ctx, storage, cfg.Until, restore.TruncateSafePointFileName); err != nil {
			return err
		}
	}

	metas := restore.StreamMetadataSet{
		BeforeDoWriteBack: func(path string, last, current *backuppb.Metadata) (skip bool) {
			log.Info("Updating metadata.", zap.String("file", path),
				zap.Int("data-file-before", len(last.GetFiles())),
				zap.Int("data-file-after", len(current.GetFiles())))
			return cfg.DryRun
		},
	}
	if err := metas.LoadFrom(ctx, storage); err != nil {
		return err
	}

	fileCount := 0
	minTS := oracle.GoTimeToTS(time.Now())
	shiftUntilTS := ShiftTS(cfg.Until)
	metas.IterateFilesFullyBefore(shiftUntilTS, func(d *backuppb.DataFileInfo) (shouldBreak bool) {
		if d.MaxTs < minTS {
			minTS = d.MaxTs
		}
		fileCount++
		return
	})
	console.Printf("We are going to remove %s files, until %s.\n",
		em(fileCount),
		em(formatTS(minTS)),
	)
	if !cfg.SkipPrompt && !console.PromptBool(warn("Sure? ")) {
		return nil
	}

	removed := metas.RemoveDataBefore(shiftUntilTS)

	console.Print("Removing metadata... ")
	if !cfg.DryRun {
		if err := metas.DoWriteBack(ctx, storage); err != nil {
			return err
		}
	}
	console.Println(done("DONE"))
	console.Print("Clearing data files... ")
	for _, f := range removed {
		if !cfg.DryRun {
			if err := storage.DeleteFile(ctx, f.Path); err != nil {
				log.Warn("File not deleted.", zap.String("path", f.Path), logutil.ShortError(err))
				console.Print("\n"+em(f.Path), "not deleted, you may clear it manually:", warn(err))
			}
		}
	}
	console.Println(done("DONE"))
	return nil
}

// RunStreamRestore restores stream log.
func RunStreamRestore(
	c context.Context,
	g glue.Glue,
	cmdName string,
	cfg *RestoreConfig,
) error {
	ctx, cancelFn := context.WithCancel(c)
	defer cancelFn()

	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("task.RunStreamRestore", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	logMinTS, logMaxTS, err := getLogRange(ctx, &cfg.Config)
	if err != nil {
		return errors.Trace(err)
	}
	if cfg.RestoreTS == 0 {
		cfg.RestoreTS = logMaxTS
	}

	if len(cfg.FullBackupStorage) > 0 {
		if cfg.StartTS, err = getFullBackupTS(ctx, cfg); err != nil {
			return errors.Trace(err)
		}
		if cfg.StartTS < logMinTS {
			return errors.Annotatef(berrors.ErrInvalidArgument,
				"it has gap between full backup ts:%d(%s) and log backup ts:%d(%s). ",
				cfg.StartTS, oracle.GetTimeFromTS(cfg.StartTS),
				logMinTS, oracle.GetTimeFromTS(logMinTS))
		}
	}

	log.Info("start restore on point",
		zap.Uint64("restore-from", cfg.StartTS), zap.Uint64("restore-to", cfg.RestoreTS),
		zap.Uint64("log-min-ts", logMinTS), zap.Uint64("log-max-ts", logMaxTS))
	if err := checkLogRange(cfg.StartTS, cfg.RestoreTS, logMinTS, logMaxTS); err != nil {
		return errors.Trace(err)
	}

	// restore full snapshot.
	if len(cfg.FullBackupStorage) > 0 {
		logStorage := cfg.Config.Storage
		cfg.Config.Storage = cfg.FullBackupStorage
		// TiFlash replica is restored to down-stream on 'pitr' currently.
		cfg.skipTiflash = true
		if err = RunRestore(ctx, g, FullRestoreCmd, cfg); err != nil {
			return errors.Trace(err)
		}
		cfg.Config.Storage = logStorage
	}
	// restore log.
	if err := restoreStream(ctx, g, cfg, logMinTS, logMaxTS); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// RunStreamRestore start restore job
func restoreStream(
	c context.Context,
	g glue.Glue,
	cfg *RestoreConfig,
	logMinTS, logMaxTS uint64,
) error {
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
		cfg.CheckRequirements, true)
	if err != nil {
		return errors.Trace(err)
	}
	defer mgr.Close()

	client, err := createRestoreClient(ctx, g, cfg, mgr)
	if err != nil {
		return errors.Annotate(err, "failed to create restore client")
	}
	defer client.Close()

	currentTS, err := client.GetTS(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	client.SetRestoreRangeTS(cfg.StartTS, cfg.RestoreTS, ShiftTS(cfg.StartTS))
	client.SetCurrentTS(currentTS)

	restoreSchedulers, err := restorePreWork(ctx, client, mgr, false)
	if err != nil {
		return errors.Trace(err)
	}
	// Always run the post-work even on error, so we don't stuck in the import
	// mode or emptied schedulers
	defer restorePostWork(ctx, client, restoreSchedulers)

	// read meta by given ts.
	metas, err := client.ReadStreamMetaByTS(ctx, cfg.RestoreTS)
	if err != nil {
		return errors.Trace(err)
	}
	if len(metas) == 0 {
		log.Info("nothing to restore.")
		return nil
	}

	// read data file by given ts.
	dmlFiles, ddlFiles, err := client.ReadStreamDataFiles(ctx, metas)
	if err != nil {
		return errors.Trace(err)
	}

	// get full backup meta to generate rewrite rules.
	fullBackupTables, err := initFullBackupTables(ctx, cfg)
	if err != nil {
		return errors.Trace(err)
	}

	// get the schemas ID replace information.
	schemasReplace, err := client.InitSchemasReplaceForDDL(&fullBackupTables, cfg.TableFilter)
	if err != nil {
		return errors.Trace(err)
	}

	pm := g.StartProgress(ctx, "Restore Meta Files", int64(len(ddlFiles)), !cfg.LogProgress)
	if err = withProgress(pm, func(p glue.Progress) error {
		return client.RestoreMetaKVFiles(ctx, ddlFiles, schemasReplace, p.Inc)
	}); err != nil {
		return errors.Annotate(err, "failed to restore meta files")
	}

	// perform restore kv files
	rewriteRules, err := initRewriteRules(client, fullBackupTables)
	if err != nil {
		return errors.Trace(err)
	}
	updateRewriteRules(rewriteRules, schemasReplace)

	pd := g.StartProgress(ctx, "Restore KV Files", int64(len(dmlFiles)), !cfg.LogProgress)
	if cfg.Concurrency > defaultRestoreStreamConcurrency {
		log.Info("set restore kv files concurrency", zap.Int("concurrency", defaultRestoreStreamConcurrency))
		client.SetConcurrency(defaultRestoreConcurrency)
	}
	err = withProgress(pd, func(p glue.Progress) error {
		return client.RestoreKVFiles(ctx, rewriteRules, dmlFiles, p.Inc)
	})
	if err != nil {
		return errors.Annotate(err, "failed to restore kv files")
	}

	if err = client.CleanUpKVFiles(ctx); err != nil {
		return errors.Annotate(err, "failed to clean up")
	}

	if err = client.SaveSchemas(ctx, schemasReplace, logMinTS, cfg.RestoreTS); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func createRestoreClient(ctx context.Context, g glue.Glue, cfg *RestoreConfig, mgr *conn.Mgr) (*restore.Client, error) {
	var err error
	keepaliveCfg := GetKeepalive(&cfg.Config)
	keepaliveCfg.PermitWithoutStream = true
	client := restore.NewRestoreClient(mgr.GetPDClient(), mgr.GetTLSConfig(), keepaliveCfg, false)
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

	opts := storage.ExternalStorageOptions{
		NoCredentials:   cfg.NoCreds,
		SendCredentials: cfg.SendCreds,
	}
	if err = client.SetStorage(ctx, u, &opts); err != nil {
		return nil, errors.Trace(err)
	}
	client.SetRateLimit(cfg.RateLimit)
	client.SetCrypter(&cfg.CipherInfo)
	client.SetConcurrency(uint(cfg.Concurrency))
	client.SetSwitchModeInterval(cfg.SwitchModeInterval)
	client.InitClients(u, false)

	rawKVClient, err := newRawBatchClient(ctx, cfg.PD, cfg.TLS)
	if err != nil {
		return nil, errors.Trace(err)
	}
	client.SetRawKVClient(rawKVClient)

	err = client.LoadRestoreStores(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return client, nil
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

// nolint: unused, deadcode
func countIndices(ts map[int64]*metautil.Table) int64 {
	result := int64(0)
	for _, t := range ts {
		result += int64(len(t.Info.Indices))
	}
	return result
}

// getLogRange gets the log-min-ts and log-max-ts of starting log backup.
func getLogRange(
	ctx context.Context,
	cfg *Config,
) (uint64, uint64, error) {
	_, s, err := GetStorage(ctx, cfg.Storage, cfg)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}

	// logStartTS: Get log start ts from backupmeta file.
	metaData, err := s.ReadFile(ctx, metautil.MetaFile)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}
	backupMeta := &backuppb.BackupMeta{}
	if err = backupMeta.Unmarshal(metaData); err != nil {
		return 0, 0, errors.Trace(err)
	}
	logStartTS := backupMeta.GetStartVersion()

	// truncateTS: get log truncate ts from TruncateSafePointFileName.
	// If truncateTS equals 0, which represents the stream log has never been truncated.
	truncateTS, err := restore.GetTSFromFile(ctx, s, restore.TruncateSafePointFileName)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}
	logMinTS := mathutil.Max(logStartTS, truncateTS)

	// get max global resolved ts from metas.
	logMaxTS, err := getGlobalResolvedTS(ctx, s)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}
	logMaxTS = mathutil.Max(logMinTS, logMaxTS)

	return logMinTS, logMaxTS, nil
}

// getFullBackupTS gets the snapshot-ts of full bakcup
func getFullBackupTS(
	ctx context.Context,
	cfg *RestoreConfig,
) (uint64, error) {
	_, s, err := GetStorage(ctx, cfg.FullBackupStorage, &cfg.Config)
	if err != nil {
		return 0, errors.Trace(err)
	}

	metaData, err := s.ReadFile(ctx, metautil.MetaFile)
	if err != nil {
		return 0, errors.Trace(err)
	}

	backupmeta := &backuppb.BackupMeta{}
	if err = backupmeta.Unmarshal(metaData); err != nil {
		return 0, errors.Trace(err)
	}

	return backupmeta.GetEndVersion(), nil
}

func getGlobalResolvedTS(
	ctx context.Context,
	s storage.ExternalStorage,
) (uint64, error) {
	storeMap := make(map[int64]uint64)

	opt := &storage.WalkOption{SubDir: restore.GetStreamBackupMetaPrefix()}
	err := s.WalkDir(ctx, opt, func(path string, size int64) error {
		if strings.Contains(path, restore.GetStreamBackupMetaPrefix()) {
			m := &backuppb.Metadata{}
			b, err := s.ReadFile(ctx, path)
			if err != nil {
				return errors.Trace(err)
			}
			err = m.Unmarshal(b)
			if err != nil {
				return errors.Trace(err)
			}

			if resolveTS, exist := storeMap[m.StoreId]; !exist || resolveTS < m.ResolvedTs {
				storeMap[m.StoreId] = m.ResolvedTs
			}
		}
		return nil
	})
	if err != nil {
		return 0, errors.Trace(err)
	}

	var globalCheckpointTS uint64 = 0
	for _, resolveTS := range storeMap {
		if resolveTS < globalCheckpointTS || globalCheckpointTS == 0 {
			globalCheckpointTS = resolveTS
		}
	}

	return globalCheckpointTS, nil
}

func initFullBackupTables(
	ctx context.Context,
	cfg *RestoreConfig,
) (map[int64]*metautil.Table, error) {
	var storage string
	if len(cfg.FullBackupStorage) > 0 {
		storage = cfg.FullBackupStorage
	} else {
		storage = cfg.Storage
	}
	_, s, err := GetStorage(ctx, storage, &cfg.Config)
	if err != nil {
		return nil, errors.Trace(err)
	}

	metaFileName := metautil.CreateMetaFileName(cfg.StartTS)
	exist, err := s.FileExists(ctx, metaFileName)
	if err != nil {
		return nil, errors.Annotatef(err, "failed to check filename:%s ", metaFileName)
	} else if !exist {
		metaFileName = metautil.MetaFile
	}

	log.Info("read schemas", zap.String("backupmeta", metaFileName))
	metaData, err := s.ReadFile(ctx, metaFileName)
	if err != nil {
		return nil, errors.Trace(err)
	}
	backupMeta := &backuppb.BackupMeta{}
	if err = backupMeta.Unmarshal(metaData); err != nil {
		return nil, errors.Trace(err)
	}

	// read full backup databases to get map[table]table.Info
	reader := metautil.NewMetaReader(backupMeta, s, nil)
	databases, err := utils.LoadBackupTables(ctx, reader)
	if err != nil {
		return nil, errors.Trace(err)
	}

	tables := make(map[int64]*metautil.Table)
	for _, db := range databases {
		dbName := db.Info.Name.O
		if name, ok := utils.GetSysDBName(db.Info.Name); utils.IsSysDB(name) && ok {
			dbName = name
		}

		if !cfg.TableFilter.MatchSchema(dbName) {
			continue
		}

		for _, table := range db.Tables {
			// check this db is empty.
			if table.Info == nil {
				tables[db.Info.ID] = table
				continue
			}
			if !cfg.TableFilter.MatchTable(dbName, table.Info.Name.O) {
				continue
			}
			tables[table.Info.ID] = table
		}
	}

	return tables, nil
}

func initRewriteRules(client *restore.Client, tables map[int64]*metautil.Table) (map[int64]*restore.RewriteRules, error) {
	// compare table exists in cluster and map[table]table.Info to get rewrite rules.
	rules := make(map[int64]*restore.RewriteRules)
	for _, t := range tables {
		if name, ok := utils.GetSysDBName(t.DB.Name); utils.IsSysDB(name) && ok {
			// skip system table for now
			continue
		}
		if t.Info == nil {
			continue
		}

		newTableInfo, err := client.GetTableSchema(client.GetDomain(), t.DB.Name, t.Info.Name)
		if err != nil {
			// If table not existed, skip it directly.
			continue
		}
		// we don't handle index rule in pitr. since we only support pitr on non-exists table.
		tableRules := restore.GetRewriteRulesMap(newTableInfo, t.Info, 0, false)
		for tableID, tableRule := range tableRules {
			rules[tableID] = tableRule
		}

		log.Info("Using rewrite rule for table.", zap.Stringer("table", t.Info.Name),
			zap.Stringer("database", t.DB.Name),
			zap.Int("old-id", int(t.Info.ID)),
			zap.Array("rewrite-rules", zapcore.ArrayMarshalerFunc(func(ae zapcore.ArrayEncoder) error {
				for _, r := range rules {
					for _, rule := range r.Data {
						if err := ae.AppendObject(logutil.RewriteRuleObject(rule)); err != nil {
							return err
						}
					}
				}
				return nil
			})),
		)
	}
	return rules, nil
}

func updateRewriteRules(rules map[int64]*restore.RewriteRules, schemasReplace *stream.SchemasReplace) {
	filter := schemasReplace.TableFilter

	for _, dbReplace := range schemasReplace.DbMap {
		if dbReplace.OldDBInfo == nil ||
			utils.IsSysDB(dbReplace.OldDBInfo.Name.O) ||
			!filter.MatchSchema(dbReplace.OldDBInfo.Name.O) {
			continue
		}

		for oldTableID, tableReplace := range dbReplace.TableMap {
			if tableReplace.OldTableInfo == nil ||
				!filter.MatchTable(dbReplace.OldDBInfo.Name.O, tableReplace.OldTableInfo.Name.O) {
				continue
			}

			if _, exist := rules[oldTableID]; !exist {
				log.Info("add rewrite rule",
					zap.String("tableName", dbReplace.OldDBInfo.Name.O+"."+tableReplace.OldTableInfo.Name.O),
					zap.Int64("oldID", oldTableID), zap.Int64("newID", tableReplace.NewTableID))
				rules[oldTableID] = restore.GetRewriteRuleOfTable(
					oldTableID, tableReplace.NewTableID, 0, tableReplace.IndexMap, false)
			}

			for oldID, newID := range tableReplace.PartitionMap {
				if _, exist := rules[oldID]; !exist {
					log.Info("add rewrite rule",
						zap.String("tableName", dbReplace.OldDBInfo.Name.O+"."+tableReplace.OldTableInfo.Name.O),
						zap.Int64("oldID", oldID), zap.Int64("newID", newID))
					rules[oldID] = restore.GetRewriteRuleOfTable(oldID, newID, 0, tableReplace.IndexMap, false)
				}
			}
		}
	}
}

func newRawBatchClient(
	ctx context.Context,
	pdAddrs []string,
	tlsConfig TLSConfig,
) (*restore.RawKVBatchClient, error) {
	security := config.Security{
		ClusterSSLCA:   tlsConfig.CA,
		ClusterSSLCert: tlsConfig.Cert,
		ClusterSSLKey:  tlsConfig.Key,
	}
	rawkvClient, err := restore.NewRawkvClient(ctx, pdAddrs, security)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return restore.NewRawKVBatchClient(rawkvClient, rawKVBatchCount), nil
}

// ShiftTS gets a smaller shiftTS than startTS.
// It has a safe duration between shiftTS and startTS for trasaction.
func ShiftTS(startTS uint64) uint64 {
	physical := oracle.ExtractPhysical(startTS)
	logical := oracle.ExtractLogical(startTS)

	shiftPhysical := physical - streamShiftDuration.Milliseconds()
	if shiftPhysical < 0 {
		return 0
	} else {
		return oracle.ComposeTS(shiftPhysical, logical)
	}
}

func buildPauseSafePointName(taskName string) string {
	return fmt.Sprintf("%s_pause_safepoint", taskName)
}
