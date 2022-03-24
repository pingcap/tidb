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
	"github.com/pingcap/kvproto/pkg/metapb"
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
	"github.com/spf13/pflag"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/rawkv"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	flagYes                     = "yes"
	flagDryRun                  = "dry-run"
	flagUntil                   = "until"
	flagStreamTaskName          = "task-name"
	flagStreamTaskNameDefault   = "all" // used for get status for all of tasks.
	flagStreamStartTS           = "start-ts"
	flagStreamEndTS             = "end-ts"
	flagGCSafePointTTS          = "gc-ttl"
	flagStreamRestoreTS         = "restore-ts"
	flagStreamFullBackupStorage = "full-backup-storage"
)

var (
	StreamStart    = "stream start"
	StreamStop     = "stream stop"
	StreamPause    = "stream pause"
	StreamResume   = "stream resume"
	StreamStatus   = "stream status"
	StreamRestore  = "stream restore"
	StreamTruncate = "stream truncate"
)

var StreamCommandMap = map[string]func(c context.Context, g glue.Glue, cmdName string, cfg *StreamConfig) error{
	StreamStart:    RunStreamStart,
	StreamStop:     RunStreamStop,
	StreamPause:    RunStreamPause,
	StreamResume:   RunStreamResume,
	StreamStatus:   RunStreamStatus,
	StreamRestore:  RunStreamRestore,
	StreamTruncate: RunStreamTruncate,
}

// StreamConfig specifies the configure about backup stream
type StreamConfig struct {
	// common part that all of stream commands need
	Config

	// FullBackupStorage is used to find the maps between table name and table id during restoration.
	// if not specified. we cannot apply kv directly.
	FullBackupStorage string `json:"full-backup-storage" toml:"full-backup-storage"`
	TaskName          string `json:"task-name" toml:"task-name"`

	// StartTs usually equals the tso of full-backup, but user can reset it
	StartTS uint64 `json:"start-ts" toml:"start-ts"`
	EndTS   uint64 `json:"end-ts" toml:"end-ts"`
	// SafePointTTL ensures TiKV can scan entries not being GC at [startTS, currentTS]
	SafePointTTL int64  `json:"safe-point-ttl" toml:"safe-point-ttl"`
	RestoreTS    uint64 `json:"restore-ts" toml:"restore-ts"`

	// Spec for the command `truncate`, we should truncate the until when?
	Until      uint64 `json:"until" toml:"until"`
	DryRun     bool   `json:"dry-run" toml:"dry-run"`
	SkipPrompt bool   `json:"skip-prompt" toml:"skip-prompt"`
}

func (sc *StreamConfig) makeStorage(ctx context.Context) (storage.ExternalStorage, error) {
	u, err := storage.ParseBackend(sc.Storage, &sc.BackendOptions)
	if err != nil {
		return nil, errors.Trace(err)
	}
	opts := storage.ExternalStorageOptions{
		NoCredentials:   sc.NoCreds,
		SendCredentials: sc.SendCreds,
	}
	storage, err := storage.New(ctx, u, &opts)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return storage, nil
}

func (cfg *StreamConfig) adjustRestoreConfig() {
	cfg.Config.adjust()
	if cfg.Concurrency == 0 {
		cfg.Concurrency = 32
	}
}

// DefineStreamStartFlags defines flags used for `stream start`
func DefineStreamStartFlags(flags *pflag.FlagSet) {
	flags.String(flagStreamStartTS, "",
		"usually equals last full backupTS, used for backup log. Default value is current ts.\n"+
			"support TSO or datetime, e.g. '400036290571534337' or '2018-05-11 01:42:23'.")
	flags.String(flagStreamEndTS, "2035-1-1 00:00:00", "end ts, indicate stopping observe after endTS"+
		"support TSO or datetime")
	flags.Int64(flagGCSafePointTTS, utils.DefaultStreamGCSafePointTTL,
		"the TTL (in seconds) that PD holds for BR's GC safepoint")
}

// DefineStreamRestoreFlags defines flags used for `stream restore`
func DefineStreamRestoreFlags(flags *pflag.FlagSet) {
	flags.String(flagStreamRestoreTS, "", "restore ts, used for restore kv.\n"+
		"support TSO or datetime, e.g. '400036290571534337' or '2018-05-11 01:42:23'")
	flags.String(flagStreamFullBackupStorage, "", "find the maps between table id and table name")
	_ = flags.MarkHidden(flagStreamFullBackupStorage)
}

// DefineStreamCommonFlags define common flags for `stream task`
func DefineStreamCommonFlags(flags *pflag.FlagSet) {
	flags.String(flagStreamTaskName, "", "The task name for backup stream log.")
}

func DefineStreamStatusCommonFlags(flags *pflag.FlagSet) {
	flags.String(flagStreamTaskName, flagStreamTaskNameDefault,
		"The task name for backup stream log. If default, get status of all of tasks",
	)
}

func DefineStreamTruncateLogFlags(flags *pflag.FlagSet) {
	flags.String(flagUntil, "", "Remove all backup data until this TS."+
		"(support TSO or datetime, e.g. '400036290571534337' or '2018-05-11 01:42:23'.)")
	flags.Bool(flagDryRun, false, "Run the command but don't really delete the files.")
	flags.BoolP(flagYes, "y", false, "Skip all prompts and always execute the command.")
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

func (cfg *StreamConfig) ParseStreamRestoreFromFlags(flags *pflag.FlagSet) error {
	tsString, err := flags.GetString(flagStreamRestoreTS)
	if err != nil {
		return errors.Trace(err)
	}
	if cfg.RestoreTS, err = ParseTSString(tsString); err != nil {
		return errors.Trace(err)
	}
	if cfg.FullBackupStorage, err = flags.GetString(flagStreamFullBackupStorage); err != nil {
		return errors.Trace(err)
	}
	if len(cfg.FullBackupStorage) == 0 {
		cfg.FullBackupStorage = cfg.Config.Storage
	}
	return nil
}

// ParseStreamStartFromFlags parse parameters for `stream start`
func (cfg *StreamConfig) ParseStreamStartFromFlags(flags *pflag.FlagSet) error {
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
		cfg.SafePointTTL = utils.DefaultStreamGCSafePointTTL
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
		return errors.Annotate(berrors.ErrInvalidArgument, "Miss parameters taskName")
	}
	return nil
}

type streamMgr struct {
	cfg     *StreamConfig
	mgr     *conn.Mgr
	bc      *backup.Client
	httpCli *http.Client
}

func NewStreamMgr(ctx context.Context, cfg *StreamConfig, g glue.Glue, isStreamStart bool,
) (*streamMgr, error) {
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
	currentTS, err := s.getTS(ctx)
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

// setGCSafePoint specifies currentTS should belong to (gcSafePoint, currentTS),
// and set startTS as a serverSafePoint to PD
func (s *streamMgr) setGCSafePoint(ctx context.Context) error {
	if err := s.adjustAndCheckStartTS(ctx); err != nil {
		return errors.Trace(err)
	}

	err := utils.CheckGCSafePoint(ctx, s.mgr.GetPDClient(), s.cfg.StartTS)
	if err != nil {
		return errors.Trace(err)
	}

	sp := utils.BRServiceSafePoint{
		ID:       utils.MakeSafePointID(),
		TTL:      s.cfg.SafePointTTL,
		BackupTS: s.cfg.StartTS,
	}
	err = utils.UpdateServiceSafePoint(ctx, s.mgr.GetPDClient(), sp)
	if err != nil {
		return errors.Trace(err)
	}

	log.Info("set stream safePoint", zap.Object("safePoint", sp))
	return nil
}

func (s *streamMgr) getTS(ctx context.Context) (uint64, error) {
	p, l, err := s.mgr.GetPDClient().GetTS(ctx)
	if err != nil {
		return 0, errors.Trace(err)
	}

	return oracle.ComposeTS(p, l), nil
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
	allStores, err := conn.GetAllTiKVStoresWithRetry(ctx, s.mgr.GetPDClient(), conn.SkipTiFlash)
	if err != nil {
		return false, errors.Trace(err)
	}

	type backupStream struct {
		EnableStreaming bool `json:"enable-streaming"`
	}
	type config struct {
		BackupStream backupStream `json:"backup-stream"`
	}

	supportBackupStream := true
	hasTiKV := false
	for _, store := range allStores {
		if store.State != metapb.StoreState_Up {
			continue
		}
		hasTiKV = true
		// we need make sure every available store support backup-stream otherwise we might lose data.
		// so check every store's config
		addr := fmt.Sprintf("%s/config", store.GetStatusAddress())
		err = utils.WithRetry(ctx, func() error {
			resp, e := s.httpCli.Get(addr)
			if e != nil {
				return e
			}
			c := &config{}
			e = json.NewDecoder(resp.Body).Decode(c)
			if e != nil {
				return e
			}
			supportBackupStream = supportBackupStream && c.BackupStream.EnableStreaming
			_ = resp.Body.Close()
			return nil
		}, utils.NewPDReqBackoffer())
		if err != nil {
			// if one store failed, break and return error
			break
		}
	}
	return hasTiKV && supportBackupStream, err
}

func (s *streamMgr) backupFullSchemas(ctx context.Context, g glue.Glue) error {
	metaWriter := metautil.NewMetaWriter(s.bc.GetStorage(), metautil.MetaFileSize, false, nil)
	schemas, err := backup.BuildFullSchema(s.mgr.GetStorage(), s.cfg.StartTS)
	if err != nil {
		return errors.Trace(err)
	}

	schemasConcurrency := uint(utils.MinInt(backup.DefaultSchemaConcurrency, schemas.Len()))
	updateCh := g.StartProgress(ctx, "Checksum", int64(schemas.Len()), !s.cfg.LogProgress)

	err = schemas.BackupSchemas(ctx, metaWriter, s.mgr.GetStorage(), nil,
		s.cfg.StartTS, schemasConcurrency, 0, true, updateCh)
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
	cfg.adjust()
	defer summary.Summary(cmdName)
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
		return errors.New("Unable to create stream task. " +
			"please set tikv config `backup-stream.enable-streaming` to true." +
			"and restart tikv")
	}

	if err = streamMgr.setGCSafePoint(ctx); err != nil {
		return errors.Trace(err)
	}

	cli := stream.NewMetaDataClient(streamMgr.mgr.GetDomain().GetEtcdClient())
	// It supports single stream log task currently.
	if count, err := cli.GetTaskCount(ctx); err != nil {
		return errors.Trace(err)
	} else if count > 0 {
		return errors.Annotate(berrors.ErrStreamLogTaskExist, "It supports single stream log task current")
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

	err = cli.DeleteTask(ctx, cfg.TaskName)
	if err != nil {
		return errors.Trace(err)
	}

	summary.Log(cmdName, logutil.StreamBackupTaskInfo(&ti.Info))
	return nil
}

// RunStreamPause specifies pausing a stream task
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
	ti, err := cli.GetTask(ctx, cfg.TaskName)
	if err != nil {
		return errors.Trace(err)
	}

	err = cli.PauseTask(ctx, cfg.TaskName)
	if err != nil {
		return errors.Trace(err)
	}
	summary.Log(cmdName, logutil.StreamBackupTaskInfo(&ti.Info))
	return nil
}

// RunStreamResume specifies resuming a stream task
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
	ti, err := cli.GetTask(ctx, cfg.TaskName)
	if err != nil {
		return errors.Trace(err)
	}

	err = cli.ResumeTask(ctx, cfg.TaskName)
	if err != nil {
		return errors.Trace(err)
	}
	summary.Log(cmdName, logutil.StreamBackupTaskInfo(&ti.Info))
	return nil
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

	streamMgr, err := NewStreamMgr(ctx, cfg, g, false)
	if err != nil {
		return errors.Trace(err)
	}
	defer streamMgr.close()

	cli := stream.NewMetaDataClient(streamMgr.mgr.GetDomain().GetEtcdClient())

	// to add backoff
	if flagStreamTaskNameDefault == cfg.TaskName {
		// get status about all of tasks
		tasks, err := cli.GetAllTasks(ctx)
		if err != nil {
			return errors.Trace(err)
		}

		fields := make([]zapcore.Field, 0, len(tasks))
		for _, task := range tasks {
			fields = append(fields, logutil.StreamBackupTaskInfo(&task.Info))
		}
		summary.Log(cmdName, fields...)
	} else {
		// get status about TaskName
		task, err := cli.GetTask(ctx, cfg.TaskName)
		if err != nil {
			return errors.Trace(err)
		}
		summary.Log(cmdName, logutil.StreamBackupTaskInfo(&task.Info))
	}
	return nil
}

func RunStreamTruncate(c context.Context, g glue.Glue, cmdName string, cfg *StreamConfig) error {
	console := glue.GetConsole(g)
	em := color.New(color.Bold, color.FgHiWhite).SprintFunc()
	done := color.New(color.FgGreen).SprintFunc()
	warn := color.New(color.Bold, color.FgHiRed).SprintFunc()
	formatTs := func(ts uint64) string {
		return oracle.GetTimeFromTS(ts).Format("2006-01-02 15:04:05.0000")
	}

	cfg.adjustRestoreConfig()

	ctx, cancelFn := context.WithCancel(c)
	defer cancelFn()

	storage, err := cfg.makeStorage(ctx)
	if err != nil {
		return err
	}

	sp, err := restore.GetTruncateSafepoint(ctx, storage)
	if err != nil {
		return err
	}

	if cfg.Until < sp {
		console.Println("According to the log, you have truncated backup data before", em(formatTs(sp)))
		if !cfg.SkipPrompt && !console.PromptBool("Continue? ") {
			return nil
		}
	}
	if cfg.Until > sp && !cfg.DryRun {
		if err := restore.SetTruncateSafepoint(ctx, storage, cfg.Until); err != nil {
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
	minTs := oracle.GoTimeToTS(time.Now())
	metas.IterateFilesFullyBefore(cfg.Until, func(d *backuppb.DataFileInfo) (shouldBreak bool) {
		if d.MaxTs < minTs {
			minTs = d.MaxTs
		}
		fileCount++
		return
	})
	console.Println("We are going to remove ",
		em(fileCount),
		" files, until ",
		em(formatTs(minTs))+".",
	)
	if !cfg.SkipPrompt && !console.PromptBool(warn("Sure? ")) {
		return nil
	}

	removed := metas.RemoveDataBefore(cfg.Until)

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

// RunStreamRestore start restore job
func RunStreamRestore(
	c context.Context,
	g glue.Glue,
	cmdName string,
	cfg *StreamConfig,
) error {
	cfg.adjustRestoreConfig()
	ctx, cancelFn := context.WithCancel(c)
	defer cancelFn()

	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan(
			"task.RunStreamRestore",
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

	keepaliveCfg := GetKeepalive(&cfg.Config)
	keepaliveCfg.PermitWithoutStream = true
	client := restore.NewRestoreClient(streamMgr.mgr.GetPDClient(), streamMgr.mgr.GetTLSConfig(), keepaliveCfg, false)
	err = client.Init(g, streamMgr.mgr.GetStorage())
	if err != nil {
		return errors.Trace(err)
	}
	defer client.Close()

	u, err := storage.ParseBackend(cfg.Storage, &cfg.BackendOptions)
	if err != nil {
		return errors.Trace(err)
	}
	opts := storage.ExternalStorageOptions{
		NoCredentials:   cfg.NoCreds,
		SendCredentials: cfg.SendCreds,
	}

	if err = client.SetStorage(ctx, u, &opts); err != nil {
		return errors.Trace(err)
	}
	client.SetRateLimit(cfg.RateLimit)
	client.SetCrypter(&cfg.CipherInfo)
	client.SetConcurrency(uint(cfg.Concurrency))
	client.SetSwitchModeInterval(cfg.SwitchModeInterval)
	safepoint := client.GetTruncateSafepoint(ctx)
	if cfg.RestoreTS < safepoint {
		// TODO: maybe also filter records less than safepoint.
		return errors.Annotatef(berrors.ErrInvalidArgument,
			"the restore ts %d(%s) is truncated, please restore to longer than %d(%s)",
			cfg.RestoreTS, oracle.GetTimeFromTS(cfg.RestoreTS),
			safepoint, oracle.GetTimeFromTS(safepoint),
		)
	}
	err = client.LoadRestoreStores(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	client.InitClients(u, false)
	currentTS, err := streamMgr.getTS(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	if cfg.RestoreTS == 0 {
		cfg.RestoreTS = currentTS
	}
	client.SetRestoreTs(cfg.RestoreTS)
	client.SetCurrentTS(currentTS)
	log.Info("start restore on point", zap.Uint64("ts", cfg.RestoreTS))

	// get full backup meta to generate rewrite rules.
	fullBackupTables, err := initFullBackupTables(ctx, cfg.FullBackupStorage, cfg)
	if err != nil {
		return errors.Trace(err)
	}

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
	dmlFiles, ddlFiles, err := client.ReadStreamDataFiles(ctx, metas, safepoint, cfg.RestoreTS)
	if err != nil {
		return errors.Trace(err)
	}

	// perform restore meta kv files
	schemasReplace, err := client.InitSchemasReplaceForDDL(&fullBackupTables, cfg.TableFilter)
	if err != nil {
		return errors.Trace(err)
	}
	rawkvClient, err := newRawkvClient(ctx, cfg.PD, cfg.TLS)
	if err != nil {
		return errors.Trace(err)
	}
	defer rawkvClient.Close()

	pm := g.StartProgress(ctx, "Restore DDL files", int64(len(ddlFiles)), cfg.LogProgress)
	if err = withProgress(pm, func(p glue.Progress) error {
		return client.RestoreMetaKVFiles(ctx, rawkvClient, ddlFiles, schemasReplace, p.Inc)
	}); err != nil {
		return errors.Annotate(err, "failed to restore DDL files")
	}

	// perform restore kv files
	rewriteRules, err := initRewriteRules(client, fullBackupTables)
	if err != nil {
		return errors.Trace(err)
	}
	updateRewriteRules(rewriteRules, schemasReplace)

	pd := g.StartProgress(ctx, "Restore DML Files", int64(len(dmlFiles)), cfg.LogProgress)
	err = withProgress(pd, func(p glue.Progress) error {
		return client.RestoreKVFiles(ctx, rewriteRules, dmlFiles, p.Inc)
	})
	if err != nil {
		return errors.Annotate(err, "failed to restore DML files")
	}

	// fix indices.
	// to do:
	// No need to fix indices if backup stream all of tables, because the index recored has been observed.
	pi := g.StartProgress(ctx, "Restore Index", countIndices(fullBackupTables), cfg.LogProgress)
	err = withProgress(pi, func(p glue.Progress) error {
		return client.FixIndicesOfTables(ctx, fullBackupTables, p.Inc)
	})
	if err != nil {
		return errors.Annotate(err, "failed to fix index for some table")
	}

	// TODO split put and delete files
	return nil
}

// withProgress execute some logic with the progress, and close it once the execution done.
func withProgress(p glue.Progress, cc func(p glue.Progress) error) error {
	defer p.Close()
	return cc(p)
}

func countIndices(ts map[int64]*metautil.Table) int64 {
	result := int64(0)
	for _, t := range ts {
		result += int64(len(t.Info.Indices))
	}
	return result
}

func initFullBackupTables(
	ctx context.Context,
	fullBackupStorage string,
	cfg *StreamConfig,
) (map[int64]*metautil.Table, error) {
	_, s, err := GetStorage(ctx, fullBackupStorage, &cfg.Config)
	if err != nil {
		return nil, errors.Trace(err)
	}
	metaData, err := s.ReadFile(ctx, metautil.MetaFile)
	if err != nil {
		return nil, errors.Trace(err)
	}
	backupMeta := &backuppb.BackupMeta{}
	err = backupMeta.Unmarshal(metaData)
	if err != nil {
		return nil, errors.Trace(err)
	}
	reader := metautil.NewMetaReader(backupMeta, s, nil)

	// read full backup databases to get map[table]table.Info
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
		newTableInfo, err := client.GetTableSchema(client.GetDomain(), t.DB.Name, t.Info.Name)
		if err != nil {
			return nil, errors.Trace(err)
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
		if utils.IsSysDB(dbReplace.OldName) || utils.IsSysDB(dbReplace.NewName) {
			continue
		}
		if !filter.MatchSchema(dbReplace.NewName) && !filter.MatchSchema(dbReplace.OldName) {
			continue
		}

		for oldTableID, tableReplace := range dbReplace.TableMap {
			if !filter.MatchTable(dbReplace.OldName, tableReplace.OldName) &&
				!filter.MatchTable(dbReplace.NewName, tableReplace.NewName) {
				continue
			}

			if _, exist := rules[oldTableID]; !exist {
				log.Info("add rewrite rule", zap.String("tableName", dbReplace.NewName+"."+tableReplace.NewName),
					zap.Int64("oldID", oldTableID), zap.Int64("newID", tableReplace.TableID))
				rules[oldTableID] = restore.GetRewriteRuleOfTable(
					oldTableID, tableReplace.TableID, 0, tableReplace.IndexMap, false)
			}

			for oldID, newID := range tableReplace.PartitionMap {
				if _, exist := rules[oldID]; !exist {
					log.Info("add rewrite rule", zap.String("tableName", dbReplace.NewName+"."+tableReplace.NewName),
						zap.Int64("oldID", oldID), zap.Int64("newID", newID))
					rules[oldID] = restore.GetRewriteRuleOfTable(oldID, newID, 0, tableReplace.IndexMap, false)
				}
			}
		}
	}
}

func newRawkvClient(ctx context.Context, pdAddrs []string, tlsConfig TLSConfig) (*rawkv.Client, error) {
	security := config.Security{
		ClusterSSLCA:   tlsConfig.CA,
		ClusterSSLCert: tlsConfig.Cert,
		ClusterSSLKey:  tlsConfig.Key,
	}
	return rawkv.NewClient(ctx, pdAddrs, security,
		pd.WithCustomTimeoutOption(10*time.Second),
		pd.WithMaxErrorRetry(3))
}
