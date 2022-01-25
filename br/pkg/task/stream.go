// Copyright 2015 PingCAP, Inc.
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
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/restore"
	"sort"
	"strings"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/backup"
	"github.com/pingcap/tidb/br/pkg/conn"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/kv"
	"github.com/spf13/pflag"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	flagStreamTaskName          = "task-name"
	flagStreamTaskNameDefault   = "all" // used for get status for all of tasks.
	flagStreamStartTS           = "start-ts"
	flagStreamEndTS             = "end-ts"
	flagGCSafePointTTS          = "gc-ttl"
	flagStreamRestoreTS         = "restore-ts"
	flagStreamFullBackupStorage = "full-backup-storage"
)

var (
	StreamStart   = "stream start"
	StreamStop    = "stream stop"
	StreamPause   = "stream pause"
	StreamResume  = "stream resume"
	StreamStatus  = "stream status"
	StreamRestore = "stream restore"
)

var StreamCommandMap = map[string]func(c context.Context, g glue.Glue, cmdName string, cfg *StreamConfig) error{
	StreamStart:   RunStreamStart,
	StreamStop:    RunStreamStop,
	StreamPause:   RunStreamPause,
	StreamResume:  RunStreamResume,
	StreamStatus:  RunStreamStatus,
	StreamRestore: RunStreamRestore,
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
}

func (sc *StreamConfig) adjustRestoreConfig() {
	sc.Config.adjust()
	if sc.Concurrency == 0 {
		sc.Concurrency = 32
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
		return errors.New("must specify full backup storage.")
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
		errors.Trace(err)
	}

	if len(cfg.TaskName) <= 0 {
		return errors.Annotate(berrors.ErrInvalidArgument, "Miss parameters taskName")
	}
	return nil
}

type streamMgr struct {
	Cfg *StreamConfig
	mgr *conn.Mgr
	bc  *backup.Client
}

func NewStreamMgr(ctx context.Context, cfg *StreamConfig, g glue.Glue, needStorage bool,
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
		Cfg: cfg,
		mgr: mgr,
	}
	if needStorage {
		backend, err := storage.ParseBackend(cfg.Storage, &cfg.BackendOptions)
		if err != nil {
			return nil, errors.Trace(err)
		}

		opts := storage.ExternalStorageOptions{
			NoCredentials:   cfg.NoCreds,
			SendCredentials: cfg.SendCreds,
			SkipCheckPath:   cfg.SkipCheckPath,
		}
		client, err := backup.NewBackupClient(ctx, mgr)
		if err != nil {
			return nil, errors.Trace(err)
		}

		if err = client.SetStorage(ctx, backend, &opts); err != nil {
			return nil, errors.Trace(err)
		}
		s.bc = client
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
	if s.Cfg.StartTS == 0 {
		s.Cfg.StartTS = currentTS
	}

	if currentTS < s.Cfg.StartTS || s.Cfg.EndTS <= currentTS {
		return errors.Annotatef(berrors.ErrInvalidArgument,
			"invalid timestamps, startTS %d should be smaller than currentTS %d",
			s.Cfg.StartTS, currentTS)
	}
	if s.Cfg.EndTS <= currentTS {
		return errors.Annotatef(berrors.ErrInvalidArgument,
			"invalid timestamps, endTS %d should be larger than currentTS %d",
			s.Cfg.EndTS, currentTS)
	}

	return nil
}

// setGCSafePoint specifies currentTS should belong to (gcSafePoint, currentTS),
// and set startTS as a serverSafePoint to PD
func (s *streamMgr) setGCSafePoint(ctx context.Context) error {
	if err := s.adjustAndCheckStartTS(ctx); err != nil {
		return errors.Trace(err)
	}

	err := utils.CheckGCSafePoint(ctx, s.mgr.GetPDClient(), s.Cfg.StartTS)
	if err != nil {
		return errors.Trace(err)
	}

	sp := utils.BRServiceSafePoint{
		ID:       utils.MakeSafePointID(),
		TTL:      s.Cfg.SafePointTTL,
		BackupTS: s.Cfg.StartTS,
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
		s.Cfg.TableFilter,
		s.Cfg.StartTS,
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

// RunStreamCommand run all kinds of `stream task``
func RunStreamCommand(
	ctx context.Context,
	g glue.Glue,
	cmdName string,
	cfg *StreamConfig,
) error {
	cfg.adjust()
	commandFn, exist := StreamCommandMap[cmdName]
	if !exist {
		return errors.Annotatef(berrors.ErrInvalidArgument, "invalid command %s\n", cmdName)
	}

	if err := commandFn(ctx, g, cmdName, cfg); err != nil {
		log.Error("failed to stream", zap.String("command", cmdName), zap.Error(err))
		return err
	}

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

	if err := streamMgr.setGCSafePoint(ctx); err != nil {
		return errors.Trace(err)
	}
	if err := streamMgr.setLock(ctx); err != nil {
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
	cli := stream.NewMetaDataClient(streamMgr.mgr.GetDomain().GetEtcdClient())
	// It supports single stream log task current
	count, err := cli.GetTaskCount(ctx)
	if err != nil {
		return errors.Trace(err)
	} else if count > 0 {
		return errors.Annotate(berrors.ErrStreamLogTaskExist, "It supports single stream log task current")
	}

	if err := cli.PutTask(ctx, ti); err != nil {
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
	client, err := restore.NewRestoreClient(g, streamMgr.mgr.GetPDClient(), streamMgr.mgr.GetStorage(), streamMgr.mgr.GetTLSConfig(), keepaliveCfg)
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
		SkipCheckPath:   cfg.SkipCheckPath,
	}

	client.InitClients(u)
	if err = client.SetStorage(ctx, u, &opts); err != nil {
		return errors.Trace(err)
	}
	client.SetRateLimit(cfg.RateLimit)
	client.SetCrypter(&cfg.CipherInfo)
	client.SetConcurrency(uint(cfg.Concurrency))
	client.SetSwitchModeInterval(cfg.SwitchModeInterval)
	err = client.LoadRestoreStores(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	currentTs, err := streamMgr.getTS(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	if cfg.RestoreTS == 0 {
		cfg.RestoreTS = currentTs
	}
	log.Info("start restore on point", zap.Uint64("ts", cfg.RestoreTS))

	// get full backup meta to generate rewrite rules.
	fullBackupTables, err := initFullBackupTables(ctx, cfg.FullBackupStorage, cfg)
	if err != nil {
		return errors.Trace(err)
	}
	rewriteRules, err := initRewriteRules(client, fullBackupTables)
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
	datas, err := client.ReadStreamDataFiles(ctx, metas, cfg.RestoreTS)

	// TODO split put and delete files
	// perform restore kv files
	return client.RestoreKVFiles(ctx, rewriteRules, datas)
}

func initFullBackupTables(ctx context.Context, fullBackupStorage string, cfg *StreamConfig) (map[string]*metautil.Table, error) {
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
	tables := make(map[string]*metautil.Table, 0)
	for _, db := range databases {
		dbName := db.Info.Name.O
		if name, ok := utils.GetSysDBName(db.Info.Name); utils.IsSysDB(name) && ok {
			dbName = name
		}
		for _, table := range db.Tables {
			if !cfg.TableFilter.MatchTable(dbName, table.Info.Name.O) {
				continue
			}
			tables[utils.UniqueID(dbName, table.Info.Name.String())] = table
		}
	}
	return tables, nil

}

func initRewriteRules(client *restore.Client, tables map[string]*metautil.Table) (map[int64]*restore.RewriteRules, error) {
	// compare table exists in cluster and map[table]table.Info to get rewrite rules.
	rules := make(map[int64]*restore.RewriteRules)
	for _, t := range tables {
		newTableInfo, err := client.GetTableSchema(client.GetDomain(), t.DB.Name, t.Info.Name)
		if err != nil {
			return nil, errors.Trace(err)
		}
		rules[t.Info.ID] = restore.GetRewriteRules(newTableInfo, t.Info, 0)
	}
	return rules, nil
}
