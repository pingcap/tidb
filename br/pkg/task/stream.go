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
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/kv"
	"github.com/spf13/pflag"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

const (
	flagStreamTaskName = "taskName"
	flagStreamStartTS  = "startTS"
	flagStreamEndTS    = "endTS"
	flagGCSafePointTTS = "gcTTL"
)

var (
	StreamStart  = "stream start"
	StreamStop   = "stream stop"
	StreamPause  = "stream pause"
	StreamResume = "stream resume"
)

var StreamCommandMap = map[string]func(c context.Context, g glue.Glue, cmdName string, cfg *StreamConfig) error{
	StreamStart:  RunStreamStart,
	StreamStop:   RunStreamStop,
	StreamPause:  RunStreamPause,
	StreamResume: RunStreamResume,
}

// StreamConfig specifies the configure about backup stream
type StreamConfig struct {
	// common part that all of stream commands need
	Config
	taskName string

	// this part only stream start includes

	// startTs usually equals the tso of full-backup, but user can reset it
	startTS uint64
	endTS   uint64
	// safePointTTL ensures TiKV can scan entries not being GC at [startTS, currentTS]
	safePointTTL int64
}

// DefineStreamStartFlags defines flags used for `stream start`
func DefineStreamStartFlags(flags *pflag.FlagSet) {
	flags.String(flagStreamStartTS, "", "start ts, usually equals last full backupTS, used for backup log.\n"+
		"support TSO or datetime, e.g. '400036290571534337' or '2018-05-11 01:42:23'")
	flags.String(flagStreamEndTS, "2035-1-1 00:00:00", "end ts, indicate stopping observe after endTS"+
		"support TSO or datetime")
	flags.Int64(flagGCSafePointTTS, utils.DefaultStreamGCSafePointTTL,
		"the TTL (in seconds) that PD holds for BR's GC safepoint")
}

// DefineStreamCommonFlags define common flags for `stream task`
func DefineStreamCommonFlags(flags *pflag.FlagSet) {
	flags.String(flagStreamTaskName, "", "The task name for backup stream log.")
}

// ParseStreamStartFromFlags parse parameters for `stream start`
func (cfg *StreamConfig) ParseStreamStartFromFlags(flags *pflag.FlagSet) error {
	tsString, err := flags.GetString(flagStreamStartTS)
	if err != nil {
		return errors.Trace(err)
	}
	if len(tsString) <= 0 {
		return errors.Annotate(berrors.ErrInvalidArgument, "Miss parameters startTS")
	}

	if cfg.startTS, err = ParseTSString(tsString); err != nil {
		return errors.Trace(err)
	}

	tsString, err = flags.GetString(flagStreamEndTS)
	if err != nil {
		return errors.Trace(err)
	}

	if cfg.endTS, err = ParseTSString(tsString); err != nil {
		return errors.Trace(err)
	}

	if cfg.safePointTTL, err = flags.GetInt64(flagGCSafePointTTS); err != nil {
		return errors.Trace(err)
	}

	if cfg.safePointTTL <= 0 {
		cfg.safePointTTL = utils.DefaultStreamGCSafePointTTL
	}

	return nil
}

// ParseCommonFromFlags parse parameters for `stream task`
func (cfg *StreamConfig) ParseCommonFromFlags(flags *pflag.FlagSet) error {
	var err error
	if err = cfg.Config.ParseFromFlags(flags); err != nil {
		return errors.Trace(err)
	}

	cfg.taskName, err = flags.GetString(flagStreamTaskName)
	if err != nil {
		errors.Trace(err)
	}

	if len(cfg.taskName) <= 0 {
		return errors.Annotate(berrors.ErrInvalidArgument, "Miss parameters taskName")
	}
	return nil
}

type streamStartMgr struct {
	Cfg *StreamConfig
	mgr *conn.Mgr
	bc  *backup.Client
}

// NewStreamStartMgr specifies Creating a stream Mgr
func NewStreamStartMgr(ctx context.Context, cfg *StreamConfig, g glue.Glue,
) (*streamStartMgr, error) {
	mgr, err := NewMgr(ctx, g, cfg.PD, cfg.TLS, GetKeepalive(&cfg.Config),
		cfg.CheckRequirements, false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer func() {
		if err != nil {
			mgr.Close()
		}
	}()

	client, err := backup.NewBackupClient(ctx, mgr)
	if err != nil {
		return nil, errors.Trace(err)
	}

	backend, err := storage.ParseBackend(cfg.Storage, &cfg.BackendOptions)
	if err != nil {
		return nil, errors.Trace(err)
	}

	opts := storage.ExternalStorageOptions{
		NoCredentials:   cfg.NoCreds,
		SendCredentials: cfg.SendCreds,
		SkipCheckPath:   cfg.SkipCheckPath,
	}
	if err = client.SetStorage(ctx, backend, &opts); err != nil {
		return nil, errors.Trace(err)
	}

	s := &streamStartMgr{
		Cfg: cfg,
		mgr: mgr,
		bc:  client,
	}
	return s, nil
}

func (s *streamStartMgr) close() {
	s.mgr.Close()
}

func (s *streamStartMgr) setLock(ctx context.Context) error {
	return s.bc.SetLockFile(ctx)
}

// checkStartTS checks that startTS should be smaller than currentTS,
// and endTS is larger than currentTS.
func (s *streamStartMgr) checkStartTS(ctx context.Context) error {
	currentTS, err := s.getTS(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	if currentTS <= s.Cfg.startTS || s.Cfg.endTS <= currentTS {
		return errors.Annotatef(berrors.ErrInvalidArgument,
			"invalid timestamps, startTS %d should be smaller than currentTS %d",
			s.Cfg.startTS, currentTS)
	}
	if s.Cfg.endTS <= currentTS {
		return errors.Annotatef(berrors.ErrInvalidArgument,
			"invalid timestamps, endTS %d should be larger than currentTS %d",
			s.Cfg.endTS, currentTS)
	}

	return nil
}

// setGCSafePoint specifies currentTS should belong to (gcSafePoint, currentTS),
// and set startTS as a serverSafePoint to PD
func (s *streamStartMgr) setGCSafePoint(ctx context.Context) error {
	if err := s.checkStartTS(ctx); err != nil {
		return errors.Trace(err)
	}

	err := utils.CheckGCSafePoint(ctx, s.mgr.GetPDClient(), s.Cfg.startTS)
	if err != nil {
		return errors.Trace(err)
	}

	sp := utils.BRServiceSafePoint{
		ID:       utils.MakeSafePointID(),
		TTL:      s.Cfg.safePointTTL,
		BackupTS: s.Cfg.startTS,
	}
	err = utils.UpdateServiceSafePoint(ctx, s.mgr.GetPDClient(), sp)
	if err != nil {
		return errors.Trace(err)
	}

	log.Info("set stream safePoint", zap.Object("safePoint", sp))
	return nil
}

func (s *streamStartMgr) getTS(ctx context.Context) (uint64, error) {
	p, l, err := s.mgr.PdController.GetPDClient().GetTS(ctx)
	if err != nil {
		return 0, errors.Trace(err)
	}

	return oracle.ComposeTS(p, l), nil
}

func (s *streamStartMgr) buildObserveRanges(ctx context.Context) ([]kv.KeyRange, error) {
	dRanges, err := stream.BuildObserveDataRanges(
		s.mgr.GetStorage(),
		s.Cfg.TableFilter,
		s.Cfg.startTS,
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

	streamMgr, err := NewStreamStartMgr(ctx, cfg, g)
	if err != nil {
		return errors.Trace(err)
	}
	defer streamMgr.close()

	if err := streamMgr.setLock(ctx); err != nil {
		return errors.Trace(err)
	}
	if err := streamMgr.setGCSafePoint(ctx); err != nil {
		return errors.Trace(err)
	}

	ranges, err := streamMgr.buildObserveRanges(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	// nothing to backup
	if len(ranges) == 0 {
		pdAddress := strings.Join(cfg.PD, ",")
		log.Warn("Nothing to observe, maybe connected to cluster for restoring",
			zap.String("PD address", pdAddress))
		return errors.Annotate(berrors.ErrInvalidArgument, "nothing need to observe")
	}

	ti := stream.TaskInfo{
		PBInfo: backuppb.StreamBackupTaskInfo{
			Storage:     streamMgr.bc.GetStorageBackend(),
			StartTs:     cfg.startTS,
			EndTs:       cfg.endTS,
			Name:        cfg.taskName,
			TableFilter: cfg.FilterStr,
		},
		Ranges:  ranges,
		Pausing: false,
	}

	cli := stream.NewMetaDataClient(streamMgr.mgr.GetDomain().GetEtcdClient())
	if err := cli.PutTask(ctx, ti); err != nil {
		return errors.Trace(err)
	}
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

	streamMgr, err := NewStreamStartMgr(ctx, cfg, g)
	if err != nil {
		return errors.Trace(err)
	}
	defer streamMgr.close()

	cli := stream.NewMetaDataClient(streamMgr.mgr.GetDomain().GetEtcdClient())
	// to add backoff
	_, err = cli.GetTask(ctx, cfg.taskName)
	if err != nil {
		return errors.Trace(err)
	}

	err = cli.DeleteTask(ctx, cfg.taskName)
	if err != nil {
		return errors.Trace(err)
	}
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

	streamMgr, err := NewStreamStartMgr(ctx, cfg, g)
	if err != nil {
		return errors.Trace(err)
	}
	defer streamMgr.close()

	cli := stream.NewMetaDataClient(streamMgr.mgr.GetDomain().GetEtcdClient())
	// to add backoff
	_, err = cli.GetTask(ctx, cfg.taskName)
	if err != nil {
		return errors.Trace(err)
	}

	err = cli.PauseTask(ctx, cfg.taskName)
	if err != nil {
		return errors.Trace(err)
	}
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

	streamMgr, err := NewStreamStartMgr(ctx, cfg, g)
	if err != nil {
		return errors.Trace(err)
	}
	defer streamMgr.close()

	cli := stream.NewMetaDataClient(streamMgr.mgr.GetDomain().GetEtcdClient())
	// to add backoff
	_, err = cli.GetTask(ctx, cfg.taskName)
	if err != nil {
		return errors.Trace(err)
	}

	err = cli.ResumeTask(ctx, cfg.taskName)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}
