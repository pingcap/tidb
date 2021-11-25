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
	"github.com/pingcap/tidb/kv"
	"github.com/spf13/pflag"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

const (
	flagStreamTaskName = "task-name"
	flagStreamStartTS  = "startTS"
	flagStreamEndTS    = "endTS"
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

type StreamConfig struct {
	// common part that all of stream commands include
	Config
	taskName string

	// this part that only stream start includes
	startTS uint64
	endTS   uint64
}

func DefineStreamTSFlags(flags *pflag.FlagSet) {
	flags.String(flagStreamStartTS, "0", "start ts, use for backup stream log. "+
		"support TSO or datetime, e.g. '400036290571534337', '2018-05-11 01:42:23'")
	flags.String(flagStreamEndTS, "2099-1-1 00:00:01", "start ts, use for backup stream log. "+
		"support TSO or datetime")
}

func DefineStreamTaskNameFlags(flags *pflag.FlagSet) {
	flags.String(flagStreamTaskName, "", "The task name for backup stream log.")
}

func (cfg *StreamConfig) ParseTSFromFlags(flags *pflag.FlagSet) error {
	tsString, err := flags.GetString(flagStartTS)
	if err != nil {
		return errors.Trace(err)
	}

	if cfg.startTS, err = ParseTSString(tsString); err != nil {
		return errors.Trace(err)
	}

	tsString, err = flags.GetString(flagEndTS)
	if err != nil {
		return errors.Trace(err)
	}

	cfg.endTS, err = ParseTSString(tsString)
	return errors.Trace(err)
}

func (cfg *StreamConfig) ParseCommonFromFlags(flags *pflag.FlagSet) error {
	var err error
	if err = cfg.Config.ParseFromFlags(flags); err != nil {
		return errors.Trace(err)
	}

	cfg.taskName, err = flags.GetString(flagStreamTaskName)
	return errors.Trace(err)
}

/////////////////////////////////////////////////////////////////
type streamStartMgr struct {
	Cfg    *StreamConfig
	mgr    *conn.Mgr
	bc     *backup.Client
	Ranges []kv.KeyRange
}

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

func (s *streamStartMgr) Close() {
	s.mgr.Close()
}

func (s *streamStartMgr) SetLock(ctx context.Context) error {
	return s.bc.SetLockFile(ctx)
}

func (s *streamStartMgr) getTS(ctx context.Context) (uint64, error) {
	p, l, err := s.mgr.PdController.GetPDClient().GetTS(ctx)
	if err != nil {
		return 0, errors.Trace(err)
	}

	return oracle.ComposeTS(p, l), nil
}

func RunStreamStart(c context.Context, g glue.Glue, cmdName string, cfg *StreamConfig) error {
	ctx, cancelFn := context.WithCancel(c)
	defer cancelFn()

	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("task.RunStream", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	streamMgr, err := NewStreamStartMgr(ctx, cfg, g)
	if err != nil {
		return errors.Trace(err)
	}
	defer streamMgr.Close()

	if err := streamMgr.SetLock(ctx); err != nil {
		return errors.Trace(err)
	}

	currentTS, err := streamMgr.getTS(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	ranges, _, err := backup.BuildBackupRangeAndSchema(streamMgr.mgr.GetStorage(), cfg.TableFilter, currentTS)
	if err != nil {
		return errors.Trace(err)
	}

	// nothing to backup
	if ranges == nil || len(ranges) <= 0 {
		pdAddress := strings.Join(cfg.PD, ",")
		log.Warn("Nothing to backup, maybe connected to cluster for restoring",
			zap.String("PD address", pdAddress))
		return errors.Annotate(berrors.ErrInvalidArgument, "nothing need to backup")
	}

	sort.Slice(ranges, func(i, j int) bool {
		return bytes.Compare(ranges[i].StartKey, ranges[j].EndKey) < 0
	})

	ti := stream.TaskInfo{
		StreamBackupTaskInfo: backuppb.StreamBackupTaskInfo{
			Storage:     streamMgr.bc.GetStorageBackend(),
			StartTs:     cfg.startTS,
			EndTs:       cfg.endTS,
			Name:        cfg.taskName,
			TableFilter: cfg.FilterStr,
		},
		Ranges:  ranges,
		Pausing: false,
	}
	streamClient := stream.NewMetaDataClient()
	if err := streamClient.PutTask(ctx, ti); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func RunStreamStop(c context.Context, g glue.Glue, cmdName string, cfg *StreamConfig) error {
	ctx, cancelFn := context.WithCancel(c)
	defer cancelFn()

	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("task.RunStream", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	streamClient := stream.NewMetaDataClient()
	// to add backoff
	_, err := streamClient.GetTask(ctx, cfg.taskName)
	if err != nil {
		return errors.Trace(err)
	}

	err = streamClient.DeleteTask(ctx, cfg.taskName)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func RunStreamPause(c context.Context, g glue.Glue, cmdName string, cfg *StreamConfig) error {
	ctx, cancelFn := context.WithCancel(c)
	defer cancelFn()

	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("task.RunStream", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	streamClient := stream.NewMetaDataClient()
	// to add backoff
	_, err := streamClient.GetTask(ctx, cfg.taskName)
	if err != nil {
		return errors.Trace(err)
	}

	err = streamClient.PauseTask(ctx, cfg.taskName)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func RunStreamResume(c context.Context, g glue.Glue, cmdName string, cfg *StreamConfig) error {
	ctx, cancelFn := context.WithCancel(c)
	defer cancelFn()

	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("task.RunStream", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	streamClient := stream.NewMetaDataClient()
	// to add backoff
	_, err := streamClient.GetTask(ctx, cfg.taskName)
	if err != nil {
		return errors.Trace(err)
	}

	err = streamClient.ResumeTask(ctx, cfg.taskName)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}
