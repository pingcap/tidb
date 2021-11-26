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

// StreamConfig specifies the configure about backup stream
type StreamConfig struct {
	// common part that all of stream commands need
	Config
	taskName string

	// this part only stream start includes
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
	tsString, err := flags.GetString(flagStreamStartTS)
	if err != nil {
		return errors.Trace(err)
	}

	if cfg.startTS, err = ParseTSString(tsString); err != nil {
		return errors.Trace(err)
	}

	tsString, err = flags.GetString(flagStreamEndTS)
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

func (s *streamStartMgr) getTS(ctx context.Context) (uint64, error) {
	p, l, err := s.mgr.PdController.GetPDClient().GetTS(ctx)
	if err != nil {
		return 0, errors.Trace(err)
	}

	return oracle.ComposeTS(p, l), nil
}

func (s *streamStartMgr) buildObserveRanges(ctx context.Context) ([]kv.KeyRange, error) {
	currentTS, err := s.getTS(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	mRange, err := stream.BuildObserveMetaRange()
	if err != nil {
		return nil, errors.Trace(err)
	}

	dRanges, err := stream.BuildObserveDataRanges(s.mgr.GetStorage(), s.Cfg.TableFilter, currentTS)
	if err != nil {
		return nil, errors.Trace(err)
	}

	rs := append([]kv.KeyRange{*mRange}, dRanges...)
	sort.Slice(rs, func(i, j int) bool {
		return bytes.Compare(rs[i].StartKey, rs[j].EndKey) < 0
	})

	return rs, nil
}

// RunStreamStart specifies starting a stream task
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
	defer streamMgr.close()

	if err := streamMgr.setLock(ctx); err != nil {
		return errors.Trace(err)
	}

	ranges, err := streamMgr.buildObserveRanges(ctx)
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

// RunStreamStop specifies stoping a stream task
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

// RunStreamPause specifies pausing a stream task
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

// RunStreamResume specifies resuming a stream task
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
