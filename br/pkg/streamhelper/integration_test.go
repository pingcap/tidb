// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.
// This package tests the login in MetaClient with a embed etcd.

package streamhelper_test

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/kv"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/mvcc"
)

func getRandomLocalAddr() url.URL {
	listen, err := net.Listen("tcp", "127.0.0.1:")
	defer func() {
		if err := listen.Close(); err != nil {
			log.Panic("failed to release temporary port", logutil.ShortError(err))
		}
	}()
	if err != nil {
		log.Panic("failed to listen random port", logutil.ShortError(err))
	}
	u, err := url.Parse(fmt.Sprintf("http://%s", listen.Addr().String()))
	if err != nil {
		log.Panic("failed to parse url", logutil.ShortError(err))
	}
	return *u
}

func runEtcd(t *testing.T) (*embed.Etcd, *clientv3.Client) {
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	clientURL := getRandomLocalAddr()
	cfg.LCUrls = []url.URL{clientURL}
	cfg.LPUrls = []url.URL{getRandomLocalAddr()}
	cfg.LogLevel = "fatal"
	etcd, err := embed.StartEtcd(cfg)
	if err != nil {
		log.Panic("failed to start etcd server", logutil.ShortError(err))
	}
	<-etcd.Server.ReadyNotify()
	cliCfg := clientv3.Config{
		Endpoints: []string{clientURL.String()},
	}
	cli, err := clientv3.New(cliCfg)
	if err != nil {
		log.Panic("failed to connect to etcd server", logutil.ShortError(err))
	}
	return etcd, cli
}

func simpleRanges(tableCount int) streamhelper.Ranges {
	ranges := streamhelper.Ranges{}
	for i := 0; i < tableCount; i++ {
		base := int64(i*2 + 1)
		ranges = append(ranges, streamhelper.Range{
			StartKey: tablecodec.EncodeTablePrefix(base),
			EndKey:   tablecodec.EncodeTablePrefix(base + 1),
		})
	}
	return ranges
}

func simpleTask(name string, tableCount int) streamhelper.TaskInfo {
	backend, _ := storage.ParseBackend("noop://", nil)
	task, err := streamhelper.NewTask(name).
		FromTS(1).
		UntilTS(1000).
		WithRanges(simpleRanges(tableCount)...).
		WithTableFilter("*.*", "!mysql").
		ToStorage(backend).
		Check()
	if err != nil {
		panic(err)
	}
	return *task
}

func keyIs(t *testing.T, key, value []byte, etcd *embed.Etcd) {
	r, err := etcd.Server.KV().Range(context.TODO(), key, nil, mvcc.RangeOptions{})
	require.NoError(t, err)
	require.Len(t, r.KVs, 1)
	require.Equal(t, key, r.KVs[0].Key)
	require.Equal(t, value, r.KVs[0].Value)
}

func keyExists(t *testing.T, key []byte, etcd *embed.Etcd) {
	r, err := etcd.Server.KV().Range(context.TODO(), key, nil, mvcc.RangeOptions{})
	require.NoError(t, err)
	require.Len(t, r.KVs, 1)
}

func keyNotExists(t *testing.T, key []byte, etcd *embed.Etcd) {
	r, err := etcd.Server.KV().Range(context.TODO(), key, nil, mvcc.RangeOptions{})
	require.NoError(t, err)
	require.Len(t, r.KVs, 0)
}

func rangeMatches(t *testing.T, ranges streamhelper.Ranges, etcd *embed.Etcd) {
	r, err := etcd.Server.KV().Range(context.TODO(), ranges[0].StartKey, ranges[len(ranges)-1].EndKey, mvcc.RangeOptions{})
	require.NoError(t, err)
	if len(r.KVs) != len(ranges) {
		t.Logf("len(ranges) not match len(response.KVs) [%d vs %d]", len(ranges), len(r.KVs))
		t.Fail()
		return
	}
	for i, rng := range ranges {
		require.Equalf(t, r.KVs[i].Key, []byte(rng.StartKey), "the %dth of ranges not matched.(key)", i)
		require.Equalf(t, r.KVs[i].Value, []byte(rng.EndKey), "the %dth of ranges not matched.(value)", i)
	}
}

func rangeIsEmpty(t *testing.T, prefix []byte, etcd *embed.Etcd) {
	r, err := etcd.Server.KV().Range(context.TODO(), prefix, kv.PrefixNextKey(prefix), mvcc.RangeOptions{})
	require.NoError(t, err)
	require.Len(t, r.KVs, 0)
}

func TestIntegration(t *testing.T) {
	etcd, cli := runEtcd(t)
	defer etcd.Server.Stop()
	metaCli := streamhelper.MetaDataClient{Client: cli}
	t.Run("TestBasic", func(t *testing.T) { testBasic(t, metaCli, etcd) })
	t.Run("TestForwardProgress", func(t *testing.T) { testForwardProgress(t, metaCli, etcd) })
	t.Run("TestStreamListening", func(t *testing.T) { testStreamListening(t, streamhelper.TaskEventClient{MetaDataClient: metaCli}) })
}

func TestChecking(t *testing.T) {
	noop, _ := storage.ParseBackend("noop://", nil)
	// The name must not contains slash.
	_, err := streamhelper.NewTask("/root").
		WithRange([]byte("1"), []byte("2")).
		WithTableFilter("*.*").
		ToStorage(noop).
		Check()
	require.ErrorIs(t, errors.Cause(err), berrors.ErrPiTRInvalidTaskInfo)
	// Must specify the external storage.
	_, err = streamhelper.NewTask("root").
		WithRange([]byte("1"), []byte("2")).
		WithTableFilter("*.*").
		Check()
	require.ErrorIs(t, errors.Cause(err), berrors.ErrPiTRInvalidTaskInfo)
	// Must specift the table filter and range?
	_, err = streamhelper.NewTask("root").
		ToStorage(noop).
		Check()
	require.ErrorIs(t, errors.Cause(err), berrors.ErrPiTRInvalidTaskInfo)
	// Happy path.
	_, err = streamhelper.NewTask("root").
		WithRange([]byte("1"), []byte("2")).
		WithTableFilter("*.*").
		ToStorage(noop).
		Check()
	require.NoError(t, err)
}

func testBasic(t *testing.T, metaCli streamhelper.MetaDataClient, etcd *embed.Etcd) {
	ctx := context.Background()
	taskName := "two_tables"
	task := simpleTask(taskName, 2)
	taskData, err := task.PBInfo.Marshal()
	require.NoError(t, err)
	require.NoError(t, metaCli.PutTask(ctx, task))
	keyIs(t, []byte(streamhelper.TaskOf(taskName)), taskData, etcd)
	keyNotExists(t, []byte(streamhelper.Pause(taskName)), etcd)
	rangeMatches(t, []streamhelper.Range{
		{StartKey: []byte(streamhelper.RangeKeyOf(taskName, tablecodec.EncodeTablePrefix(1))), EndKey: tablecodec.EncodeTablePrefix(2)},
		{StartKey: []byte(streamhelper.RangeKeyOf(taskName, tablecodec.EncodeTablePrefix(3))), EndKey: tablecodec.EncodeTablePrefix(4)},
	}, etcd)

	remoteTask, err := metaCli.GetTask(ctx, taskName)
	require.NoError(t, err)
	require.NoError(t, remoteTask.Pause(ctx))
	keyExists(t, []byte(streamhelper.Pause(taskName)), etcd)
	require.NoError(t, metaCli.PauseTask(ctx, taskName))
	keyExists(t, []byte(streamhelper.Pause(taskName)), etcd)
	paused, err := remoteTask.IsPaused(ctx)
	require.NoError(t, err)
	require.True(t, paused)
	require.NoError(t, metaCli.ResumeTask(ctx, taskName))
	keyNotExists(t, []byte(streamhelper.Pause(taskName)), etcd)
	require.NoError(t, metaCli.ResumeTask(ctx, taskName))
	keyNotExists(t, []byte(streamhelper.Pause(taskName)), etcd)
	paused, err = remoteTask.IsPaused(ctx)
	require.NoError(t, err)
	require.False(t, paused)

	require.NoError(t, metaCli.DeleteTask(ctx, taskName))
	keyNotExists(t, []byte(streamhelper.TaskOf(taskName)), etcd)
	rangeIsEmpty(t, []byte(streamhelper.RangesOf(taskName)), etcd)
}

func testForwardProgress(t *testing.T, metaCli streamhelper.MetaDataClient, etcd *embed.Etcd) {
	ctx := context.Background()
	taskName := "many_tables"
	taskInfo := simpleTask(taskName, 65)
	defer func() {
		require.NoError(t, metaCli.DeleteTask(ctx, taskName))
	}()

	require.NoError(t, metaCli.PutTask(ctx, taskInfo))
	task, err := metaCli.GetTask(ctx, taskName)
	require.NoError(t, err)
	require.NoError(t, task.Step(ctx, 1, 41))
	require.NoError(t, task.Step(ctx, 1, 42))
	require.NoError(t, task.Step(ctx, 2, 40))
	rs, err := task.Ranges(ctx)
	require.NoError(t, err)
	require.Equal(t, simpleRanges(65), rs)
	store1Checkpoint, err := task.MinNextBackupTS(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, store1Checkpoint, uint64(42))
	store2Checkpoint, err := task.MinNextBackupTS(ctx, 2)
	require.NoError(t, err)
	require.Equal(t, store2Checkpoint, uint64(40))
}

func testStreamListening(t *testing.T, metaCli streamhelper.TaskEventClient) {
	ctx, cancel := context.WithCancel(context.Background())
	taskName := "simple"
	taskInfo := simpleTask(taskName, 4)

	require.NoError(t, metaCli.PutTask(ctx, taskInfo))
	ch := make(chan streamhelper.TaskEvent, 1024)
	require.NoError(t, metaCli.Begin(ctx, ch))
	require.NoError(t, metaCli.DeleteTask(ctx, taskName))

	taskName2 := "simple2"
	taskInfo2 := simpleTask(taskName2, 4)
	require.NoError(t, metaCli.PutTask(ctx, taskInfo2))
	require.NoError(t, metaCli.DeleteTask(ctx, taskName2))
	first := <-ch
	require.Equal(t, first.Type, streamhelper.EventAdd)
	require.Equal(t, first.Name, taskName)
	second := <-ch
	require.Equal(t, second.Type, streamhelper.EventDel)
	require.Equal(t, second.Name, taskName)
	third := <-ch
	require.Equal(t, third.Type, streamhelper.EventAdd)
	require.Equal(t, third.Name, taskName2)
	forth := <-ch
	require.Equal(t, forth.Type, streamhelper.EventDel)
	require.Equal(t, forth.Name, taskName2)
	cancel()
	_, ok := <-ch
	require.False(t, ok)
}
