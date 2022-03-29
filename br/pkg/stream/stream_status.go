// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/fatih/color"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/conn"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/httputil"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type TaskStatus struct {
	Info backuppb.StreamBackupTaskInfo
	// Progress maps the StoreID to NextBackupTs.
	Progress map[uint64]uint64
	// Total QPS of the task in recent seconds.
	QPS float64
}

type TaskPrinter interface {
	AddTask(t TaskStatus)
	PrintTasks()
}

// PrintTaskByTable make a TaskPrinter,
// which prints the task by the `Table` implement of the console.
func PrintTaskByTable(t *glue.Table) TaskPrinter {
	return printByTable{inner: t}
}

// PrintTaskWithJSON make a TaskPrinter,
// which prints tasks as json to the console directly.
func PrintTaskWithJSON(c glue.ConsoleOperations) TaskPrinter {
	return &printByJSON{
		console: c,
	}
}

type printByTable struct {
	inner *glue.Table
}

func (t TaskStatus) getCheckpoint() uint64 {
	checkpoint := t.Info.StartTs
	for _, ts := range t.Progress {
		if checkpoint == 0 || ts < checkpoint {
			checkpoint = ts
		}
	}
	return checkpoint
}

func (p printByTable) AddTask(task TaskStatus) {
	table := p.inner
	table.Add("name", task.Info.Name)
	table.Add("start", fmt.Sprint(oracle.GetTimeFromTS(task.Info.StartTs)))
	if task.Info.EndTs > 0 {
		table.Add("end", fmt.Sprint(oracle.GetTimeFromTS(task.Info.EndTs)))
	}
	s := storage.FormatBackendURL(task.Info.GetStorage())
	table.Add("storage", s.String())
	table.Add("speed(est.)", fmt.Sprintf("%s ops/s", color.New(color.Bold).Sprintf("%.2f", task.QPS)))

	now := time.Now()
	formatTS := func(ts uint64) string {
		pTime := oracle.GetTimeFromTS(ts)
		gap := now.Sub(pTime).Round(time.Second)
		gapColor := color.New(color.FgGreen)
		if gap > 5*time.Minute {
			gapColor = color.New(color.FgRed)
		}
		info := fmt.Sprintf("%s; gap=%s", pTime, gapColor.Sprint(gap))
		return info
	}
	table.Add("checkpoint[global]", formatTS(task.getCheckpoint()))
	for store, p := range task.Progress {
		table.Add(fmt.Sprintf("checkpoint[store=%d]", store), formatTS(p))
	}
}

func (p printByTable) PrintTasks() {
	p.inner.Print()
}

type printByJSON struct {
	cache   []TaskStatus
	console glue.ConsoleOperations
}

func (p *printByJSON) AddTask(t TaskStatus) {
	p.cache = append(p.cache, t)
}

func (p *printByJSON) PrintTasks() {
	type storeProgress struct {
		StoreID    uint64 `json:"store_id"`
		Checkpoint uint64 `json:"checkpoint"`
	}
	type jsonTask struct {
		Name        string          `json:"name"`
		StartTS     uint64          `json:"start_ts,omitempty"`
		EndTS       uint64          `json:"end_ts,omitempty"`
		TableFilter []string        `json:"table_filter"`
		Progress    []storeProgress `json:"progress"`
		Storage     string          `json:"storage"`
		Checkpoint  uint64          `json:"checkpoint"`
		EstQPS      float64         `json:"estimate_qps"`
	}
	taskToJSON := func(t TaskStatus) jsonTask {
		s := storage.FormatBackendURL(t.Info.GetStorage())
		sp := make([]storeProgress, 0, len(t.Progress))
		for store, checkpoint := range t.Progress {
			sp = append(sp, storeProgress{
				StoreID:    store,
				Checkpoint: checkpoint,
			})
		}
		return jsonTask{
			Name:        t.Info.GetName(),
			StartTS:     t.Info.GetStartTs(),
			EndTS:       t.Info.GetEndTs(),
			TableFilter: t.Info.GetTableFilter(),
			Progress:    sp,
			Storage:     s.String(),
			Checkpoint:  t.getCheckpoint(),
			EstQPS:      t.QPS,
		}
	}
	mustMarshal := func(i interface{}) string {
		r, err := json.Marshal(i)
		if err != nil {
			log.Panic("Failed to marshal a trivial struct to json", zap.Reflect("object", i), zap.Error(err))
		}
		return string(r)
	}

	tasks := make([]jsonTask, 0, len(p.cache))
	for _, task := range p.cache {
		tasks = append(tasks, taskToJSON(task))
	}
	p.console.Println(mustMarshal(tasks))
}

var logCountSumRe = regexp.MustCompile(`tikv_stream_handle_kv_batch_sum ([0-9]+)`)

// MaybeQPS get a number like the QPS of last seconds for each store via the prometheus interface.
// TODO: this is a temporary solution(aha, like in a Hackthon),
//       we MUST find a better way for providing this information.
func MaybeQPS(ctx context.Context, mgr *conn.Mgr) (float64, error) {
	c := mgr.GetPDClient()
	prefix := "http://"
	if mgr.GetTLSConfig() != nil {
		prefix = "https://"
	}
	ss, err := c.GetAllStores(ctx)
	if err != nil {
		return 0, err
	}
	cli := httputil.NewClient(mgr.GetTLSConfig())
	getCount := func(statusAddr string) (uint64, error) {
		before, err := cli.Get(prefix + statusAddr + "/metrics")
		if err != nil {
			return 0, err
		}
		defer before.Body.Close()
		data, err := io.ReadAll(before.Body)
		if err != nil {
			return 0, err
		}
		matches := logCountSumRe.FindSubmatch(data)
		if len(matches) < 2 {
			return 42, nil
		}
		log.Info("get qps", zap.ByteStrings("matches", matches), logutil.Redact(zap.String("addr", statusAddr)))
		return strconv.ParseUint(string(matches[1]), 10, 64)
	}
	qpsMap := new(sync.Map)
	eg, _ := errgroup.WithContext(ctx)
	getQPS := func(s *metapb.Store) error {
		if s.GetState() != metapb.StoreState_Up {
			return nil
		}
		statusAddr := s.StatusAddress
		c0, err := getCount(statusAddr)
		if err != nil {
			return errors.Annotatef(err, "failed to get count from %s", statusAddr)
		}
		start := time.Now()
		time.Sleep(1 * time.Second)
		c1, err := getCount(statusAddr)
		if err != nil {
			return errors.Annotatef(err, "failed to get count from %s", statusAddr)
		}
		elapsed := float64(time.Since(start)) / float64(time.Second)
		log.Info("calc qps", zap.Uint64("diff", c1-c0), zap.Float64("elapsed", elapsed))

		qpsMap.Store(s.GetId(), float64(c1-c0)/elapsed)
		return nil
	}

	for _, s := range ss {
		store := s
		eg.Go(func() error {
			return getQPS(store)
		})
	}
	if err := eg.Wait(); err != nil {
		log.Warn("failed to get est QPS", logutil.ShortError(err))
	}
	qps := 0.0
	qpsMap.Range(func(key, value interface{}) bool {
		qps += value.(float64)
		return true
	})
	return qps, nil
}
