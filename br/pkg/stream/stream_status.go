// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
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

const WildCard = "*"

type TaskStatus struct {
	// Info is the stream task information.
	Info backuppb.StreamBackupTaskInfo
	// paused checks whether the task is paused.
	paused bool
	// Progress maps the StoreID to NextBackupTs.
	Progress map[uint64]uint64
	// Total QPS of the task in recent seconds.
	QPS float64
	// Last error reported by the store.
	LastErrors map[uint64]backuppb.StreamBackupError
}

type TaskPrinter interface {
	AddTask(t TaskStatus)
	PrintTasks()
}

// PrintTaskByTable make a TaskPrinter,
// which prints the task by the `Table` implement of the console.
func PrintTaskByTable(c glue.ConsoleOperations) TaskPrinter {
	return &printByTable{console: c}
}

// PrintTaskWithJSON make a TaskPrinter,
// which prints tasks as json to the console directly.
func PrintTaskWithJSON(c glue.ConsoleOperations) TaskPrinter {
	return &printByJSON{
		console: c,
	}
}

type printByTable struct {
	console       glue.ConsoleOperations
	pendingTables []*glue.Table
}

// statusOK make a string like <green>●</green> <bold>{message}</bold>
func statusOK(message string) string {
	return color.GreenString("●") + color.New(color.Bold).Sprintf(" %s", message)
}

// statusErr make a string like <red>○</red> <bold>{message}</bold>
func statusErr(message string) string {
	return color.RedString("○") + color.New(color.Bold).Sprintf(" %s", message)
}

// statusBlock make a string like <yellow>●</yellow> <bold>{message}</bold>
func statusBlock(message string) string {
	return color.YellowString("●") + color.New(color.Bold).Sprintf(" %s", message)
}

func (t *TaskStatus) colorfulStatusString() string {
	// Maybe we need 3 kinds of status: ERROR/NORMAL/PAUSE.
	// And should return "ERROR" when find error information in PD.
	if t.paused && len(t.LastErrors) > 0 {
		return statusErr("ERROR")
	}
	if t.paused {
		return statusBlock("PAUSE")
	}
	return statusOK("NORMAL")
}

// GetCheckpoint calculates the checkpoint of the task.
func (t TaskStatus) GetCheckpoint() uint64 {
	initialized := false
	checkpoint := t.Info.StartTs
	for _, ts := range t.Progress {
		if !initialized || ts < checkpoint {
			initialized = true
			checkpoint = ts
		}
	}
	return checkpoint
}

func (p *printByTable) AddTask(task TaskStatus) {
	table := p.console.CreateTable()
	table.Add("name", task.Info.Name)
	table.Add("status", task.colorfulStatusString())
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
	table.Add("checkpoint[global]", formatTS(task.GetCheckpoint()))
	for store, p := range task.Progress {
		table.Add(fmt.Sprintf("checkpoint[store=%d]", store), formatTS(p))
	}
	for store, e := range task.LastErrors {
		table.Add(fmt.Sprintf("error[store=%d]", store), e.ErrorCode)
		table.Add(fmt.Sprintf("error-happen-at[store=%d]", store), formatTS(oracle.ComposeTS(int64(e.HappenAt), 0)))
		table.Add(fmt.Sprintf("error-message[store=%d]", store), e.ErrorMessage)
	}
	p.pendingTables = append(p.pendingTables, table)
}

func (p *printByTable) PrintTasks() {
	if len(p.pendingTables) == 0 {
		p.console.Println(statusErr("No Task Yet."))
		return
	}
	p.console.Println(statusOK(fmt.Sprintf("Total %d Tasks.", len(p.pendingTables))))
	for i, t := range p.pendingTables {
		p.console.Printf("> #%d <\n", i+1)
		t.Print()
	}
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
	type storeLastError struct {
		StoreID   uint64                     `json:"store_id"`
		LastError backuppb.StreamBackupError `json:"last_error"`
	}
	type jsonTask struct {
		Name        string           `json:"name"`
		StartTS     uint64           `json:"start_ts,omitempty"`
		EndTS       uint64           `json:"end_ts,omitempty"`
		TableFilter []string         `json:"table_filter"`
		Progress    []storeProgress  `json:"progress"`
		Storage     string           `json:"storage"`
		Checkpoint  uint64           `json:"checkpoint"`
		EstQPS      float64          `json:"estimate_qps"`
		LastErrors  []storeLastError `json:"last_errors"`
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
		se := make([]storeLastError, 0, len(t.LastErrors))
		for store, lastError := range t.LastErrors {
			se = append(se, storeLastError{
				StoreID:   store,
				LastError: lastError,
			})
		}
		return jsonTask{
			Name:        t.Info.GetName(),
			StartTS:     t.Info.GetStartTs(),
			EndTS:       t.Info.GetEndTs(),
			TableFilter: t.Info.GetTableFilter(),
			Progress:    sp,
			Storage:     s.String(),
			Checkpoint:  t.GetCheckpoint(),
			EstQPS:      t.QPS,
			LastErrors:  se,
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

// StatusController is the controller type (or context type) for the command `stream status`.
type StatusController struct {
	meta *MetaDataClient
	mgr  *conn.Mgr
	view TaskPrinter
}

// NewStatusContorller make a status controller via some resource accessors.
func NewStatusController(meta *MetaDataClient, mgr *conn.Mgr, view TaskPrinter) *StatusController {
	return &StatusController{
		meta: meta,
		mgr:  mgr,
		view: view,
	}
}

func isTiFlash(store *metapb.Store) bool {
	for _, l := range store.GetLabels() {
		if strings.EqualFold(l.Key, "engine") && strings.EqualFold(l.Value, "tiflash") {
			return true
		}
	}
	return false
}

// fillTask queries and fills the extra information for a raw task.
func (ctl *StatusController) fillTask(ctx context.Context, task Task) (TaskStatus, error) {
	var err error
	s := TaskStatus{
		Info: task.Info,
	}

	if s.paused, err = task.IsPaused(ctx); err != nil {
		return s, errors.Annotatef(err, "failed to get pause status of task %s", s.Info.Name)
	}

	if s.Progress, err = task.NextBackupTSList(ctx); err != nil {
		return s, errors.Annotatef(err, "failed to get progress of task %s", s.Info.Name)
	}

	stores, err := ctl.mgr.GetPDClient().GetAllStores(ctx)
	if err != nil {
		return s, errors.Annotate(err, "failed to get stores from PD")
	}
	for _, store := range stores {
		if isTiFlash(store) {
			continue
		}
		if _, ok := s.Progress[store.GetId()]; !ok {
			s.Progress[store.GetId()] = s.Info.StartTs
		}
	}

	s.LastErrors, err = task.LastError(ctx)
	if err != nil {
		return s, err
	}

	s.QPS, err = MaybeQPS(ctx, ctl.mgr)
	if err != nil {
		return s, errors.Annotatef(err, "failed to get QPS of task %s", s.Info.Name)
	}
	return s, nil
}

// getTask fetches the task by the name, if the name is the wildcard ("*"), fetch all tasks.
func (ctl *StatusController) getTask(ctx context.Context, name string) ([]TaskStatus, error) {
	if name == WildCard {
		// get status about all of tasks
		tasks, err := ctl.meta.GetAllTasks(ctx)
		if err != nil {
			return nil, err
		}

		result := make([]TaskStatus, 0, len(tasks))
		for _, task := range tasks {
			status, err := ctl.fillTask(ctx, task)
			if err != nil {
				return nil, err
			}
			result = append(result, status)
		}
		return result, nil
	}
	// get status about TaskName
	task, err := ctl.meta.GetTask(ctx, name)
	if err != nil {
		return nil, err
	}
	status, err := ctl.fillTask(ctx, *task)
	if err != nil {
		return nil, err
	}
	return []TaskStatus{status}, nil
}

func (ctl *StatusController) printToView(tasks []TaskStatus) {
	for _, task := range tasks {
		ctl.view.AddTask(task)
	}
	ctl.view.PrintTasks()
}

// PrintStatusOfTask prints the status of tasks with the name. When the name is *, print all tasks.
func (ctl *StatusController) PrintStatusOfTask(ctx context.Context, name string) error {
	tasks, err := ctl.getTask(ctx, name)
	if err != nil {
		return err
	}
	ctl.printToView(tasks)
	return nil
}
