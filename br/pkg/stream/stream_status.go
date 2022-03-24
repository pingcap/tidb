// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package stream

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/fatih/color"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

type TaskStatus struct {
	Info backuppb.StreamBackupTaskInfo
	// Progress maps the StoreID to NextBackupTs.
	Progress map[uint64]uint64
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
	checkpoint := uint64(0)
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
	type jsonTask struct {
		Name        string            `json:"name"`
		StartTS     uint64            `json:"start_ts,omitempty"`
		EndTS       uint64            `json:"end_ts,omitempty"`
		TableFilter []string          `json:"table_filter"`
		Progress    map[uint64]uint64 `json:"progress"`
		Storage     string            `json:"storage"`
		Checkpoint  uint64            `json:"checkpoint"`
	}
	taskToJSON := func(t TaskStatus) jsonTask {
		s := storage.FormatBackendURL(t.Info.GetStorage())
		return jsonTask{
			Name:        t.Info.GetName(),
			StartTS:     t.Info.GetStartTs(),
			EndTS:       t.Info.GetEndTs(),
			TableFilter: t.Info.GetTableFilter(),
			Progress:    t.Progress,
			Storage:     s.String(),
			Checkpoint:  t.getCheckpoint(),
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
