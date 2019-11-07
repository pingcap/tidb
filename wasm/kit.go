package main

import (
	"context"
	"fmt"
	"syscall/js"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/sqlexec"
)

// Kit is a utility to run sql.
type Kit struct {
	session session.Session
}

// NewKit returns a new *Kit.
func NewKit(store kv.Storage) *Kit {
	kit := &Kit{}

	if se, err := session.CreateSession(store); err != nil {
		panic(err)
	} else {
		kit.session = se
	}
	if !kit.session.Auth(&auth.UserIdentity{
		Username:     "root",
		Hostname:     "localhost",
		AuthUsername: "root",
		AuthHostname: "localhost",
	}, nil, nil) {
		panic("auth failed")
	}

	return kit
}

// Exec executes a sql statement.
func (k *Kit) Exec(sql string) (sqlexec.RecordSet, error) {
	ctx := context.Background()
	rss, err := k.session.Execute(ctx, sql)
	if err == nil && len(rss) > 0 {
		return rss[0], nil
	}

	if err != nil {
		return nil, errors.Trace(err)
	}

	loadStats := k.session.Value(executor.LoadStatsVarKey)
	if loadStats != nil {
		defer k.session.SetValue(executor.LoadStatsVarKey, nil)
		if err := k.handleLoadStats(ctx, loadStats.(*executor.LoadStatsInfo)); err != nil {
			return nil, errors.Trace(err)
		}
	}

	loadDataInfo := k.session.Value(executor.LoadDataVarKey)
	if loadDataInfo != nil {
		defer k.session.SetValue(executor.LoadDataVarKey, nil)
		if err = handleLoadData(ctx, k.session, loadDataInfo.(*executor.LoadDataInfo)); err != nil {
			return nil, err
		}
	}

	return nil, nil
}

// ExecFile let user upload a file and execute sql statements from that file.
func (k *Kit) ExecFile() error {
	c := make(chan error)
	js.Global().Get("upload").Invoke(js.FuncOf(func(this js.Value, args []js.Value) interface{} {
		go func() {
			fmt.Println("success")
			_, e := k.Exec(args[0].String())
			c <- e
		}()
		return nil
	}), js.FuncOf(func(this js.Value, args []js.Value) interface{} {
		go func() {
			c <- errors.New(args[0].String())
		}()
		return nil
	}))

	select {
	case e := <-c:
		return e
	case <-time.After(30 * time.Second):
		return errors.New("upload timeout")
	}
	return <-c
}

// ResultSetToStringSlice converts sqlexec.RecordSet to string slice slices.
func (k *Kit) ResultSetToStringSlice(rs sqlexec.RecordSet) ([][]string, error) {
	return session.ResultSetToStringSlice(context.Background(), k.session, rs)
}

// handleLoadData does the additional work after processing the 'load data' query.
// It let user upload a file, then reads the file content, inserts data into database.
func handleLoadData(ctx context.Context, se session.Session, loadDataInfo *executor.LoadDataInfo) error {
	if loadDataInfo == nil {
		return errors.New("load data info is empty")
	}
	loadDataInfo.InitQueues()
	loadDataInfo.SetMaxRowsInBatch(uint64(loadDataInfo.Ctx.GetSessionVars().DMLBatchSize))
	loadDataInfo.StartStopWatcher()

	if err := loadDataInfo.Ctx.NewTxn(ctx); err != nil {
		return err
	}

	if err := processData(ctx, loadDataInfo); err != nil {
		return err
	}

	if err := loadDataInfo.CommitWork(ctx); err != nil {
		return err
	}
	loadDataInfo.SetMessage()

	var txn kv.Transaction
	var err1 error
	txn, err1 = loadDataInfo.Ctx.Txn(true)
	if err1 == nil {
		if txn != nil && txn.Valid() {
			return se.CommitTxn(sessionctx.SetCommitCtx(ctx, loadDataInfo.Ctx))
		}
	}
	// Should never reach here.
	panic(err1)
}

// processData process input data from uploaded file and enqueue commit task
func processData(ctx context.Context, loadDataInfo *executor.LoadDataInfo) error {
	var err error
	var prevData, curData []byte
	defer func() {
		r := recover()
		if err != nil || r != nil {
			loadDataInfo.ForceQuit()
		} else {
			loadDataInfo.CloseTaskQueue()
		}
	}()

	c := make(chan error)
	js.Global().Get("upload").Invoke(js.FuncOf(func(this js.Value, args []js.Value) interface{} {
		go func() {
			curData = []byte(args[0].String())
			// prepare batch and enqueue task
			prevData, err = insertDataWithCommit(ctx, prevData, curData, loadDataInfo)
			if err == nil {
				loadDataInfo.EnqOneTask(ctx)
			}
			c <- err
		}()
		return nil
	}), js.FuncOf(func(this js.Value, args []js.Value) interface{} {
		go func() {
			c <- errors.New(args[0].String())
		}()
		return nil
	}))

	select {
	case e := <-c:
		return e
	case <-time.After(30 * time.Second):
		return errors.New("upload timeout")
	}
	return <-c
}

func insertDataWithCommit(ctx context.Context, prevData,
	curData []byte, loadDataInfo *executor.LoadDataInfo) ([]byte, error) {
	var err error
	var reachLimit bool
	for {
		prevData, reachLimit, err = loadDataInfo.InsertData(ctx, prevData, curData)
		if err != nil {
			return nil, err
		}
		if !reachLimit {
			break
		}
		// push into commit task queue
		err = loadDataInfo.EnqOneTask(ctx)
		if err != nil {
			return prevData, err
		}
		curData = prevData
		prevData = nil
	}
	return prevData, nil
}

// handleLoadStats does the additional work after processing the 'load stats' query.
// It let user upload a file, then reads the file content, loads it into the storage.
func (k *Kit) handleLoadStats(ctx context.Context, loadStatsInfo *executor.LoadStatsInfo) error {
	if loadStatsInfo == nil {
		return errors.New("load stats: info is empty")
	}

	c := make(chan error)
	js.Global().Get("upload").Invoke(js.FuncOf(func(this js.Value, args []js.Value) interface{} {
		go func() {
			loadStatsInfo.Update([]byte(args[0].String()))
			c <- nil
		}()
		return nil
	}), js.FuncOf(func(this js.Value, args []js.Value) interface{} {
		go func() {
			c <- errors.New(args[0].String())
		}()
		return nil
	}))

	select {
	case e := <-c:
		return e
	case <-time.After(30 * time.Second):
		return errors.New("upload timeout")
	}
	return <-c
}
