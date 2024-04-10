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

package addindextestutil

import (
	"context"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var (
	skippedErrorCode = []string{
		"8028",     // ErrInfoSchemaChanged
		"9007",     // ErrWriteConflict
		"global:2", // execution result undetermined
	}
	wCtx = workload{}
)

func initWorkloadParams(ctx *SuiteContext) {
	ctx.ctx, ctx.cancel = context.WithCancel(context.Background())
	ctx.workload = &wCtx
	ctx.tkPool = &sync.Pool{New: func() any {
		return testkit.NewTestKit(ctx.t, ctx.store)
	}}
}

func initWorkLoadContext(ctx *workload, colIDs ...int) {
	if len(colIDs) < 2 {
		return
	}

	ctx.tableID = colIDs[0]
	ctx.tableName = "t" + strconv.Itoa(ctx.tableID)
	ctx.colID = ctx.colID[:0]
	for i := 1; i < len(colIDs); i++ {
		ctx.colID = append(ctx.colID, colIDs[i])
	}
	// set started insert id to 10000
	ctx.insertID = 10000
	ctx.date = "2008-02-02"
}

type workChan struct {
	err      error
	finished bool
}

type worker struct {
	id       int
	loadType int // 0 is insert, 1 update, 2 delete
	wChan    chan *workChan
}

type workload struct {
	tableName string
	tableID   int
	colID     []int
	workerNum int
	wr        []*worker
	insertID  int
	date      string
}

func newWorker(wID int, ty int) *worker {
	wr := worker{
		id:       wID,
		loadType: ty,
		wChan:    make(chan *workChan, 1),
	}
	return &wr
}

func (w *worker) run(ctx *SuiteContext, wl *workload) {
	var err error
	tk := ctx.getTestKit()
	tk.MustExec("use addindex")
	tk.MustExec("set @@tidb_general_log = 1;")
	for {
		if ctx.done() {
			break
		}
		if w.loadType == 0 {
			// insert workload
			err = insertWorker(ctx, tk)
		} else if w.loadType == 1 {
			// update workload
			err = updateWorker(ctx, tk)
		} else if w.loadType == 2 {
			// delete workload
			err = deleteWorker(ctx, wl, tk)
		}
		if err != nil {
			break
		}
	}
	var wchan workChan
	wchan.err = err
	wchan.finished = true
	w.wChan <- &wchan
}

func (w *workload) start(ctx *SuiteContext, colIDs ...int) {
	initWorkLoadContext(w, colIDs...)
	w.wr = w.wr[:0]
	for i := 0; i < 3; i++ {
		worker := newWorker(i, i)
		w.wr = append(w.wr, worker)
		go worker.run(ctx, w)
	}
}

func (w *workload) stop(ctx *SuiteContext, tableID int) (err error) {
	ctx.cancel()
	count := 3
	for i := 0; i < 3; i++ {
		wChan := <-w.wr[i].wChan
		if wChan.err != nil {
			require.NoError(ctx.t, wChan.err)
			return wChan.err
		}
		if wChan.finished {
			count--
		}
		if count == 0 {
			break
		}
	}
	return err
}

func isSkippedError(err error, ctx *SuiteContext) bool {
	if err == nil {
		return true
	}
	if ctx.isPK || ctx.isUnique {
		pos := strings.Index(err.Error(), "1062")
		if pos > 0 {
			return true
		}
	}
	for _, errCode := range skippedErrorCode {
		pos := strings.Index(err.Error(), errCode)
		if pos >= 0 {
			return true
		}
	}
	return false
}
func insertStr(tableName string, id int, date string) string {
	insStr := "insert into addindex." + tableName + "(c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28)" +
		" values(" + strconv.Itoa(id) + "," +
		"3, 3, 3, 3, 3, " + strconv.Itoa(id) + ", 3, 3.0, 3.0, 1113.1111, adddate('" + date + "', " + strconv.Itoa(id-10000) + "), '11:11:13', '2001-01-03 11:11:13', '2001-01-03 11:11:11.123456', 2001, 'cccc', 'cccc', 'cccc', '" + "aaaa" + strconv.Itoa(id) + "', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', 'cccc', '{\"name\": \"Beijing\", \"population\": 102}')"
	return insStr
}

func insertWorker(ctx *SuiteContext, tk *testkit.TestKit) error {
	insStr := insertStr(wCtx.tableName, wCtx.insertID, wCtx.date)
	rs, err := tk.Exec(insStr)
	if !isSkippedError(err, ctx) {
		logutil.BgLogger().Info("workload insert failed", zap.String("category", "add index test"), zap.String("sql", insStr), zap.Error(err))
		return err
	}
	if rs != nil {
		if err := rs.Close(); err != nil {
			return err
		}
	}
	wCtx.insertID++

	time.Sleep(time.Duration(10) * time.Millisecond)
	return nil
}

func updateStr(ctx *SuiteContext, tableName string, colID []int) (uStr string) {
	var updateStr string
	id := rand.Intn(ctx.rowNum + 1)
	for i := 0; i < len(colID); i++ {
		colNewValue := genColval(colID[i])
		if colNewValue == "" {
			return ""
		}
		if i == 0 {
			updateStr = " set c" + strconv.Itoa(colID[i]) + "=" + colNewValue
		} else {
			updateStr = updateStr + ", c" + strconv.Itoa(colID[i]) + "=" + colNewValue
		}
	}
	updateStr = "update addindex." + tableName + updateStr + " where c0=" + strconv.Itoa(id)
	return updateStr
}

func updateWorker(ctx *SuiteContext, tk *testkit.TestKit) error {
	upStr := updateStr(ctx, wCtx.tableName, wCtx.colID)
	rs, err := tk.Exec(upStr)

	if !isSkippedError(err, ctx) {
		logutil.BgLogger().Info("workload update failed", zap.String("category", "add index test"), zap.String("sql", upStr), zap.Error(err))
		return err
	}
	if rs != nil {
		if err := rs.Close(); err != nil {
			return err
		}
	}
	time.Sleep(time.Duration(10) * time.Millisecond)
	return nil
}

func deleteStr(tableName string, id int) string {
	delStr := "delete from addindex." + tableName + " where c0 =" + strconv.Itoa(id)
	return delStr
}

func deleteWorker(ctx *SuiteContext, wl *workload, tk *testkit.TestKit) error {
	id := rand.Intn(ctx.rowNum + 1)
	delStr := deleteStr(wl.tableName, id)
	rs, err := tk.Exec(delStr)
	if !isSkippedError(err, ctx) {
		logutil.BgLogger().Info("workload delete failed", zap.String("category", "add index test"), zap.String("sql", delStr), zap.Error(err))
		return err
	}
	if rs != nil {
		if err := rs.Close(); err != nil {
			return err
		}
	}
	time.Sleep(time.Duration(10) * time.Millisecond)
	return nil
}

func genColval(colID int) string {
	switch colID {
	case 1, 2, 3, 4, 5, 7:
		return "c" + strconv.Itoa(colID) + " + 1"
	case 6:
		return "c" + strconv.Itoa(colID) + " - 64"
	case 8, 9, 10:
		return strconv.FormatFloat(rand.Float64(), 10, 16, 32)
	case 11:
		return "adddate(c11, 90)"
	case 12, 13, 14, 15:
		return time.Now().String()
	case 17:
		return strconv.Itoa(time.Now().Year())
	case 19:
		return "c19 + c0"
	case 18, 20, 21, 22, 23, 24, 25, 26, 27:
		return "ABCDEEEF"
	case 28:
		return "json_object('name', 'NanJing', 'population', 2566)"
	default:
		return ""
	}
}
