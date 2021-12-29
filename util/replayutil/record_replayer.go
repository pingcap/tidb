package replayutil

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
)

// RecordReplayer is used to replay sql
var RecordReplayer *recordReplayer
// Sessions is a map
var Sessions map[string]session.Session

// StartReplay starts replay
func StartReplay(filename string, store kv.Storage) {
	RecordReplayer = newRecordPlayer(filename, store)
	go RecordReplayer.start()
}

// StopReplay stops replay
func StopReplay() {
	RecordReplayer.close <- struct{}{}
}

func newRecordPlayer(filename string, store kv.Storage) *recordReplayer {
	r := &recordReplayer{
		store:    store,
		fileName: filename,
		close:    make(chan struct{}),
	}
	return r
}

type recordReplayer struct {
	store    kv.Storage
	close    chan struct{}
	fileName string
	scanner  *bufio.Scanner
}

func (r *recordReplayer) start() {
	f, err := os.OpenFile(r.fileName, os.O_RDONLY, os.ModePerm)
	defer f.Close()
	if err != nil {
		fmt.Printf("Open file error %s\n", err.Error())
		return
	}

	r.scanner = bufio.NewScanner(f)
	Sessions = make(map[string]session.Session)
	start := time.Now()
	for r.scanner.Scan() {
		select {
		case <-r.close:
			break
		default:
		}
		text := r.scanner.Text()
		record := strings.SplitN(text, " ", 4)
		if len(record) < 4 {
			fmt.Printf("invalid sql log %v, len:%d\n", record, len(record))
			continue
		}
		ts, _ := strconv.ParseFloat(record[1], 10)
		if sleepTime := ts - time.Since(start).Seconds(); sleepTime > 0 {
			fmt.Printf("sleep time:%v\n", sleepTime)
			time.Sleep(time.Duration(sleepTime) * time.Second)
		}
		if s, exist := Sessions[record[0]]; !exist {
			se, err := session.CreateSession(r.store)
			se.GetSessionVars().CurrentDB = record[2]
			if err != nil {
				log.Info("init replay session fail")
				return
			}
			Sessions[record[0]] = se
			go replayExecuteSQL(record[3], se, record[0])
		} else {
			go replayExecuteSQL(record[3], s, record[0])
		}
	}
}

func replayExecuteSQL(sql string, s session.Session, connection string) error {
	ctx := context.Background()
	stmts, err := s.Parse(ctx, sql)
	if err != nil {
		return err
	}
	for _, stmt := range stmts {
		_, err := s.ExecuteStmt(ctx, stmt)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}
