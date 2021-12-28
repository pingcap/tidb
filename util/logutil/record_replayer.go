package logutil

import (
	"bufio"
	"context"
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"os"
	"strings"
)

// RecordReplayer is used to replay sql
var RecordReplayer *recordReplayer
var Sessions map[string]session.Session

// StartReplay starts replay
func StartReplay(filename string, store kv.Storage) {
	RecordReplayer = newRecordPlayer(filename, store)
	RecordReplayer.start()
}

// StopReplay stops replay
func StopReplay() {
	RecordReplayer.close <- struct{}{}
}

func newRecordPlayer(filename string, store kv.Storage) *recordReplayer {

	r := &recordReplayer{
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
	for r.scanner.Scan() {
		select {
		case <-r.close:
			break
		default:
		}
		text := r.scanner.Text()
		record := strings.SplitN(text, " ", 2)
		if len(record) < 3 {
			fmt.Printf("invalid sql log %v\n", record)
			continue
		}
		// fake code
		if s, exist := Sessions[record[0]]; !exist {
			se, err := session.CreateSession(r.store)
			if err != nil {
				log.Info("init recordPlayer fail")
				return
			}
			Sessions[record[0]] = se
			go ReplayExecuteSQL(record[2], s)
		} else {
			go ReplayExecuteSQL(record[2], s)
		}
	}
}

func ReplayExecuteSQL(sql string, s session.Session) error {
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
