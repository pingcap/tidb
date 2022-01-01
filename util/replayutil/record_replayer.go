package replayutil

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

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
			go replayExecuteSQL(record[3], se)
		} else {
			go replayExecuteSQL(record[3], s)
		}
	}
}

func replayExecuteSQL(sql string, s session.Session) error {
	ctx := context.Background()
	fmt.Println(sql)
	args := strings.Split(sql, "[arguments: (")
	fmt.Println(args)
	if len(args) > 1{
		argument := strings.Split(args[1][:len(args[1])-2], ", ")
		sql = helper(args[0], argument)
	}
	fmt.Println(sql)
	stmts, err := s.Parse(ctx, sql)
	if err != nil {
		return err
	}
	for _, stmt := range stmts {
		s.ExecuteStmt(ctx, stmt)
	}
	return nil
}

func helper(sql string, args []string) string {
	newsql := ""
	i := 0
	for _, b := range []byte(sql) {
		if b == byte('?'){
			newsql += args[i]
			i++
		}else{
			newsql += string(b)
		}
	}
	return newsql
}
