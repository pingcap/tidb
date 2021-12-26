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
	"strconv"
	"strings"
	"time"
)

const TxnChannelSize = 10000

// RecordReplayer is used to replay sql
var RecordReplayer *recordReplayer
var TxnChannel = make(chan TxnRecord, TxnChannelSize)

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
	se, err := session.CreateSession(store)
	if err != nil {
		log.Info("init recordPlayer fail")
		return nil
	}
	r := &recordReplayer{
		Se:       se,
		fileName: filename,
		close:    make(chan struct{}),
	}
	return r
}

type recordReplayer struct {
	Se       session.Session
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
	Txns := make(map[string]*TxnRecord)
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
		inTxn := record[0] != "0"
		if inTxn {
			txnID := record[0]
			ts := record[1]
			sql := record[2]
			if txn, ok := Txns[txnID]; !ok {
				Txns[record[0]] = &TxnRecord{
					StartTS: ts,
					Sqls:    make([]string, 10),
				}
			} else {
				txn.Sqls = append(txn.Sqls, sql)
				if sql == strings.ToLower("commit") {
					Txns[txnID].CommitTS = ts
					TxnChannel <- *Txns[txnID]
					delete(Txns, txnID)
				} else {
					// todo: create a session
					// replay single sql
				}
			}

		} else {
			// replay not txn
		}
	}
}

type TxnRecord struct {
	StartTS  string
	CommitTS string
	Sqls     []string
}

type TxnReplayer struct {
	LogicTs	time.Time
	Se    session.Session
	close chan struct{}
}

func (tr *TxnReplayer) replay() {
	for {
		select {
		case record := <-TxnChannel:
			startTs, _ := strconv.ParseFloat(record.StartTS,2)
			if sleepTime := startTs-time.Since(tr.LogicTs).Seconds();sleepTime>0 {
				time.Sleep(time.Duration(sleepTime) * time.Second)
			}
			go tr.TxnExecute("", record)
		case <-tr.close:
			break
		default:
		}
	}
}

func (tr *TxnReplayer) TxnExecute(sql string, record TxnRecord) error {

	ctx := context.Background()
	stmts, err := tr.Se.Parse(ctx, sql)
	if err != nil {
		return err
	}
	for _, stmt := range stmts {
		_, err := tr.Se.ExecuteStmt(ctx, stmt)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
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
