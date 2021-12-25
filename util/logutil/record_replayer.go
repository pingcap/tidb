package logutil

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// RecordReplayer is used to replay sql
var RecordReplayer *recordReplayer

// StartReplay starts replay
func StartReplay(filename string) {
	RecordReplayer = newRecordPlayer(filename)
	RecordReplayer.start()
}

// StopReplay stops replay
func StopReplay() {
	RecordReplayer.close <- struct{}{}
}

func newRecordPlayer(filename string) *recordReplayer {
	r := &recordReplayer{
		fileName: filename,
		close:   make(chan struct{}),
	}
	return r
}

type recordReplayer struct {
	close   chan struct{}
	fileName      string
	scanner *bufio.Scanner
}

func (r *recordReplayer) start() {
	f, err := os.OpenFile(r.fileName, os.O_RDONLY, os.ModePerm)
	defer f.Close()
	if err != nil {
		fmt.Printf("Open file error %s\n", err.Error())
		return
	}

	r.scanner = bufio.NewScanner(f)
	txns := make(map[string][]string)
	for r.scanner.Scan() {
		select {
		case <-r.close:
			break
		default:
		}
		text := r.scanner.Text()
		s := strings.Split(text, " ")
		if len(s) < 2 {
			fmt.Printf("invalid sql log %v\n", s)
			continue
		}
		inTxn := len(s) == 3
		if inTxn {
			txnID := s[0]
			sql := s[2]
			txn := txns[txnID]
			txn = append(txn, sql)
			if sql == strings.ToLower("commit") {
				tr := &txnReplayer{
					close: make(chan struct{}),
					sqls:  txn,
				}
				go tr.replay()
			} else {
				// todo
				// replay single sql
			}
		}
	}
}

type txnReplayer struct {
	close chan struct{}
	sqls  []string
}

func (tr *txnReplayer) replay() {
	for _, sql := range tr.sqls {
		select {
		case <-tr.close:
			break
		default:
			// todo
			// replay single sql
		}
	}
}
