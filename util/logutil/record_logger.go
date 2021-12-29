package logutil

import (
	"bufio"
	"os"
	"sync"

	"github.com/pingcap/tidb/metrics"
	"go.uber.org/atomic"
)

// RecordLogger is used to log replay record
var RecordLogger *recordLogger

// InitRecord initialize logger
func InitRecord(filename string) {
	RecordLogger = newRecordLogger(filename)
}

// StopRecord stops goroutine
func StopRecord() {
	RecordLogger.stopLogWorker()
}

// PutRecordOrDrop put record
func PutRecordOrDrop(record string) {
	select {
	case RecordLogger.recordChan <- record:
	default:
		metrics.ReplayDropCounter.Inc()
	}
}

// recordLogger receives record from channel, and log or drop them as needed
type recordLogger struct {
	a          atomic.String
	writer     *bufio.Writer
	recordChan chan string
	close      chan struct{}
	wg         sync.WaitGroup
}

func newRecordLogger(filename string) *recordLogger {
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return nil
	}
	l := &recordLogger{
		writer:     bufio.NewWriter(f),
		recordChan: make(chan string, 10000),
		close:      make(chan struct{}),
	}
	l.wg.Add(1)
	go l.startLogWorker()
	return l
}

// startLogWorker starts a log flushing worker that flushes log periodically or when batch is full
func (re *recordLogger) startLogWorker() {
	for {
		select {
		case str := <-re.recordChan:
			re.writer.WriteString(str)
		case <-re.close:
			currLen := len(re.recordChan)
			for i := 0; i < currLen; i++ {
				str := <-re.recordChan
				re.writer.WriteString(str)
			}
			re.writer.Flush()
			re.wg.Done()
			return
		}
	}
}

func (re *recordLogger) stopLogWorker() {
	close(re.close)
	re.wg.Wait()
}
