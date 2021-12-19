package logutil

import (
	"bufio"
	"github.com/pingcap/tidb/metrics"
	"go.uber.org/atomic"
	"os"
	"sync"
)

// ReplayLogger is used to log replay record
var ReplayLogger *replayLogger

// InitReplay initialize logger
func InitReplay(filename string) {
	ReplayLogger = newReplayLogger(filename)
}

// StopReplay stops goroutine
func StopReplay() {
	ReplayLogger.stopLogWorker()
}

func PutRecordOrDrop(record string) {
	select {
	case ReplayLogger.recordChan <- record:
	default:
		metrics.ReplayDropCounter.Inc()
	}
}

// replayLogger receives record from channel, and log or drop them as needed
type replayLogger struct {
	a          atomic.String
	writer     *bufio.Writer
	recordChan chan string
	close      chan struct{}
	wg         sync.WaitGroup
}

func newReplayLogger(filename string) *replayLogger {
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return nil
	}
	l := &replayLogger{
		writer:     bufio.NewWriter(f),
		recordChan: make(chan string, 10000),
		close:      make(chan struct{}),
	}
	l.wg.Add(1)
	go l.startLogWorker()
	return l
}

// startLogWorker starts a log flushing worker that flushes log periodically or when batch is full
func (re *replayLogger) startLogWorker() {
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

func (re *replayLogger) stopLogWorker() {
	close(re.close)
	re.wg.Wait()
}
