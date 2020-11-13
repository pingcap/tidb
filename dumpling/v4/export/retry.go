package export

import (
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
)

const (
	dumpChunkRetryTime       = 3
	dumpChunkWaitInterval    = 50 * time.Millisecond
	dumpChunkMaxWaitInterval = 200 * time.Millisecond
)

func newDumpChunkBackoffer() *dumpChunkBackoffer {
	return &dumpChunkBackoffer{
		attempt:      dumpChunkRetryTime,
		delayTime:    dumpChunkWaitInterval,
		maxDelayTime: dumpChunkMaxWaitInterval,
	}
}

type dumpChunkBackoffer struct {
	attempt      int
	delayTime    time.Duration
	maxDelayTime time.Duration
}

func (b *dumpChunkBackoffer) NextBackoff(err error) time.Duration {
	err = errors.Cause(err)
	if _, ok := err.(*mysql.MySQLError); ok && !dbutil.IsRetryableError(err) {
		b.attempt = 0
		return 0
	} else if _, ok := err.(*writerError); ok {
		// the uploader writer's retry logic is already done in aws client. needn't retry here
		b.attempt = 0
		return 0
	}
	b.delayTime = 2 * b.delayTime
	b.attempt--
	if b.delayTime > b.maxDelayTime {
		return b.maxDelayTime
	}
	return b.delayTime
}

func (b *dumpChunkBackoffer) Attempt() int {
	return b.attempt
}
