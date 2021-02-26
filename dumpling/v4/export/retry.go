// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"strings"
	"time"

	tcontext "github.com/pingcap/dumpling/v4/context"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"go.uber.org/zap"
)

const (
	dumpChunkRetryTime       = 3
	lockTablesRetryTime      = 5
	dumpChunkWaitInterval    = 50 * time.Millisecond
	dumpChunkMaxWaitInterval = 200 * time.Millisecond
	// ErrNoSuchTable is the error code no such table in MySQL/TiDB
	ErrNoSuchTable uint16 = 1146
)

func newDumpChunkBackoffer(shouldRetry bool) *dumpChunkBackoffer { // revive:disable-line:flag-parameter
	if !shouldRetry {
		return &dumpChunkBackoffer{
			attempt: 1,
		}
	}
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

func newLockTablesBackoffer(tctx *tcontext.Context, blockList map[string]map[string]interface{}) *lockTablesBackoffer {
	return &lockTablesBackoffer{
		tctx:      tctx,
		attempt:   lockTablesRetryTime,
		blockList: blockList,
	}
}

type lockTablesBackoffer struct {
	tctx      *tcontext.Context
	attempt   int
	blockList map[string]map[string]interface{}
}

func (b *lockTablesBackoffer) NextBackoff(err error) time.Duration {
	err = errors.Cause(err)
	if mysqlErr, ok := err.(*mysql.MySQLError); ok && mysqlErr.Number == ErrNoSuchTable {
		b.attempt--
		db, table, err := getTableFromMySQLError(mysqlErr.Message)
		if err != nil {
			b.tctx.L().Error("retry lock tables meet error", zap.Error(err))
			b.attempt = 0
			return 0
		}
		if _, ok := b.blockList[db]; !ok {
			b.blockList[db] = make(map[string]interface{})
		}
		b.blockList[db][table] = struct{}{}
		return 0
	}
	b.attempt = 0
	return 0
}

func (b *lockTablesBackoffer) Attempt() int {
	return b.attempt
}

func getTableFromMySQLError(msg string) (db, table string, err error) {
	// examples of the error msg:
	// Error 1146: Table 'pingcap.t1' doesn't exist
	// Error 1146: Table 'quo`te/database.quo.te/database-1.quo.te/table-1' doesn't exist /* doesn't support */
	msg = strings.TrimPrefix(msg, "Table '")
	msg = strings.TrimSuffix(msg, "' doesn't exist")
	failPart := strings.Split(msg, ".")
	if len(failPart) != 2 {
		err = errors.Errorf("doesn't support retry lock table %s", msg)
		return
	}
	return failPart[0], failPart[1], nil
}
