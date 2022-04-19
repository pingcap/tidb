// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/dbutil"
	"go.uber.org/zap"

	"github.com/pingcap/tidb/br/pkg/utils"
	tcontext "github.com/pingcap/tidb/dumpling/context"
)

const (
	dumpChunkRetryTime       = 3
	lockTablesRetryTime      = 5
	dumpChunkWaitInterval    = 50 * time.Millisecond
	dumpChunkMaxWaitInterval = 200 * time.Millisecond
	// ErrNoSuchTable is the error code no such table in MySQL/TiDB
	ErrNoSuchTable uint16 = 1146
)

type backOfferResettable interface {
	utils.Backoffer
	Reset()
}

func newRebuildConnBackOffer(shouldRetry bool) backOfferResettable { // revive:disable-line:flag-parameter
	if !shouldRetry {
		return &noopBackoffer{
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

func (b *dumpChunkBackoffer) Reset() {
	b.attempt = dumpChunkRetryTime
	b.delayTime = dumpChunkWaitInterval
}

type noopBackoffer struct {
	attempt int
}

func (b *noopBackoffer) NextBackoff(err error) time.Duration {
	b.attempt--
	return time.Duration(0)
}

func (b *noopBackoffer) Attempt() int {
	return b.attempt
}

func (b *noopBackoffer) Reset() {
	b.attempt = 1
}

func newLockTablesBackoffer(tctx *tcontext.Context, blockList map[string]map[string]interface{}, conf *Config) *lockTablesBackoffer {
	if conf.specifiedTables {
		return &lockTablesBackoffer{
			tctx:      tctx,
			attempt:   1,
			blockList: blockList,
		}
	}
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
			b.tctx.L().Error("fail to retry lock tables", zap.Error(err))
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
