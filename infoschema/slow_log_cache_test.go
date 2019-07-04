package infoschema

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
)

var _ = Suite(&testSlowLogBufferSuit{})

type testSlowLogBufferSuit struct {
}

func (*testSlowLogBufferSuit) TestRingBufferBasic(c *C) {
	rb := newRingBuffer(5)
	testRingBufferBasic(rb, c)
	rb.clear()
	testRingBufferBasic(rb, c)
}

func testRingBufferBasic(rb *ringBuffer, c *C) {
	checkFunc := func(l int, startValue, endValue interface{}, allValues []interface{}) {
		if l > 0 {
			c.Assert(rb.isEmpty(), IsFalse)
		}
		c.Assert(rb.len(), Equals, l)
		c.Assert(rb.getStart(), DeepEquals, startValue)
		c.Assert(rb.getEnd(), DeepEquals, endValue)
		c.Assert(rb.readAll(), DeepEquals, allValues)
		if len(allValues) == len(rb.data) {
			c.Assert(rb.full, IsTrue)
		}
	}
	checkFunc(0, nil, nil, nil)

	rb.write(1)
	checkFunc(1, 1, 1, []interface{}{1})

	rb.write(2)
	checkFunc(2, 1, 2, []interface{}{1, 2})

	rb.write(3)
	checkFunc(3, 1, 3, []interface{}{1, 2, 3})

	rb.write(4)
	checkFunc(4, 1, 4, []interface{}{1, 2, 3, 4})

	rb.write(5)
	checkFunc(5, 1, 5, []interface{}{1, 2, 3, 4, 5})

	rb.write(6)
	checkFunc(5, 2, 6, []interface{}{2, 3, 4, 5, 6})

	rb.write(7)
	checkFunc(5, 3, 7, []interface{}{3, 4, 5, 6, 7})

	rb.write(8)
	checkFunc(5, 4, 8, []interface{}{4, 5, 6, 7, 8})

	rb.write(9)
	checkFunc(5, 5, 9, []interface{}{5, 6, 7, 8, 9})

	rb.write(10)
	checkFunc(5, 6, 10, []interface{}{6, 7, 8, 9, 10})

	rb.write(11)
	checkFunc(5, 7, 11, []interface{}{7, 8, 9, 10, 11})

	rb.resize(6)
	checkFunc(5, 7, 11, []interface{}{7, 8, 9, 10, 11})

	rb.write(12)
	checkFunc(6, 7, 12, []interface{}{7, 8, 9, 10, 11, 12})

	rb.write(13)
	checkFunc(6, 8, 13, []interface{}{8, 9, 10, 11, 12, 13})

	rb.resize(3)
	checkFunc(3, 11, 13, []interface{}{11, 12, 13})

	rb.resize(5)
	checkFunc(3, 11, 13, []interface{}{11, 12, 13})
}

func setSlowQueryCacheSize(size uint64) {
	newConf := config.NewConfig()
	newConf.Log.SlowQueryCacheSize = size
	config.StoreGlobalConfig(newConf)
}

func (*testSlowLogBufferSuit) TestSlowQueryReader(c *C) {
	logFile := "tidb-slow.log"
	logNum := 10
	defer func() { setSlowQueryCacheSize(logutil.DefaultSlowQueryCacheSize) }()
	setSlowQueryCacheSize(uint64(logNum))
	prepareSlowLogFile(logFile, c, logNum)
	// readFileFirst indicate read file first or read cache first. Read file first will update the cache.
	checkCache := func(totalSize, cacheSize, beforeCacheSize, afterCacheSize int, updateCache bool) {
		// get total tuples by parse file.
		parseFileTuples, err := parseSlowLogDataFromFile(time.Local, logFile, 0, nil, nil)
		c.Assert(err, IsNil)
		parseFileRows := convertSlowLogTuplesToDatums(parseFileTuples)
		c.Assert(len(parseFileRows), Equals, totalSize)
		if updateCache {
			globalSlowQueryReader.updateCache(logFile, parseFileTuples)
		}

		// Check cache parameter.
		c.Assert(globalSlowQueryReader.cache.filePath, Equals, logFile)
		c.Assert(globalSlowQueryReader.cache.buf.len(), Equals, cacheSize)
		c.Assert(globalSlowQueryReader.cache.getEndOffset() != 0, IsTrue)
		// Get before cached tuples.
		beforeCachedTuples, _, err := globalSlowQueryReader.parseSlowLogFileBeforeCachedAndCheckTruncate(time.Local)
		c.Assert(err, IsNil)
		c.Assert(len(beforeCachedTuples), Equals, beforeCacheSize)
		c.Assert(globalSlowQueryReader.getSlowLogRowsFromCache(time.Local, nil), DeepEquals, parseFileRows[beforeCacheSize:totalSize-afterCacheSize])
		// Get after cached tuples.
		cacheEndTime := globalSlowQueryReader.cache.getEndTime()
		cacheEndOffset := globalSlowQueryReader.cache.getEndOffset()
		afterCachedTuples, err := globalSlowQueryReader.parseSlowLogFileAfterCached(time.Local, cacheEndTime, cacheEndOffset)
		c.Assert(err, IsNil)
		c.Assert(len(afterCachedTuples), Equals, afterCacheSize)
	}
	checkCache(10, 10, 0, 0, true)

	// Test append new log data.
	appendToSlowLogFile(logFile, c, 1)
	checkCache(11, 10, 0, 1, false)
	checkCache(11, 10, 1, 0, true)

	appendToSlowLogFile(logFile, c, 1)
	parseFileRows, err := ParseSlowLogRows(logFile, time.Local)
	c.Assert(err, IsNil)
	readWithCacheRows, err := globalSlowQueryReader.getSlowLogDataWithCache(time.Local)
	c.Assert(err, IsNil)
	c.Assert(parseFileRows, DeepEquals, readWithCacheRows)
	checkCache(12, 10, 2, 0, false)

	// Test truncate log file.
	cacheRows := globalSlowQueryReader.getSlowLogRowsFromCache(time.Local, nil)
	truncateSlowLogFile(logFile, c)
	parseFileRows, err = ParseSlowLogRows(logFile, time.Local)
	c.Assert(err, IsNil)
	c.Assert(len(parseFileRows), Equals, 0)

	readWithCacheRows, err = globalSlowQueryReader.getSlowLogDataWithCache(time.Local)
	c.Assert(err, IsNil)
	c.Assert(readWithCacheRows, DeepEquals, cacheRows)

	// Test append new log after truncate.
	appendToSlowLogFile(logFile, c, 1)
	parseFileRows, err = ParseSlowLogRows(logFile, time.Local)
	c.Assert(err, IsNil)
	c.Assert(len(parseFileRows), Equals, 1)

	readWithCacheRows, err = globalSlowQueryReader.getSlowLogDataWithCache(time.Local)
	c.Assert(err, IsNil)
	c.Assert(len(readWithCacheRows), Equals, 11)
	c.Assert(readWithCacheRows[:10], DeepEquals, cacheRows)
	c.Assert(readWithCacheRows[10], DeepEquals, parseFileRows[0])

	err = os.Remove(logFile)
	c.Assert(err, IsNil)
}

// ParseSlowLogRows reads slow log data by parse slow log file.
// It won't use the cache data.
// It is exporting for testing.
func ParseSlowLogRows(filePath string, tz *time.Location) ([][]types.Datum, error) {
	tuples, err := parseSlowLogDataFromFile(tz, filePath, 0, nil, nil)
	if err != nil {
		return nil, err
	}
	return convertSlowLogTuplesToDatums(tuples), nil
}

func prepareSlowLogFile(filePath string, c *C, num int) {
	f, err := os.Create(filePath)
	c.Assert(err, IsNil)
	appendToSlowLogFile(filePath, c, num)
	err = f.Close()
	c.Assert(err, IsNil)
}

func appendToSlowLogFile(filePath string, c *C, num int) {
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, 0644)
	c.Assert(err, IsNil)
	for i := 0; i < num; i++ {
		now := time.Now()
		logStr := fmt.Sprintf("# Time: %v\n", now.Format(logutil.SlowLogTimeFormat))
		logStr += fmt.Sprintf("# Txn_start_ts: %v\n", now.Unix())
		logStr += fmt.Sprintf("# Query_time: %v\n", rand.Float64())
		logStr += fmt.Sprintf("# Process_time: %v\n", rand.Float64())
		logStr += fmt.Sprintf("# Is_internal: %v\n", true)
		logStr += fmt.Sprintf("# Digest: %v\n", "abcdefghijk")
		logStr += fmt.Sprintf("# Stats: %v\n", "pseudo")
		logStr += fmt.Sprintf("select * from t%v;\n", i)
		_, err = f.WriteString(logStr)
		c.Assert(err, IsNil)
	}
	err = f.Close()
	c.Assert(err, IsNil)
}

func truncateSlowLogFile(filePath string, c *C) {
	f, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	c.Assert(err, IsNil)
	err = f.Truncate(0)
	c.Assert(err, IsNil)
	err = f.Close()
	c.Assert(err, IsNil)
}
