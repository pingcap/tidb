package infoschema

import (
	"fmt"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/logutil"
	"math/rand"
	"os"
	"time"
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

func (*testSlowLogBufferSuit) TestSlowQueryReader(c *C) {
	logFile := "tidb-slow.log"
	logNum := 10
	slowQueryBufferSize = 10
	prepareSlowLogFile(logFile, c, logNum)
	// readFileFirst indicate read file first or read cache first. Read file first will update the cache.
	checkCache := func(totalSize, cacheSize, beforeCacheSize, afterCacheSize int, updateCache bool) {
		// get total tuples by parse file.
		readFileRows, readFileTuples, err := globalSlowQueryReader.ReadSlowLogDataFromFileWithoutUpdateCache(time.Local, logFile, 0, nil, nil)
		c.Assert(err, IsNil)
		c.Assert(len(readFileRows), Equals, totalSize)
		if updateCache {
			globalSlowQueryReader.updateCache(logFile, readFileTuples)
		}

		// Check cache parameter.
		c.Assert(globalSlowQueryReader.cache.filePath, Equals, logFile)
		c.Assert(globalSlowQueryReader.cache.buf.len(), Equals, cacheSize)
		c.Assert(globalSlowQueryReader.cache.getEndPos() != 0, IsTrue)
		// Get before cached tuples.
		beforeCachedTuples, _, err := globalSlowQueryReader.parseSlowLogDataFromFileBeforeCachedAndCheckTruncate(time.Local)
		c.Assert(err, IsNil)
		c.Assert(len(beforeCachedTuples), Equals, beforeCacheSize)
		c.Assert(globalSlowQueryReader.getSlowLogDataFromCache(time.Local, nil), DeepEquals, readFileRows[beforeCacheSize:totalSize-afterCacheSize])
		// Get after cached tuples.
		cacheEndTime := globalSlowQueryReader.readCacheAtEnd().time
		cacheEndPos := globalSlowQueryReader.cache.getEndPos()
		afterCachedTuples, err := globalSlowQueryReader.parseSlowLogDataFromFileAfterCached(time.Local, cacheEndTime, cacheEndPos)
		c.Assert(err, IsNil)
		c.Assert(len(afterCachedTuples), Equals, afterCacheSize)
	}
	checkCache(10, 10, 0, 0, true)

	// Test append new log data.
	appendToSlowLogFile(logFile, c, 1)
	checkCache(11, 10, 0, 1, false)
	checkCache(11, 10, 1, 0, true)

	appendToSlowLogFile(logFile, c, 1)
	readFileRows, _, err := globalSlowQueryReader.ReadSlowLogDataFromFileWithoutUpdateCache(time.Local, logFile, 0, nil, nil)
	c.Assert(err, IsNil)
	readCacheRows, err := globalSlowQueryReader.readSlowLogDataWithCache(time.Local)
	c.Assert(readFileRows, DeepEquals, readCacheRows)
	checkCache(12, 10, 2, 0, false)

	// Test truncate log file.
	truncateSlowLogFile(logFile, c)
	readFileRowsAfterTruncate, _, err := globalSlowQueryReader.ReadSlowLogDataFromFileWithoutUpdateCache(time.Local, logFile, 0, nil, nil)
	c.Assert(err, IsNil)
	c.Assert(len(readFileRowsAfterTruncate), Equals, 0)
	readCacheRowsAfterTruncate, err := globalSlowQueryReader.readSlowLogDataWithCache(time.Local)
	c.Assert(readCacheRowsAfterTruncate, DeepEquals, readFileRows[2:])

	appendToSlowLogFile(logFile, c, 1)
	readFileRowsAfterTruncate, _, err = globalSlowQueryReader.ReadSlowLogDataFromFileWithoutUpdateCache(time.Local, logFile, 0, nil, nil)
	c.Assert(err, IsNil)
	c.Assert(len(readFileRowsAfterTruncate), Equals, 1)

	readCacheRowsAfterTruncate, err = globalSlowQueryReader.readSlowLogDataWithCache(time.Local)
	c.Assert(len(readCacheRowsAfterTruncate), Equals, 11)
	c.Assert(readCacheRowsAfterTruncate[:10], DeepEquals, readFileRows[2:])
	c.Assert(readCacheRowsAfterTruncate[10], DeepEquals, readFileRowsAfterTruncate[0])
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
