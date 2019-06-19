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
	readFileTuples, err := globalSlowQueryReader.ReadSlowLogDataFromFile(time.Local, logFile, 0, nil, nil)
	c.Assert(err, IsNil)
	c.Assert(len(readFileTuples), Equals, logNum)
	c.Assert(globalSlowQueryReader.cache.filePath, Equals, logFile)
	c.Assert(globalSlowQueryReader.cache.buf.len(), Equals, 10)
}

func prepareSlowLogFile(filePath string, c *C, num int) {
	f, err := os.Create(filePath)
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
