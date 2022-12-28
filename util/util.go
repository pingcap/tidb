// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// SliceToMap converts slice to map
// nolint:unused
func SliceToMap(slice []string) map[string]interface{} {
	sMap := make(map[string]interface{})
	for _, str := range slice {
		sMap[str] = struct{}{}
	}
	return sMap
}

// StringsToInterfaces converts string slice to interface slice
func StringsToInterfaces(strs []string) []interface{} {
	is := make([]interface{}, 0, len(strs))
	for _, str := range strs {
		is = append(is, str)
	}

	return is
}

// GetJSON fetches a page and parses it as JSON. The parsed result will be
// stored into the `v`. The variable `v` must be a pointer to a type that can be
// unmarshalled from JSON.
//
// Example:
//
//	client := &http.Client{}
//	var resp struct { IP string }
//	if err := util.GetJSON(client, "http://api.ipify.org/?format=json", &resp); err != nil {
//		return errors.Trace(err)
//	}
//	fmt.Println(resp.IP)
//
// nolint:unused
func GetJSON(client *http.Client, url string, v interface{}) error {
	resp, err := client.Get(url)
	if err != nil {
		return errors.Trace(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return errors.Trace(err)
		}
		return errors.Errorf("get %s http status code != 200, message %s", url, string(body))
	}

	return errors.Trace(json.NewDecoder(resp.Body).Decode(v))
}

// ChanMap creates a channel which applies the function over the input Channel.
// Hint of Resource Leakage:
// In golang, channel isn't an interface so we must create a goroutine for handling the inputs.
// Hence the input channel must be closed properly or this function may leak a goroutine.
func ChanMap[T, R any](c <-chan T, f func(T) R) <-chan R {
	outCh := make(chan R)
	go func() {
		defer close(outCh)
		for item := range c {
			outCh <- f(item)
		}
	}()
	return outCh
}

// Str2Int64Map converts a string to a map[int64]struct{}.
func Str2Int64Map(str string) map[int64]struct{} {
	strs := strings.Split(str, ",")
	res := make(map[int64]struct{}, len(strs))
	for _, s := range strs {
		id, _ := strconv.ParseInt(s, 10, 64)
		res[id] = struct{}{}
	}
	return res
}

// GenLogFields generate log fields.
func GenLogFields(costTime time.Duration, info *ProcessInfo, needTruncateSQL bool) []zap.Field {
	if info.RefCountOfStmtCtx != nil && !info.RefCountOfStmtCtx.TryIncrease() {
		return nil
	}
	defer info.RefCountOfStmtCtx.Decrease()

	logFields := make([]zap.Field, 0, 20)
	logFields = append(logFields, zap.String("cost_time", strconv.FormatFloat(costTime.Seconds(), 'f', -1, 64)+"s"))
	execDetail := info.StmtCtx.GetExecDetails()
	logFields = append(logFields, execDetail.ToZapFields()...)
	if copTaskInfo := info.StmtCtx.CopTasksDetails(); copTaskInfo != nil {
		logFields = append(logFields, copTaskInfo.ToZapFields()...)
	}
	if statsInfo := info.StatsInfo(info.Plan); len(statsInfo) > 0 {
		var buf strings.Builder
		firstComma := false
		vStr := ""
		for k, v := range statsInfo {
			if v == 0 {
				vStr = "pseudo"
			} else {
				vStr = strconv.FormatUint(v, 10)
			}
			if firstComma {
				buf.WriteString("," + k + ":" + vStr)
			} else {
				buf.WriteString(k + ":" + vStr)
				firstComma = true
			}
		}
		logFields = append(logFields, zap.String("stats", buf.String()))
	}
	if info.ID != 0 {
		logFields = append(logFields, zap.Uint64("conn_id", info.ID))
	}
	if len(info.User) > 0 {
		logFields = append(logFields, zap.String("user", info.User))
	}
	if len(info.DB) > 0 {
		logFields = append(logFields, zap.String("database", info.DB))
	}
	var tableIDs, indexNames string
	if len(info.StmtCtx.TableIDs) > 0 {
		tableIDs = strings.Replace(fmt.Sprintf("%v", info.StmtCtx.TableIDs), " ", ",", -1)
		logFields = append(logFields, zap.String("table_ids", tableIDs))
	}
	if len(info.StmtCtx.IndexNames) > 0 {
		indexNames = strings.Replace(fmt.Sprintf("%v", info.StmtCtx.IndexNames), " ", ",", -1)
		logFields = append(logFields, zap.String("index_names", indexNames))
	}
	logFields = append(logFields, zap.Uint64("txn_start_ts", info.CurTxnStartTS))
	if memTracker := info.MemTracker; memTracker != nil {
		logFields = append(logFields, zap.String("mem_max", fmt.Sprintf("%d Bytes (%v)", memTracker.MaxConsumed(), memTracker.FormatBytes(memTracker.MaxConsumed()))))
	}

	const logSQLLen = 1024 * 8
	var sql string
	if len(info.Info) > 0 {
		sql = info.Info
		if info.RedactSQL {
			sql = parser.Normalize(sql)
		}
	}
	if len(sql) > logSQLLen && needTruncateSQL {
		sql = fmt.Sprintf("%s len(%d)", sql[:logSQLLen], len(sql))
	}
	logFields = append(logFields, zap.String("sql", sql))
	return logFields
}

// PrintableASCII detects if b is a printable ASCII character.
// Ref to:http://facweb.cs.depaul.edu/sjost/it212/documents/ascii-pr.htm
func PrintableASCII(b byte) bool {
	if b < 32 || b > 127 {
		return false
	}

	return true
}

// FmtNonASCIIPrintableCharToHex turns non-printable-ASCII characters into Hex
func FmtNonASCIIPrintableCharToHex(str string) string {
	var b bytes.Buffer
	b.Grow(len(str) * 2)
	for i := 0; i < len(str); i++ {
		if PrintableASCII(str[i]) {
			b.WriteByte(str[i])
			continue
		}

		b.WriteString(`\x`)
		// turns non-printable-ASCII character into hex-string
		b.WriteString(fmt.Sprintf("%02X", str[i]))
	}
	return b.String()
}

// TCPConnWithIOCounter is a wrapper of net.TCPConn with counter that accumulates
// the bytes this connection reads/writes.
type TCPConnWithIOCounter struct {
	*net.TCPConn
	c *atomic.Uint64
}

// NewTCPConnWithIOCounter creates a new TCPConnWithIOCounter.
func NewTCPConnWithIOCounter(conn *net.TCPConn, c *atomic.Uint64) net.Conn {
	return &TCPConnWithIOCounter{
		TCPConn: conn,
		c:       c,
	}
}

func (t *TCPConnWithIOCounter) Read(b []byte) (n int, err error) {
	n, err = t.TCPConn.Read(b)
	t.c.Add(uint64(n))
	return n, err
}

func (t *TCPConnWithIOCounter) Write(b []byte) (n int, err error) {
	n, err = t.TCPConn.Write(b)
	t.c.Add(uint64(n))
	return n, err
}
