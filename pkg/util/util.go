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
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"
)

// SliceToMap converts slice to map
// nolint:unused
func SliceToMap(slice []string) map[string]any {
	sMap := make(map[string]any)
	for _, str := range slice {
		sMap[str] = struct{}{}
	}
	return sMap
}

// StringsToInterfaces converts string slice to interface slice
func StringsToInterfaces(strs []string) []any {
	is := make([]any, 0, len(strs))
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
func GetJSON(client *http.Client, url string, v any) error {
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
	copTaskInfo := info.StmtCtx.CopTasksDetails()
	logFields = append(logFields, copTaskInfo.ToZapFields()...)
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
		logFields = append(logFields, zap.Uint64("conn", info.ID))
	}
	if len(info.User) > 0 {
		logFields = append(logFields, zap.String("user", info.User))
	}
	if len(info.DB) > 0 {
		logFields = append(logFields, zap.String("database", info.DB))
	}
	var tableIDs, indexNames string
	if len(info.TableIDs) > 0 {
		tableIDs = strings.ReplaceAll(fmt.Sprintf("%v", info.TableIDs), " ", ",")
		logFields = append(logFields, zap.String("table_ids", tableIDs))
	}
	if len(info.IndexNames) > 0 {
		indexNames = strings.ReplaceAll(fmt.Sprintf("%v", info.IndexNames), " ", ",")
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
		sql = parser.Normalize(sql, info.RedactSQL)
	}
	if len(sql) > logSQLLen && needTruncateSQL {
		sql = fmt.Sprintf("%s len(%d)", sql[:logSQLLen], len(sql))
	}
	logFields = append(logFields, zap.String("sql", sql))
	logFields = append(logFields, zap.String("session_alias", info.SessionAlias))
	logFields = append(logFields, zap.Uint64("affected rows", info.StmtCtx.AffectedRows()))
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
	for i := range len(str) {
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

// ReadLine tries to read a complete line from bufio.Reader.
// maxLineSize specifies the maximum size of a single line.
func ReadLine(reader *bufio.Reader, maxLineSize int) ([]byte, error) {
	var resByte []byte
	lineByte, isPrefix, err := reader.ReadLine()
	if isPrefix {
		// Need to read more data.
		resByte = make([]byte, len(lineByte), len(lineByte)*2)
	} else {
		resByte = make([]byte, len(lineByte))
	}
	// Use copy here to avoid shallow copy problem.
	copy(resByte, lineByte)
	if err != nil {
		return resByte, err
	}
	var tempLine []byte
	for isPrefix {
		tempLine, isPrefix, err = reader.ReadLine()
		resByte = append(resByte, tempLine...) // nozero
		// Use maxLineSize to check the single line length.
		if len(resByte) > maxLineSize {
			return resByte, errors.Errorf("single line length exceeds limit: %v", maxLineSize)
		}
		if err != nil {
			return resByte, err
		}
	}
	return resByte, err
}

// ReadLines tries to read lines from bufio.Reader.
// count specifies the number of lines.
// maxLineSize specifies the maximum size of a single line.
func ReadLines(reader *bufio.Reader, count int, maxLineSize int) ([][]byte, error) {
	lines := make([][]byte, 0, count)
	for range count {
		line, err := ReadLine(reader, maxLineSize)
		if err == io.EOF && len(lines) > 0 {
			return lines, nil
		}
		if err != nil {
			return nil, err
		}
		lines = append(lines, line)
	}
	return lines, nil
}

// IsInCorrectIdentifierName checks if the identifier is incorrect.
// See https://dev.mysql.com/doc/refman/5.7/en/identifiers.html
func IsInCorrectIdentifierName(name string) bool {
	if len(name) == 0 {
		return true
	}
	if name[len(name)-1] == ' ' {
		return true
	}
	return false
}

// GetRecoverError gets the error from recover.
func GetRecoverError(r any) error {
	if err, ok := r.(error); ok {
		// Runtime panic also implements error interface.
		// So do not forget to add stack info for it.
		return errors.Trace(err)
	}
	return errors.Errorf("%v", r)
}

// ProtoV1Clone clones a V1 proto message.
func ProtoV1Clone[T protoadapt.MessageV1](p T) T {
	return protoadapt.MessageV1Of(proto.Clone(protoadapt.MessageV2Of(p))).(T)
}

// CheckIfSameCluster reads PD addresses registered in etcd from two sources, to
// check if there are common addresses in both sources. If there are common
// addresses, the first return value is true which means we have confidence that
// the two sources are in the same cluster. If there are no common addresses, the
// first return value is false, which means 1) the two sources are in different
// clusters, or 2) the two sources may be in the same cluster but the getter
// function does not return the common addresses.
//
// The getters should keep the same format of the returned addresses, like both
// have URL scheme or not.
//
// The second and third return values are the PD addresses from the first and
// second getters respectively. The fourth return value is the error occurred.
func CheckIfSameCluster(
	ctx context.Context,
	pdAddrsGetter, pdAddrsGetter2 func(context.Context) ([]string, error),
) (bool, []string, []string, error) {
	addrs, err := pdAddrsGetter(ctx)
	if err != nil {
		return false, nil, nil, errors.Trace(err)
	}
	addrsMap := make(map[string]struct{}, len(addrs))
	for _, a := range addrs {
		addrsMap[a] = struct{}{}
	}

	addrs2, err := pdAddrsGetter2(ctx)
	if err != nil {
		return false, nil, nil, errors.Trace(err)
	}
	for _, a := range addrs2 {
		if _, ok := addrsMap[a]; ok {
			return true, addrs, addrs2, nil
		}
	}
	return false, addrs, addrs2, nil
}

// GetPDsAddrWithoutScheme returns a function that read all PD nodes' first etcd
// client URL by SQL query. This is done by query INFORMATION_SCHEMA.CLUSTER_INFO
// table and its executor memtableRetriever.dataForTiDBClusterInfo.
func GetPDsAddrWithoutScheme(db *sql.DB) func(context.Context) ([]string, error) {
	return func(ctx context.Context) ([]string, error) {
		rows, err := db.QueryContext(ctx, "SELECT STATUS_ADDRESS FROM INFORMATION_SCHEMA.CLUSTER_INFO WHERE TYPE = 'pd'")
		if err != nil {
			return nil, errors.Trace(err)
		}
		defer rows.Close()
		var ret []string
		for rows.Next() {
			var addr string
			err = rows.Scan(&addr)
			if err != nil {
				return nil, errors.Trace(err)
			}

			// if intersection is not empty, we can say URLs from TiDB and PD are from the
			// same cluster. See comments above pdTiDBFromSameClusterCheckItem struct.
			ret = append(ret, addr)
		}
		if err = rows.Err(); err != nil {
			return nil, errors.Trace(err)
		}
		return ret, nil
	}
}
