// Copyright 2023 PingCAP, Inc.
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

package parse

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/server/internal/handshake"
	util2 "github.com/pingcap/tidb/pkg/server/internal/util"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// maxFetchSize constants
const (
	maxFetchSize = 1024
)

// StmtFetchCmd parse COM_STMT_FETCH command
func StmtFetchCmd(data []byte) (stmtID uint32, fetchSize uint32, err error) {
	if len(data) != 8 {
		return 0, 0, mysql.ErrMalformPacket
	}
	// Please refer to https://dev.mysql.com/doc/internals/en/com-stmt-fetch.html
	stmtID = binary.LittleEndian.Uint32(data[0:4])
	fetchSize = min(binary.LittleEndian.Uint32(data[4:8]), maxFetchSize)
	return
}

// HandshakeResponseHeader parses the common header of SSLRequest and Response41.
func HandshakeResponseHeader(ctx context.Context, packet *handshake.Response41, data []byte) (parsedBytes int, err error) {
	// Ensure there are enough data to read:
	// http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::SSLRequest
	if len(data) < 4+4+1+23 {
		logutil.Logger(ctx).Warn("got malformed handshake response", zap.ByteString("packetData", data))
		return 0, mysql.ErrMalformPacket
	}

	offset := 0
	// capability
	capability := binary.LittleEndian.Uint32(data[:4])
	packet.Capability = capability
	offset += 4
	// skip max packet size
	offset += 4
	// charset, skip, if you want to use another charset, use set names
	packet.Collation = data[offset]
	offset++
	// skip reserved 23[00]
	offset += 23

	return offset, nil
}

// HandshakeResponseBody parse the HandshakeResponse (except the common header part).
func HandshakeResponseBody(ctx context.Context, packet *handshake.Response41, data []byte, offset int) (err error) {
	defer func() {
		// Check malformat packet cause out of range is disgusting, but don't panic!
		if r := recover(); r != nil {
			logutil.Logger(ctx).Error("handshake panic", zap.ByteString("packetData", data))
			err = mysql.ErrMalformPacket
		}
	}()
	// user name
	packet.User = string(data[offset : offset+bytes.IndexByte(data[offset:], 0)])
	offset += len(packet.User) + 1

	if packet.Capability&mysql.ClientPluginAuthLenencClientData > 0 {
		// MySQL client sets the wrong capability, it will set this bit even server doesn't
		// support ClientPluginAuthLenencClientData.
		// https://github.com/mysql/mysql-server/blob/5.7/sql-common/client.c#L3478
		if data[offset] == 0x1 { // No auth data
			offset += 2
		} else {
			num, null, off := util2.ParseLengthEncodedInt(data[offset:])
			offset += off
			if !null {
				packet.Auth = data[offset : offset+int(num)]
				offset += int(num)
			}
		}
	} else if packet.Capability&mysql.ClientSecureConnection > 0 {
		// auth length and auth
		authLen := int(data[offset])
		offset++
		packet.Auth = data[offset : offset+authLen]
		offset += authLen
	} else {
		packet.Auth = data[offset : offset+bytes.IndexByte(data[offset:], 0)]
		offset += len(packet.Auth) + 1
	}

	if packet.Capability&mysql.ClientConnectWithDB > 0 {
		if len(data[offset:]) > 0 {
			idx := bytes.IndexByte(data[offset:], 0)
			packet.DBName = string(data[offset : offset+idx])
			offset += idx + 1
		}
	}

	if packet.Capability&mysql.ClientPluginAuth > 0 {
		idx := bytes.IndexByte(data[offset:], 0)
		s := offset
		f := offset + idx
		if s < f { // handle unexpected bad packets
			packet.AuthPlugin = string(data[s:f])
		}
		offset += idx + 1
	}

	if packet.Capability&mysql.ClientConnectAtts > 0 {
		if len(data[offset:]) == 0 {
			// Defend some ill-formated packet, connection attribute is not important and can be ignored.
			return nil
		}
		if num, null, intOff := util2.ParseLengthEncodedInt(data[offset:]); !null {
			if num > 1<<20 { // 1 MiB hard limit
				return errors.New("connection refused: session connection attributes exceed the 1 MiB hard limit")
			}
			offset += intOff // Length of variable length encoded integer itself in bytes
			row := data[offset : offset+int(num)]
			attrs, warning, err := parseAttrs(row)
			if err != nil {
				logutil.Logger(ctx).Warn("parse attrs failed", zap.Error(err))
				return nil
			}
			if warning != "" {
				logutil.Logger(ctx).Warn(warning)
			}
			packet.Attrs = attrs
			offset += int(num) // Length of attributes
		}
	}

	if packet.Capability&mysql.ClientZstdCompressionAlgorithm > 0 {
		packet.ZstdLevel = int(data[offset])
	}

	return nil
}

func parseAttrs(data []byte) (map[string]string, string, error) {
	attrs := make(map[string]string)
	pos := 0
	var totalSize int64
	var acceptedSize int64
	truncated := false
	limit := vardef.ConnectAttrsSize.Load()

	for pos < len(data) {
		key, _, off, err := util2.ParseLengthEncodedBytes(data[pos:])
		if err != nil {
			return attrs, "", err
		}
		pos += off
		value, _, off, err := util2.ParseLengthEncodedBytes(data[pos:])
		if err != nil {
			return attrs, "", err
		}
		pos += off

		kvSize := int64(len(key)) + int64(len(value))
		totalSize += kvSize

		if limit >= 0 && totalSize > limit {
			if !truncated {
				truncated = true
				vardef.ConnectAttrsLost.Add(1)
			}
			continue
		}

		if !truncated {
			attrs[string(key)] = string(value)
			acceptedSize += kvSize
		}
	}

	// Update LongestSeen only for normal-sized payloads (< 64 KiB).
	// Abnormally large payloads are still accepted (up to 1 MiB) but should
	// not skew this monitoring metric.
	if totalSize < 65536 {
		for {
			old := vardef.ConnectAttrsLongestSeen.Load()
			if totalSize <= old {
				break
			}
			if vardef.ConnectAttrsLongestSeen.CompareAndSwap(old, totalSize) {
				break
			}
		}
	}

	var warning string
	if truncated {
		truncatedBytes := totalSize - acceptedSize
		attrs["_truncated"] = strconv.FormatInt(truncatedBytes, 10)
		warning = fmt.Sprintf(
			"session connection attributes truncated: total size %d bytes exceeds "+
				"performance_schema_session_connect_attrs_size (%d), %d bytes were discarded",
			totalSize, limit, truncatedBytes)
	}
	return attrs, warning, nil
}
