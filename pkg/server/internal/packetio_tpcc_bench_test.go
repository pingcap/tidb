// Copyright 2026 PingCAP, Inc.
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

package internal

import (
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/server/internal/util"
	"github.com/stretchr/testify/require"
)

var packetIOReadTPCCSink []byte

func BenchmarkPacketIOReadOnePacketTPCC(b *testing.B) {
	for _, size := range []int{1, 32, 128, 1024} {
		for _, timeout := range []time.Duration{0, 30 * time.Second} {
			name := fmt.Sprintf("payload=%d/timeout=%t", size, timeout > 0)
			b.Run(name, func(b *testing.B) {
				packet, payload := makeBenchmarkPacket(size)
				conn := &replayPacketConn{packet: packet}
				packetIO := NewPacketIO(util.NewBufferedReadConn(conn))
				packetIO.SetReadTimeout(timeout)

				b.ReportAllocs()
				b.SetBytes(int64(len(packet)))
				b.ResetTimer()
				for b.Loop() {
					conn.reset()
					packetIO.sequence = 0
					packetIO.accumulatedLength = 0
					data, err := packetIO.readOnePacket()
					if err != nil {
						b.Fatal(err)
					}
					packetIOReadTPCCSink = data
				}
				b.StopTimer()

				require.Equal(b, payload, packetIOReadTPCCSink)
				if timeout > 0 {
					require.Equal(b, 2, conn.readDeadlineCalls)
				} else {
					require.Zero(b, conn.readDeadlineCalls)
				}
				require.Zero(b, conn.closeCalls)
			})
		}
	}
}

func TestPacketIOReadOnePacketDeadlineAndOwnership(t *testing.T) {
	for _, timeout := range []time.Duration{0, 30 * time.Second} {
		packet, payload := makeBenchmarkPacket(128)
		conn := &replayPacketConn{packet: packet}
		packetIO := NewPacketIO(util.NewBufferedReadConn(conn))
		packetIO.SetReadTimeout(timeout)

		data, err := packetIO.readOnePacket()
		require.NoError(t, err)
		require.Equal(t, payload, data)
		if timeout > 0 {
			require.Equal(t, 2, conn.readDeadlineCalls)
		} else {
			require.Zero(t, conn.readDeadlineCalls)
		}
		require.Zero(t, conn.closeCalls)

		conn.packet[4] ^= 0xff
		require.Equal(t, payload, data)
	}
}

func makeBenchmarkPacket(payloadSize int) (packet, payload []byte) {
	packet = make([]byte, payloadSize+4)
	packet[0] = byte(payloadSize)
	packet[1] = byte(payloadSize >> 8)
	packet[2] = byte(payloadSize >> 16)
	packet[3] = 0
	for i := range payloadSize {
		packet[i+4] = byte(i*31 + 7)
	}
	payload = append([]byte(nil), packet[4:]...)
	return packet, payload
}

type replayPacketConn struct {
	packet            []byte
	offset            int
	readDeadlineCalls int
	closeCalls        int
}

func (c *replayPacketConn) reset() {
	c.offset = 0
	c.readDeadlineCalls = 0
}

func (c *replayPacketConn) Read(dst []byte) (int, error) {
	if c.offset == len(c.packet) {
		return 0, io.EOF
	}
	n := copy(dst, c.packet[c.offset:])
	c.offset += n
	return n, nil
}

func (*replayPacketConn) Write(src []byte) (int, error) {
	return len(src), nil
}

func (c *replayPacketConn) Close() error {
	c.closeCalls++
	return nil
}

func (*replayPacketConn) LocalAddr() net.Addr {
	return nil
}

func (*replayPacketConn) RemoteAddr() net.Addr {
	return nil
}

func (*replayPacketConn) SetDeadline(time.Time) error {
	return nil
}

func (c *replayPacketConn) SetReadDeadline(time.Time) error {
	c.readDeadlineCalls++
	return nil
}

func (*replayPacketConn) SetWriteDeadline(time.Time) error {
	return nil
}
