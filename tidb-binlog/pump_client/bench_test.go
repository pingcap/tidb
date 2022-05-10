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

package client

import (
	"net"
	"sync"
	"testing"
	"time"

	pb "github.com/pingcap/tipb/go-binlog"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func Benchmark100Thread(b *testing.B) {
	benchmarkTest(b, 100)
}

func Benchmark200Thread(b *testing.B) {
	benchmarkTest(b, 200)
}

func Benchmark300Thread(b *testing.B) {
	benchmarkTest(b, 300)
}

func Benchmark400Thread(b *testing.B) {
	benchmarkTest(b, 400)
}

func benchmarkTest(b *testing.B, threadCount int) {
	pumpClient, pumpServer := createMockPumpsClientAndServer(b)
	writeFakeBinlog(b, pumpClient, threadCount, b.N)
	pumpServer.Close()
}

func writeFakeBinlog(b *testing.B, pumpClient *PumpsClient, threadCount int, num int) {
	var wg sync.WaitGroup
	wg.Add(threadCount)
	for i := 0; i < threadCount; i++ {
		go func(baseTS int) {
			for j := 0; j < num/threadCount; j++ {
				pBinlog := &pb.Binlog{
					Tp:      pb.BinlogType_Prewrite,
					StartTs: int64(baseTS + j),
				}
				err := pumpClient.WriteBinlog(pBinlog)
				require.NoError(b, err)

				cBinlog := &pb.Binlog{
					Tp:       pb.BinlogType_Commit,
					StartTs:  int64(baseTS + j),
					CommitTs: int64(baseTS + j),
				}
				err = pumpClient.WriteBinlog(cBinlog)
				require.NoError(b, err)
			}
			wg.Done()
		}(10000000 * i)
	}

	wg.Wait()
}

func createMockPumpsClientAndServer(b *testing.B) (*PumpsClient, *mockPumpServer) {
	addr := "127.0.0.1:15049"
	pumpServer, err := createMockPumpServer(addr, "tcp", false)
	if err != nil {
		b.Fatal(err)
	}

	opt := grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
		return net.DialTimeout("tcp", addr, timeout)
	})

	clientCon, err := grpc.Dial(addr, opt, grpc.WithInsecure())
	if err != nil {
		b.Fatal(err)
	}
	if clientCon == nil {
		b.Fatal("grpc client is nil")
	}
	pumpClient := mockPumpsClient(pb.NewPumpClient(clientCon), false)

	return pumpClient, pumpServer
}
