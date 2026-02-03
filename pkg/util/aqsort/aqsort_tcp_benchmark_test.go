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

package aqsort

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"net"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"
)

// BenchmarkTCPByteSorters measures an end-to-end pipeline over TCP:
// client encodes and sends keys -> server decodes -> sorts -> returns a small checksum.
//
// It is designed to stress byte-orderable sort keys and highlight cases with long shared prefixes.
func BenchmarkTCPByteSorters(b *testing.B) {
	type dataset struct {
		name    string
		keyLen  int
		gen     func(rng *rand.Rand, n int) [][]byte
		wantAQS bool // expected AQS advantage (prefix-heavy)
	}

	datasets := []dataset{
		{
			name:   "random_len40",
			keyLen: 40,
			gen: func(rng *rand.Rand, n int) [][]byte {
				keys := make([][]byte, n)
				for i := 0; i < n; i++ {
					k := make([]byte, 40)
					_, _ = rng.Read(k)
					keys[i] = k
				}
				return keys
			},
		},
		{
			name:    "common_prefix32_suffix8",
			keyLen:  40,
			wantAQS: true,
			gen: func(rng *rand.Rand, n int) [][]byte {
				prefix := make([]byte, 32)
				_, _ = rng.Read(prefix)
				keys := make([][]byte, n)
				for i := 0; i < n; i++ {
					k := make([]byte, 40)
					copy(k, prefix)
					_, _ = rng.Read(k[32:])
					keys[i] = k
				}
				return keys
			},
		},
	}

	sizes := []int{10_000, 100_000}

	sorters := []struct {
		name string
		sort func(keys [][]byte)
	}{
		{
			name: "AQSReuse",
			sort: func() func(keys [][]byte) {
				s := &Sorter{}
				return s.SortBytes
			}(),
		},
		{
			name: "SlicesSortFunc",
			sort: func(keys [][]byte) {
				slices.SortFunc(keys, bytes.Compare)
			},
		},
		{
			name: "StdSortSlice",
			sort: func(keys [][]byte) {
				sort.Slice(keys, func(i, j int) bool {
					return bytes.Compare(keys[i], keys[j]) < 0
				})
			},
		},
	}

	for _, ds := range datasets {
		for _, n := range sizes {
			seed := int64(1)
			keys := ds.gen(rand.New(rand.NewSource(seed)), n)
			req := encodeFixedLenKeysRequest(keys, ds.keyLen)

			for _, sorter := range sorters {
				b.Run(fmt.Sprintf("%s/%s/n=%d", sorter.name, ds.name, n), func(b *testing.B) {
					b.ReportAllocs()

					addr, stop, err := startSortTCPServer(sorter.sort)
					if err != nil {
						b.Fatalf("start server: %v", err)
					}
					b.Cleanup(stop)

					conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
					if err != nil {
						b.Fatalf("dial: %v", err)
					}
					b.Cleanup(func() { _ = conn.Close() })

					resp := make([]byte, 8)

					// Warm up once to allocate/reuse server-side buffers before timing.
					if err := writeAll(conn, req); err != nil {
						b.Fatalf("warmup write: %v", err)
					}
					if _, err := io.ReadFull(conn, resp); err != nil {
						b.Fatalf("warmup read: %v", err)
					}

					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if err := writeAll(conn, req); err != nil {
							b.Fatalf("write: %v", err)
						}
						if _, err := io.ReadFull(conn, resp); err != nil {
							b.Fatalf("read: %v", err)
						}
					}
					b.StopTimer()

					if ds.wantAQS && sorter.name == "AQSReuse" && len(resp) != 8 {
						// Keep resp live to avoid overly-aggressive elimination in future refactors.
						b.Fatalf("unexpected response size: %d", len(resp))
					}
				})
			}
		}
	}
}

func encodeFixedLenKeysRequest(keys [][]byte, keyLen int) []byte {
	// Request format:
	//   uint32 n
	//   uint16 keyLen
	//   n * keyLen bytes (keys concatenated)
	if keyLen < 0 || keyLen > 65535 {
		panic("invalid keyLen")
	}
	n := len(keys)
	total := 4 + 2 + n*keyLen
	buf := make([]byte, 0, total)

	var hdr [6]byte
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(n))
	binary.LittleEndian.PutUint16(hdr[4:6], uint16(keyLen))
	buf = append(buf, hdr[:]...)

	for _, k := range keys {
		if len(k) != keyLen {
			panic("key length mismatch")
		}
		buf = append(buf, k...)
	}
	return buf
}

func startSortTCPServer(sorter func(keys [][]byte)) (addr string, stop func(), err error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", nil, err
	}

	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				select {
				case <-stopCh:
					return
				default:
					return
				}
			}
			wg.Add(1)
			go func(c net.Conn) {
				defer wg.Done()
				defer func() { _ = c.Close() }()
				handleSortConn(c, sorter)
			}(conn)
		}
	}()

	stop = func() {
		close(stopCh)
		_ = ln.Close()
		wg.Wait()
	}

	return ln.Addr().String(), stop, nil
}

func handleSortConn(conn net.Conn, sorter func(keys [][]byte)) {
	var (
		keys     [][]byte
		keyBytes []byte
	)

	header := make([]byte, 6)
	resp := make([]byte, 8)

	for {
		if _, err := io.ReadFull(conn, header); err != nil {
			return
		}
		n := int(binary.LittleEndian.Uint32(header[0:4]))
		keyLen := int(binary.LittleEndian.Uint16(header[4:6]))
		if n <= 0 || keyLen <= 0 {
			return
		}

		total := n * keyLen
		if cap(keyBytes) < total {
			keyBytes = make([]byte, total)
		} else {
			keyBytes = keyBytes[:total]
		}
		if _, err := io.ReadFull(conn, keyBytes); err != nil {
			return
		}

		if cap(keys) < n {
			keys = make([][]byte, n)
		} else {
			keys = keys[:n]
		}
		for i := 0; i < n; i++ {
			off := i * keyLen
			keys[i] = keyBytes[off : off+keyLen]
		}

		sorter(keys)

		// Minimal, deterministic use of the sorted result to prevent elision:
		// checksum = first8 ^ last8 (or shorter if keyLen < 8).
		var csum uint64
		if keyLen >= 8 {
			csum = binary.LittleEndian.Uint64(keys[0][:8]) ^ binary.LittleEndian.Uint64(keys[len(keys)-1][:8])
		} else {
			var first, last uint64
			for i := 0; i < keyLen; i++ {
				shift := uint(i * 8)
				first |= uint64(keys[0][i]) << shift
				last |= uint64(keys[len(keys)-1][i]) << shift
			}
			csum = first ^ last
		}
		binary.LittleEndian.PutUint64(resp, csum)
		if err := writeAll(conn, resp); err != nil {
			return
		}
	}
}

func writeAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}
