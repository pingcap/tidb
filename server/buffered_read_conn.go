// Copyright 2017 PingCAP, Inc.
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

package server

// const defaultReaderSize = 16 * 1024

// // bufferedReadConn is a net.Conn compatible structure that reads from bufio.Reader.
// type bufferedReadConn struct {
// 	netpoll.Connection
// 	//rb *bufio.Reader
// }

// func (conn bufferedReadConn) Read(b []byte) (n int, err error) {
// 	r := conn.Reader
// 	return r.R
// }

// func newBufferedReadConn(conn netpoll.Connection) *bufferedReadConn {
// 	return &bufferedReadConn{
// 		Conn: conn,
// 		// rb:   bufio.NewReaderSize(conn, defaultReaderSize),
// 	}
// }
