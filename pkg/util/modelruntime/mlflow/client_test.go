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

package mlflow

import (
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClientPredictBatch(t *testing.T) {
	socketPath := filepath.Join(t.TempDir(), "mlflow.sock")
	ln, err := net.Listen("unix", socketPath)
	require.NoError(t, err)
	defer ln.Close()

	go func() {
		conn, _ := ln.Accept()
		defer conn.Close()
		_ = writeResponse(conn, [][]float32{{0.5}, {1.0}})
	}()

	client := NewClient(ClientOptions{Dial: func(context.Context) (net.Conn, error) {
		return net.Dial("unix", ln.Addr().String())
	}})
	out, err := client.PredictBatch(context.Background(), PredictRequest{Inputs: [][]float32{{0.1}, {0.2}}})
	require.NoError(t, err)
	require.Equal(t, [][]float32{{0.5}, {1.0}}, out)
}

func TestReadResponseRejectsOversizedPayload(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, binary.Write(&buf, binary.BigEndian, uint32(maxResponseFrameSize+1)))
	_, err := readResponse(&buf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "payload too large")
}
