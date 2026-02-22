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
	"encoding/binary"
	"encoding/json"
	"io"
)

// PredictRequest is the request payload sent to the sidecar.
type PredictRequest struct {
	ModelPath string      `json:"model_path"`
	Inputs    [][]float32 `json:"inputs"`
}

type predictResponse struct {
	Outputs [][]float32 `json:"outputs"`
}

func writeRequest(w io.Writer, req PredictRequest) error {
	payload, err := json.Marshal(req)
	if err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, uint32(len(payload))); err != nil {
		return err
	}
	_, err = w.Write(payload)
	return err
}

func readResponse(r io.Reader) ([][]float32, error) {
	var size uint32
	if err := binary.Read(r, binary.BigEndian, &size); err != nil {
		return nil, err
	}
	buf := make([]byte, size)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	var resp predictResponse
	if err := json.Unmarshal(buf, &resp); err != nil {
		return nil, err
	}
	return resp.Outputs, nil
}

func writeResponse(w io.Writer, outputs [][]float32) error {
	payload, err := json.Marshal(predictResponse{Outputs: outputs})
	if err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, uint32(len(payload))); err != nil {
		return err
	}
	_, err = w.Write(payload)
	return err
}
