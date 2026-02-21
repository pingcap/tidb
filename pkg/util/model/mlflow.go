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

package model

import (
	"github.com/pingcap/errors"
	"gopkg.in/yaml.v3"
)

// MLflowModelInfo captures parsed MLflow PyFunc metadata.
type MLflowModelInfo struct {
	HasPyFunc   bool
	InputNames  []string
	OutputNames []string
}

type mlflowDoc struct {
	Flavors   map[string]any `yaml:"flavors"`
	Signature struct {
		Inputs  []mlflowField `yaml:"inputs"`
		Outputs []mlflowField `yaml:"outputs"`
	} `yaml:"signature"`
}

type mlflowField struct {
	Name string `yaml:"name"`
	Type string `yaml:"type"`
}

// ParseMLflowModel parses an MLflow MLmodel file and returns PyFunc metadata.
func ParseMLflowModel(data []byte) (MLflowModelInfo, error) {
	var doc mlflowDoc
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return MLflowModelInfo{}, errors.Annotate(err, "parse MLmodel")
	}
	info := MLflowModelInfo{
		HasPyFunc: doc.Flavors != nil && doc.Flavors["python_function"] != nil,
	}
	for _, in := range doc.Signature.Inputs {
		info.InputNames = append(info.InputNames, in.Name)
	}
	for _, out := range doc.Signature.Outputs {
		info.OutputNames = append(info.OutputNames, out.Name)
	}
	if len(info.InputNames) == 0 || len(info.OutputNames) == 0 {
		return MLflowModelInfo{}, errors.New("mlflow signature is required")
	}
	return info, nil
}
