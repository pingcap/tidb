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
	"github.com/pingcap/errors"
	"gopkg.in/yaml.v3"
)

// ModelInfo captures parsed MLflow PyFunc metadata.
type ModelInfo struct {
	HasPyFunc   bool
	InputNames  []string
	OutputNames []string
}

type modelDoc struct {
	Flavors   map[string]any `yaml:"flavors"`
	Signature struct {
		Inputs  []modelField `yaml:"inputs"`
		Outputs []modelField `yaml:"outputs"`
	} `yaml:"signature"`
}

type modelField struct {
	Name string `yaml:"name"`
	Type string `yaml:"type"`
}

// ParseModel parses an MLflow MLmodel file and returns PyFunc metadata.
func ParseModel(data []byte) (ModelInfo, error) {
	var doc modelDoc
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return ModelInfo{}, errors.Annotate(err, "parse MLmodel")
	}
	info := ModelInfo{
		HasPyFunc: doc.Flavors != nil && doc.Flavors["python_function"] != nil,
	}
	for _, in := range doc.Signature.Inputs {
		info.InputNames = append(info.InputNames, in.Name)
	}
	for _, out := range doc.Signature.Outputs {
		info.OutputNames = append(info.OutputNames, out.Name)
	}
	if len(info.InputNames) == 0 || len(info.OutputNames) == 0 {
		return ModelInfo{}, errors.New("mlflow signature is required")
	}
	return info, nil
}
