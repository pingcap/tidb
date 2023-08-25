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

package main

import (
	_ "embed"
	"os"
	"text/template"
)

//go:embed unicode_ci.go.tpl
var unicodeCIImpl string

type data struct {
	Name     string
	ImplName string
}

func generateFile(filename string, d data) {
	tpl, err := template.New("unicode_ci_impl").Parse(unicodeCIImpl)
	if err != nil {
		panic(err)
	}

	file, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	err = tpl.Execute(file, d)
	if err != nil {
		panic(err)
	}
}

func main() {
	switch os.Args[len(os.Args)-1] {
	case "unicode_0400_ci_generated.go":
		generateFile("unicode_0400_ci_generated.go", data{
			Name:     "unicodeCICollator",
			ImplName: "unicode0400Impl",
		})
	case "unicode_0900_ai_ci_generated.go":
		generateFile("unicode_0900_ai_ci_generated.go", data{
			Name:     "unicode0900AICICollator",
			ImplName: "unicode0900Impl",
		})
	default:
		panic("unreachable")
	}
}
