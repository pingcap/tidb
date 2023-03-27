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
	"errors"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/bazelbuild/buildtools/build"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

func main() {
	if _, err := os.Stat("WORKSPACE"); errors.Is(err, os.ErrNotExist) {
		log.Fatal("It should run from the project root")
	}
	err := filepath.Walk(".", func(path string, d fs.FileInfo, _ error) error {
		if d.IsDir() {
			return nil
		}
		if d.Name() != "BUILD.bazel" {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			log.Fatal("error", zap.Error(err))
		}
		buildfile, err := build.ParseBuild(d.Name(), data)
		if err != nil {
			log.Fatal("error", zap.Error(err))
		}
		gotest := buildfile.Rules("go_test")
		if len(gotest) != 0 {
			if gotest[0].AttrString("timeout") == "" {
				gotest[0].SetAttr("timeout", &build.StringExpr{Value: "short"})
			}
			if !SkipFlaky(path) && gotest[0].AttrLiteral("flaky") == "" {
				gotest[0].SetAttr("flaky", &build.LiteralExpr{Token: "True"})
			}
		}
		write(path, buildfile)
		return nil
	})
	if err != nil {
		log.Fatal("error")
	}

}
