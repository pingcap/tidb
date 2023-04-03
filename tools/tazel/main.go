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
	"golang.org/x/tools/go/analysis/singlechecker"
)

func main() {
	singlechecker.Main(Analyzer)
	if _, err := os.Stat("WORKSPACE"); errors.Is(err, os.ErrNotExist) {
		log.Fatal("It should run from the project root")
	}
	err := filepath.Walk(".", func(path string, d fs.FileInfo, _ error) error {
		if d.IsDir() || d.Name() != "BUILD.bazel" || skipTazel(path) {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			log.Fatal("fail to read file", zap.Error(err), zap.String("path", path))
		}
		buildfile, err := build.ParseBuild(d.Name(), data)
		if err != nil {
			log.Fatal("fail to parser BUILD.bazel", zap.Error(err), zap.String("path", path))
		}
		gotest := buildfile.Rules("go_test")
		toWrite := false
		if len(gotest) != 0 {
			if gotest[0].AttrString("timeout") == "" {
				gotest[0].SetAttr("timeout", &build.StringExpr{Value: "short"})
				toWrite = true
			}
			if !skipFlaky(path) && gotest[0].AttrLiteral("flaky") == "" {
				gotest[0].SetAttr("flaky", &build.LiteralExpr{Token: "True"})
				toWrite = true
			}
		}
		if toWrite {
			log.Info("write file", zap.String("path", path))
			write(path, buildfile)
		}
		return nil
	})
	if err != nil {
		log.Fatal("fail to filepath.Walk", zap.Error(err))
	}
}
