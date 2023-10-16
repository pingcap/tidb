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
	"strconv"

	"github.com/bazelbuild/buildtools/build"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"go.uber.org/zap"
)

const maxShardCount = 50

func main() {
	initCount()
	walk()
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
		if len(gotest) != 0 {
			if gotest[0].AttrString("timeout") == "" {
				gotest[0].SetAttr("timeout", &build.StringExpr{Value: "short"})
			}
			if !skipFlaky(path) && gotest[0].AttrLiteral("flaky") == "" {
				gotest[0].SetAttr("flaky", &build.LiteralExpr{Token: "True"})
			}
			if !skipShardCount(path) {
				abspath, err := filepath.Abs(path)
				if err != nil {
					return err
				}
				if cnt, ok := testMap[filepath.Dir(abspath)]; ok {
					if cnt > 2 {
						gotest[0].SetAttr("shard_count",
							&build.LiteralExpr{Token: strconv.FormatUint(uint64(mathutil.Min(cnt, maxShardCount)), 10)})
					} else {
						gotest[0].DelAttr("shard_count")
					}
				}
			}
		}
		write(path, buildfile)
		return nil
	})
	if err != nil {
		log.Fatal("fail to filepath.Walk", zap.Error(err))
	}
}
