// Copyright 2021 PingCAP, Inc.
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

package restore

import (
	"context"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"

	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/worker"
	"github.com/pingcap/tidb/br/pkg/storage"
)

var _ = Suite(&checkInfoSuite{})

type checkInfoSuite struct{}

func (s *checkInfoSuite) TestLocalResource(c *C) {
	dir := c.MkDir()
	mockStore, err := storage.NewLocalStorage(dir)
	c.Assert(err, IsNil)

	err = failpoint.Enable("github.com/pingcap/tidb/br/pkg/lightning/common/GetStorageSize", "return(2048)")
	c.Assert(err, IsNil)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/br/pkg/lightning/common/GetStorageSize")
	}()

	cfg := config.NewConfig()
	cfg.Mydumper.SourceDir = dir
	cfg.TikvImporter.SortedKVDir = dir
	cfg.TikvImporter.Backend = "local"
	rc := &Controller{
		cfg:       cfg,
		store:     mockStore,
		ioWorkers: worker.NewPool(context.Background(), 1, "io"),
	}

	// 1. source-size is smaller than disk-size, won't trigger error information
	rc.checkTemplate = NewSimpleTemplate()
	err = rc.localResource(1000)
	c.Assert(err, IsNil)
	tmpl := rc.checkTemplate.(*SimpleTemplate)
	c.Assert(tmpl.warnFailedCount, Equals, 1)
	c.Assert(tmpl.criticalFailedCount, Equals, 0)
	c.Assert(tmpl.normalMsgs[1], Matches, "local disk resources are rich, estimate sorted data size 1000B, local available is 2KiB")

	// 2. source-size is bigger than disk-size, with default disk-quota will trigger a critical error
	rc.checkTemplate = NewSimpleTemplate()
	err = rc.localResource(4096)
	c.Assert(err, IsNil)
	tmpl = rc.checkTemplate.(*SimpleTemplate)
	c.Assert(tmpl.warnFailedCount, Equals, 1)
	c.Assert(tmpl.criticalFailedCount, Equals, 1)
	c.Assert(tmpl.criticalMsgs[0], Matches, "local disk space may not enough to finish import, estimate sorted data size is 4KiB, but local available is 2KiB, please set `tikv-importer.disk-quota` to a smaller value than 2KiB or change `mydumper.sorted-kv-dir` to another disk with enough space to finish imports")

	// 3. source-size is bigger than disk-size, with a vaild disk-quota will trigger a warning
	rc.checkTemplate = NewSimpleTemplate()
	rc.cfg.TikvImporter.DiskQuota = config.ByteSize(1024)
	err = rc.localResource(4096)
	c.Assert(err, IsNil)
	tmpl = rc.checkTemplate.(*SimpleTemplate)
	c.Assert(tmpl.warnFailedCount, Equals, 1)
	c.Assert(tmpl.criticalFailedCount, Equals, 0)
	c.Assert(tmpl.normalMsgs[1], Matches, "local disk space may not enough to finish import, estimate sorted data size is 4KiB, but local available is 2KiB,we will use disk-quota \\(size: 1KiB\\) to finish imports, which may slow down import")
}
