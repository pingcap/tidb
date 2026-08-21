// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package export

import (
	"context"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/scheduler"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/planner/extstore"
)

var _ scheduler.Cleaner = (*exportCleaner)(nil)

type exportCleaner struct{}

func newExportCleaner() scheduler.Cleaner {
	return &exportCleaner{}
}

// Clean removes the task's plan metadata from global external storage.
func (*exportCleaner) Clean(ctx context.Context, task *proto.Task) error {
	store, err := extstore.GetGlobalExtStorage(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	files := make([]string, 0, 16)
	walkOpt := &storeapi.WalkOption{SubDir: strconv.FormatInt(task.ID, 10) + "/"}
	err = store.WalkDir(ctx, walkOpt, func(path string, _ int64) error {
		files = append(files, path)
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(store.DeleteFiles(ctx, files))
}

func init() {
	scheduler.RegisterCleanerFactory(proto.Export, newExportCleaner)
}
