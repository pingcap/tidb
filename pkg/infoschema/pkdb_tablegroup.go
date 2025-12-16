// Copyright 2025 PingCAP, Inc.
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

package infoschema

import (
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/meta"
)

func applyCreateTableGroup(b *Builder, m meta.Reader, tgID int64) error {
	tgInfo, err := m.GetTableGroup(tgID)
	if err != nil {
		return errors.Trace(err)
	}
	if tgInfo == nil {
		return ErrTableGroupNotExists.GenWithStackByArgs(fmt.Sprintf("(TableGroup ID %d)", tgID))
	}
	b.infoSchema.setTableGroup(tgInfo)
	return nil
}

func applyDropTableGroup(b *Builder, tgID int64) {
	tgInfo, _ := b.infoSchema.TableGroupByID(tgID)
	if tgInfo == nil {
		return
	}
	b.infoSchema.deleteTableGroup(tgInfo.Name.L)
}

func applyReloadTableGroup(b *Builder, m meta.Reader, tgID int64) error {
	applyDropTableGroup(b, tgID)
	return applyCreateTableGroup(b, m, tgID)
}
