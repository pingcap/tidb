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

package taskkey

import (
	"strconv"
	"strings"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/meta/model"
)

// BackfillKeyBuilder is used to build task key for the backfill job.
type BackfillKeyBuilder struct {
	multiSchemaSeq int32
	mergeTempIdx   bool
}

// NewBackfillKeyBuilder creates a new BackfillKeyBuilder.
func NewBackfillKeyBuilder() *BackfillKeyBuilder {
	return &BackfillKeyBuilder{multiSchemaSeq: -1}
}

// SetMergeTempIndex sets whether to merge the temporary index.
func (b *BackfillKeyBuilder) SetMergeTempIndex(flag bool) *BackfillKeyBuilder {
	b.mergeTempIdx = flag
	return b
}

// SetMultiSchema sets the multi-schema change information.
func (b *BackfillKeyBuilder) SetMultiSchema(info *model.MultiSchemaInfo) *BackfillKeyBuilder {
	if info != nil {
		b.multiSchemaSeq = info.Seq
	}
	return b
}

// Build builds the task key for the backfill job.
func (b *BackfillKeyBuilder) Build(jobID int64) string {
	labels := make([]string, 0, 8)
	if kerneltype.IsNextGen() {
		labels = append(labels, keyspace.GetKeyspaceNameBySettings())
	}
	labels = append(labels, "ddl", proto.Backfill.String(), strconv.FormatInt(jobID, 10))
	if b.multiSchemaSeq >= 0 {
		labels = append(labels, strconv.Itoa(int(b.multiSchemaSeq)))
	}
	if b.mergeTempIdx {
		labels = append(labels, "merge")
	}
	return strings.Join(labels, "/")
}

// ForBackfillJob returns the task key for a backfill job without a multi-schema sequence.
func ForBackfillJob(jobID int64, mergeTempIdx bool) string {
	return NewBackfillKeyBuilder().SetMergeTempIndex(mergeTempIdx).Build(jobID)
}
