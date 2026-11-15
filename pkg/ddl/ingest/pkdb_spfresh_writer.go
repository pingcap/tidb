// Copyright 2022 PingCAP, Inc.
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

package ingest

import (
	"context"

	"github.com/pingcap/errors"
)

// SPFreshBuildRecordWriter writes SPFresh build records into a remote build engine.
type SPFreshBuildRecordWriter interface {
	WriteSPFreshBuildRecord(ctx context.Context, encodedHandle, vectorPayload, storedPayload []byte) error
}

// WriteSPFreshBuildRecord appends one encoded SPFresh build record.
func (wCtx *writerContext) WriteSPFreshBuildRecord(ctx context.Context, encodedHandle, vectorPayload, storedPayload []byte) error {
	if wCtx.spfreshLWriter == nil {
		return errors.Errorf("backend writer %T does not support SPFresh build records", wCtx.lWrite)
	}
	return wCtx.spfreshLWriter.AppendSPFreshBuildRecord(ctx, encodedHandle, vectorPayload, storedPayload)
}
