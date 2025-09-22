// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package importer

import (
	"context"
	goerrors "errors"

	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	verify "github.com/pingcap/tidb/pkg/lightning/verification"
)

const (
	sampleRowCount = 10
)

func sampleIndexRatio(
	ctx context.Context,
	parser mydump.Parser,
	encoder *TableKVEncoder,
	keyspace []byte,
	chunk *checkpoints.ChunkCheckpoint,
	groupChecksum *verify.KVGroupChecksum,
) *chunkEncoder {
	var count int
	sendFn := func(ctx context.Context, batch *encodedKVGroupBatch) error {
		count++
		if count >= sampleRowCount {
			return goerrors.New("stop iteration")
		}
		return nil
	}
	chunkEnc := &chunkEncoder{
		chunkName:     chunk.GetKey(),
		readFn:        parserEncodeReader(parser, chunk.Chunk.EndOffset, chunk.GetKey()),
		sendFn:        sendFn,
		encoder:       encoder,
		keyspace:      keyspace,
		groupChecksum: verify.NewKVGroupChecksumWithKeyspace(keyspace),
	}
	err := chunkEnc.encodeLoop(ctx)
}
