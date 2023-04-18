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

package loaddata

import (
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
)

func toChunkCheckpoint(chunk Chunk) checkpoints.ChunkCheckpoint {
	return checkpoints.ChunkCheckpoint{
		FileMeta: mydump.SourceFileMeta{
			Path:        chunk.Path,
			Type:        chunk.Type,
			Compression: chunk.Compression,
			FileSize:    chunk.EndOffset,
		},
		Chunk: mydump.Chunk{
			PrevRowIDMax: chunk.PrevRowIDMax,
			Offset:       chunk.Offset,
		},
		Timestamp: chunk.Timestamp,
	}
}

func toChunk(chunkCheckpoint checkpoints.ChunkCheckpoint) Chunk {
	return Chunk{
		Path:         chunkCheckpoint.FileMeta.Path,
		Offset:       chunkCheckpoint.Chunk.Offset,
		EndOffset:    chunkCheckpoint.Chunk.EndOffset,
		PrevRowIDMax: chunkCheckpoint.Chunk.PrevRowIDMax,
		Type:         chunkCheckpoint.FileMeta.Type,
		Compression:  chunkCheckpoint.FileMeta.Compression,
		Timestamp:    chunkCheckpoint.Timestamp,
	}
}
