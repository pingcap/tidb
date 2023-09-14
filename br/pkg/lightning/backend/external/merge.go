package external

import (
	"context"

	"github.com/pingcap/tidb/br/pkg/storage"
)

func MergeOverlappingFiles(
	ctx context.Context,
	paths []string,
	store storage.ExternalStorage,
	readBufferSize int,
	newFilePrefix string,
	writerID string,
	memSizeLimit uint64,
	writeBatchCount uint64,
	propSizeDist uint64,
	propKeysDist uint64,
	onClose OnCloseFunc,
) error {
	zeroOffsets := make([]uint64, len(paths))
	iter, err := NewMergeKVIter(ctx, paths, zeroOffsets, store, readBufferSize)
	if err != nil {
		return err
	}
	defer iter.Close()

	writer := NewWriterBuilder().
		SetMemorySizeLimit(memSizeLimit).
		SetWriterBatchCount(writeBatchCount).
		SetPropKeysDistance(propKeysDist).
		SetPropSizeDistance(propSizeDist).
		SetOnCloseFunc(onClose).
		Build(store, newFilePrefix, writerID)

	for iter.Next() {
		err = writer.WriteRow(ctx, iter.Key(), iter.Value(), nil)
		if err != nil {
			return err
		}
	}
	err = iter.Error()
	if err != nil {
		return err
	}
	return writer.Close(ctx)
}
