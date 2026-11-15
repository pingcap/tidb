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
// See the License for the specific language governing permissions and
// limitations under the License.

package pkdbspfreshbulk

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"time"
	"unicode/utf8"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/metrics"
)

// WriteImmutableObject writes data at an exact key and fails if the key already exists.
// It is a demo fail-fast check, not a concurrent compare-and-swap protocol.
func WriteImmutableObject(ctx context.Context, store storage.ExternalStorage, key string, data []byte) error {
	exists, err := store.FileExists(ctx, key)
	if err != nil {
		return fmt.Errorf("check object %q: %w", key, err)
	}
	if exists {
		return fmt.Errorf("object %q already exists", key)
	}
	if err := store.WriteFile(ctx, key, data); err != nil {
		return fmt.Errorf("write object %q: %w", key, err)
	}
	return nil
}

// ReadObject reads an immutable object into one exactly sized buffer. A length
// change fails instead of accepting truncated bytes or growing an unbounded
// buffer. Artifact decoding and the caller's pinned SHA still authenticate it.
func ReadObject(ctx context.Context, store storage.ExternalStorage, key string) ([]byte, error) {
	reader, err := OpenObject(ctx, store, key)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return reader.ReadAll(ctx)
}

// ObjectReader exposes a validated length before allocating the body, allowing
// callers to reserve memory through decode/consumption rather than just IO.
type ObjectReader struct {
	reader      storage.ExternalFileReader
	key         string
	size        int64
	openSeconds float64
	prefix      []byte
}

// OpenObject opens one current immutable object without reading its body.
func OpenObject(ctx context.Context, store storage.ExternalStorage, key string) (*ObjectReader, error) {
	started := time.Now()
	reader, err := store.Open(ctx, key, nil)
	if err != nil {
		return nil, fmt.Errorf("open object %q: %w", key, err)
	}
	size, err := reader.GetFileSize()
	if err != nil {
		reader.Close()
		return nil, fmt.Errorf("size object %q: %w", key, err)
	}
	if size < 0 || uint64(size) > uint64(^uint(0)>>1) {
		reader.Close()
		return nil, fmt.Errorf("object %q length %d does not fit memory bounds", key, size)
	}
	openSeconds := time.Since(started).Seconds()
	metrics.SPFreshBulkObjectReadDuration.WithLabelValues("open").Observe(openSeconds)
	return &ObjectReader{reader: reader, key: key, size: size, openSeconds: openSeconds}, nil
}

// Size is the current opened object's exact nonnegative length.
func (r *ObjectReader) Size() int64 { return r.size }

// Close releases the storage reader on success, cancellation or failure.
func (r *ObjectReader) Close() error { return r.reader.Close() }

// PrefetchSortedRunRecordCount reads at most the bounded envelope/header from
// this same stream. The count is untrusted and used only for memory admission;
// ReadPinned must authenticate all bytes before decoding or importing records.
// No payload allocation or second object GET is performed here.
func (r *ObjectReader) PrefetchSortedRunRecordCount(ctx context.Context) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if r.prefix != nil {
		return 0, fmt.Errorf("SortedRun header already prefetched")
	}
	started := time.Now()
	fixed := make([]byte, envelopeFixedLen)
	if _, err := io.ReadFull(r.reader, fixed); err != nil {
		return 0, fmt.Errorf("read SortedRun envelope: %w", err)
	}
	kind, headerLen, payloadLen, err := decodeEnvelopeLengths(fixed, uint64(r.size))
	if err != nil {
		return 0, err
	}
	if kind != KindSortedRun {
		return 0, fmt.Errorf("expected SortedRun kind, got %d", kind)
	}
	r.prefix = make([]byte, envelopeFixedLen+int(headerLen))
	copy(r.prefix, fixed)
	headerBytes := r.prefix[envelopeFixedLen:]
	if _, err := io.ReadFull(r.reader, headerBytes); err != nil {
		return 0, fmt.Errorf("read SortedRun header: %w", err)
	}
	if !utf8.Valid(headerBytes) {
		return 0, fmt.Errorf("artifact header is not valid UTF-8")
	}
	if err := requireHeaderFields(headerBytes, sortedRunHeaderFields); err != nil {
		return 0, err
	}
	var header SortedRunHeader
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		return 0, fmt.Errorf("decode SortedRun header: %w", err)
	}
	if header.RecordCount == 0 || header.RecordCount > payloadLen/12 {
		return 0, fmt.Errorf("SortedRun record count %d cannot fit payload of %d bytes", header.RecordCount, payloadLen)
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	elapsed := time.Since(started).Seconds()
	r.openSeconds += elapsed
	metrics.SPFreshBulkObjectReadDuration.WithLabelValues("header_prefetch").Observe(elapsed)
	return header.RecordCount, nil
}

// ReadAll consumes the opened object exactly once. Reservation waits between
// OpenObject and ReadAll are excluded from the IO total and measured by callers.
func (r *ObjectReader) ReadAll(ctx context.Context) ([]byte, error) {
	return r.readAll(ctx, nil)
}

const objectHashBlockBytes = 8 << 20

// ReadPinned overlaps hashing completed blocks with reading the next blocks.
// All blocks are immutable disjoint views into the one admitted body buffer.
// Authentication and the hash worker finish before any bytes reach a decoder.
func (r *ObjectReader) ReadPinned(ctx context.Context, reference ManifestRef) ([]byte, error) {
	if r.size <= objectHashBlockBytes {
		data, err := r.ReadAll(ctx)
		if err != nil {
			return nil, err
		}
		if err := reference.VerifyBytes(data); err != nil {
			return nil, err
		}
		return data, nil
	}
	blocks := make(chan []byte, 1)
	result := make(chan [sha256.Size]byte, 1)
	go func() {
		digest := sha256.New()
		var elapsed time.Duration
		for block := range blocks {
			started := time.Now()
			_, _ = digest.Write(block)
			elapsed += time.Since(started)
		}
		metrics.SPFreshBulkObjectReadDuration.WithLabelValues("pinned_sha_work").Observe(elapsed.Seconds())
		var sum [sha256.Size]byte
		copy(sum[:], digest.Sum(nil))
		result <- sum
	}()
	data, err := r.readAll(ctx, blocks)
	close(blocks)
	started := time.Now()
	digest := <-result // Join on success, read error and cancellation.
	metrics.SPFreshBulkObjectReadDuration.WithLabelValues("pinned_sha_tail").Observe(time.Since(started).Seconds())
	if err != nil {
		return nil, err
	}
	if NewManifestRefFromDigest(reference.URI, digest) != reference {
		return nil, fmt.Errorf("SPFresh object %q SHA-256 does not match exact bytes", reference.URI)
	}
	return data, nil
}

func (r *ObjectReader) readAll(ctx context.Context, blocks chan<- []byte) ([]byte, error) {
	reader, key, size := r.reader, r.key, r.size
	readStarted := time.Now()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data := make([]byte, int(size))
	prefetched := copy(data, r.prefix)
	r.prefix = nil
	if blocks == nil {
		if _, err := io.ReadFull(reader, data[prefetched:]); err != nil {
			return nil, fmt.Errorf("read object %q: %w", key, err)
		}
	} else {
		for offset := 0; offset < len(data); offset += objectHashBlockBytes {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			block := data[offset:min(offset+objectHashBlockBytes, len(data))]
			// Prefetched bytes remain part of the pinned hash, but are never
			// fetched again. The prefix is shorter than one hash block.
			unread := block[max(0, prefetched-offset):]
			if _, err := io.ReadFull(reader, unread); err != nil {
				return nil, fmt.Errorf("read object %q: %w", key, err)
			}
			select {
			case blocks <- block:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	var extra [1]byte
	if n, err := io.ReadFull(reader, extra[:]); n != 0 {
		return nil, fmt.Errorf("object %q changed length", key)
	} else if err != io.EOF {
		return nil, fmt.Errorf("read object %q EOF: %w", key, err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	metrics.SPFreshBulkObjectReadDuration.WithLabelValues("read").Observe(time.Since(readStarted).Seconds())
	metrics.SPFreshBulkObjectReadDuration.WithLabelValues("total").Observe(r.openSeconds + time.Since(readStarted).Seconds())
	return data, nil
}
