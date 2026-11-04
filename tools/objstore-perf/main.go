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

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/recording"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
)

const (
	modeWrite = "write"
	modeRead  = "read"
)

type config struct {
	mode string
	url  string

	prefix   string
	duration time.Duration
	workers  int

	objectSize int64
	blockSize  int64

	writerConcurrency int
	partSize          int64

	prepareRead  bool
	prepareFiles int
	prefetchSize int

	cleanup bool
}

type result struct {
	bytes   uint64
	objects uint64
	elapsed time.Duration
}

func main() {
	cfg, err := parseFlags()
	if err != nil {
		fmt.Printf("config error: %v\n", err)
		return
	}

	baseCtx := context.Background()
	acc := &recording.AccessStats{}
	backend, err := objstore.ParseBackend(cfg.url, nil)
	if err != nil {
		fmt.Printf("parse url failed: %v\n", err)
		return
	}
	store, err := objstore.New(baseCtx, backend, &storeapi.Options{AccessRecording: acc})
	if err != nil {
		fmt.Printf("open object store failed: %v\n", err)
		return
	}
	defer store.Close()

	fmt.Printf("mode=%s url=%s prefix=%s workers=%d duration=%s object-size=%s block-size=%s writer-concurrency=%d part-size=%s\n",
		cfg.mode, cfg.url, cfg.prefix, cfg.workers, cfg.duration,
		units.BytesSize(float64(cfg.objectSize)), units.BytesSize(float64(cfg.blockSize)), cfg.writerConcurrency,
		units.BytesSize(float64(cfg.partSize)))

	var runRes result
	start := time.Now()
	switch cfg.mode {
	case modeWrite:
		runRes, err = runWrite(baseCtx, store, cfg)
	case modeRead:
		runRes, err = runRead(baseCtx, store, cfg)
	default:
		err = fmt.Errorf("unsupported mode: %s", cfg.mode)
	}
	if err != nil {
		fmt.Printf("run failed: %v\n", err)
		return
	}
	runRes.elapsed = time.Since(start)

	mb := float64(runRes.bytes) / units.MiB
	sec := runRes.elapsed.Seconds()
	if sec <= 0 {
		sec = 1
	}
	fmt.Printf(
		"done: bytes=%d (%.2f MiB) objects=%d elapsed=%s throughput=%.2f MiB/s\n",
		runRes.bytes, mb, runRes.objects, runRes.elapsed, mb/sec,
	)
	fmt.Printf("objstore access: requests=%s traffic=%s\n", acc.Requests.String(), acc.Traffic.String())

	if cfg.cleanup {
		if err := cleanupPrefix(baseCtx, store, cfg.prefix); err != nil {
			fmt.Printf("cleanup failed: %v\n", err)
			return
		}
		fmt.Println("cleanup done")
	}
}

func parseFlags() (config, error) {
	var cfg config
	var (
		objectSizeStr = "256MiB"
		blockSizeStr  = "8MiB"
		partSizeStr   = "5MiB"
	)

	flag.StringVar(&cfg.mode, "mode", "", "mode: write or read")
	flag.StringVar(&cfg.url, "url", "", "object store URL (required), e.g. s3://bucket/prefix")
	flag.StringVar(&cfg.prefix, "prefix", "", "test object prefix under the URL prefix")
	flag.DurationVar(&cfg.duration, "duration", 2*time.Minute, "test duration")
	flag.IntVar(&cfg.workers, "workers", runtime.GOMAXPROCS(0), "number of concurrent workers")

	flag.StringVar(&objectSizeStr, "object-size", objectSizeStr, "bytes per object, e.g. 256MiB")
	flag.StringVar(&blockSizeStr, "block-size", blockSizeStr, "write/read block size, e.g. 8MiB")
	flag.IntVar(&cfg.writerConcurrency, "writer-concurrency", 20, "multipart upload concurrency per object")
	flag.StringVar(&partSizeStr, "part-size", partSizeStr, "multipart part size, e.g. 5MiB")

	flag.BoolVar(&cfg.prepareRead, "prepare-read", true, "prepare read objects before read mode")
	flag.IntVar(&cfg.prepareFiles, "prepare-files", 64, "number of objects to prepare for read mode")
	flag.IntVar(&cfg.prefetchSize, "prefetch-size", 0, "reader prefetch size in bytes (0 disables)")
	flag.BoolVar(&cfg.cleanup, "cleanup", false, "delete all objects under prefix after run")
	flag.Parse()

	if cfg.mode != modeWrite && cfg.mode != modeRead {
		return cfg, errors.New("-mode must be write or read")
	}
	if strings.TrimSpace(cfg.url) == "" {
		return cfg, errors.New("-url is required")
	}
	if cfg.duration <= 0 {
		return cfg, errors.New("-duration must be > 0")
	}
	if cfg.workers <= 0 {
		return cfg, errors.New("-workers must be > 0")
	}
	if cfg.writerConcurrency <= 0 {
		return cfg, errors.New("-writer-concurrency must be > 0")
	}
	if cfg.prepareFiles <= 0 {
		return cfg, errors.New("-prepare-files must be > 0")
	}

	var err error
	cfg.objectSize, err = units.RAMInBytes(objectSizeStr)
	if err != nil {
		return cfg, fmt.Errorf("parse -object-size failed: %w", err)
	}
	cfg.blockSize, err = units.RAMInBytes(blockSizeStr)
	if err != nil {
		return cfg, fmt.Errorf("parse -block-size failed: %w", err)
	}
	cfg.partSize, err = units.RAMInBytes(partSizeStr)
	if err != nil {
		return cfg, fmt.Errorf("parse -part-size failed: %w", err)
	}

	if cfg.objectSize <= 0 || cfg.blockSize <= 0 || cfg.partSize <= 0 {
		return cfg, errors.New("-object-size, -block-size, -part-size must be > 0")
	}
	if cfg.prefix == "" {
		cfg.prefix = filepath.ToSlash(filepath.Join("objstore-perf", time.Now().UTC().Format("20060102T150405")))
	}
	if cfg.partSize < external.MinUploadPartSize {
		return cfg, fmt.Errorf("-part-size must be >= %s", units.BytesSize(float64(external.MinUploadPartSize)))
	}
	return cfg, nil
}

func runWrite(ctx context.Context, store storeapi.Storage, cfg config) (result, error) {
	deadlineCtx, cancel := context.WithTimeout(ctx, cfg.duration)
	defer cancel()

	writeBuf := make([]byte, cfg.blockSize)
	for i := range writeBuf {
		writeBuf[i] = byte(i)
	}

	var (
		bytesTotal  atomic.Uint64
		objectTotal atomic.Uint64
		wg          sync.WaitGroup
		errOnce     sync.Once
		runErr      error
	)

	workerFn := func(workerID int) {
		defer wg.Done()
		for seq := 0; ; seq++ {
			if deadlineCtx.Err() != nil {
				return
			}
			path := filepath.ToSlash(filepath.Join(cfg.prefix, "write", fmt.Sprintf("worker-%03d", workerID), fmt.Sprintf("obj-%09d.bin", seq)))
			n, err := writeOneObject(deadlineCtx, store, path, cfg.objectSize, writeBuf, cfg.writerConcurrency, cfg.partSize)
			if err != nil {
				if shouldStop(err) {
					return
				}
				errOnce.Do(func() {
					runErr = err
					cancel()
				})
				return
			}
			bytesTotal.Add(uint64(n))
			objectTotal.Add(1)
		}
	}

	for i := 0; i < cfg.workers; i++ {
		wg.Add(1)
		go workerFn(i)
	}
	wg.Wait()

	if runErr != nil {
		return result{}, runErr
	}
	return result{bytes: bytesTotal.Load(), objects: objectTotal.Load()}, nil
}

func runRead(ctx context.Context, store storeapi.Storage, cfg config) (result, error) {
	if cfg.prepareRead {
		if err := prepareReadFiles(ctx, store, cfg); err != nil {
			return result{}, err
		}
	}
	paths, err := collectFiles(ctx, store, filepath.ToSlash(filepath.Join(cfg.prefix, "read")))
	if err != nil {
		return result{}, err
	}
	if len(paths) == 0 {
		return result{}, errors.New("no input files for read mode")
	}

	deadlineCtx, cancel := context.WithTimeout(ctx, cfg.duration)
	defer cancel()

	readBuf := make([]byte, cfg.blockSize)

	var (
		bytesTotal  atomic.Uint64
		objectTotal atomic.Uint64
		nextIdx     atomic.Uint64
		wg          sync.WaitGroup
		errOnce     sync.Once
		runErr      error
	)

	workerFn := func() {
		defer wg.Done()
		for {
			if deadlineCtx.Err() != nil {
				return
			}
			i := int(nextIdx.Add(1)-1) % len(paths)
			n, err := readOneObject(deadlineCtx, store, paths[i], readBuf, cfg.prefetchSize)
			if err != nil {
				if shouldStop(err) {
					return
				}
				errOnce.Do(func() {
					runErr = err
					cancel()
				})
				return
			}
			bytesTotal.Add(uint64(n))
			objectTotal.Add(1)
		}
	}

	for i := 0; i < cfg.workers; i++ {
		wg.Add(1)
		go workerFn()
	}
	wg.Wait()

	if runErr != nil {
		return result{}, runErr
	}
	return result{bytes: bytesTotal.Load(), objects: objectTotal.Load()}, nil
}

func prepareReadFiles(ctx context.Context, store storeapi.Storage, cfg config) error {
	writeBuf := make([]byte, cfg.blockSize)
	for i := range writeBuf {
		writeBuf[i] = byte(i)
	}

	var (
		nextTask uint64
		wg       sync.WaitGroup
		errOnce  sync.Once
		runErr   error
	)

	workerFn := func(workerID int) {
		defer wg.Done()
		for {
			idx := int(atomic.AddUint64(&nextTask, 1) - 1)
			if idx >= cfg.prepareFiles {
				return
			}
			path := filepath.ToSlash(filepath.Join(cfg.prefix, "read", fmt.Sprintf("seed-worker-%03d", workerID), fmt.Sprintf("obj-%09d.bin", idx)))
			_, err := writeOneObject(ctx, store, path, cfg.objectSize, writeBuf, cfg.writerConcurrency, cfg.partSize)
			if err != nil {
				errOnce.Do(func() { runErr = err })
				return
			}
		}
	}

	workers := min(cfg.workers, cfg.prepareFiles)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go workerFn(i)
	}
	wg.Wait()
	return runErr
}

func writeOneObject(
	ctx context.Context,
	store storeapi.Storage,
	path string,
	objectSize int64,
	buf []byte,
	concurrency int,
	partSize int64,
) (int64, error) {
	writer, err := store.Create(ctx, path, &storeapi.WriterOption{
		Concurrency: concurrency,
		PartSize:    partSize,
	})
	if err != nil {
		return 0, err
	}

	var written int64
	for written < objectSize {
		chunk := buf
		remaining := objectSize - written
		if int64(len(chunk)) > remaining {
			chunk = chunk[:remaining]
		}
		n, err := writer.Write(ctx, chunk)
		written += int64(n)
		if err != nil {
			_ = writer.Close(ctx)
			return written, err
		}
	}
	if err := writer.Close(ctx); err != nil {
		return written, err
	}
	return written, nil
}

func readOneObject(
	ctx context.Context,
	store storeapi.Storage,
	path string,
	buf []byte,
	prefetchSize int,
) (int64, error) {
	opt := &storeapi.ReaderOption{PrefetchSize: prefetchSize}
	reader, err := store.Open(ctx, path, opt)
	if err != nil {
		return 0, err
	}
	defer reader.Close()

	var total int64
	for {
		n, err := reader.Read(buf)
		total += int64(n)
		if err == nil {
			continue
		}
		if errors.Is(err, io.EOF) {
			break
		}
		return total, err
	}
	return total, nil
}

func collectFiles(ctx context.Context, store storeapi.Storage, subDir string) ([]string, error) {
	files := make([]string, 0, 128)
	err := store.WalkDir(ctx, &storeapi.WalkOption{SubDir: subDir}, func(path string, size int64) error {
		if size == objstore.TombstoneSize {
			return nil
		}
		files = append(files, path)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return files, nil
}

func cleanupPrefix(ctx context.Context, store storeapi.Storage, prefix string) error {
	files, err := collectFiles(ctx, store, prefix)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return nil
	}
	return store.DeleteFiles(ctx, files)
}

func shouldStop(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
