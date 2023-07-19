package main

import (
	"context"
	"fmt"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"io"
	"runtime"
	"strconv"
	"sync"
	"time"
)

func main() {
	bucket := "globalsorttest"
	prefix := "tools_test_data/sharedisk"
	uri := fmt.Sprintf("s3://%s/%s&force-path-style=true",
		bucket, prefix)
	//uri := fmt.Sprintf("s3://%s/%s?access-key=%s&secret-access-key=%s&endpoint=http://%s:%s&force-path-style=true",
	//	bucket, prefix, "minioadmin", "minioadmin", "127.0.0.1", "9000")

	backend, err := storage.ParseBackend(uri, nil)
	if err != nil {
		log.Fatal(err.Error())
	}
	st, err := storage.New(context.Background(), backend, &storage.ExternalStorageOptions{})
	if err != nil {
		log.Fatal(err.Error())
	}

	dataFileName := make([]string, 0)
	for i := 0; i < 1500; i++ {
		dataFileName = append(dataFileName, "test/"+strconv.Itoa(i))
	}

	var wg sync.WaitGroup
	wg.Add(1500)
	readers := make([]storage.ExternalFileReader, 0)
	for _, fileName := range dataFileName {
		fileName := fileName
		go func() {
			reader, err := st.Open(context.Background(), fileName)
			if err != nil {
				log.Fatal(err.Error())
			}
			readers = append(readers, reader)
			wg.Done()
		}()
	}
	wg.Wait()

	var startMemory runtime.MemStats
	runtime.ReadMemStats(&startMemory)
	readBuffer := make([]byte, 16*1024)
	run := true
	i := 0
	for run {
		for _, reader := range readers {
			ts := time.Now()
			n, err := io.ReadFull(reader, readBuffer[0:])
			if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
				log.Fatal(err.Error())
			}
			log.Info("read", zap.Int("n", n), zap.Duration("time", time.Since(ts)))
			if n == 0 {
				run = false
				break
			}
			i++
			//time.Sleep(time.Millisecond * 50)
			if i%100 == 0 {
				runtime.ReadMemStats(&startMemory)
				logutil.BgLogger().Info("meminfo before read", zap.Any("alloc", startMemory.Alloc), zap.Any("heapInUse", startMemory.HeapInuse), zap.Any("gc/ns", startMemory.PauseTotalNs), zap.Any("total", startMemory.TotalAlloc))
			}
		}
	}
}
