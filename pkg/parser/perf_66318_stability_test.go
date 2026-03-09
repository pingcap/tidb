package parser

import (
	"bufio"
	"context"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"os"
	"runtime"
)

func perf66318StabilityCorpus() []string {
	// Keep the corpus representative but bounded; we want steady-state behavior.
	return []string{
		"SELECT c FROM sbtest1 WHERE id=1",
		"SELECT c FROM sbtest1 WHERE id=?",
		"SELECT c FROM sbtest1 WHERE id=? FOR UPDATE",
		"UPDATE sbtest1 SET k=k+1 WHERE id=?",
		"UPDATE sbtest1 SET c=? WHERE id=?",
		"DELETE FROM sbtest1 WHERE id=?",
		"INSERT INTO sbtest1(id,k,c,pad) VALUES(?,?,?,?)",
		"SET autocommit = 1",
		"SET NAMES utf8mb4",
		"SHOW VARIABLES LIKE 'tidb%'",
		"SHOW STATUS LIKE 'Threads_running'",
		strings.TrimSpace(`
SELECT t1.a, t2.b
FROM t1
LEFT JOIN t2 ON t1.id = t2.id
WHERE t1.a > 10 AND t2.b IS NOT NULL
ORDER BY t2.b DESC
LIMIT 100`),
		strings.TrimSpace(`
WITH cte AS (
  SELECT id, v FROM t WHERE v > 10
)
SELECT * FROM cte WHERE id < 100`),
		strings.TrimSpace(`
SELECT id,
       SUM(v) OVER (PARTITION BY id % 10 ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s
FROM t`),
		"SELECT /*+ USE_INDEX(t idx_a) */ * FROM t WHERE a = 1 AND b = 2",
		"SELECT '中文' AS zh, '🙂' AS emj",
	}
}

func parseEnvDuration(t *testing.T, key string, def time.Duration) time.Duration {
	t.Helper()
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		t.Fatalf("invalid %s=%q: %v", key, v, err)
	}
	return d
}

func parseEnvInt(t *testing.T, key string, def int) int {
	t.Helper()
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		t.Fatalf("invalid %s=%q: %v", key, v, err)
	}
	if n <= 0 {
		t.Fatalf("invalid %s=%q: must be > 0", key, v)
	}
	return n
}

func readRSSBytesLinux() (uint64, bool) {
	f, err := os.Open("/proc/self/status")
	if err != nil {
		return 0, false
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "VmRSS:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0, false
		}
		kb, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return 0, false
		}
		return kb * 1024, true
	}
	return 0, false
}

func TestPerf66318Stability(t *testing.T) {
	duration := parseEnvDuration(t, "PERF66318_DURATION", 10*time.Minute)
	concurrency := parseEnvInt(t, "PERF66318_CONC", 16)
	reportEvery := parseEnvDuration(t, "PERF66318_REPORT_EVERY", 1*time.Minute)

	corpus := perf66318StabilityCorpus()
	if len(corpus) == 0 {
		t.Fatal("empty corpus")
	}

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	var ops atomic.Uint64
	errCh := make(chan error, 1)

	start := time.Now()
	lastAt := start
	var lastOps uint64

	wg := sync.WaitGroup{}
	wg.Add(concurrency)
	for w := 0; w < concurrency; w++ {
		workerID := w
		go func() {
			defer wg.Done()
			p := New()
			idx := workerID
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				sql := corpus[idx%len(corpus)]
				idx++
				_, _, err := p.ParseSQL(sql)
				if err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
				ops.Add(1)
			}
		}()
	}

	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()

	ticker := time.NewTicker(reportEvery)
	defer ticker.Stop()

	t.Logf("Perf66318 stability start: duration=%s conc=%d reportEvery=%s corpus=%d", duration, concurrency, reportEvery, len(corpus))

	for {
		select {
		case err := <-errCh:
			cancel()
			<-doneCh
			t.Fatalf("parse error: %v", err)
		case <-ticker.C:
			now := time.Now()
			curOps := ops.Load()
			intervalOps := curOps - lastOps
			intervalSec := now.Sub(lastAt).Seconds()
			if intervalSec <= 0 {
				intervalSec = 1
			}
			intervalOpsPerSec := float64(intervalOps) / intervalSec
			totalOpsPerSec := float64(curOps) / now.Sub(start).Seconds()

			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			rssBytes, rssOK := readRSSBytesLinux()
			rssMB := float64(rssBytes) / (1024 * 1024)

			msg := ""
			if rssOK {
				msg = " rssMB=" + strconv.FormatFloat(rssMB, 'f', 1, 64)
			}
			t.Logf(
				"elapsed=%s ops=%d intervalOps=%d intervalOps/s=%.0f totalOps/s=%.0f heapAllocMB=%.1f heapInuseMB=%.1f numGC=%d%s",
				now.Sub(start).Round(time.Second),
				curOps,
				intervalOps,
				intervalOpsPerSec,
				totalOpsPerSec,
				float64(ms.HeapAlloc)/(1024*1024),
				float64(ms.HeapInuse)/(1024*1024),
				ms.NumGC,
				msg,
			)

			lastAt = now
			lastOps = curOps
		case <-doneCh:
			elapsed := time.Since(start)
			curOps := ops.Load()
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			rssBytes, rssOK := readRSSBytesLinux()
			msg := ""
			if rssOK {
				msg = " rssMB=" + strconv.FormatFloat(float64(rssBytes)/(1024*1024), 'f', 1, 64)
			}
			t.Logf(
				"Perf66318 stability done: elapsed=%s ops=%d totalOps/s=%.0f heapAllocMB=%.1f heapInuseMB=%.1f numGC=%d%s",
				elapsed.Round(time.Second),
				curOps,
				float64(curOps)/elapsed.Seconds(),
				float64(ms.HeapAlloc)/(1024*1024),
				float64(ms.HeapInuse)/(1024*1024),
				ms.NumGC,
				msg,
			)
			return
		}
	}
}
