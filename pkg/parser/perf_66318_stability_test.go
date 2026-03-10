package parser

import (
	"bufio"
	stdctx "context"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type perf66318StabilityCorpusInfo struct {
	Source   string
	Shuffled bool
	Seed     int64
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

func parseEnvInt64(t *testing.T, key string, def int64) int64 {
	t.Helper()
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return def
	}
	n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	if err != nil {
		t.Fatalf("invalid %s=%q: %v", key, v, err)
	}
	return n
}

func parseEnvBool(t *testing.T, key string, def bool) bool {
	t.Helper()
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return def
	}
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "t", "yes", "y":
		return true
	case "0", "false", "f", "no", "n":
		return false
	default:
		t.Fatalf("invalid %s=%q: expected bool (0/1/true/false)", key, v)
		return def
	}
}

func perf66318LoadStabilityCorpusFile(t *testing.T, path string) []string {
	t.Helper()
	path = strings.TrimSpace(path)
	if path == "" {
		t.Fatal("empty corpus file path")
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open corpus file %q: %v", path, err)
	}
	defer f.Close()

	// Allow long SQL lines while keeping Scan() simple.
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024), 4*1024*1024)

	var corpus []string
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		corpus = append(corpus, line)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("read corpus file %q: %v", path, err)
	}
	return corpus
}

func perf66318StabilityCorpus(t *testing.T) ([]string, perf66318StabilityCorpusInfo) {
	t.Helper()

	seed := parseEnvInt64(t, "PERF66318_SEED", 1)
	shuffle := parseEnvBool(t, "PERF66318_SHUFFLE", true)

	corpusFile := strings.TrimSpace(os.Getenv("PERF66318_CORPUS_FILE"))
	if corpusFile == "" {
		t.Fatal("PERF66318_CORPUS_FILE must be set (path to a .txt corpus, e.g. perf_66318_stability_corpus.txt)")
	}

	corpus := perf66318LoadStabilityCorpusFile(t, corpusFile)
	source := "PERF66318_CORPUS_FILE=" + corpusFile

	if len(corpus) == 0 {
		t.Fatalf("empty corpus (source=%s)", source)
	}

	if shuffle && len(corpus) > 1 {
		r := rand.New(rand.NewSource(seed))
		r.Shuffle(len(corpus), func(i, j int) {
			corpus[i], corpus[j] = corpus[j], corpus[i]
		})
	}

	return corpus, perf66318StabilityCorpusInfo{
		Source:   source,
		Shuffled: shuffle,
		Seed:     seed,
	}
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

	corpus, corpusInfo := perf66318StabilityCorpus(t)

	ctx, cancel := stdctx.WithTimeout(stdctx.Background(), duration)
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
					cancel()
					return
				}
				ops.Add(1)
			}
		}()
	}

	doneCh := make(chan struct{}, 1)
	go func() {
		wg.Wait()
		doneCh <- struct{}{}
	}()

	ticker := time.NewTicker(reportEvery)
	defer ticker.Stop()

	t.Logf(
		"Perf66318 stability start: duration=%s conc=%d reportEvery=%s corpus=%d (%s shuffle=%t seed=%d)",
		duration, concurrency, reportEvery, len(corpus), corpusInfo.Source, corpusInfo.Shuffled, corpusInfo.Seed,
	)

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
			select {
			case err := <-errCh:
				t.Fatalf("parse error: %v", err)
			default:
			}

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
