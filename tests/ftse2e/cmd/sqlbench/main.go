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

// sqlbench sets up an owned corpus and measures Local MATCH over persistent
// connections. Cluster lifecycle and artifact collection live in scripts/h124.
package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"net"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/tidb/tests/ftse2e/benchdata"
)

type manifest struct {
	Schema, Parser, Collation, Version, State string
	Rows, Bytes, NgramSize, MinSize, MaxSize  int
}

type options struct {
	mode, dsn, file, parser, collation                           string
	path, search, profileURL, profileFile                        string
	profileFormat                                                string
	profileSeconds                                               int
	rows, size, concurrency, iterations, trials, warmup, threads int
	stopwords                                                    bool
	duration, timeout                                            time.Duration
}

var schemaName = regexp.MustCompile(`^fts_bench_[0-9]+$`)

func parseOptions(args []string) (options, error) {
	var o options
	f := flag.NewFlagSet("sqlbench", flag.ContinueOnError)
	f.StringVar(&o.mode, "mode", "check", "check, setup, or run")
	f.StringVar(&o.dsn, "dsn", "root@tcp(127.0.0.1:24000)/?charset=utf8mb4", "local TiDB DSN")
	f.StringVar(&o.file, "manifest", "", "exclusive corpus manifest path")
	f.StringVar(&o.parser, "parser", "standard", "standard or ngram (setup)")
	f.StringVar(&o.collation, "collation", "utf8mb4_bin", "utf8mb4_bin or utf8mb4_general_ci (setup)")
	f.IntVar(&o.rows, "rows", 100000, "corpus rows (setup)")
	f.IntVar(&o.size, "bytes", 128, "bytes per document, 128..1048576 (setup)")
	f.IntVar(&o.concurrency, "concurrency", 1, "persistent SQL connections per path")
	f.IntVar(&o.iterations, "iterations", 30, "total queries per path/trial when duration=0")
	f.DurationVar(&o.duration, "duration", 0, "measurement duration per path/trial; overrides iterations")
	f.DurationVar(&o.timeout, "timeout", 30*time.Minute, "deadline per setup/verification/query")
	f.IntVar(&o.trials, "trials", 3, "trials with alternating path order")
	f.IntVar(&o.warmup, "warmup", 3, "warmup queries on each connection")
	f.IntVar(&o.threads, "tiflash-threads", 8, "tidb_max_tiflash_threads per connection")
	f.BoolVar(&o.stopwords, "stopwords", false, "enable builtin stopwords")
	f.StringVar(&o.path, "path", "both", "timed path: both, local, or native; always verify both")
	f.StringVar(&o.search, "search", "all", "all, word, phrase, prefix, cjk, or miss")
	f.StringVar(&o.profileURL, "profile-url", "", "loopback TiFlash status URL, e.g. http://127.0.0.1:40292")
	f.StringVar(&o.profileFile, "profile-file", "", "new protobuf CPU profile file; never overwritten")
	f.IntVar(&o.profileSeconds, "profile-seconds", 30, "CPU sampling seconds at 99Hz")
	f.StringVar(&o.profileFormat, "profile-format", "protobuf", "protobuf or svg CPU profile")
	if err := f.Parse(args); err != nil {
		return o, err
	}
	if f.NArg() != 0 || (o.mode != "check" && o.mode != "setup" && o.mode != "run") {
		return o, fmt.Errorf("invalid mode or positional arguments")
	}
	if o.mode != "check" && o.file == "" {
		return o, fmt.Errorf("-manifest is required")
	}
	if (o.parser != "standard" && o.parser != "ngram") || (o.collation != "utf8mb4_bin" && o.collation != "utf8mb4_general_ci") {
		return o, fmt.Errorf("unsupported corpus parser/collation")
	}
	if o.rows <= 0 || o.size < 128 || o.size > 1048576 || o.concurrency <= 0 || o.concurrency > 64 || o.iterations <= 0 || (o.duration == 0 && o.iterations < o.concurrency) || o.trials <= 0 || o.warmup < 0 || o.threads <= 0 || o.timeout <= 0 || o.duration < 0 {
		return o, fmt.Errorf("invalid size, concurrency, iteration, or time bounds")
	}
	if o.path != "both" && o.path != "local" && o.path != "native" {
		return o, fmt.Errorf("invalid timed path")
	}
	if _, err := searches(o.search); err != nil {
		return o, err
	}
	if err := validateProfileOptions(o); err != nil {
		return o, err
	}
	return o, nil
}

func searches(name string) ([]string, error) {
	choices := map[string]string{"word": "+quick -slow", "phrase": `+"quick brown fox"`, "prefix": "+pre*", "cjk": "+数据库", "miss": "+absentterm"}
	if name == "all" {
		return []string{choices["word"], choices["phrase"], choices["prefix"], choices["cjk"]}, nil
	}
	if q, ok := choices[name]; ok {
		return []string{q}, nil
	}
	return nil, fmt.Errorf("unknown search %q", name)
}

func openDB(dsn string) (*sql.DB, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	host, _, err := net.SplitHostPort(cfg.Addr)
	if cfg.Net != "tcp" || err != nil || (host != "localhost" && !net.ParseIP(host).IsLoopback()) || cfg.DBName != "" {
		return nil, fmt.Errorf("DSN must use local TCP and no default database")
	}
	cfg.Timeout = 5 * time.Second
	cfg.InterpolateParams = true
	return sql.Open("mysql", cfg.FormatDSN())
}

func saveManifest(file string, m manifest, exclusive bool) error {
	flags := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	if exclusive {
		flags = os.O_WRONLY | os.O_CREATE | os.O_EXCL
	}
	f, err := os.OpenFile(file, flags, 0600)
	if err != nil {
		return err
	}
	encodeErr := json.NewEncoder(f).Encode(m)
	return errors.Join(encodeErr, f.Close())
}

func check(ctx context.Context, db *sql.DB) (string, error) {
	var version string
	if err := db.QueryRowContext(ctx, "SELECT tidb_version()").Scan(&version); err != nil {
		return "", err
	}
	var enabled int
	if err := db.QueryRowContext(ctx, "SELECT @@tidb_enable_local_match_against").Scan(&enabled); err != nil {
		return "", fmt.Errorf("binary lacks Local MATCH: %w", err)
	}
	return version, nil
}

func setup(ctx context.Context, db *sql.DB, o options) error {
	version, err := check(ctx, db)
	if err != nil {
		return err
	}
	m := manifest{Schema: fmt.Sprintf("fts_bench_%d", time.Now().UnixNano()), Parser: o.parser, Collation: o.collation, Version: version, State: "creating", Rows: o.rows, Bytes: o.size}
	if err = db.QueryRowContext(ctx, "SELECT @@global.ngram_token_size,@@global.innodb_ft_min_token_size,@@global.innodb_ft_max_token_size").Scan(&m.NgramSize, &m.MinSize, &m.MaxSize); err != nil {
		return err
	}
	if err = saveManifest(o.file, m, true); err != nil {
		return err
	}
	if _, err = db.ExecContext(ctx, "CREATE DATABASE `"+m.Schema+"`"); err != nil {
		return err
	}
	parserSQL := ""
	if o.parser == "ngram" {
		parserSQL = " WITH PARSER NGRAM"
	}
	for _, path := range []string{"local", "native"} {
		ddl := "CREATE TABLE `" + m.Schema + "`.`docs_" + path + "` (id BIGINT PRIMARY KEY,body MEDIUMTEXT COLLATE " + o.collation + ",FULLTEXT INDEX ft(body)" + parserSQL + ")"
		if _, err = db.ExecContext(ctx, ddl); err != nil {
			return err
		}
	}
	batch := min(500, max(1, (2<<20)/o.size))
	for start := 0; start < o.rows; start += batch {
		n := min(batch, o.rows-start)
		args := make([]any, 0, n*2)
		for i := range n {
			args = append(args, start+i+1, benchdata.Document(start+i, o.size))
		}
		values := strings.TrimSuffix(strings.Repeat("(?,?),", n), ",")
		for _, path := range []string{"local", "native"} {
			if _, err = db.ExecContext(ctx, "INSERT INTO `"+m.Schema+"`.`docs_"+path+"` VALUES "+values, args...); err != nil {
				return err
			}
		}
		if start%(batch*100) == 0 {
			fmt.Fprintf(os.Stderr, "inserted %d/%d paired rows\n", start+n, o.rows)
		}
	}
	if _, err = db.ExecContext(ctx, "ALTER TABLE `"+m.Schema+"`.`docs_native` SET TIFLASH REPLICA 1"); err != nil {
		return err
	}
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		var ready int
		if err = db.QueryRowContext(ctx, "SELECT AVAILABLE FROM information_schema.tiflash_replica WHERE TABLE_SCHEMA=? AND TABLE_NAME='docs_native'", m.Schema).Scan(&ready); err != nil {
			return err
		}
		if ready == 1 {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
	m.State = "ready"
	return saveManifest(o.file, m, false)
}

func connect(ctx context.Context, db *sql.DB, m manifest, o options, path string) (*sql.Conn, error) {
	c, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	engine := "tikv"
	if path == "native" {
		engine = "tiflash"
		if err = requireDefaultMPPSettings(ctx, c); err != nil {
			c.Close()
			return nil, err
		}
	}
	stop := "OFF"
	if o.stopwords {
		stop = "ON"
	}
	for _, q := range []string{"USE `" + m.Schema + "`", "SET SESSION tidb_enable_local_match_against=ON", "SET SESSION tidb_isolation_read_engines='" + engine + "'", "SET SESSION innodb_ft_enable_stopword=" + stop, fmt.Sprintf("SET SESSION tidb_max_tiflash_threads=%d", o.threads)} {
		if _, err = c.ExecContext(ctx, q); err != nil {
			c.Close()
			return nil, err
		}
	}
	return c, nil
}

func requireDefaultMPPSettings(ctx context.Context, c *sql.Conn) error {
	var allow, cop, enforce int
	if err := c.QueryRowContext(ctx, "SELECT @@tidb_allow_mpp,@@tidb_allow_tiflash_cop,@@tidb_enforce_mpp").Scan(&allow, &cop, &enforce); err != nil {
		return err
	}
	if allow != 1 || cop != 0 || enforce != 0 {
		return fmt.Errorf("expected default MPP settings (allow=1,cop=0,enforce=0), got (%d,%d,%d)", allow, cop, enforce)
	}
	return nil
}

func plan(ctx context.Context, c *sql.Conn, query, path string) (string, error) {
	rows, err := c.QueryContext(ctx, "EXPLAIN FORMAT='brief' "+query)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return "", err
	}
	if len(columns) < 5 {
		return "", fmt.Errorf("unexpected brief EXPLAIN columns: %v", columns)
	}
	var b strings.Builder
	scan, match := false, false
	for rows.Next() {
		values := make([]sql.RawBytes, len(columns))
		dest := make([]any, len(values))
		for i := range values {
			dest[i] = &values[i]
		}
		if err = rows.Scan(dest...); err != nil {
			return "", err
		}
		parts := make([]string, len(values))
		for i := range values {
			parts[i] = string(values[i])
		}
		b.WriteString(strings.Join(parts, "\t") + "\n")
		operator, task, info := strings.ToLower(parts[0]), strings.ToLower(parts[2]), strings.ToLower(parts[len(parts)-1])
		if task == "cop[tici]" || task == "batchcop[tici]" || task == "mpp[tici]" {
			return "", fmt.Errorf("unexpected TiCI")
		}
		isFlash := task == "mpp[tiflash]"
		if isFlash && (strings.Contains(operator, "tablefullscan") || strings.Contains(operator, "tablerangescan")) {
			scan = true
		}
		if strings.Contains(info, "match_against(") {
			if !strings.Contains(operator, "selection") || (path == "native" && !isFlash) || (path == "local" && task != "root") {
				return "", fmt.Errorf("wrong MATCH placement: %s", b.String())
			}
			match = true
		}
	}
	if err = rows.Err(); err != nil {
		return "", err
	}
	if !match || (path == "native" && !scan) {
		return "", fmt.Errorf("missing MATCH/scan proof: %s", b.String())
	}
	return b.String(), nil
}

func resultFingerprint(ctx context.Context, c *sql.Conn, predicate, path string) (int64, string, error) {
	rows, err := c.QueryContext(ctx, "SELECT id FROM docs_"+path+" WHERE "+predicate+" ORDER BY id")
	if err != nil {
		return 0, "", err
	}
	defer rows.Close()
	h := sha256.New()
	var count int64
	var buf [8]byte
	for rows.Next() {
		var id int64
		if err = rows.Scan(&id); err != nil {
			return 0, "", err
		}
		binary.LittleEndian.PutUint64(buf[:], uint64(id))
		h.Write(buf[:])
		count++
	}
	return count, hex.EncodeToString(h.Sum(nil)), rows.Err()
}

type measurement struct {
	Kind, Path, Search, Schema            string
	Trial, Concurrency, Completed, Errors int
	Matched                               int64
	ElapsedMS, QPS, P50MS, P95MS, P99MS   float64
	Error                                 string `json:",omitempty"`
}

func percentile(samples []float64, p float64) float64 {
	if len(samples) == 0 {
		return 0
	}
	return samples[max(0, min(len(samples)-1, int(math.Ceil(p*float64(len(samples))))-1))]
}

func measure(ctx context.Context, conns []*sql.Conn, query string, want int64, o options) (measurement, error) {
	// Persistent connections and warmup are established before the clock starts.
	for _, c := range conns {
		for range o.warmup {
			var got int64
			warmupCtx, warmupCancel := context.WithTimeout(ctx, o.timeout)
			err := c.QueryRowContext(warmupCtx, query).Scan(&got)
			warmupCancel()
			if err != nil {
				return measurement{}, err
			}
			if got != want {
				return measurement{}, fmt.Errorf("warmup result changed")
			}
		}
	}
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var profileDone <-chan error
	if o.profileURL != "" {
		profileDone = collectCPUProfile(workCtx, o)
		// The endpoint has no sampling-start acknowledgement. Allow its setup
		// to settle before issuing work, and keep work longer than sampling.
		select {
		case err := <-profileDone:
			if err == nil {
				err = fmt.Errorf("profile ended before workload")
			}
			return measurement{}, err
		case <-time.After(300 * time.Millisecond):
		case <-ctx.Done():
			return measurement{}, ctx.Err()
		}
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	var errorsCount int
	latencies := make([]float64, 0, o.iterations)
	start := time.Now()
	deadline := start.Add(o.duration)
	for worker, c := range conns {
		wg.Add(1)
		go func(worker int, c *sql.Conn) {
			defer wg.Done()
			for i := worker; ; i += len(conns) {
				if workCtx.Err() != nil || (o.duration == 0 && i >= o.iterations) || (o.duration > 0 && !time.Now().Before(deadline)) {
					return
				}
				queryCtx, queryCancel := context.WithTimeout(workCtx, o.timeout)
				tick := time.Now()
				var got int64
				err := c.QueryRowContext(queryCtx, query).Scan(&got)
				latency := float64(time.Since(tick)) / float64(time.Millisecond)
				queryCancel()
				if err == nil && got != want {
					err = fmt.Errorf("result changed: want %d, got %d", want, got)
				}
				mu.Lock()
				if err != nil {
					errorsCount++
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
					cancel()
					return
				}
				latencies = append(latencies, latency)
				mu.Unlock()
			}
		}(worker, c)
	}
	wg.Wait()
	elapsed := time.Since(start)
	if profileDone != nil {
		if firstErr != nil {
			cancel()
		}
		if err := <-profileDone; err != nil && firstErr == nil {
			firstErr, errorsCount = err, errorsCount+1
		}
	}
	sort.Float64s(latencies)
	m := measurement{Kind: "measurement", Concurrency: len(conns), Completed: len(latencies), Matched: want, ElapsedMS: float64(elapsed) / float64(time.Millisecond), QPS: float64(len(latencies)) / elapsed.Seconds(), P50MS: percentile(latencies, .5), P95MS: percentile(latencies, .95), P99MS: percentile(latencies, .99)}
	if firstErr != nil {
		m.Errors = errorsCount
		m.Error = firstErr.Error()
	}
	if err := ctx.Err(); firstErr == nil && err != nil {
		firstErr = err
		m.Errors = 1
		m.Error = err.Error()
	}
	return m, firstErr
}

func run(ctx context.Context, db *sql.DB, o options) error {
	f, err := os.Open(o.file)
	if err != nil {
		return err
	}
	var m manifest
	err = json.NewDecoder(f).Decode(&m)
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if !schemaName.MatchString(m.Schema) || m.State != "ready" || m.Rows <= 0 {
		return fmt.Errorf("manifest does not identify a ready owned corpus")
	}
	var ngramSize, minSize, maxSize int
	if err = db.QueryRowContext(ctx, "SELECT @@global.ngram_token_size,@@global.innodb_ft_min_token_size,@@global.innodb_ft_max_token_size").Scan(&ngramSize, &minSize, &maxSize); err != nil {
		return err
	}
	if ngramSize != m.NgramSize || minSize != m.MinSize || maxSize != m.MaxSize {
		return fmt.Errorf("analyzer settings differ from corpus setup")
	}
	var ready, localReplicas int
	if err = db.QueryRowContext(ctx, "SELECT AVAILABLE FROM information_schema.tiflash_replica WHERE TABLE_SCHEMA=? AND TABLE_NAME='docs_native'", m.Schema).Scan(&ready); err != nil {
		return err
	}
	if err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.tiflash_replica WHERE TABLE_SCHEMA=? AND TABLE_NAME='docs_local'", m.Schema).Scan(&localReplicas); err != nil {
		return err
	}
	if ready != 1 || localReplicas != 0 {
		return fmt.Errorf("wrong replica placement/readiness")
	}
	conns := make(map[string][]*sql.Conn)
	defer func() {
		for _, cs := range conns {
			for _, c := range cs {
				c.Close()
			}
		}
	}()
	for _, path := range []string{"local", "native"} {
		for range o.concurrency {
			c, err := connect(ctx, db, m, o, path)
			if err != nil {
				return err
			}
			conns[path] = append(conns[path], c)
		}
		var rows int
		if err = conns[path][0].QueryRowContext(ctx, "SELECT COUNT(*) FROM docs_"+path).Scan(&rows); err != nil {
			return err
		}
		if rows != m.Rows {
			return fmt.Errorf("%s row count changed", path)
		}
	}
	enc := json.NewEncoder(os.Stdout)
	version, err := check(ctx, db)
	if err != nil {
		return err
	}
	var mppSettings struct {
		AllowMPP, AllowTiFlashCop, EnforceMPP int
	}
	if err = conns["native"][0].QueryRowContext(ctx, "SELECT @@tidb_allow_mpp,@@tidb_allow_tiflash_cop,@@tidb_enforce_mpp").Scan(&mppSettings.AllowMPP, &mppSettings.AllowTiFlashCop, &mppSettings.EnforceMPP); err != nil {
		return err
	}
	if err = enc.Encode(map[string]any{"Kind": "environment", "Corpus": m, "CurrentVersion": version, "MPPSettings": mppSettings, "Options": struct {
		Concurrency, Iterations, Trials, Warmup, Threads int
		Stopwords                                        bool
		Duration, Timeout, Path, Search, ProfileFile     string
	}{o.concurrency, o.iterations, o.trials, o.warmup, o.threads, o.stopwords, o.duration.String(), o.timeout.String(), o.path, o.search, o.profileFile}, "CorpusKind": "repetitive synthetic baseline"}); err != nil {
		return err
	}
	selected, _ := searches(o.search)
	for _, search := range selected {
		predicate := "MATCH(body) AGAINST('" + search + "' IN BOOLEAN MODE)"
		var want int64
		var fingerprint string
		for _, path := range []string{"local", "native"} {
			query := "SELECT COUNT(*) FROM docs_" + path + " WHERE " + predicate
			verifyCtx, cancel := context.WithTimeout(ctx, o.timeout)
			p, err := plan(verifyCtx, conns[path][0], query, path)
			if err != nil {
				cancel()
				return err
			}
			count, hash, err := resultFingerprint(verifyCtx, conns[path][0], predicate, path)
			cancel()
			if err != nil {
				return err
			}
			if path == "local" {
				want, fingerprint = count, hash
			} else if count != want || hash != fingerprint {
				return fmt.Errorf("native/local result mismatch for %s", search)
			}
			if err = enc.Encode(map[string]any{"Kind": "verification", "Path": path, "Search": search, "Count": count, "IDSHA256": hash, "Plan": p}); err != nil {
				return err
			}
		}
		for trial := 1; trial <= o.trials; trial++ {
			paths := []string{"local", "native"}
			if o.path != "both" {
				paths = []string{o.path}
			}
			if len(paths) == 2 && trial%2 == 0 {
				paths[0], paths[1] = paths[1], paths[0]
			}
			for _, path := range paths {
				query := "SELECT COUNT(*) FROM docs_" + path + " WHERE " + predicate
				mResult, measureErr := measure(ctx, conns[path], query, want, o)
				mResult.Path, mResult.Search, mResult.Schema, mResult.Trial = path, search, m.Schema, trial
				if err = enc.Encode(mResult); err != nil {
					return err
				}
				if measureErr != nil {
					return measureErr
				}
			}
		}
	}
	return nil
}

func main() {
	o, err := parseOptions(os.Args[1:])
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	db, err := openDB(o.dsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer db.Close()
	db.SetMaxOpenConns(2*o.concurrency + 2)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	switch o.mode {
	case "check":
		var version string
		checkCtx, stop := context.WithTimeout(ctx, 10*time.Second)
		version, err = check(checkCtx, db)
		stop()
		if err == nil {
			fmt.Println(version)
		}
	case "setup":
		setupCtx, stop := context.WithTimeout(ctx, o.timeout)
		err = setup(setupCtx, db, o)
		stop()
	case "run":
		err = run(ctx, db, o)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
