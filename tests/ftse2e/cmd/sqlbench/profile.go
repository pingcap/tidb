// Copyright 2026 PingCAP, Inc. Licensed under Apache License 2.0.
package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"time"
)

func validateProfileOptions(o options) error {
	if o.profileURL == "" && o.profileFile == "" {
		return nil
	}
	if o.profileFormat != "protobuf" && o.profileFormat != "svg" {
		return fmt.Errorf("invalid profile format")
	}
	u, err := url.Parse(o.profileURL)
	if err != nil || u.Scheme != "http" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") || !net.ParseIP(u.Hostname()).IsLoopback() {
		return fmt.Errorf("profile URL must be an HTTP loopback status address")
	}
	if o.mode != "run" || o.path != "native" || o.search == "all" || o.concurrency != 1 || o.trials != 1 || o.profileFile == "" || o.profileSeconds < 1 || o.profileSeconds > 300 || o.duration < time.Duration(o.profileSeconds+2)*time.Second {
		return fmt.Errorf("profiling requires run/native, one search/connection/trial, a new output file, seconds 1..300, and duration >= seconds+2")
	}
	return nil
}

// Profiling is process-wide, not SQL-specific. Correctness/plan verification
// and warmup finish first; this request overlaps only the selected workload.
func collectCPUProfile(ctx context.Context, o options) <-chan error {
	done := make(chan error, 1)
	go func() {
		var result error
		defer func() { done <- result }()
		f, err := os.OpenFile(o.profileFile, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			result = err
			return
		}
		defer f.Close()
		u, _ := url.Parse(o.profileURL)
		u.Path = "/debug/pprof/profile"
		u.RawQuery = fmt.Sprintf("seconds=%d&frequency=99", o.profileSeconds)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			result = err
			return
		}
		if o.profileFormat == "protobuf" {
			req.Header.Set("Content-Type", "application/protobuf")
		}
		client := &http.Client{Timeout: time.Duration(o.profileSeconds+30) * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return fmt.Errorf("profile redirect rejected") }}
		resp, err := client.Do(req)
		if err != nil {
			result = err
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			result = fmt.Errorf("profile HTTP %d: %s", resp.StatusCode, body)
			return
		}
		n, err := io.Copy(f, io.LimitReader(resp.Body, (64<<20)+1))
		if err != nil {
			result = err
			return
		}
		if n == 0 || n > 64<<20 {
			result = fmt.Errorf("invalid profile size %d", n)
			return
		}
		result = f.Sync()
	}()
	return done
}
