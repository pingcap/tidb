// Copyright 2026 PingCAP, Inc. Licensed under Apache License 2.0.
package main

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestProfileOptionGuards(t *testing.T) {
	base := []string{"-mode", "run", "-manifest", "corpus.json", "-path", "native", "-search", "word", "-trials", "1", "-duration", "32s", "-profile-url", "http://127.0.0.1:40292", "-profile-file", "new.pb"}
	if _, err := parseOptions(base); err != nil {
		t.Fatal(err)
	}
	for _, extra := range [][]string{{"-path", "both"}, {"-search", "all"}, {"-concurrency", "2"}, {"-trials", "2"}, {"-duration", "30s"}, {"-profile-url", "http://10.0.0.1:40292"}, {"-profile-url", "http://127.0.0.1:40292/other"}, {"-profile-format", "unknown"}} {
		if _, err := parseOptions(append(append([]string{}, base...), extra...)); err == nil {
			t.Fatalf("accepted %v", extra)
		}
	}
	for _, search := range []string{"all", "word", "phrase", "prefix", "cjk", "miss"} {
		if _, err := searches(search); err != nil {
			t.Fatal(err)
		}
	}
}

func TestProfileStartsAfterWarmupAndPropagatesFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for range 2 {
		mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(7))
	}
	c, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error("profile started before warmup finished:", err)
		}
		http.Error(w, "profiling unavailable", 500)
	}))
	defer server.Close()
	_, err = measure(context.Background(), []*sql.Conn{c}, "SELECT COUNT(*)", 7, options{
		warmup: 2, timeout: time.Second, duration: 3 * time.Second,
		profileURL: server.URL, profileFile: filepath.Join(t.TempDir(), "failed"), profileSeconds: 1,
	})
	if err == nil {
		t.Fatal("profiling error was ignored")
	}
}

func TestCollectCPUProfile(t *testing.T) {
	for _, format := range []string{"protobuf", "svg"} {
		t.Run(format, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/debug/pprof/profile" || r.URL.Query().Get("frequency") != "99" || r.URL.Query().Get("seconds") != "1" {
					t.Error("incorrect sampling request")
				}
				if (r.Header.Get("Content-Type") == "application/protobuf") != (format == "protobuf") {
					t.Error("incorrect format")
				}
				_, _ = w.Write([]byte("test profile"))
			}))
			defer server.Close()
			o := options{profileURL: server.URL, profileFile: filepath.Join(t.TempDir(), "profile"), profileSeconds: 1, profileFormat: format}
			if err := <-collectCPUProfile(context.Background(), o); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(o.profileFile)
			if err != nil || string(data) != "test profile" {
				t.Fatalf("profile not saved: %s %v", data, err)
			}
			if err := <-collectCPUProfile(context.Background(), o); err == nil {
				t.Fatal("overwrote profile")
			}
		})
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "busy", 409) }))
	defer server.Close()
	if err := <-collectCPUProfile(context.Background(), options{profileURL: server.URL, profileFile: filepath.Join(t.TempDir(), "failed"), profileSeconds: 1}); err == nil {
		t.Fatal("accepted failed profiling")
	}
}
