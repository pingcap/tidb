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

// Command sessionmigration checks frontend continuity through a released TiProxy.
package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"
)

func main() {
	mode := flag.String("mode", "COMPATIBLE", "COMPATIBLE, TRANSITION, or STRICT (the latter two require two new binaries)")
	keep := flag.Bool("keep", false, "keep temporary stores, certificates, and logs after success")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	ctx, timeout := context.WithTimeout(ctx, 4*time.Minute)
	defer timeout()
	dir, err := os.MkdirTemp("", "tidb-sessionmigration-")
	if err == nil {
		err = run(ctx, dir, strings.ToUpper(*mode))
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: %v\nArtifacts: %s\n", err, dir)
		os.Exit(1)
	}
	if *keep {
		fmt.Printf("Artifacts: %s\n", dir)
	} else {
		_ = os.RemoveAll(dir)
	}
}

// A gate closes only its listener during draining. Existing byte streams remain
// intact, so TiProxy must migrate them after its static-backend health check
// fails. This avoids both patching TiProxy and killing the source SQL session.
type gate struct {
	addr, target string
	mu           sync.Mutex
	listener     net.Listener
	conns        map[net.Conn]struct{}
	closed       bool
	wg           sync.WaitGroup
}

func (g *gate) enable() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	ln, err := net.Listen("tcp", g.addr)
	if err != nil {
		return err
	}
	g.listener = ln
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		for {
			front, err := ln.Accept()
			if err != nil {
				return
			}
			g.mu.Lock()
			if g.closed {
				g.mu.Unlock()
				_ = front.Close()
				return
			}
			g.conns[front] = struct{}{}
			g.wg.Add(1)
			g.mu.Unlock()
			go g.forward(front)
		}
	}()
	return nil
}

func (g *gate) forward(front net.Conn) {
	defer g.wg.Done()
	defer func() {
		_ = front.Close()
		g.mu.Lock()
		delete(g.conns, front)
		g.mu.Unlock()
	}()
	back, err := net.DialTimeout("tcp", g.target, time.Second)
	if err != nil {
		return
	}
	defer back.Close()
	done := make(chan struct{})
	go func() {
		_, _ = io.Copy(back, front)
		_ = back.Close()
		close(done)
	}()
	_, _ = io.Copy(front, back)
	_ = front.Close()
	<-done
}

func (g *gate) disable() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.listener != nil {
		_ = g.listener.Close()
		g.listener = nil
	}
}

func (g *gate) close() {
	g.mu.Lock()
	g.closed = true
	if g.listener != nil {
		_ = g.listener.Close()
	}
	for conn := range g.conns {
		_ = conn.Close()
	}
	g.mu.Unlock()
	g.wg.Wait()
}

func freeAddr() (string, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	addr := ln.Addr().String()
	return addr, ln.Close()
}

func start(ctx context.Context, dir, name, binary string, args ...string) (func(), error) {
	log, err := os.Create(filepath.Join(dir, name+".log"))
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		_ = log.Close()
		return nil, err
	}
	done := make(chan struct{})
	go func() { _ = cmd.Wait(); close(done) }()
	return func() {
		select {
		case <-done:
		default:
			_ = cmd.Process.Signal(syscall.SIGTERM)
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				_ = cmd.Process.Kill()
				<-done
			}
		}
		_ = log.Close()
	}, nil
}

func certificates(dir string) error {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "sessionmigration-local"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true, IsCA: true, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}
	cert, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return err
	}
	der, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "cert.pem"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert}), 0600); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "key.pem"), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der}), 0600)
}

func openDB(addr, user, network string) (*sql.DB, error) {
	cfg := mysql.NewConfig()
	cfg.User, cfg.Net, cfg.Addr = user, network, addr
	if user != "root" {
		cfg.Passwd = "migration-local-test-only"
	}
	cfg.TLSConfig = "migration-local"
	cfg.Timeout, cfg.ReadTimeout, cfg.WriteTimeout = 3*time.Second, 10*time.Second, 10*time.Second
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err == nil {
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
	}
	return db, err
}

func wait(ctx context.Context, f func() error) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	var last error
	for {
		if last = f(); last == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("readiness timeout: %w (last error: %v)", ctx.Err(), last)
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func api(ctx context.Context, addr, method, path string, body any) error {
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://"+addr+path, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("TiProxy %s %s: status %d", method, path, resp.StatusCode)
	}
	return nil
}

func run(ctx context.Context, dir, mode string) error {
	if mode != "COMPATIBLE" && mode != "TRANSITION" && mode != "STRICT" {
		return errors.New("invalid -mode")
	}
	bins := []string{os.Getenv("TIDB_OLD_BIN"), os.Getenv("TIDB_NEW_BIN"), os.Getenv("TIPROXY_BIN")}
	for _, bin := range bins {
		if bin == "" {
			return errors.New("set absolute TIDB_OLD_BIN, TIDB_NEW_BIN, and TIPROXY_BIN paths")
		}
		if !filepath.IsAbs(bin) {
			return errors.New("binary paths must be absolute")
		}
	}
	version, err := exec.CommandContext(ctx, bins[2], "--version").CombinedOutput()
	if err != nil {
		return fmt.Errorf("checking TiProxy version: %w", err)
	}
	if !strings.Contains(string(version), "v1.3.2") {
		return errors.New("TIPROXY_BIN must be an unchanged TiProxy v1.3.2 binary")
	}
	if err := certificates(dir); err != nil {
		return err
	}
	// TLS is deliberately local-only; both backends share the signing key.
	if err := mysql.RegisterTLSConfig("migration-local", &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true}); err != nil {
		return err
	}
	defer mysql.DeregisterTLSConfig("migration-local")
	var sqlAddrs [2]string
	var gates [2]*gate
	for i, name := range []string{"old", "new"} {
		sqlAddr, err := freeAddr()
		if err != nil {
			return err
		}
		statusAddr, err := freeAddr()
		if err != nil {
			return err
		}
		_, sqlPort, _ := net.SplitHostPort(sqlAddr)
		_, statusPort, _ := net.SplitHostPort(statusAddr)
		conf := fmt.Sprintf("host = '127.0.0.1'\nport = %s\nstore = 'mocktikv'\npath = %q\n[status]\nstatus-host = '127.0.0.1'\nstatus-port = %s\n[security]\nssl-cert = %q\nssl-key = %q\nsession-token-signing-cert = %q\nsession-token-signing-key = %q\n", sqlPort, filepath.Join(dir, name+"-store"), statusPort, filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem"), filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem"))
		confPath := filepath.Join(dir, name+".toml")
		if err := os.WriteFile(confPath, []byte(conf), 0600); err != nil {
			return err
		}
		stop, err := start(ctx, dir, name, bins[i], "--config", confPath)
		if err != nil {
			return err
		}
		defer stop()
		db, err := openDB(sqlAddr, "root", "tcp")
		if err != nil {
			return err
		}
		defer db.Close()
		if err := wait(ctx, func() error { return db.PingContext(ctx) }); err != nil {
			return fmt.Errorf("%s TiDB: %w", name, err)
		}
		for _, query := range []string{
			"CREATE DATABASE migration_test",
			"CREATE TABLE migration_test.t (n int)",
			"INSERT INTO migration_test.t VALUES (40)",
			"CREATE USER migration_user IDENTIFIED BY 'migration-local-test-only'",
			"GRANT ALL ON migration_test.* TO migration_user",
		} {
			if _, err := db.ExecContext(ctx, query); err != nil {
				return fmt.Errorf("%s bootstrap: %w", name, err)
			}
		}
		if mode != "COMPATIBLE" {
			if _, err := db.ExecContext(ctx, "SET GLOBAL tidb_session_token_mode='"+mode+"'"); err != nil {
				return fmt.Errorf("%s mode: %w", name, err)
			}
		}
		addr, err := freeAddr()
		if err != nil {
			return err
		}
		g := &gate{addr: addr, target: sqlAddr, conns: make(map[net.Conn]struct{})}
		defer g.close()
		sqlAddrs[i], gates[i] = sqlAddr, g
	}
	if err := gates[0].enable(); err != nil {
		return err
	}
	proxyAddr, err := freeAddr()
	if err != nil {
		return err
	}
	apiAddr, err := freeAddr()
	if err != nil {
		return err
	}
	proxyConf := fmt.Sprintf("workdir = %q\n[proxy]\naddr = %q\npd-addrs = ''\ngraceful-close-conn-timeout = 0\n[api]\naddr = %q\n[security.sql-tls]\nskip-ca = true\n[security.server-tls]\nauto-certs = true\nrsa-key-size = 2048\n[balance]\npolicy = 'connection'\n", filepath.Join(dir, "proxy-work"), proxyAddr, apiAddr)
	proxyConfPath := filepath.Join(dir, "proxy.toml")
	if err := os.WriteFile(proxyConfPath, []byte(proxyConf), 0600); err != nil {
		return err
	}
	stop, err := start(ctx, dir, "tiproxy", bins[2], "--config", proxyConfPath)
	if err != nil {
		return err
	}
	defer stop()
	namespace := map[string]any{"namespace": "default", "frontend": map[string]any{"user": "migration_user"}, "backend": map[string]any{"instances": []string{gates[0].addr, gates[1].addr}}}
	if err := wait(ctx, func() error { return api(ctx, apiAddr, http.MethodPut, "/api/admin/namespace/default", namespace) }); err != nil {
		return err
	}
	if err := api(ctx, apiAddr, http.MethodPost, "/api/admin/namespace/commit", nil); err != nil {
		return err
	}
	if err := wait(ctx, func() error { return api(ctx, apiAddr, http.MethodGet, "/api/debug/health", nil) }); err != nil {
		return err
	}
	var dials atomic.Int32
	mysql.RegisterDialContext("migration-frontend", func(ctx context.Context, addr string) (net.Conn, error) {
		if dials.Add(1) != 1 {
			return nil, errors.New("frontend reconnection attempted")
		}
		return (&net.Dialer{Timeout: 3 * time.Second}).DialContext(ctx, "tcp", addr)
	})
	frontDB, err := openDB(proxyAddr, "migration_user", "migration-frontend")
	if err != nil {
		return err
	}
	defer frontDB.Close()
	front, err := frontDB.Conn(ctx)
	if err != nil {
		return err
	}
	defer front.Close()
	for _, query := range []string{"USE migration_test", "SET @counter=41, @arg=2, time_zone='+08:00'", "PREPARE text_stmt FROM 'SELECT n+? FROM t'"} {
		if _, err := front.ExecContext(ctx, query); err != nil {
			return err
		}
	}
	stmt, err := front.PrepareContext(ctx, "SELECT n+?+@counter FROM t")
	if err != nil {
		return err
	}
	defer stmt.Close()
	check := func(wantPort string, counter int) error {
		var db, zone string
		var actualCounter, port, value int
		if err := front.QueryRowContext(ctx, "SELECT DATABASE(), @@time_zone, @counter, @@port").Scan(&db, &zone, &actualCounter, &port); err != nil {
			return err
		}
		if db != "migration_test" || zone != "+08:00" || actualCounter != counter || strconv.Itoa(port) != wantPort {
			return fmt.Errorf("state mismatch: db=%s zone=%s counter=%d port=%d (want %s)", db, zone, actualCounter, port, wantPort)
		}
		if err := front.QueryRowContext(ctx, "EXECUTE text_stmt USING @arg").Scan(&value); err != nil || value != 42 {
			return fmt.Errorf("text prepared statement: value=%d err=%v", value, err)
		}
		if err := stmt.QueryRowContext(ctx, 2).Scan(&value); err != nil || value != 42+counter {
			return fmt.Errorf("binary prepared statement: value=%d err=%v", value, err)
		}
		if dials.Load() != 1 {
			return fmt.Errorf("frontend dial count is %d", dials.Load())
		}
		return nil
	}
	_, initialPort, _ := net.SplitHostPort(sqlAddrs[0])
	if err := check(initialPort, 41); err != nil {
		return err
	}
	for step, target := range []int{1, 0} {
		if err := gates[target].enable(); err != nil {
			return err
		}
		gates[1-target].disable()
		_, wantPort, _ := net.SplitHostPort(sqlAddrs[target])
		// Query failures are fatal; only a still-unchanged backend is polled.
		deadline := time.Now().Add(45 * time.Second)
		for {
			var port string
			if err := front.QueryRowContext(ctx, "SELECT @@port").Scan(&port); err != nil {
				return fmt.Errorf("migration %d broke frontend: %w", step+1, err)
			}
			if port == wantPort {
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("migration %d did not reach backend port %s", step+1, wantPort)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
		}
		counter := 41 + step
		if err := check(wantPort, counter); err != nil {
			return fmt.Errorf("migration %d: %w", step+1, err)
		}
		if _, err := front.ExecContext(ctx, "SET @counter=@counter+1"); err != nil {
			return err
		}
		if err := check(wantPort, counter+1); err != nil {
			return err
		}
		fmt.Printf("PASS migration %d: backend=%s mode=%s frontend_dials=%d db/time_zone/user_variable/text_prepare/binary_prepare preserved\n", step+1, sqlAddrs[target], mode, dials.Load())
	}
	return nil
}
