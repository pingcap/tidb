// Copyright 2022 PingCAP, Inc.
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

package importer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pingcap/tidb/br/pkg/lightning/importer/kv"
	"github.com/tikv/client-go/v2/tikv"
	"golang.org/x/sync/errgroup"
)

const (
	defaultServerAddr = ":8287"
	defaultPDAddr     = "127.0.0.1:2379"
	defaultDataDir    = "/tmp/importer"

	queryParamStartKey  = "startKey"
	queryParamEndKey    = "endKey"
	queryParamEndpoints = "endpoints"

	defaultPropKeysIndexDistance = 4096
	defaultPropSizeIndexDistance = 1 << 20
)

type serverOptions struct {
	addr    string
	pdAddr  string
	dataDir string
}

var defaultServerOptions = serverOptions{
	addr:    defaultServerAddr,
	pdAddr:  defaultPDAddr,
	dataDir: defaultDataDir,
}

type funcServerOption struct {
	f func(*serverOptions)
}

func (fdo *funcServerOption) apply(do *serverOptions) {
	fdo.f(do)
}

func newFuncServerOption(f func(*serverOptions)) *funcServerOption {
	return &funcServerOption{
		f: f,
	}
}

type ServerOption interface {
	apply(*serverOptions)
}

func WithServerAddr(addr string) ServerOption {
	return newFuncServerOption(func(o *serverOptions) {
		o.addr = addr
	})
}

func WithPDAddr(pdAddr string) ServerOption {
	return newFuncServerOption(func(o *serverOptions) {
		o.pdAddr = pdAddr
	})
}

func WithDataDir(dataDir string) ServerOption {
	return newFuncServerOption(func(o *serverOptions) {
		o.dataDir = dataDir
	})
}

type Server struct {
	opts serverOptions

	kvStore *tikv.KVStore

	idGen       atomic.Int64
	compacted   atomic.Bool
	compactOnce sync.Once
	pendingSSTs struct {
		sync.Mutex
		ssts []*kv.SSTMeta
	}
	sortedSSTs []*kv.SSTMeta
}

func NewServer(opts ...ServerOption) (*Server, error) {
	options := defaultServerOptions
	for _, opt := range opts {
		opt.apply(&options)
	}
	if err := os.MkdirAll(options.dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir %s: %w", options.dataDir, err)
	}
	_ = filepath.Walk(options.dataDir,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				return nil
			}
			if strings.HasSuffix(path, ".sst") {
				return os.Remove(path)
			}
			return nil
		})

	return &Server{
		opts: options,
	}, nil
}

func (s *Server) Run() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/read", s.handleRead)
	mux.HandleFunc("/write", s.handleWrite)
	mux.HandleFunc("/compact", s.handleCompact)
	mux.HandleFunc("/import", s.handleImport)
	mux.HandleFunc("/properties", s.handleProperties)

	httpServer := http.Server{
		Addr:    s.opts.addr,
		Handler: mux,
	}
	log.Printf("server listening on %s", s.opts.addr)
	return httpServer.ListenAndServe()
}

func (s *Server) handleRead(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		w.Header().Set("Allow", http.MethodGet)
		return
	}
	if !s.compacted.Load() {
		writeError(w, fmt.Errorf("ssts has not been compacted"))
		return
	}

	startKey := []byte(r.URL.Query().Get(queryParamStartKey))
	endKey := []byte(r.URL.Query().Get(queryParamEndKey))

	sw := kv.NewStreamWriter(nopWriteCloser{w})
	for _, sst := range s.sortedSSTs {
		if !sst.Range.Overlaps(kv.KeyRange{StartKey: startKey, EndKey: endKey}) {
			continue
		}
		if err := func() error {
			reader, err := kv.NewSSTReader(sst.Path, sst.Range)
			if err != nil {
				return err
			}
			defer reader.Close()
			return kv.Copy(sw, reader)
		}(); err != nil {
			writeError(w, err)
			return
		}
	}
	if err := sw.Close(); err != nil {
		writeError(w, err)
	}
}

type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }

func (s *Server) handleWrite(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		w.Header().Set("Allow", http.MethodPost)
		return
	}
	if s.compacted.Load() {
		writeError(w, fmt.Errorf("ssts has been compacted"))
		return
	}

	sr := kv.NewStreamReader(r.Body)
	defer sr.Close()
	sst, err := s.writeSSTUnsorted(sr)
	if err != nil {
		writeError(w, err)
		return
	}
	if sst.TotalKeys == 0 {
		_ = os.Remove(sst.Path)
		return
	}
	log.Printf("write new sst %s", filepath.Base(sst.Path))
	s.pendingSSTs.Lock()
	s.pendingSSTs.ssts = append(s.pendingSSTs.ssts, sst)
	s.pendingSSTs.Unlock()
}

func (s *Server) handleCompact(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		w.Header().Set("Allow", http.MethodPost)
		return
	}
	s.compactOnce.Do(func() {
		s.compact()
		s.compacted.Store(true)
	})
}

func (s *Server) handleImport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		w.Header().Set("Allow", http.MethodPost)
		return
	}
	if !s.compacted.Load() {
		writeError(w, fmt.Errorf("ssts has not been compacted"))
		return
	}

	startKey := []byte(r.URL.Query().Get(queryParamStartKey))
	endKey := []byte(r.URL.Query().Get(queryParamEndKey))
	kr := kv.KeyRange{StartKey: startKey, EndKey: endKey}
	endpoints := strings.Split(r.URL.Query().Get(queryParamEndpoints), ",")

	ingestor, err := NewIngestor(s.opts.pdAddr)
	if err != nil {
		writeError(w, err)
		return
	}
	defer ingestor.Close()

	if err := ingestor.Ingest(r.Context(), kr, endpoints); err != nil {
		writeError(w, err)
		return
	}
}

func (s *Server) handleProperties(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		w.Header().Set("Allow", http.MethodGet)
		return
	}
	if !s.compacted.Load() {
		writeError(w, fmt.Errorf("ssts has not been compacted"))
		return
	}

	var props []kv.Properties
	for _, sst := range s.sortedSSTs {
		props = append(props, sst.Properties)
	}
	merged := kv.MergeProperties(props...)
	if err := json.NewEncoder(w).Encode(&merged); err != nil {
		writeError(w, err)
	}
}

func (s *Server) compact() {
	s.pendingSSTs.Lock()
	defer s.pendingSSTs.Unlock()

	ssts := s.pendingSSTs.ssts
	sort.Slice(ssts, func(i, j int) bool {
		return ssts[i].Range.Less(ssts[j].Range)
	})

	resultCh := make(chan *kv.SSTMeta, len(ssts))
	eg, _ := errgroup.WithContext(context.Background())
	eg.SetLimit(16)
	lastIdx := 0
	for i := 1; i < len(ssts); i++ {
		if !ssts[i].Range.Overlaps(ssts[lastIdx].Range) {
			overlappedSSTs := ssts[lastIdx : i-1]
			lastIdx = i
			eg.Go(func() error {
				result, err := s.compactSSTs(overlappedSSTs)
				if err != nil {
					return err
				}
				if result.TotalKeys == 0 {
					_ = os.Remove(result.Path)
					return nil
				}
				resultCh <- result
				return nil
			})
		}
	}
	eg.Go(func() error {
		result, err := s.compactSSTs(ssts[lastIdx:])
		if err != nil {
			return err
		}
		if result != nil {
			resultCh <- result
		}
		return nil
	})

	if err := eg.Wait(); err != nil {
		log.Fatalf("compact sst files: %v", err)
	}

	close(resultCh)
	var results []*kv.SSTMeta
	for result := range resultCh {
		results = append(results, result)
	}
	s.sortedSSTs = results

	for _, sst := range ssts {
		_ = os.Remove(sst.Path)
	}
}

func (s *Server) compactSSTs(ssts []*kv.SSTMeta) (*kv.SSTMeta, error) {
	sstNames := make([]string, 0, len(ssts))
	for _, sst := range ssts {
		sstNames = append(sstNames, filepath.Base(sst.Path))
	}
	log.Printf("compacting %s", strings.Join(sstNames, ","))

	var readers []kv.Reader
	closeAll := func() {
		for _, r := range readers {
			_ = r.Close()
		}
	}
	defer closeAll()

	for _, sst := range ssts {
		r, err := kv.NewSSTReader(sst.Path, sst.Range)
		if err != nil {
			return nil, err
		}
		readers = append(readers, r)
	}

	mergedReader, err := kv.MergeReaders(readers...)
	if err != nil {
		return nil, err
	}
	return s.writeSSTSorted(mergedReader)
}

func (s *Server) writeSSTSorted(r kv.Reader) (*kv.SSTMeta, error) {
	sstPath := s.generateSSTPath()
	sstWriter, err := kv.NewSSTWriter(sstPath)
	if err != nil {
		return nil, err
	}
	pc := kv.NewPropertiesCollector(sstWriter, defaultPropSizeIndexDistance, defaultPropKeysIndexDistance)

	if err := kv.Copy(pc, r); err != nil {
		_ = pc.Close()
		_ = os.Remove(sstPath)
		return nil, err
	}
	if err := pc.Close(); err != nil {
		return nil, err
	}
	return &kv.SSTMeta{
		Path:       sstPath,
		Properties: pc.Properties(),
	}, nil
}

func (s *Server) writeSSTUnsorted(r kv.Reader) (*kv.SSTMeta, error) {
	var (
		buf  []byte
		keys [][]byte
		vals [][]byte
	)

	clone := func(b []byte) []byte {
		if len(buf) < len(b) {
			buf = make([]byte, 1<<20)
		}
		cloned := buf[:len(b)]
		buf = buf[len(b):]
		copy(cloned, b)
		return cloned
	}

	for {
		key, val, err := r.Read()
		if err != nil {
			return nil, err
		}
		if key == nil {
			break
		}
		keys = append(keys, clone(key))
		vals = append(vals, clone(val))
	}
	sort.Sort(keySorter{keys: keys, vals: vals})

	sstPath := s.generateSSTPath()
	sstWriter, err := kv.NewSSTWriter(sstPath)
	if err != nil {
		return nil, err
	}
	pc := kv.NewPropertiesCollector(sstWriter, defaultPropSizeIndexDistance, defaultPropKeysIndexDistance)

	for i := range keys {
		if err := pc.Write(keys[i], vals[i]); err != nil {
			_ = pc.Close()
			_ = os.Remove(sstPath)
			return nil, err
		}
	}
	if err := pc.Close(); err != nil {
		return nil, err
	}

	return &kv.SSTMeta{
		Path:       sstPath,
		Properties: pc.Properties(),
	}, nil
}

type keySorter struct {
	keys [][]byte
	vals [][]byte
}

func (ks keySorter) Len() int {
	return len(ks.keys)
}

func (ks keySorter) Less(i, j int) bool {
	return bytes.Compare(ks.keys[i], ks.keys[j]) < 0
}

func (ks keySorter) Swap(i, j int) {
	ks.keys[i], ks.keys[j] = ks.keys[j], ks.keys[i]
	ks.vals[i], ks.vals[j] = ks.vals[j], ks.vals[i]
}

func (s *Server) generateSSTPath() string {
	return fmt.Sprintf("%s/%d.sst", s.opts.dataDir, s.idGen.Add(1))
}

func writeError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusInternalServerError)
	w.Write([]byte(err.Error()))
	log.Printf("server encountered error: %v", err)
}
