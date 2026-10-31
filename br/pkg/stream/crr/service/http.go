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

package service

import (
	"encoding/json"
	"net/http"
)

// Register installs the CRR status endpoints onto the provided mux.
func (s *Service) Register(mux *http.ServeMux) {
	if mux == nil {
		panic("service: nil mux")
	}
	mux.HandleFunc("/livez", s.handleLiveness)
	mux.HandleFunc("/readyz", s.handleReadiness)
	mux.HandleFunc("/status", s.handleStatus)
}

func (s *Service) handleLiveness(w http.ResponseWriter, _ *http.Request) {
	snapshot := s.Status()
	if !snapshot.Live {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Service) handleReadiness(w http.ResponseWriter, _ *http.Request) {
	snapshot := s.Status()
	if !snapshot.Ready {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Service) handleStatus(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(s.Status()); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
