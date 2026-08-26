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

package server

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/metrics"
	storeerr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const (
	diagnosticProtocolVersion = "1.0"
	diagnosticCursorVersion   = 1
	maxDiagnosticCursorBytes  = 4096
)

type diagnosticAPIHandler struct {
	dom              *domain.Domain
	cfg              config.DiagnosticAPI
	requestTimeout   time.Duration
	cursorTTL        time.Duration
	semaphore        chan struct{}
	allowedDatasets  map[string]struct{}
	cursorSigningKey [sha256.Size]byte
	redactor         *diagnosticRedactor
	now              func() time.Time
}

type diagnosticCursor struct {
	Version    int    `json:"v"`
	Dataset    string `json:"dataset"`
	SnapshotTS uint64 `json:"snapshot_ts"`
	IssuedAt   int64  `json:"issued_at"`
	DBID       int64  `json:"db_id,omitempty"`
	TableID    int64  `json:"table_id,omitempty"`
	SubID      int64  `json:"sub_id,omitempty"`
	RowID      int64  `json:"row_id,omitempty"`
}

type diagnosticEnvelope struct {
	ProtocolVersion   string `json:"protocol_version"`
	Dataset           string `json:"dataset"`
	SnapshotID        string `json:"snapshot_id"`
	SnapshotTS        uint64 `json:"snapshot_ts"`
	SchemaVersion     int64  `json:"schema_version"`
	SnapshotStartedAt string `json:"snapshot_started_at"`
	CapturedAt        string `json:"captured_at"`
	SensitivityLevel  string `json:"sensitivity_level"`
	RedactionProfile  string `json:"redaction_profile"`
	RedactionVersion  int    `json:"redaction_version"`
	RedactionKeyID    string `json:"redaction_key_id,omitempty"`
	RecordCount       int    `json:"record_count"`
	Records           []any  `json:"records"`
	NextCursor        string `json:"next_cursor,omitempty"`
	Complete          bool   `json:"complete"`
}

type diagnosticErrorResponse struct {
	Code              string `json:"code"`
	Message           string `json:"message"`
	Retryable         bool   `json:"retryable"`
	RestartSnapshot   bool   `json:"restart_snapshot,omitempty"`
	SuggestedPageSize int    `json:"suggested_page_size,omitempty"`
}

type diagnosticCapabilities struct {
	ProtocolVersion string                        `json:"protocol_version"`
	RequiresMTLS    bool                          `json:"requires_mtls"`
	SnapshotModel   string                        `json:"snapshot_model"`
	Redaction       diagnosticRedactionCapability `json:"redaction"`
	Limits          diagnosticCapabilityLimits    `json:"limits"`
	Datasets        []diagnosticDatasetCapability `json:"datasets"`
}

type diagnosticCapabilityLimits struct {
	MaxConcurrentRequests uint   `json:"max_concurrent_requests"`
	DefaultPageSize       uint   `json:"default_page_size"`
	MaxPageSize           uint   `json:"max_page_size"`
	RequestTimeout        string `json:"request_timeout"`
	CursorTTL             string `json:"cursor_ttl"`
	MaxResponseBytes      uint64 `json:"max_response_bytes"`
}

type diagnosticDatasetCapability struct {
	Name             string                  `json:"name"`
	SensitivityLevel string                  `json:"sensitivity_level"`
	RedactionProfile string                  `json:"redaction_profile"`
	RedactionVersion int                     `json:"redaction_version"`
	Fields           []string                `json:"fields"`
	FieldPolicies    []diagnosticFieldPolicy `json:"field_policies"`
}

func newDiagnosticAPIHandler(dom *domain.Domain, cfg config.DiagnosticAPI) (*diagnosticAPIHandler, error) {
	if dom == nil {
		return nil, errors.New("diagnostic API requires a domain")
	}
	requestTimeout, err := time.ParseDuration(cfg.RequestTimeout)
	if err != nil {
		return nil, errors.Trace(err)
	}
	cursorTTL, err := time.ParseDuration(cfg.CursorTTL)
	if err != nil {
		return nil, errors.Trace(err)
	}
	redactor, err := newDiagnosticRedactor(cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}
	h := &diagnosticAPIHandler{
		dom:             dom,
		cfg:             cfg,
		requestTimeout:  requestTimeout,
		cursorTTL:       cursorTTL,
		semaphore:       make(chan struct{}, cfg.MaxConcurrentRequests),
		allowedDatasets: make(map[string]struct{}, len(cfg.Datasets)),
		redactor:        redactor,
		now:             time.Now,
	}
	if _, err := rand.Read(h.cursorSigningKey[:]); err != nil {
		return nil, errors.Annotate(err, "generate diagnostic cursor signing key")
	}
	for _, dataset := range cfg.Datasets {
		if _, ok := diagnosticDatasetDescriptorFor(dataset); !ok {
			return nil, errors.Errorf("missing descriptor for diagnostic dataset %q", dataset)
		}
		h.allowedDatasets[dataset] = struct{}{}
	}
	return h, nil
}

func (h *diagnosticAPIHandler) serveCapabilities(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(w, r) {
		return
	}
	capabilities := diagnosticCapabilities{
		ProtocolVersion: diagnosticProtocolVersion,
		RequiresMTLS:    h.cfg.RequireMTLS,
		SnapshotModel:   "fixed-mvcc-per-dataset; restart on HTTP 409",
		Redaction:       h.redactor.capability(),
		Limits: diagnosticCapabilityLimits{
			MaxConcurrentRequests: h.cfg.MaxConcurrentRequests,
			DefaultPageSize:       h.cfg.DefaultPageSize,
			MaxPageSize:           h.cfg.MaxPageSize,
			RequestTimeout:        h.cfg.RequestTimeout,
			CursorTTL:             h.cfg.CursorTTL,
			MaxResponseBytes:      h.cfg.MaxResponseBytes,
		},
		Datasets: make([]diagnosticDatasetCapability, 0, len(h.cfg.Datasets)),
	}
	for _, dataset := range h.cfg.Datasets {
		descriptor, _ := diagnosticDatasetDescriptorFor(dataset)
		capabilities.Datasets = append(capabilities.Datasets, diagnosticDatasetCapability{
			Name:             dataset,
			SensitivityLevel: descriptor.SensitivityLevel,
			RedactionProfile: h.redactor.profile,
			RedactionVersion: h.redactor.version,
			Fields:           descriptor.fieldNames(),
			FieldPolicies:    descriptor.fieldPolicies(h.redactor.profile),
		})
	}
	h.writeJSON(w, http.StatusOK, capabilities)
}

func (h *diagnosticAPIHandler) serveDataset(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	metricDataset := "unknown"
	result := "error"
	responseBytes := 0
	defer func() {
		metrics.DiagnosticAPIRequestCounter.WithLabelValues(metricDataset, result).Inc()
		metrics.DiagnosticAPIRequestDuration.WithLabelValues(metricDataset).Observe(time.Since(started).Seconds())
		if responseBytes > 0 {
			metrics.DiagnosticAPIResponseBytes.WithLabelValues(metricDataset).Observe(float64(responseBytes))
		}
	}()

	if !h.authorize(w, r) {
		result = "unauthorized"
		return
	}
	dataset := mux.Vars(r)["dataset"]
	if _, ok := h.allowedDatasets[dataset]; !ok {
		result = "not_found"
		h.writeError(w, http.StatusNotFound, diagnosticErrorResponse{
			Code: "dataset_not_available", Message: "the diagnostic dataset is not enabled", Retryable: false,
		})
		return
	}
	metricDataset = dataset

	pageSize, err := h.parsePageSize(r)
	if err != nil {
		result = "bad_request"
		h.writeError(w, http.StatusBadRequest, diagnosticErrorResponse{
			Code: "invalid_page_size", Message: err.Error(), Retryable: false,
		})
		return
	}

	cursorToken := r.URL.Query().Get("cursor")
	cursorProvided := cursorToken != ""
	var cursor diagnosticCursor
	if cursorProvided {
		cursor, err = h.decodeCursor(cursorToken, dataset)
		if err != nil {
			result = "conflict"
			h.writeError(w, http.StatusConflict, diagnosticErrorResponse{
				Code: "snapshot_restart_required", Message: err.Error(), Retryable: true, RestartSnapshot: true,
			})
			return
		}
	}

	select {
	case h.semaphore <- struct{}{}:
		metrics.DiagnosticAPIActiveRequests.Inc()
		defer func() {
			metrics.DiagnosticAPIActiveRequests.Dec()
			<-h.semaphore
		}()
	default:
		result = "busy"
		w.Header().Set("Retry-After", "1")
		h.writeError(w, http.StatusTooManyRequests, diagnosticErrorResponse{
			Code: "concurrency_limit", Message: "the diagnostic API concurrency limit is exhausted", Retryable: true,
		})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.requestTimeout)
	defer cancel()
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnTools)

	if !cursorProvided {
		version, err := h.dom.Store().CurrentVersion(kv.GlobalTxnScope)
		if err != nil {
			result = "error"
			h.writeInternalError(w, dataset, err)
			return
		}
		cursor = diagnosticCursor{
			Version:    diagnosticCursorVersion,
			Dataset:    dataset,
			SnapshotTS: version.Ver,
			IssuedAt:   h.now().Unix(),
		}
	}

	snapshot := h.dom.Store().GetSnapshot(kv.NewVersion(cursor.SnapshotTS))
	readTimeoutMS := h.requestTimeout.Milliseconds()
	if readTimeoutMS < 1 {
		readTimeoutMS = 1
	}
	snapshot.SetOption(kv.TiKVClientReadTimeout, uint64(readTimeoutMS))
	reader := meta.NewReader(snapshot)
	schemaVersion, err := reader.GetSchemaVersion()
	if err != nil {
		if cursorProvided && storeerr.ErrTxnAbortedByGC.Equal(err) {
			result = "conflict"
			h.writeError(w, http.StatusConflict, diagnosticErrorResponse{
				Code: "snapshot_restart_required", Message: "the diagnostic snapshot is older than the GC safe point", Retryable: true, RestartSnapshot: true,
			})
			return
		}
		result = h.resultForContext(ctx)
		h.writeInternalError(w, dataset, err)
		return
	}

	records, next, complete, err := h.collectDiagnosticPage(ctx, reader, dataset, cursor, pageSize)
	if err != nil {
		if errors.ErrorEqual(err, errDiagnosticCursorState) || (cursorProvided && storeerr.ErrTxnAbortedByGC.Equal(err)) {
			result = "conflict"
			h.writeError(w, http.StatusConflict, diagnosticErrorResponse{
				Code: "snapshot_restart_required", Message: err.Error(), Retryable: true, RestartSnapshot: true,
			})
			return
		}
		result = h.resultForContext(ctx)
		h.writeInternalError(w, dataset, err)
		return
	}

	nextCursor := ""
	if !complete {
		nextCursor, err = h.encodeCursor(next)
		if err != nil {
			result = "error"
			h.writeInternalError(w, dataset, err)
			return
		}
	}
	now := h.now().UTC()
	descriptor, ok := diagnosticDatasetDescriptorFor(dataset)
	if !ok {
		result = "error"
		h.writeInternalError(w, dataset, errors.Errorf("missing descriptor for diagnostic dataset %q", dataset))
		return
	}
	envelope := diagnosticEnvelope{
		ProtocolVersion:   diagnosticProtocolVersion,
		Dataset:           dataset,
		SnapshotID:        strconv.FormatUint(cursor.SnapshotTS, 36),
		SnapshotTS:        cursor.SnapshotTS,
		SchemaVersion:     schemaVersion,
		SnapshotStartedAt: time.Unix(cursor.IssuedAt, 0).UTC().Format(time.RFC3339),
		CapturedAt:        now.Format(time.RFC3339Nano),
		SensitivityLevel:  descriptor.SensitivityLevel,
		RedactionProfile:  h.redactor.profile,
		RedactionVersion:  h.redactor.version,
		RedactionKeyID:    h.redactor.keyID,
		RecordCount:       len(records),
		Records:           records,
		NextCursor:        nextCursor,
		Complete:          complete,
	}
	body, err := json.Marshal(envelope)
	if err != nil {
		result = "error"
		h.writeInternalError(w, dataset, err)
		return
	}
	if uint64(len(body)) > h.cfg.MaxResponseBytes {
		result = "too_large"
		h.writeError(w, http.StatusRequestEntityTooLarge, diagnosticErrorResponse{
			Code: "page_too_large", Message: "the encoded page exceeds max-response-bytes", Retryable: true,
			SuggestedPageSize: max(1, pageSize/2),
		})
		return
	}

	responseBytes = len(body)
	result = "success"
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Diagnostic-Snapshot-ID", envelope.SnapshotID)
	w.Header().Set("X-Diagnostic-Record-Count", strconv.Itoa(len(records)))
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(body); err != nil {
		logutil.BgLogger().Warn("write diagnostic API response failed", zap.String("dataset", dataset), zap.Error(err))
	}
}

func (h *diagnosticAPIHandler) authorize(w http.ResponseWriter, r *http.Request) bool {
	if !h.cfg.RequireMTLS {
		return true
	}
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 || len(r.TLS.VerifiedChains) == 0 {
		h.writeError(w, http.StatusUnauthorized, diagnosticErrorResponse{
			Code: "client_certificate_required", Message: "a verified client certificate is required", Retryable: false,
		})
		return false
	}
	return true
}

func (h *diagnosticAPIHandler) parsePageSize(r *http.Request) (int, error) {
	raw := r.URL.Query().Get("page_size")
	if raw == "" {
		return int(h.cfg.DefaultPageSize), nil
	}
	pageSize, err := strconv.Atoi(raw)
	if err != nil || pageSize <= 0 {
		return 0, fmt.Errorf("page_size must be a positive integer")
	}
	if uint(pageSize) > h.cfg.MaxPageSize {
		return 0, fmt.Errorf("page_size must not exceed %d", h.cfg.MaxPageSize)
	}
	return pageSize, nil
}

func (h *diagnosticAPIHandler) encodeCursor(cursor diagnosticCursor) (string, error) {
	payload, err := json.Marshal(cursor)
	if err != nil {
		return "", errors.Trace(err)
	}
	mac := hmac.New(sha256.New, h.cursorSigningKey[:])
	_, _ = mac.Write(payload)
	return base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

func (h *diagnosticAPIHandler) decodeCursor(token, dataset string) (diagnosticCursor, error) {
	var cursor diagnosticCursor
	if len(token) > maxDiagnosticCursorBytes {
		return cursor, errors.New("the diagnostic cursor is too large")
	}
	payloadPart, signaturePart, ok := strings.Cut(token, ".")
	if !ok || payloadPart == "" || signaturePart == "" {
		return cursor, errors.New("the diagnostic cursor is malformed")
	}
	payload, err := base64.RawURLEncoding.DecodeString(payloadPart)
	if err != nil {
		return cursor, errors.New("the diagnostic cursor payload is malformed")
	}
	signature, err := base64.RawURLEncoding.DecodeString(signaturePart)
	if err != nil {
		return cursor, errors.New("the diagnostic cursor signature is malformed")
	}
	mac := hmac.New(sha256.New, h.cursorSigningKey[:])
	_, _ = mac.Write(payload)
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return cursor, errors.New("the diagnostic cursor signature is invalid")
	}
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cursor); err != nil {
		return cursor, errors.New("the diagnostic cursor payload is invalid")
	}
	if cursor.Version != diagnosticCursorVersion || cursor.Dataset != dataset || cursor.SnapshotTS == 0 || cursor.IssuedAt == 0 {
		return cursor, errors.New("the diagnostic cursor does not match this request")
	}
	issuedAt := time.Unix(cursor.IssuedAt, 0)
	now := h.now()
	if issuedAt.After(now.Add(time.Minute)) || now.Sub(issuedAt) > h.cursorTTL {
		return cursor, errors.New("the diagnostic cursor has expired")
	}
	return cursor, nil
}

func (h *diagnosticAPIHandler) resultForContext(ctx context.Context) string {
	if ctx.Err() != nil {
		return "timeout"
	}
	return "error"
}

func (h *diagnosticAPIHandler) writeInternalError(w http.ResponseWriter, dataset string, err error) {
	logutil.BgLogger().Warn("diagnostic API request failed", zap.String("dataset", dataset), zap.Error(err))
	status := http.StatusInternalServerError
	code := "internal_error"
	message := "the diagnostic page could not be generated"
	if errors.Cause(err) == context.DeadlineExceeded || errors.Cause(err) == context.Canceled {
		status = http.StatusGatewayTimeout
		code = "request_timeout"
		message = "the diagnostic page exceeded its request deadline"
	}
	h.writeError(w, status, diagnosticErrorResponse{Code: code, Message: message, Retryable: true})
}

func (h *diagnosticAPIHandler) writeError(w http.ResponseWriter, status int, response diagnosticErrorResponse) {
	h.writeJSON(w, status, response)
}

func (*diagnosticAPIHandler) writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		logutil.BgLogger().Warn("write diagnostic API JSON failed", zap.Error(err))
	}
}

func (h *diagnosticAPIHandler) collectDiagnosticPage(
	ctx context.Context,
	reader meta.Reader,
	dataset string,
	cursor diagnosticCursor,
	pageSize int,
) ([]any, diagnosticCursor, bool, error) {
	switch dataset {
	case "schema.tables":
		return collectDiagnosticTables(ctx, reader, h.redactor, cursor, pageSize)
	case "schema.columns":
		return collectDiagnosticColumns(ctx, reader, h.redactor, cursor, pageSize)
	case "schema.indexes":
		return collectDiagnosticIndexes(ctx, reader, h.redactor, cursor, pageSize)
	case "schema.partitions":
		return collectDiagnosticPartitions(ctx, reader, h.redactor, cursor, pageSize)
	case "binding.summary":
		return h.collectDiagnosticBindings(ctx, cursor, pageSize)
	case "stats.health":
		return h.collectDiagnosticStatsHealth(ctx, cursor, pageSize)
	default:
		return nil, cursor, false, errors.Errorf("unsupported diagnostic dataset %q", dataset)
	}
}
