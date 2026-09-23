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

// Package dumpservice provides a client for exporting data through a dump service.
//
// # Implementing a service
//
// Serve cleartext HTTP/2 over a Unix socket. Start the service before Dumpling
// runs with --dump-service unix:///path/to/socket; you own the service and the
// socket's lifecycle. Restrict socket access to trusted clients.
//
// Implement the three endpoints below and accept concurrent requests on a shared
// connection. How you serialize or schedule the work is up to you. Snapshot
// selection and scan concurrency are configured outside this protocol.
//
// For a TiDB export, expose logical TiDB keys and visible values, including
// schema metadata and table data. Dumpling handles schema and row decoding and
// output formatting.
//
// # Consistency across requests
//
// Use one snapshot and one logical keyspace for the whole export: metadata,
// concurrent requests, and reconnects included. For a given decoded range,
// /shards returns the same ordered boundaries and /data the same key/value set;
// overlapping scans agree on their shared contents. Fail the request when the
// snapshot is unavailable.
//
// These guarantees cover logical contents only. Serialization and scan order may
// vary, and metrics may change between requests.
//
// # Key and value contents
//
// Keys are raw logical TiDB kv.Key bytes: no APIV2 keyspace prefix, no MVCC
// timestamp suffix, and no whole-key memcomparable encoding. Keep the internal
// field encodings defined by the packages below. Request boundaries and /shards
// results hex-encode these bytes; /data frames carry them as-is.
//
// Dumpling reads two kinds of KV pair from that snapshot:
//
//   - Schema metadata: encoded hash keys for database and table schemas
//     (pkg/meta, pkg/structure), with JSON schema values (pkg/meta/model).
//   - Table records: keys holding the physical table or partition ID and row
//     handle (pkg/tablecodec), with TiDB-encoded row values
//     (pkg/tablecodec, pkg/util/rowcodec).
//
// Resolve MVCC versions and drop deleted records before returning these pairs.
//
// # Range requests
//
// GET /shards and GET /data take the half-open range as hexadecimal query
// parameters:
//
//	?start_key_hex=00ff&end_key_hex=10
//
// The hex boundaries mean [start, end), compared lexicographically as unsigned
// bytes. Return HTTP 200 on success; for errors detected before streaming starts,
// return a non-200 status with a diagnostic body.
//
// # GET /shards
//
// Split the requested interval into independently scannable ranges and return JSON:
//
//	{"ranges":[{"start_key_hex":"00ff","end_key_hex":"01"},
//	           {"start_key_hex":"01","end_key_hex":"10"}]}
//
// Return at least one range, even for an interval holding no data. Ranges must be
// nonempty and, taken together, ordered, adjacent, and exactly covering the
// interval. Returning the whole interval as a single range is fine.
//
// How you split is up to you, as long as the coverage and repeatability above hold.
//
// Dumpling calls /shards for each physical table's record interval, then creates
// one export task per returned range. Each task scans its range with /data and
// writes the rows; the worker pool set by --threads runs tasks concurrently. A few
// well-balanced ranges let a large table use several workers. One range is correct,
// but then that table gets a single data task.
//
// A range's position in the table's combined range list becomes the output chunk
// index, and a task may produce several files when output-size limits apply. See
// shardKVTableRanges and dumpFromScanner in dumpling/export/kv.go, and
// tryToWriteTableData in dumpling/export/writer.go. Dumpling reads metadata
// directly through /data.
//
// # GET /data
//
// Stream every visible KV pair in the requested interval, each exactly once.
//
// The response body is a flat sequence of records, written back to back with no
// count, separator, or wrapper. Write each record as:
//
//	uint32 key length   (little-endian)
//	uint32 value length (little-endian)
//	key bytes           (key length bytes)
//	value bytes         (value length bytes)
//
// A record with key "k" and value "v" is these four fields in order:
//
//	01 00 00 00  key length = 1
//	01 00 00 00  value length = 1
//	6b           key bytes = "k"
//	76           value bytes = "v"
//
// Keys must be non-empty. HTTP/2 splits the body into DATA frames on its own, so
// one record may span several frames or share a frame with its neighbors; just
// write the bytes in order. Stop scanning when the request is canceled.
//
// Once the last record is written, end the body and report success with this
// HTTP trailer:
//
//	x-dumper-scan-status: complete
//
// An interval with no data is fine: send no records and the same trailer.
//
// If scanning fails after the response has started, end the body and send:
//
//	x-dumper-scan-status: failed
//	x-dumper-scan-error: <query-escaped UTF-8 diagnostic>
//
// Encode the diagnostic with url.QueryEscape or equivalent: '+' is a space and
// percent escapes are bytes. In a Go HTTP handler, declare both trailer names in
// the Trailer header before writing the body, then set them once scanning
// finishes. EOF with no completion trailer is an error.
//
// # GET /metrics
//
// Return HTTP 200 with Prometheus text exposition; metric names are yours to
// choose. Dumpling gathers and forwards these metrics while an export is active.
package dumpservice
