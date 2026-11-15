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

package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/pingcap/errors"
	pkdbspfreshbulk "github.com/pingcap/tidb/pkg/ddl/pkdb_spfresh_bulk"
)

const (
	spfreshBulkBuildPath            = "/spfresh/bulk-build"
	maxSPFreshBulkBuildResponseSize = 1 << 20
)

// SPFreshBulkBuildRequest is the fixed single-worker MVP build request.
type SPFreshBulkBuildRequest struct {
	SourceSnapshotTS uint64                         `json:"source_snapshot_ts"`
	StoredColumns    []pkdbspfreshbulk.StoredColumn `json:"stored_columns,omitempty"`
	BuildID          string                         `json:"build_id"`
	KeyspaceID       uint32                         `json:"keyspace_id"`
	TableID          int64                          `json:"table_id"`
	IndexID          int64                          `json:"index_id"`
	BaseFragments    []pkdbspfreshbulk.ManifestRef  `json:"base_fragments"`
	Dimension        uint32                         `json:"dimension"`
	Distance         string                         `json:"distance"`
	TargetOccupancy  uint32                         `json:"target_occupancy"`
	AlgorithmSeed    uint64                         `json:"algorithm_seed"`
	MinPartitionSize uint64                         `json:"min_partition_size"`
	MaxPartitionSize uint64                         `json:"max_partition_size"`
	OutputPrefix     string                         `json:"output_prefix"`
}

// SPFreshBulkBuildExpected contains the source conservation values that the
// one-shot worker response must match.
type SPFreshBulkBuildExpected struct {
	InputRows     uint64
	InputBytes    uint64
	NonRootRunURI string
	RootRunURI    string
}

// SPFreshBulkRunObject describes one immutable SortedRun emitted by the worker.
type SPFreshBulkRunObject struct {
	Reference   pkdbspfreshbulk.ManifestRef `json:"reference"`
	RecordCount uint64                      `json:"record_count"`
	ObjectBytes uint64                      `json:"object_bytes"`
}

// SPFreshBulkBuildResult is the fixed result needed by the later import and
// verification stages.
type SPFreshBulkBuildResult struct {
	InputRows          uint64                `json:"input_rows"`
	InputBytes         uint64                `json:"input_bytes"`
	LeafEntryCount     uint64                `json:"leaf_entry_count"`
	PartitionCount     uint64                `json:"partition_count"`
	LeafPartitionCount uint64                `json:"leaf_partition_count"`
	RootLevel          uint32                `json:"root_level"`
	MaxPartitionKey    uint64                `json:"max_partition_key"`
	NonRoot            *SPFreshBulkRunObject `json:"non_root"`
	Root               SPFreshBulkRunObject  `json:"root"`
}

// UnmarshalJSON requires an explicit non_root value. Null denotes a root-only
// tree; an omitted field is not a valid current worker response.
func (r *SPFreshBulkBuildResult) UnmarshalJSON(data []byte) error {
	var fields struct {
		NonRoot json.RawMessage `json:"non_root"`
	}
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	if len(fields.NonRoot) == 0 {
		return errors.New("SPFresh bulk-build response is missing non_root")
	}
	type result SPFreshBulkBuildResult
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	return decoder.Decode((*result)(r))
}

// BuildSPFreshBulkSortedRuns asks the selected worker exactly once to consume
// the exact SHA-pinned BaseFragment list and validates source conservation.
func BuildSPFreshBulkSortedRuns(
	ctx context.Context,
	httpClient *http.Client,
	workerURL string,
	request SPFreshBulkBuildRequest,
	expected SPFreshBulkBuildExpected,
) (SPFreshBulkBuildResult, error) {
	if httpClient == nil {
		return SPFreshBulkBuildResult{}, errors.New("SPFresh bulk build requires http client")
	}
	workerURL = strings.TrimSuffix(workerURL, "/")
	if workerURL == "" {
		return SPFreshBulkBuildResult{}, errors.New("SPFresh bulk build requires worker URL")
	}
	if request.SourceSnapshotTS == 0 {
		return SPFreshBulkBuildResult{}, errors.New("SPFresh bulk build requires source_snapshot_ts")
	}
	if len(request.BaseFragments) == 0 {
		return SPFreshBulkBuildResult{}, errors.New("SPFresh bulk build requires pinned BaseFragments")
	}
	for _, reference := range request.BaseFragments {
		if err := reference.Validate(request.BuildID); err != nil {
			return SPFreshBulkBuildResult{}, err
		}
	}
	body, err := json.Marshal(request)
	if err != nil {
		return SPFreshBulkBuildResult{}, errors.Trace(err)
	}
	resp, err := sendOneShotSPFreshBulkRequest(
		ctx,
		httpClient,
		http.MethodPost,
		workerURL+spfreshBulkBuildPath,
		body,
		map[string]string{"Content-Type": "application/json"},
	)
	if err != nil {
		return SPFreshBulkBuildResult{}, errors.Trace(err)
	}
	if resp.StatusCode != http.StatusOK {
		return SPFreshBulkBuildResult{}, spfreshControlRequestError(resp, "build SPFresh bulk SortedRuns")
	}
	defer resp.Body.Close()
	var result SPFreshBulkBuildResult
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxSPFreshBulkBuildResponseSize)).Decode(&result); err != nil {
		return SPFreshBulkBuildResult{}, errors.Annotate(err, "decode SPFresh bulk-build response")
	}
	if result.InputRows != expected.InputRows ||
		result.InputBytes != expected.InputBytes {
		return SPFreshBulkBuildResult{}, errors.New("SPFresh bulk-build response does not conserve source rows, bytes, and handle digest")
	}
	if err := result.Root.Reference.Validate(request.BuildID); err != nil {
		return SPFreshBulkBuildResult{}, err
	}
	if result.NonRoot != nil {
		if err := result.NonRoot.Reference.Validate(request.BuildID); err != nil {
			return SPFreshBulkBuildResult{}, err
		}
	}
	if result.Root.Reference.URI != expected.RootRunURI {
		return SPFreshBulkBuildResult{}, errors.New("SPFresh bulk-build response returned unexpected SortedRun URIs")
	}
	if result.RootLevel == 0 || result.Root.RecordCount == 0 || result.Root.ObjectBytes == 0 ||
		result.LeafEntryCount != result.InputRows || result.PartitionCount == 0 ||
		result.LeafPartitionCount == 0 || result.LeafPartitionCount > result.PartitionCount ||
		result.MaxPartitionKey != result.PartitionCount {
		return SPFreshBulkBuildResult{}, errors.New("SPFresh bulk-build response returned invalid tree facts")
	}
	if result.RootLevel == 1 {
		if result.NonRoot != nil || result.PartitionCount != 1 || result.LeafPartitionCount != 1 {
			return SPFreshBulkBuildResult{}, errors.New("SPFresh root-only tree has non-root partitions")
		}
	} else if result.NonRoot == nil || result.NonRoot.Reference.URI != expected.NonRootRunURI ||
		result.NonRoot.RecordCount == 0 || result.NonRoot.ObjectBytes == 0 || result.PartitionCount < 2 {
		return SPFreshBulkBuildResult{}, errors.New("SPFresh bulk-build response requires a non-root SortedRun")
	}

	return result, nil
}

func sendOneShotSPFreshBulkRequest(
	ctx context.Context,
	httpClient *http.Client,
	method string,
	url string,
	data []byte,
	headers map[string]string,
) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	client := *httpClient
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, &SPFreshControlSendError{
			Method: method,
			URL:    url,
			Err:    err,
		}
	}
	return resp, nil
}
