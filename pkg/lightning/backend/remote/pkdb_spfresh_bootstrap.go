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
	"context"
	"encoding/json"
	"net/http"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/meta/model"
)

const spfreshBootstrapPath = "/spfresh/bootstrap"

// BootstrapReadySPFreshIndexes notifies a worker to bootstrap ready SPFresh indexes.
func BootstrapReadySPFreshIndexes(
	ctx context.Context,
	httpClient *http.Client,
	workerURL string,
	clusterID uint64,
	keyspaceID uint32,
	tbInfo *model.TableInfo,
	indexIDs []int64,
	baseSnapshotTS uint64,
) error {
	if httpClient == nil {
		return errors.New("spfresh bootstrap requires http client")
	}
	if tbInfo == nil {
		return errors.New("spfresh bootstrap requires table info")
	}
	if baseSnapshotTS == 0 {
		return errors.New("spfresh bootstrap base snapshot ts is not set")
	}

	for _, indexID := range indexIDs {
		req, err := newSPFreshTableIndexRequest(
			keyspaceID,
			tbInfo,
			indexID,
			baseSnapshotTS,
		)
		if err != nil {
			return err
		}
		if err := postSPFreshBootstrapReadyIndex(
			ctx,
			httpClient,
			workerURL,
			clusterID,
			&req,
		); err != nil {
			return err
		}
	}
	return nil
}

// spfreshStaticIndexOptions contains only worker-owned index settings. Bulk
// layout options such as build_shard are consumed by the TiDB coordinator.
type spfreshStaticIndexOptions struct {
	RotAlgorithm     string `json:"rot_algorithm,omitempty"`
	MinPartitionSize uint64 `json:"min_partition_size,omitempty"`
	MaxPartitionSize uint64 `json:"max_partition_size,omitempty"`
	WriteBeamSize    uint64 `json:"write_beam_size,omitempty"`
}

func newSPFreshTableIndexRequest(
	keyspaceID uint32,
	tbInfo *model.TableInfo,
	indexID int64,
	baseSnapshotTS uint64,
) (spfreshIndexRequest, error) {
	if tbInfo == nil {
		return spfreshIndexRequest{}, errors.New("table info is not set")
	}
	idxInfo := model.FindIndexInfoByID(tbInfo.Indices, indexID)
	if idxInfo == nil {
		return spfreshIndexRequest{}, errors.Errorf("index %d not found", indexID)
	}
	if !idxInfo.IsSPFreshIndex() {
		return spfreshIndexRequest{}, errors.Errorf("index %d is not a SPFresh vector index", indexID)
	}
	vectorColumnID, err := model.VectorIndexColumnID(tbInfo, idxInfo)
	if err != nil {
		return spfreshIndexRequest{}, err
	}
	if baseSnapshotTS == 0 {
		return spfreshIndexRequest{}, errors.New("spfresh build base snapshot ts is not set")
	}
	distanceMetric, ok := model.SPFreshDistanceMetricAPIString(idxInfo.VectorInfo.DistanceMetric)
	if !ok {
		return spfreshIndexRequest{}, errors.Errorf("unknown vector distance metric %q", idxInfo.VectorInfo.DistanceMetric)
	}
	storedColumns, err := spfreshStoredColumnsToAPI(tbInfo, idxInfo)
	if err != nil {
		return spfreshIndexRequest{}, err
	}
	handleColumn, err := spfreshHandleColumnToAPI(tbInfo)
	if err != nil {
		return spfreshIndexRequest{}, err
	}
	runtimeConfig, err := spfreshRuntimeConfig(idxInfo.VectorInfo)
	if err != nil {
		return spfreshIndexRequest{}, err
	}

	var indexOptions *spfreshStaticIndexOptions
	if options := idxInfo.VectorInfo.IndexOptions; options != nil {
		indexOptions = &spfreshStaticIndexOptions{
			RotAlgorithm:     options.RotAlgorithm,
			MinPartitionSize: options.MinPartitionSize,
			MaxPartitionSize: options.MaxPartitionSize,
			WriteBeamSize:    options.WriteBeamSize,
		}
	}
	return spfreshIndexRequest{
		KeyspaceID:     keyspaceID,
		TableID:        tbInfo.ID,
		IndexID:        indexID,
		Dims:           idxInfo.VectorInfo.Dimension,
		DistanceMetric: distanceMetric,
		Seed:           0,
		VectorColumnID: vectorColumnID,
		StoredColumns:  storedColumns,
		HandleColumn:   handleColumn,
		BaseSnapshotTS: baseSnapshotTS,
		IndexOptions:   indexOptions,
		RuntimeConfig:  runtimeConfig,
	}, nil
}

func spfreshRuntimeConfig(vectorIndex *model.VectorIndexInfo) (model.SPFreshVectorRuntimeConfig, error) {
	if vectorIndex == nil || vectorIndex.RuntimeConfig == nil {
		return model.SPFreshVectorRuntimeConfig{}, errors.New("spfresh runtime config is not set")
	}
	return *vectorIndex.RuntimeConfig, nil
}

func postSPFreshBootstrapReadyIndex(
	ctx context.Context,
	httpClient *http.Client,
	workerURL string,
	clusterID uint64,
	req *spfreshIndexRequest,
) error {
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	headers := map[string]string{
		"Content-Type": "application/json",
	}
	return postSPFreshControlRequest(ctx, httpClient, workerURL, clusterID, spfreshBootstrapPath, body, headers, "bootstrap")
}
