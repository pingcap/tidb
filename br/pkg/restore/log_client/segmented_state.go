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

package logclient

import (
	"encoding/json"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/restore/ingestrec"
	"github.com/pingcap/tidb/pkg/meta/model"
)

const segmentedPiTRStatePayloadVersion uint64 = 1

// SegmentedPiTRState is the decoded segmented restore state stored in tidb_pitr_id_map.
type SegmentedPiTRState struct {
	DbMaps              []*backuppb.PitrDBMap
	TiFlashItems        map[int64]model.TiFlashReplicaInfo
	IngestRecorderState *ingestrec.RecorderState
}

type segmentedPiTRStatePayload struct {
	TiFlashItems        map[int64]model.TiFlashReplicaInfo `json:"tiflash_items"`
	IngestRecorderState *ingestrec.RecorderState           `json:"ingest_recorder,omitempty"`
}

func (s *SegmentedPiTRState) hasPayload() bool {
	if s == nil {
		return false
	}
	if s.TiFlashItems != nil {
		return true
	}
	return s.IngestRecorderState != nil
}

func (s *SegmentedPiTRState) toProto() (*backuppb.SegmentedPiTRState, error) {
	if s == nil {
		return nil, errors.New("segmented pitr state is nil")
	}
	state := &backuppb.SegmentedPiTRState{
		DbMaps: s.DbMaps,
	}
	if !s.hasPayload() {
		return state, nil
	}
	payload := segmentedPiTRStatePayload{
		TiFlashItems:        s.TiFlashItems,
		IngestRecorderState: s.IngestRecorderState,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, errors.Trace(err)
	}
	state.SegmentedPitrStateVer = segmentedPiTRStatePayloadVersion
	state.SegmentedPitrState = [][]byte{data}
	return state, nil
}

func segmentedPiTRStateFromProto(state *backuppb.SegmentedPiTRState) (*SegmentedPiTRState, error) {
	if state == nil {
		return nil, nil
	}
	result := &SegmentedPiTRState{
		DbMaps: state.GetDbMaps(),
	}
	if state.GetSegmentedPitrStateVer() == 0 || len(state.GetSegmentedPitrState()) == 0 {
		return result, nil
	}
	if state.GetSegmentedPitrStateVer() != segmentedPiTRStatePayloadVersion {
		return nil, errors.Errorf("unsupported segmented pitr state version: %d", state.GetSegmentedPitrStateVer())
	}
	var payload segmentedPiTRStatePayload
	if err := json.Unmarshal(state.GetSegmentedPitrState()[0], &payload); err != nil {
		return nil, errors.Trace(err)
	}
	result.TiFlashItems = payload.TiFlashItems
	result.IngestRecorderState = payload.IngestRecorderState
	return result, nil
}
