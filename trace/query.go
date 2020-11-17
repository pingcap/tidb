package trace

import (
	"io/ioutil"
	"path"
	"strconv"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
)

// Response of a trace query
type Response struct {
	TraceID  int64     `json:"trace_id"`
	SpanSets []SpanSet `json:"span_sets"`
}

// SpanSet information of a node
type SpanSet struct {
	NodeType string `json:"node_type"`
	Spans    []Span `json:"spans"`
}

// Span information
type Span struct {
	SpanID          uint64     `json:"span_id"`
	ParentID        uint64     `json:"parent_id"`
	BeginUnixTimeNs uint64     `json:"begin_unix_time_ns"`
	DurationNs      uint64     `json:"duration_ns"`
	Event           string     `json:"event"`
	Properties      []Property `json:"properties,omitempty"`
}

// Property information
type Property struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Retrieve trace results by traceID
func Retrieve(traceID uint64) *Response {
	dir := StorageDir.Load()
	if len(dir) == 0 {
		return nil
	}

	bs, err := ioutil.ReadFile(path.Join(dir, strconv.FormatUint(traceID, 10)))
	if err != nil {
		return nil
	}

	var traceDetail kvrpcpb.TraceDetail
	err = traceDetail.Unmarshal(bs)
	if err != nil {
		return nil
	}

	spanSets := make([]SpanSet, 0, len(traceDetail.SpanSets))
	for _, set := range traceDetail.SpanSets {
		spans := make([]Span, 0, len(set.Spans))
		idConv := idConverter{idPrefix: set.SpanIdPrefix}

		for _, span := range set.Spans {
			parentID := idConv.convert(span.ParentId)
			// Root Span
			if parentID == 0 {
				parentID = set.RootParentSpanId
			}

			spans = append(spans, Span{
				SpanID:          idConv.convert(span.Id),
				ParentID:        parentID,
				BeginUnixTimeNs: span.BeginUnixTimeNs,
				DurationNs:      span.DurationNs,
				Event:           span.Event,
				Properties:      mapProperties(span.Properties),
			})
		}

		spanSets = append(spanSets, SpanSet{
			NodeType: mapNodeType(set.NodeType),
			Spans:    spans,
		})
	}

	return &Response{
		TraceID:  int64(traceID),
		SpanSets: spanSets,
	}
}

type idConverter struct {
	idPrefix uint32
}

func (c idConverter) convert(prevID uint32) uint64 {
	return uint64(c.idPrefix)<<32 | uint64(prevID)
}

func mapProperties(pbProperties []*kvrpcpb.TraceDetail_Span_Property) (res []Property) {
	for _, p := range pbProperties {
		res = append(res, Property{
			Key:   p.Key,
			Value: p.Value,
		})
	}

	return
}

func mapNodeType(pdNodeType kvrpcpb.TraceDetail_NodeType) (res string) {
	switch pdNodeType {
	case kvrpcpb.TraceDetail_TiDB:
		res = "TiDB"
	case kvrpcpb.TraceDetail_TiKV:
		res = "TiKV"
	case kvrpcpb.TraceDetail_PD:
		res = "PD"
	case kvrpcpb.TraceDetail_TiFlash:
		res = "TiFlash"
	default:
		res = "Unknown"
	}

	return
}
