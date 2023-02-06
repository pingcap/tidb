// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package checksum

import (
	"context"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

// ExecutorBuilder is used to build a "kv.Request".
type ExecutorBuilder struct {
	table *model.TableInfo
	ts    uint64

	oldTable *metautil.Table

	concurrency uint

	oldKeyspace []byte
	newKeyspace []byte
}

// NewExecutorBuilder returns a new executor builder.
func NewExecutorBuilder(table *model.TableInfo, ts uint64) *ExecutorBuilder {
	return &ExecutorBuilder{
		table: table,
		ts:    ts,

		concurrency: variable.DefDistSQLScanConcurrency,
	}
}

// SetOldTable set a old table info to the builder.
func (builder *ExecutorBuilder) SetOldTable(oldTable *metautil.Table) *ExecutorBuilder {
	builder.oldTable = oldTable
	return builder
}

// SetConcurrency set the concurrency of the checksum executing.
func (builder *ExecutorBuilder) SetConcurrency(conc uint) *ExecutorBuilder {
	builder.concurrency = conc
	return builder
}

func (builder *ExecutorBuilder) SetOldKeyspace(keyspace []byte) *ExecutorBuilder {
	builder.oldKeyspace = keyspace
	return builder
}

func (builder *ExecutorBuilder) SetNewKeyspace(keyspace []byte) *ExecutorBuilder {
	builder.newKeyspace = keyspace
	return builder
}

// Build builds a checksum executor.
func (builder *ExecutorBuilder) Build() (*Executor, error) {
	reqs, err := buildChecksumRequest(
		builder.table,
		builder.oldTable,
		builder.ts,
		builder.concurrency,
		builder.oldKeyspace,
		builder.newKeyspace,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &Executor{reqs: reqs}, nil
}

func buildChecksumRequest(
	newTable *model.TableInfo,
	oldTable *metautil.Table,
	startTS uint64,
	concurrency uint,
	oldKeyspace []byte,
	newKeyspace []byte,
) ([]*kv.Request, error) {
	var partDefs []model.PartitionDefinition
	if part := newTable.Partition; part != nil {
		partDefs = part.Definitions
	}

	reqs := make([]*kv.Request, 0, (len(newTable.Indices)+1)*(len(partDefs)+1))
	var oldTableID int64
	if oldTable != nil {
		oldTableID = oldTable.Info.ID
	}
	rs, err := buildRequest(newTable, newTable.ID, oldTable, oldTableID, startTS, concurrency, oldKeyspace, newKeyspace)
	if err != nil {
		return nil, errors.Trace(err)
	}
	reqs = append(reqs, rs...)

	for _, partDef := range partDefs {
		var oldPartID int64
		if oldTable != nil {
			for _, oldPartDef := range oldTable.Info.Partition.Definitions {
				if oldPartDef.Name == partDef.Name {
					oldPartID = oldPartDef.ID
				}
			}
		}
		rs, err := buildRequest(newTable, partDef.ID, oldTable, oldPartID, startTS, concurrency, oldKeyspace, newKeyspace)
		if err != nil {
			return nil, errors.Trace(err)
		}
		reqs = append(reqs, rs...)
	}

	return reqs, nil
}

func buildRequest(
	tableInfo *model.TableInfo,
	tableID int64,
	oldTable *metautil.Table,
	oldTableID int64,
	startTS uint64,
	concurrency uint,
	oldKeyspace []byte,
	newKeyspace []byte,
) ([]*kv.Request, error) {
	reqs := make([]*kv.Request, 0)
	req, err := buildTableRequest(tableInfo, tableID, oldTable, oldTableID, startTS, concurrency, oldKeyspace, newKeyspace)
	if err != nil {
		return nil, errors.Trace(err)
	}
	reqs = append(reqs, req)

	for _, indexInfo := range tableInfo.Indices {
		if indexInfo.State != model.StatePublic {
			continue
		}
		var oldIndexInfo *model.IndexInfo
		if oldTable != nil {
			for _, oldIndex := range oldTable.Info.Indices {
				if oldIndex.Name == indexInfo.Name {
					oldIndexInfo = oldIndex
					break
				}
			}
			if oldIndexInfo == nil {
				log.Panic("index not found in origin table, "+
					"please check the restore table has the same index info with origin table",
					zap.Int64("table id", tableID),
					zap.Stringer("table name", tableInfo.Name),
					zap.Int64("origin table id", oldTableID),
					zap.Stringer("origin table name", oldTable.Info.Name),
					zap.Stringer("index name", indexInfo.Name))
			}
		}
		req, err = buildIndexRequest(
			tableID, indexInfo, oldTableID, oldIndexInfo, startTS, concurrency, oldKeyspace, newKeyspace)
		if err != nil {
			return nil, errors.Trace(err)
		}
		reqs = append(reqs, req)
	}

	return reqs, nil
}

func buildTableRequest(
	tableInfo *model.TableInfo,
	tableID int64,
	oldTable *metautil.Table,
	oldTableID int64,
	startTS uint64,
	concurrency uint,
	oldKeyspace []byte,
	newKeyspace []byte,
) (*kv.Request, error) {
	var rule *tipb.ChecksumRewriteRule
	if oldTable != nil {
		rule = &tipb.ChecksumRewriteRule{
			OldPrefix: append(append([]byte{}, oldKeyspace...), tablecodec.GenTableRecordPrefix(oldTableID)...),
			NewPrefix: append(append([]byte{}, newKeyspace...), tablecodec.GenTableRecordPrefix(tableID)...),
		}
	}

	checksum := &tipb.ChecksumRequest{
		ScanOn:    tipb.ChecksumScanOn_Table,
		Algorithm: tipb.ChecksumAlgorithm_Crc64_Xor,
		Rule:      rule,
	}

	var ranges []*ranger.Range
	if tableInfo.IsCommonHandle {
		ranges = ranger.FullNotNullRange()
	} else {
		ranges = ranger.FullIntRange(false)
	}

	var builder distsql.RequestBuilder
	// Use low priority to reducing impact to other requests.
	builder.Request.Priority = kv.PriorityLow
	return builder.SetHandleRanges(nil, tableID, tableInfo.IsCommonHandle, ranges, nil).
		SetStartTS(startTS).
		SetChecksumRequest(checksum).
		SetConcurrency(int(concurrency)).
		Build()
}

func buildIndexRequest(
	tableID int64,
	indexInfo *model.IndexInfo,
	oldTableID int64,
	oldIndexInfo *model.IndexInfo,
	startTS uint64,
	concurrency uint,
	oldKeyspace []byte,
	newKeyspace []byte,
) (*kv.Request, error) {
	var rule *tipb.ChecksumRewriteRule
	if oldIndexInfo != nil {
		rule = &tipb.ChecksumRewriteRule{
			OldPrefix: append(append([]byte{}, oldKeyspace...), tablecodec.EncodeTableIndexPrefix(oldTableID, oldIndexInfo.ID)...),
			NewPrefix: append(append([]byte{}, newKeyspace...), tablecodec.EncodeTableIndexPrefix(tableID, indexInfo.ID)...),
		}
	}
	checksum := &tipb.ChecksumRequest{
		ScanOn:    tipb.ChecksumScanOn_Index,
		Algorithm: tipb.ChecksumAlgorithm_Crc64_Xor,
		Rule:      rule,
	}

	ranges := ranger.FullRange()

	var builder distsql.RequestBuilder
	// Use low priority to reducing impact to other requests.
	builder.Request.Priority = kv.PriorityLow
	return builder.SetIndexRanges(nil, tableID, indexInfo.ID, ranges).
		SetStartTS(startTS).
		SetChecksumRequest(checksum).
		SetConcurrency(int(concurrency)).
		Build()
}

func sendChecksumRequest(
	ctx context.Context, client kv.Client, req *kv.Request, vars *kv.Variables,
) (resp *tipb.ChecksumResponse, err error) {
	res, err := distsql.Checksum(ctx, client, req, vars)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer func() {
		if err1 := res.Close(); err1 != nil {
			err = err1
		}
	}()

	resp = &tipb.ChecksumResponse{}

	for {
		data, err := res.NextRaw(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if data == nil {
			break
		}
		checksum := &tipb.ChecksumResponse{}
		if err = checksum.Unmarshal(data); err != nil {
			return nil, errors.Trace(err)
		}
		updateChecksumResponse(resp, checksum)
	}

	return resp, nil
}

func updateChecksumResponse(resp, update *tipb.ChecksumResponse) {
	resp.Checksum ^= update.Checksum
	resp.TotalKvs += update.TotalKvs
	resp.TotalBytes += update.TotalBytes
}

// Executor is a checksum executor.
type Executor struct {
	reqs []*kv.Request
}

// Len returns the total number of checksum requests.
func (exec *Executor) Len() int {
	return len(exec.reqs)
}

// Each executes the function to each requests in the executor.
func (exec *Executor) Each(f func(*kv.Request) error) error {
	for _, req := range exec.reqs {
		err := f(req)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// RawRequests extracts the raw requests associated with this executor.
// This is mainly used for debugging only.
func (exec *Executor) RawRequests() ([]*tipb.ChecksumRequest, error) {
	res := make([]*tipb.ChecksumRequest, 0, len(exec.reqs))
	for _, req := range exec.reqs {
		rawReq := new(tipb.ChecksumRequest)
		if err := proto.Unmarshal(req.Data, rawReq); err != nil {
			return nil, errors.Trace(err)
		}
		res = append(res, rawReq)
	}
	return res, nil
}

// Execute executes a checksum executor.
func (exec *Executor) Execute(
	ctx context.Context,
	client kv.Client,
	updateFn func(),
) (*tipb.ChecksumResponse, error) {
	checksumResp := &tipb.ChecksumResponse{}
	for _, req := range exec.reqs {
		// Pointer to SessionVars.Killed
		// Killed is a flag to indicate that this query is killed.
		//
		// It is useful in TiDB, however, it's a place holder in BR.
		killed := uint32(0)
		resp, err := sendChecksumRequest(ctx, client, req, kv.NewVariables(&killed))
		if err != nil {
			return nil, errors.Trace(err)
		}
		updateChecksumResponse(checksumResp, resp)
		updateFn()
	}
	return checksumResp, nil
}
