# Proposal: Enhance Split Table Stability and Usability

- Author(s): 

  - [qidi1](https://github.com/qidi1)
  - [IcePigZDB](https://github.com/IcePigZDB/)

- Thanks

  - [hzh0435](https://github.com/hzh0425)

- Discussion PR: https://github.com/tikv/pd/issues/4095

- Tracking Issue: https://github.com/pingcap/tidb/issues/29034

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Detailed Design](#detailed-design)
* [Test Design](#test-design)
* [Follow-up work](#Follow-up work)

## Introduction

The original region split and scatter are handled by tikv and pd respectively, in order to improve efficiency, the split is also changed to be handled and controlled by pd.

## Motivation or Background

In the current implementation, the split and scatter commands of a region are sent from TiDB to client-go. client-go forwards the split command to TiKV and then forwards the scatter command to PD for processing. In the process of splitting and scattering of a region, if there is another command to split and scatter for that region at the same time, it will cause  conflicts, as described in [issue](https://github.com/pingcap/tidb/issues/24295).

The problem is that TiKV cannot determine whether the region is in valid state, but PD can determine the state, so we give all the processing to PD, which can reduce the conflicts to some extent.

## Detailed Design

The design is divided into two main parts:

1. The client-go is modified to use split interface provided by PD.
2. Implement PD's gRPC interface `splitAndScatterRegions`.

### Change client-go `SplitRegions` interface

Move `SplitRegions` requests from TiKV to PD.

The original code was

```go
req := tikvrpc.NewRequest(tikvrpc.CmdSplitRegion, &kvrpcpb.SplitRegionRequest{
        SplitKeys: batch.Keys,
    }, kvrpcpb.Context{
        Priority: kvrpcpb.CommandPri_Normal,
    })

    sender := locate.NewRegionRequestSender(s.regionCache, s.GetTiKVClient())
    resp, err := sender.SendReq(bo, req, batch.RegionID, client.ReadTimeoutShort)
```

The changed code is

```go
resp, err := s.pdClient.SplitRegions(bo.GetCtx(), batch.Keys)
    if err != nil {
        resp := &pdpb.SplitRegionsResponse{}
        resp.Header.Error = &pdpb.Error{Message: err.Error()}
        return resp
    }
    regionIDs := r
```

### ADD `SplitAndScatterRegions` API in PD

1. **Implement `SplitAndScatterRegions` in `GrpcServer`**

   ```go
   // SplitAndScatterRegions split regions by the given split keys, and scatter regions
   func (s *GrpcServer) SplitAndScatterRegions(ctx context.Context, request *pdpb.SplitAndScatterRegionsRequest) (*pdpb.SplitAndScatterRegionsResponse, error) {
       ...
       rc := s.GetRaftCluster()
       splitFinishedPercentage, newRegionIDs := rc.GetRegionSplitter().SplitRegions(ctx, request.GetSplitKeys(), int(request.GetRetryLimit()))
       scatterFinishedPercentage, err := s.addScatterRegionsOperators(rc, newRegionIDs, request.GetGroup(), int(request.GetRetryLimit()))
       ...
       return &pdpb.SplitAndScatterRegionsResponse{
           Header:                    s.header(),
           RegionsId:                 newRegionIDs,
           SplitFinishedPercentage:   uint64(splitFinishedPercentage),
           ScatterFinishedPercentage: uint64(scatterFinishedPercentage),
       }, nil
   }
   ```

    We first use the `SplitRegion` function to split the region by splitKeys, and then call `addScatterRegionsOperators` to scatter the previously splited region.

2. **Add ``SplitAndScatterRegions`` API in client**

   ```go
   func (c *client) SplitAndScatterRegions(ctx context.Context, splitKeys [][]byte, opts ...RegionsOption) (*pdpb.SplitAndScatterRegionsResponse, error) {
       ...
       resp, err := c.getClient().SplitAndScatterRegions(ctx, req)
       ...
   }
   ```

    Here we call the `SplitAndScatter` of `GrpcSever` interface.

## Test Design

We will test the stability and performance by controlling variables under different parameter conditions and record the time spent in these cases.

For example:

- Split a empty table into 32 regions / 400 regions ....
- Split a table that contains data into 32 regions / 400 regions ....
- Split a table which dose not contain primary key into 32 regions / 400 regions ....
- ...............

## Follow-up work

Enhance split region statement.