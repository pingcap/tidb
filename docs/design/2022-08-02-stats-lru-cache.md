# Proposal: Stats LRU Cache

- Author(s): [Yisaer](https://github.com/yisaer)
- Tracking Issue: https://github.com/pingcap/tidb/issues/34052

## Abstract

This proposes a design of how to maintain stats cache by lru according to memory usage

## Background

Previously, tidb maintained the all the indices' stats and some columns' stats which is needed during query. 
As the maintained stats grows, the total memory usage of the stats will increase and makes tidb server OOM.
Thus we use lru to maintain stats cache in order to keep memory safe.

### Goal

- Use LRU to maintain the stats cache in memory
- Keep the total memory usage of stats cache under the quota
- Support loading stats back into memory when tidb server needs it

### Non-Goals

- Considering the stats cache are in memory, we don't provide changing stats cache into LRU without restarting

## Proposal

### Stats Cache Interface

We will provide a Stats Cache Interface which is implemented by LRU Cache and Map Cache. 
If the tidb server didn't enable Stats LRU Cache, it will use Map Cache by default. Also, we will provide config and global session variable to control whether enable Stats LRU Cache and the capacity of it.

```go
// statsCacheInner is the interface to manage the statsCache, it can be implemented by map, lru cache or other structures.
type statsCacheInner interface {
	Get(int64) (*statistics.Table, bool)
	Put(int64, *statistics.Table)
	Del(int64)
	Cost() int64
	Len() int
	SetCapacity(int64)
}
```

### Stats LRU Cache Policy

For column or index stats, we maintained following data structure for stats: 

- `Histogram` 
- `TopN`
- `CMSketch`

And we will also maintain status for each column and index stats in order to indicate its stats loading status like following:

- `AllLoaded`
- `OnlyCMSEvcited`
- `OnlyHistRemained`
- `AllEvicted`

When the column or index stats load all data structures into memory, the status will be `AllLoaded`. 
When the Stats LRU Cache memory usage exceeds the quota, the LRU Cache will select one column or index stats to evict the data structures by following rules to reduce the memory usage:

- If the status is `AllLoaded`, it will discard the `CMSketch` and the status will be changed into `OnlyCMSEvcited`
- If the status is `OnlyCMSEvcited`, it will discard the `TopN` and the status will be changed into `OnlyHistRemained`
- If the status is `OnlyHistRemained`, it will discard the `Histogram` and the status will be changed into `AllEvicted`

### Sync Stats Compatibility

Previously tidb server has Sync Stats and asynchronously Loading Histograms in order to load column stats into memory during query.
As Stats LRU Cache Policy may evict index stats in memory, we also need Sync Stats and asynchronously Loading Histograms to support loading index stats according to its loading status to keep compatible.
During the query optimization, tidb server will collect the used columns and indices, if their stats are not fully loaded, tidb-server will try to load their stats back into the memory.
