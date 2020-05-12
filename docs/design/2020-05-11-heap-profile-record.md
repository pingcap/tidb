# Heap Profile Recorder

- Author(s):     [Yisaer](https://github.com/Yisaer) (Song Gao)
- Last updated:  2020-05-11
- Discussion at: https://github.com/pingcap/tidb/pull/16777

## Abstract

"Heap Profiler Recorder" profile the flat heap usage periodically in an extra goroutine to record the global `SimpleLRUCache` memory usage with an similar value.

## Background

Currently, we have support memory usage and disk usage tracker for `Executor`. In [#15407](https://github.com/pingcap/tidb/issues/15407), we are going to support the `Global Memory Tracker`.
However, it would be too much work to realize calculating the memory usage of each Implementation of `Plan` and it might also causing much cpu consuming. To track the memory usage of `SimpleLRUCache`, we are trying to search the memory usage from `runtime/pprof`.

## Proposal

We will record the whole `SimpleLRUCache` memory usage in the `GlobalLRUMemUsageTracker` memory tracker whose parent is `GlobalMemoryUsageTracker` memory tracker by using an extra goroutine to record the value searched and summed from heap profile periodically.

## Rationale

When an golang application started, the runtime would [startProfile](https://github.com/golang/go/blob/48a90d639d578d2b33fdc1903f03e028b4d40fa9/src/cmd/oldlink/internal/ld/main.go#L155) in default including heap usage.
And `runtime.MemProfileRate` controls the fraction of memory allocations that are recorded and reported in the memory profile. In default, `runtime.MemProfileRate` is 512 KB which is also can be configured. When the whole heap usage of `SimpleLRUCache` is larger than the `runtime.MemProfileRate`, it would be reflected in the flat value of the pprof heap profile.

To verify whether `kvcache.(*SimpleLRUCache).Put` would reflect the real heap usage, I use following test to ensure it:

1. fufill the `SimpleLRUCache` by `set @randomString = ? with 20000 times`.
2. profile the heap Usage of `github.com/pingcap/tidb/util/kvcache.(*SimpleLRUCache).Put` and the result is 2.55 MB

Let's dig into the Put then we can find the where the heap consumed:

```sh
(pprof) list Put
Total: 52.23MB
ROUTINE ======================== github.com/pingcap/tidb/util/kvcache.(*SimpleLRUCache).Put in /Users/yisa/Downloads/Github/GoProject/src/github.com/pingcap/tidb/util/kvcache/simple_lru.go
    2.55MB     3.05MB (flat, cum)  5.85% of Total
         .          .     91:   return element.Value.(*cacheEntry).value, true
         .          .     92:}
         .          .     93:
         .          .     94:// Put puts the (key, value) pair into the LRU Cache.
         .          .     95:func (l *SimpleLRUCache) Put(key Key, value Value) {
    1.50MB     1.50MB     96:   hash := string(key.Hash())
         .          .     97:   element, exists := l.elements[hash]
         .          .     98:   if exists {
         .          .     99:           l.cache.MoveToFront(element)
         .          .    100:           return
         .          .    101:   }
         .          .    102:
         .          .    103:   newCacheEntry := &cacheEntry{
         .          .    104:           key:   key,
         .          .    105:           value: value,
         .          .    106:   }
         .          .    107:   hashSize := SizeOf(hash)
         .          .    108:   singleSize := SizeOf(newCacheEntry)
         .   512.02kB    109:   element = l.cache.PushFront(newCacheEntry)
    1.05MB     1.05MB    110:   l.elements[hash] = element
         .          .    111:   l.size++
         .          .    112:   l.capacity = 200000
         .          .    113:   // Getting used memory is expensive and can be avoided by setting quota to 0.
         .          .    114:   if l.quota == 0 {

```  
We can find that the `hash` (the key of cache) and the `element`(the value of the cache) totolly consume 2.55 MB.

3. we use [sizeof](https://github.com/templarbit/sizeof) (the result is similar, but lower, not exact) to calculate the size of each key and element is 80byte and 40byte.
4. As 2.28 MB (120 byte * 20000) is similar to the 2.55MB, we can ensure that the heap profile would reflect the heap usage of `SimpleLRUCache`.

## Compatibility and Mirgration Plan

None




