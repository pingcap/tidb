# Proposal: Implement Radix Hash Join 

- Author(s):     XuHuaiyu
- Last updated:  2018-09-21

## Abstract

This proposal proposes to implement radix hash join in TiDB, aimed to improve the performance of the `HashJoin` executor. 

## Background

Currently, TiDB uses `non-partitioning hash join` to implement `HashJoinExec`. This algorithm starts by building a **single global hash table** for the inner relation which is known as the `build phase`. After the hash table be built, `probe phase` assigns equal-sized sub-relations (chunks) of the outer relation to multiple threads to probe against the global hash table simultaneously.

[Shatdal et al.](http://www.inf.uni-konstanz.de/dbis/teaching/ws0203/main-memory-dbms/download/CCA.pdf) identify that when the hash table is larger than the cache size, almost every access to the
hash table results in a cache miss. So the `build phase` and `probe phase` of `non-partitioning hash join` would cause considerable cache miss when writing or reading the single global hash table.

Partitioning the hash table into cache-sized chunks can reduce cache misses and improve performance, `partition-based hash join` introduces the `partition phase ` for hash join. `Partition phase` partitions the input relations into small pairs of partitions where one of the partitions typically **fits into one of the CPU caches**. During the `build phase`, a **separate hash table** is built for each partition respectively. During the `probe phase`, a sub-partition of outer relation will only be probed against the corresponding hash table which can be totally stored in the CPU cache.

The overall goal of this proposal is to minimize the **ratio of cache misses** when building and probing hash table in `HashJoinExec`.

## Proposal

In a nutshell, we need to introduce a `partition phase` for `HashJoinExec`. 

1. Partition algorithm

We can use the **parallel radix cluster** algorithm raised by [Balkesen et al.](https://15721.courses.cs.cmu.edu/spring2016/papers/balkesen-icde2013.pdf) to do the partitioning work. This algorithm clusters on the leftmost or rightmost **B bits** of the integer hash-value and divide a relation into **H sub-relations** parallel, where *H = 1 << B*.

The original algorithm does the clustering job using multiple passes and each pass looks at a different set of bits, to ensure that opened memory pages would be no more than the TLB entries at any point of time. As a result, the TLB miss, caused by randomly writing tuples to a large number of partitions, can be avoided. Two or three passes are needed to create cache-sized partitions, since data would be copied during every pass of `partition phase`, and the possibility of OOM may increase. So we choose 1-pass radix cluster algorithm to avoid the risk of OOM.

Furthermore, `partition phase` can be executed parallelly by subdividing both relations into sub-relations that are assigned to individual threads, but there will be contention when different threads write tuples to the same partition simultaneously, especially when data is skewed heavily. In order to resolve this problem, we scan both input relations twice. The first scan computes a set of histograms over the input data, so the exact output size is known for each thread and each partition. Next, a contiguous memory space is allocated for the output. Finally, all threads perform their partitioning without any synchronize. In addition, we use **[]\*chunk.List** to store every partition, concurrent write to **column.nullBitmap** may also raise contention problem during the second scan. So we also need to pre-write the null-value info during the first scan.

Obviously, if the input data is skewed heavily, the `partition phase` would become redundant, especially when all the data belongs to the same partition. We can determine to choose the `non-partitioning hash join` when we get the histogram after the first scan and the size of some partitions exceed the threshold value.

To sum up, we use a `parallel 1-pass radix cluster` algorithm to partition the relations. 

1. Parameters of radix clustering

Since L3 cache is often shared between multiple processors, we'd better make the size of every partition be no more than L2 cache. The size of L2 cache which is supposed to be *L* can be obtained from third-party library, we can set the size of every partition to *L \* 3/4* to ensure that one partition of inner relation, one hash table and one partition of outer relation fit into the L2 cache when the input data obey the uniform distribution.

The number of partition is controlled by the number of radix bit which is affected by the total size of inner relation. Suppose the total size of inner relation is *S*, the number of radix bit is *B*, we can infer *B = log(S / L)*.

1. Others

   3.1 We can define the partition as type chunk.List, thus `HashJoinExec` should maintain an attribute **partitions []*chunk.List**.

   3.2 For outer relation, `partition phase` and `probe phase` would be done batch-by-batch. The size of every batch should be configurable for exploring better performance.

   3.3 After all the partitions have been built, we can parallelly build the hash tables.

## Rationale

This proposal introduce a `partition phase` into `HashJoinExec` to partition the input relations into small partitions where one of the partition typically fits into the L2 cache. Through this, the ratio of cache during `build phase` and `probe phase` can be reduced and thus improve the total performance of `HashJoinExec`.

The side effect is that the memory usage of `HashJoinExec` would increase since the `partition phase` copies the input data. 

## Compatibility

This proposal has no effect on the compatibility.

## Implementation

1. Investigate the third-party libraries for fetching the L2 cache size and calculate the number of radix bit before `build phase`. @XuHuaiyu
2. Partition the inner relation parallelly. @XuHuaiyu
3. Build hash table parallelly. @XuHuaiyu
4. Partition the outer relation and probe against the corresponding hash table parallelly. @XuHuaiyu

## Open issues (if applicable)

https://github.com/pingcap/tidb/issues/7762
