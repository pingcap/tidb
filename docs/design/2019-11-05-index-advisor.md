# Proposal: Support automatic index recommendation

- Author(s): [Zou Huan](https://github.com/zouhuan1215) 
- Last updated:  Nov. 5, 2019
- Discussion at: 

## Abstract

This proposal aims at designing a new DBA tool -- index advisor, which automatically recommends indexes for input workloads. With these recommended indexes added to tables, the overall execution time of input workloads is supposed to decrease. The current proposal only considers single-column index recommendation.


## Background

Appropriate indexes can speed up query execution a lot. However, in real-world applications, determining proper indexes for a database according to a typical workload purely by hand, is an excessively challenging task. When deciding reasonable indexes for a large-scale database, this task becomes even more tough as one database may contain dozens of tables in which each table may have dozens of columns, whereas in the typical workload thousands of queries are involved in. Fortunately, we already have an excellent tool -- TiDB SQL optimizer that can be utilized to get through this tough task. The main purpose of this proposal is to utilize existing TiDB SQL optimizer to automatically recommend suitable indexes for database given the workload.

To illustrate how SQL optimizer can help us determine suitable indexes, here is an intuitive example: 

First assume that all indexes on table `persons` are available. Then comes a query like `SELECT name FROM persons WHERE age > 18`. When responding to this query, the SQL optimizer is more likely to output an execution plan which does *index_scan* upon column `age`. Now that the output execution plan requires *index_scan* upon column `age`, we can tell  that `age` is a more suitable index for current query than `name`, `address`, etc. The point here is: **by observing the execution plan output by SQL optimizer, we will know which indexes are favored by input queries.** 

## Proposal

### Design Overview

For problems like *what kind of indexes should be added to schema*, *how to value these hypothetic (potential / possible) indexes based on execution plan and its cost*, they are handled by the top layer logic of index advisor. This logic involves three steps: (1) recommend indexes for single query; (2) calculate a score for each index and get an ordered score list descending with index's score; (3) pick the top `n` indexes as the *initial recommended index set* and then launch the *Swap and Re-evaluate* algorithm. Following details the three steps:

1. **Potential indexes**. This proposal only considers single column indexes, thus all columns in a table can be potential indexes and column statistics is the corresponding index's statistics.

2. **Evaluate indexes**: 
   - For each query, compile it and get the execution plan's cost denoted as `original_cost`. Then compile the same query again **with all potential indexes added to schema**, extract selected indexes from output execution plan as `LSIs` (Local Selected Index) and denote the corresponding cost as `virtual_cost`. Calculate `reduced_cost = original_cost - virtual_cost`. 
   - Respectively add up `LSIs' reduced_cost` of each query in the workload. Denote the *sum* as `LSIs'` final scores.
   - Get the score list of `LSIs` descending with their corresponding score. 

3. **Evaluate index set**: 
   - Pick the top `n` indexes as the initial `recommended index set`. Denote the remaining unselected index set as `remaining index set`.
   - Execute the following *Swap and Re-evaluate* algorithm. 

```golang
// GCIS stands for Global Candidate Index Set
sort(GCIS, GCIS.Score, Descend_Order)
recommend_set = GCIS[:N]
remaining_set = GCIS[N:]
min_cost = EvaluateWorkload(workload, recommend_set)

for {
    v_recommend_set, v_remaining_set = SwapElements(recommend_set, remaining_set, swap_size)
    variant_cost = EvaluateWorkload(workload, v_recommend_set)
    if variant_cost < min_cost {
        min_cost = variant_cost
        recommend_set = v_recommend_set
        remaining_set = v_remaining_set
    }
    ......
    // loop until specific rounds or timeout, break
}
```

Note that executing `Swap and Re-evaluate` algorithm is necessary as the `reduced_cost` sometimes is a joint effect of several indexes and it's hard to tell each index's independent contribution to the final `reduced_cost`. For example, assume there is an extremely slow query in input workload and the desired indexes for this query is `a` and `b`. However, the number of allowed recommended indexes for the whole workload is limited and for some reason, `a` ranks top `n` in the final score list while `b` is not. But there are chances that without `b`, `a` can no more optimize that extremely slow query. 

----------------------------------------------
### A quick exmaple for single-column index recommendation

**Workload**:

```
- Query1: SELECT COUNT(*) FROM t1 where a = 1 and b = 5
- Query2: SELECT a FROM t1 ORDER BY c limit 10 
- Query3: DELETE FROM t1 WHERE a = 1 AND d = 1
```
where index on column *c* has already existed. 

- **Step1**: recommend indexes for single query. For each query, compile it and get the `original_cost` output by SQL optimizer. Then add all possible hypothetic indexes *a, b, d* to the schema, compile the same query again. After the compiling, `LSIs` can be extracted from output execution plan as well as the `virtual_cost`. Denote `reduced_cost` as *original_cost - virtual_cost*. Assume this step outputs:

```
Query 1--LSIs: {a, b}, reduced_cost: 4
Query 2--LSIs: {c},    reduced_cost: 4
Query 3--LSIs: {a, d}, reduced_cost: 3
```
- **Step2**: get the score list. Simply add up `LSIs` and `reduced_cost` of each query respectively and then sort the list descending by score. Assume this step outputs:

```
a: 4+3=7
b: 4
c: 4
d: 3
```

- **Step3**: evaluate the recommended index set. Assume the number of allowed indexes recommended by index advisor is *2* and the swap size is *1* 
  - The initial `recommended index set` is `{a, b}`, the `remaining index set` is `{c,d}` 
  - Add recommended index set `{a, b}` to the schema and at the same time mute existed index `c`
  - Compile all queries in the workload and add up each query's cost. Assume the sum is *20* and then assign it variable `min_cost`
  - Randomly exchange *1* element between the recommended set and remaining set. Assume *a* and *c* are chosen respectively, now the recommended set is `{c, b}` and the remaining set is `{a, d}`. 
  - Apply recommended index set `{c, b}` to the workload and get a sum cost, say *18*. 
  - As the cost decreases from *20* to *18*, update the recommended set from `{a, b}` to `{c, b}` and update `min_cost` to *18*. Then try swapping 1 element between `{c, b}` and `{a, d}` and execute the re-evaluate process. 

Note that if the sum cost with `{c, b}` is *22* (greater than 20), the next *swap* process should happen between `{a, b}` and `{c, d}` instead of between `{c, b}` and `{a, d}`. Loop the *Swap and Re-evaluate* process until timeout.

## Rational 

In this design, *index advisor* is implemented in a user friendly way, by adding a new SQL syntax `index advise...` instead of an independent third-party tool. However, for simplicity, syntax like `add virtual index...` which serves as building blocks of *index advisor* is not exposed. Therefore, if DBAs want some functionality like `add virtual index` or `alter invisible index`, they cannot benefit directly from current *index advisor* implementation. 

## Compatibility

No compatibility issues.

## Implementation

### <span id="compatibility">**Support from TiDB SQL optimizer** </span>

1. **add hypothetic indexes to schema** 
   - **effect**: query should be compiled with *schema* that contains input hypothetic indexes 
   - **constraints**: these hypothetic indexes should take effect only within the current index advisor session  
   - **implementation**: get a copy of *session schema* in memory. Then add hypothetic indexes to schema copy and compile the query with this modified schema copy

2. **alter invisible indexes**
   - **effect**: input existed indexes on table should be invisible to optimizer 
   - **constraints**: only within current index advisor session, input indexes are invisible to optimizer 
   - **implementation**: get a copy of *session schema* in memory. Then delete input indexes from schema copy and compile the query with this modified schema copy

3. **extract LSIs and cost from execution plan**
   - **effect**: given the execution plan output by SQL optimizer, related indexes in this plan should be extracted as local selected indexes. Besides, best plan's `cost` should be returned to the top layer of index advisor that invokes TiDB's SQL optimizer.
   - **constraints**: no
   - **implementation**: maybe rewriting the function `compiler.Compile` is a simpler way

### **Implement top layer logic of index advisor**

Add a new SQL grammar for index advisor:

```
INDEX ADVISE
  [LOCAL]
  INFILE 'file_name'
  [MAX_MINUTES number]
  [MAX_IDXNUM
     [PER_TABLE number] 
     [PER_DB number]
  ]
  [LINES 
    [STARTING BY 'string'] 
    [TERMINATED BY 'string']
  ]
```
Options in bracket `[]` is optional, while others are compulsory. The output of this grammar look like this:

| Table          | Recommended index | Num of hit queries | Reduced_cost |
| :-----------:  | :-------------: | :----------------: | :----------: |
| employee       | age             | 6                  | 100          |
| employee       | position        | 2                  | 20           |
| department     | department_id   | 3                  | 60           |

## Open issues (if applicable)

