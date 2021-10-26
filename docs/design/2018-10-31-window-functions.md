# Proposal: Support Window Functions

- Author(s): [Haibin Xie](https://github.com/lamxTyler)
- Last updated: 2018-10-31
- Discussion at:

## Abstract

This proposal aims to support the window functions. Window functions are a widely used feature, so after supporting it, TiDB will be more friendly to users.

## Background

A window function calculates results for each row using rows related to it. Let's first take an example to get a concrete feeling:

```sql
MySQL [test]> select a, b, sum(b) over(partition by a) as 'sum', avg(b) over(partition by a order by b rows between 1 preceding and 1 following) as 'avg' from t;
+------+------+------+--------+
| a    | b    | sum  | avg    |
+------+------+------+--------+
|    1 |    1 |   10 | 1.5000 |
|    1 |    2 |   10 | 2.0000 |
|    1 |    3 |   10 | 3.0000 |
|    1 |    4 |   10 | 3.5000 |
|    2 |    5 |   26 | 5.5000 |
|    2 |    6 |   26 | 6.0000 |
|    2 |    7 |   26 | 7.0000 |
|    2 |    8 |   26 | 7.5000 |
+------+------+------+--------+
8 rows in set (0.01 sec)
```

We can see that different from normal aggregate functions, window functions produce results for each query row. It can also allow users to specify the rows that are used to calculate results, like the case of `avg`.

A window specification is composed of three optional parts:

* **partition clause**:  Partition clause is used to divide query rows into partitions. Each row can only use the rows within the same partition to calculate the result.

* **order clause**: Order clause is used to sort the rows in a partition. It is important for window functions that need order, like `RANK`.

* **frame clause**: Frame clause defines the subsets of a partition. Most window functions operate on the frame of the current row. However, some functions that only operate on the partition will ignore the frame clause, like `ROW_NUMBER`.

## Proposal

From the above description, we can know that in order to support window functions, the most important part is organizing the rows according to window specification. In the following parts, the proposal will introduce how to organize rows for a window function and how to optimize the execution order of multiple window functions.

### Organize rows for a window function

Let's first take a look at the partition clause and order clause. The simplest way to meet the specification is to sort the rows by combining the two clauses. We can put a `Sort` operator below the window function. Also, the `Sort`  operator may be removed by the optimizer. 

However, we do not need to keep the partitions ordered. So another way is first hashing rows into different partitions, and then performing sorts within each partition.

For frame clause, MySQL does not support dynamic frame endpoints that depend on the value of the current row. This means that, for each row in the partition, the endpoints of the current row will not go backward compared with the previous row. So for each function, it only needs to support operations like `pop front`,  `push back` and `evaluate`.

### Optimize window functions order

For multiple window functions in the same query, it is important to optimize the order to reduce the number of data reorganizations. For example, if we have three window functions that need to be ordered by (a, b, c), (a, c) and (a,b), then it is better to order them as (a, b, c), (a, b) and finally (a,c). Also, when organizing the rows for (a,c), we can only partially sort on `c`.

If we only support partition by sorting, the simplest way is to sort the window functions by partition clause and order clause, so that the window function with the same prefix will be at near places.

If we also support partition by hashing, then finding the optimal order turns out to be an NP-hard problem. If the number of window functions is small, we can use a DP algorithm, like the join reorder problem. If the number of window functions is large, we can only use some heuristic algorithm. For example, we can first use some rules to build a partial order for two window functions, and then perform a topo-sort on the partial order graph to get the final order.

## Rationale

Window functions have been supported in a number of database systems. Without it, our users have to use some workarounds to get the same result, which is both inefficient and error-prone.

## Compatibility

Yes, this proposal makes TiDB more compatible with MySQL 8.0.

## Implementation

We can split the implementations into two stages: the first stage to support the window function, but using the easiest way; the second stage to optimize the window function execution cost.

In this first stage, we can support the grammar and functions for window functions and using the `Sort` operator to organize rows. For multiple window functions, we do not optimize their order and execute them one by one. We should also add tests as many as possible in this stage.

In the second stage, we can mainly focus on the optimization of the window functions. We can first merge the execution of window functions with the same specification. After that, we will support partition by hashing to organize the rows. Finally, we can try to optimize the window functions execution order to reduce the data reorganization cost.

## Open issues (if applicable)
