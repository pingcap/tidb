# Proposal: Change timestamp bounds read syntax for read only transaction

- Author(s):     [Xiaoguang Sun](https://github.com/sunxiaoguang)
- Last updated:  2021-01-24
- Discussion at: https://github.com/pingcap/tidb/issues/22505

## Abstract

This proposal proposes new syntax to staleness read and deprecates existing timestamp bounds read SQL syntax which is over complicate.

## Background

Existing timestamp bound read SQL syntax is verbose and complicate. 

```sql
START TRANSACTION READ ONLY WITH TIMESTAMP BOUND
START TRANSACTION READ ONLY WITH TIMESTAMP BOUND STRONG
START TRANSACTION READ ONLY WITH TIMESTAMP BOUND MAX STALENESS '00:00:10'
START TRANSACTION READ ONLY WITH TIMESTAMP BOUND EXACT STALENESS '00:00:05'
START TRANSACTION READ ONLY WITH TIMESTAMP BOUND READ TIMESTAMP '2019-11-04 00:00:00'
START TRANSACTION READ ONLY WITH TIMESTAMP BOUND MIN READ TIMESTAMP '2019-11-04 00:00:00'
START TRANSACTION READ ONLY WITH TIMESTAMP BOUND MIN TIMESTAMP '2019-11-04 00:00:00'
```

Since timestamp bounds read feature is never implemented on TiDB, it is a good time to fix it with a better design and make it more ergonomics.

## Proposal

Make it clear that staleness read can be used only when transaction is read-only. If the transaction contains any writes an error will be returned. Therefore ```READ ONLY``` and ```TIMESTAMP BOUND``` are unnecessary and can be removed. The new statements will become shorter:

```sql
START TRANSACTION WITH MAX STALENESS '00:00:10'
START TRANSACTION WITH EXACT STALENESS '00:00:05'
START TRANSACTION WITH TIMESTAMP '2019-11-04 00:00:00'
```


## Rationale

```sql
START TRANSACTION READ ONLY WITH TIMESTAMP BOUND
START TRANSACTION READ ONLY WITH TIMESTAMP BOUND STRONG
```

These statements are equivalent to START TRANSACTION READ ONLY, therefore can be removed.

```sql
START TRANSACTION READ ONLY WITH TIMESTAMP BOUND MAX STALENESS '00:00:10'
START TRANSACTION READ ONLY WITH TIMESTAMP BOUND EXACT STALENESS '00:00:05'
START TRANSACTION READ ONLY WITH TIMESTAMP BOUND READ TIMESTAMP '2019-11-04 00:00:00'
```

The redundant words ```READ ONLY``` and ```TIMESTAMP BOUND``` as well as ```READ``` are removed for clarity.

```sql
START TRANSACTION READ ONLY WITH TIMESTAMP BOUND MIN READ TIMESTAMP '2019-11-04 00:00:00'
START TRANSACTION READ ONLY WITH TIMESTAMP BOUND MIN TIMESTAMP '2019-11-04 00:00:00'
```

These statements are removed as they are obscured and useless.

## Compatibility and Mirgration Plan
Since existing syntax is never implemented on TiDB, it is unlikely to be used by anyone. We can simply remove current syntax and only support the new syntax.
