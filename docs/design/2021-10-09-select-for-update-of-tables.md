# Proposal: Support `SELECT FOR UPDATE OF TABLES`

- Author(s): [jackysp](https://github.com/jackysp)
- Tracking Issue: https://github.com/pingcap/tidb/issues/28689

## Abstract

If specific tables are named in a locking clause, then only rows coming from those tables are locked.
Any other tables used in the SELECT are simply read as usual.

## Background

In a multi-table join scenario, simply using the for update clause locks all the rows read in the tables participating in the join.
The lock granularity is too coarse, and some of the tables participating in the join may not need to be subsequently updated.
In MySQL 8.0, the `of tables` option was introduced to support locking only the specified tables.

## Proposal

### Parser

The first thing is that the parser should support the `of tables` syntax.
This has not been supported before. Store the table names in `SelectLockInfo`.

### Filter Keys

This feature is achieved by storing the information in `StmtCtx`
and using the table names in StmtCtx to filter the keys that need to be locked
when reading all the keys that need to be locked.

## Compatibility

Compatibility with MySQL 8.0 has been enhanced here.
Since the `of tables` syntax was not supported before,
it does not break any compatibility.
