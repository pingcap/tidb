# Proposal: Session Manager

- Author(s): [crazycs520](https://github.com/crazycs520)
- Tracking Issue: https://github.com/pingcap/tidb/issues/6840

## Abstract


This proposes a design of transaction savepoint.

## Background

Some Applications need to use transaction savepoint feature. And in order to be better compatible with some ORM frameworks, such as gorm.

### Goal

- Support transaction savepoint syntax.
- Support transaction savepoint feature, and compatible with MySQL.

### Non-Goals

- Not for implementing savepoint on the entire database, but savepoint on transaction modifications.

## Proposal
