# Proposal: Support Full Pessimistic Transaction

- Author(s): [jackysp](https://github.com/jackysp)
- Tracking Issue: https://github.com/pingcap/tidb/issues/28689

## Abstract

Trying to introduce an option to make implicit transactions in pessimistic mode.

1. Solve the problem of incomplete lock view
2. Reduce the cost of user understanding
3. Avoid deadlock of two transactions implicitly committed. (optimistic transaction deadlock)

## Background

Implicit transactions are still executed in optimistic mode
even if the pessimistic transaction option is turned on,
which is hard to explain to users.
And often we fall into the same trap
when troubleshooting problems ourselves.
Also, for the lock view, since it reads info from the wait manager,
the lock view is not available for optimistic transactions.
Therefore, due to the optimistic implicit transactions,
causing incomplete lock information can also cause confusion to users.


## Proposal

Unify all types of transactions in pessimistic mode,
and implicit transactions are also in pessimistic mode by introducing an option.

## Compatibility

It improves compatibility with MySQL.
