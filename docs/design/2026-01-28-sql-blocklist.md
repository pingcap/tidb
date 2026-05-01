* Author: @niubell
* Last updated: 2026-02-24
* Tracking Issue: [#66048](https://github.com/pingcap/tidb/issues/66048)

# SQL Blocklist by Digest or Keyword
## Summary

Provide a SQL blocklist that blocks statements by normalized SQL text or by
keyword matching, optionally scoped to a user. Rules are stored in
`mysql.sql_blocklist`, updated without restart, and applied cluster-wide.

## Scenarios

### Scenario 1: High-frequency SQL

A misbehaving client can generate a burst of repeated SQL or certain high-frequency SQL queries encountered during e-commerce flash sales that overwhelms the cluster (CPU, memory, or storage). Operators need a fast, low-overhead way to block that specific normalized SQL across all TiDB instances immediately, and to remove the rule once the incident is mitigated.

### Scenario 2: Dangerous DDL

DDL such as `DROP DATABASE`, `DROP TABLE`, or large-scale `ALTER TABLE` can
cause irreversible data loss or extended service impact if executed by mistake.
Operators can blocklist the exact normalized DDL to prevent accidental or
malicious execution while keeping normal traffic unaffected.

### Scenario 3: DDL or DML not allowed in the current environment

Some environments (for example, staging or production) may forbid certain DDL
or DML even if they are valid in other environments. A blocklist rule provides
a simple, centralized safeguard to block these statements without changing
application code or privileges.

## Requirements

- Support two rule types:
  - SQL digest (normalized SQL text)
  - SQL keywords (comma-separated)
- Support user scoping:
  - `username = '*'` applies to all users
  - Otherwise only the matching user is blocked
- Changes take effect immediately across all TiDB instances.
- No restart required.
- Minimal overhead on the hot path.

## Non-Goals

- No pattern language beyond keyword substring matching.
- No per-database scoping.
- No on-disk format changes outside `mysql.sql_blocklist`.

## Data Model

Table: `mysql.sql_blocklist`

```sql
CREATE TABLE IF NOT EXISTS mysql.sql_blocklist (
  type  VARCHAR(16) NOT NULL,
  value VARCHAR(1024) NOT NULL,
  username  VARCHAR(32) NOT NULL DEFAULT '*'
);
```

Rule semantics:

- `type = 'digest'`
  - `value` is normalized SQL text (e.g. `delete from t where id = ?`).
- `type = 'keyword'`
  - `value` is a comma-separated list of keywords.
- `username`
  - `'*'` applies to all users.
  - Otherwise only the matching user is blocked.
  - Case-insensitive match on the current login user.
  - Empty/NULL value is treated as `'*'`.

## Matching Rules

### Digest rules

Normalize the SQL text using TiDB's SQL normalizer and compare for exact match.

### Keyword rules

- Split the `value` by comma.
- Trim and lowercase each keyword.
- A SQL is denied if all keywords are present as substrings in the SQL text
  (case-insensitive).

### Username rules

- `username` is compared with the session login user (no host part).
- Matching is case-insensitive.
- If both a `username = '*'` rule and a specific `username` rule match, the SQL
  is denied.

## Reload and Distribution

### Local reload

`sqlblocklist.Load` reads `mysql.sql_blocklist` and rebuilds the in-memory cache.

### Cluster-wide propagation

- Each TiDB instance runs a watch loop on an etcd key.
- On updates, it reloads its local cache.

### Triggering reload

Reload is triggered by:

- `ADMIN RELOAD SQL_BLOCKLIST`
- DML changes to `mysql.sql_blocklist` that commit successfully.

## Execution Flow

1) On statement execution:
   - Compute digest and normalized SQL from `StmtCtx.SQLDigest()`.
   - If not in restricted SQL mode, check the blocklist with current user.
2) If a rule matches:
   - Return `ErrSQLDeniedByBlocklist`.

Restricted/internal SQL bypasses the blocklist.

## Error Behavior

Return `ErrSQLDeniedByBlocklist` with a message describing the rule:

- `digest_text <normalized>`
- `keywords <k1, k2, ...>`

## Performance Considerations

- Cache is read-only and stored in an `atomic.Pointer`, avoiding locks.
- When the cache is empty, checks are fast and allocate nothing.
- Keyword matching uses lowercase SQL only when keyword rules exist.

## Compatibility

- Uses a single system table `mysql.sql_blocklist`.
- No legacy compatibility is required for this feature stage.

## Testing

- Unit tests cover:
  - Digest by normalized SQL.
  - Keyword matching.
  - `ADMIN RELOAD SQL_BLOCKLIST`.
- Reload behavior is validated by clearing the auto-reload flag and forcing
  manual reload.

## Examples

### Digest rules

```sql
INSERT INTO mysql.sql_blocklist VALUES
  ('digest', 'delete from t where id = ?', '*'),
  ('digest', 'update accounts set balance = balance - ? where id = ?', 'ops'),
  ('digest', 'select * from orders where status = ? and created_at > ?', '*'),
  ('digest', 'drop database app', '*');
```

Denied SQL:

```sql
DELETE FROM t WHERE id = 1;
UPDATE accounts SET balance = balance - 10 WHERE id = 42;
SELECT * FROM orders WHERE status = 'paid' AND created_at > '2026-01-01';
DROP DATABASE app;
```

### User scoped rules

```sql
-- Only block for user 'app'
INSERT INTO mysql.sql_blocklist(type, value, username)
VALUES ('digest', 'select * from t where id = ?', 'app');

-- Block for all users
INSERT INTO mysql.sql_blocklist(type, value, username)
VALUES ('keyword', 'drop table, if exists', '*');
```

### Mixed user and global rules

```sql
-- Global rule applies to every user
INSERT INTO mysql.sql_blocklist(type, value, username)
VALUES ('keyword', 'truncate table', '*');

-- User-specific rule for 'ops' only
INSERT INTO mysql.sql_blocklist(type, value, username)
VALUES ('digest', 'drop table t', 'ops');
```

Effect:

- User `ops` is blocked by both the `truncate table` keyword rule and the
  `drop table t` digest rule.
- Other users are blocked only by the `truncate table` keyword rule.

### Case-insensitive usernames

```sql
INSERT INTO mysql.sql_blocklist(type, value, username)
VALUES ('digest', 'select * from t where id = ?', 'App');
```

The rule above blocks the login user `app` as well.

### Keyword rules

```sql
INSERT INTO mysql.sql_blocklist VALUES
  ('keyword', 'modify column, null', '*'),
  ('keyword', 'drop table, if exists', 'ops'),
  ('keyword', 'create user, identified by', '*');
```

Denied SQL:

```sql
ALTER TABLE t MODIFY COLUMN v BIGINT NULL DEFAULT -1;
DROP TABLE IF EXISTS t;
CREATE USER u IDENTIFIED BY 'pwd';
```
