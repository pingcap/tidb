# TiDB privilege Test Case Map (pkg/privilege)

## Overview

- Grouped by package directory.
- Each test file has a one-line description based on its primary test/benchmark name.
- Testdata lists files under `testdata/` mapped to their owning package directory.

## pkg/privilege/privileges

### Tests
- `pkg/privilege/privileges/cache_test.go` - Tests load user table.
- `pkg/privilege/privileges/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/privilege/privileges/privileges_test.go` - Tests check DB privilege.
- `pkg/privilege/privileges/tidb_auth_token_test.go` - Tests auth token claims.

## pkg/privilege/privileges/ldap

### Tests
- `pkg/privilege/privileges/ldap/ldap_common_test.go` - Tests canonicalize DN.
