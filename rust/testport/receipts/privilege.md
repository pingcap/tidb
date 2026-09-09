# `pkg/privilege` — Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).

## Complete inventory

The package owns 22 tracked artifacts and 8,415 lines, including the public
manager interface, the privilege cache and manager, LDAP authentication, JWT
fixtures, tests, Bazel targets, and certificate/key fixtures. Every artifact
below was read before editing. There is no generated source, benchmark, fuzz
target, or platform-specific Go variant.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `pkg/privilege/BUILD.bazel` | 17 | `3e07babb15194e15564eb489f89b5c90ad44cb8c` | `f2fefbf20229c9fabe61feb3158fc3e12ade2f527fcf4f5e2b55312a584ea35b` | public manager target |
| `pkg/privilege/conn/BUILD.bazel` | 8 | `06cad5a21aa28ed899e13903e7a61c3fbc146a6d` | `05f65b69a7e5172d0f20e682f4048bbde6c07ed165a2ee5097f719255c78f02e` | auth connection target |
| `pkg/privilege/conn/conn.go` | 30 | `6746c2fcf848ab3c4bad18f43ccaf04e3517f06c` | `f3d4a4d21159aeb8296be9a0de358829a3a1048cc0f302398c55491a3ef6bfe0` | auth packet/flush interface |
| `pkg/privilege/privilege.go` | 142 | `d1eb61a567e402524ef4501dd95c871d3ec6de85` | `24c37c45a7c59f843ddc4730f82a5b08a1b592a57d8f0aa911593bc93a4463e4` | `Manager`, context binding, verification contract |
| `pkg/privilege/privileges/BUILD.bazel` | 98 | `7b19e38624317eef4296ef0eee65277994926556` | `db456e57c17187d5cc3e43497b8387da8417c2248f230fc3ef98ae4674d7bfe3` | cache/manager library and 50-shard test target |
| `pkg/privilege/privileges/cache.go` | 2290 | `32980b89a4aec60f5b66bbb360c24883a202e2bb` | `c68e2c93ae6b92311ef4649868e25c5f532c5c26f226423a65ecfb8c661a14eb` | privilege-table cache, matching, grants, roles, visibility |
| `pkg/privilege/privileges/cache_test.go` | 589 | `3df811f7b95a24a3d3c77475df9f2b03e24097f9` | `bc52ab2c3814107adaf8c053ae352a9726bfa6955929af9ad48955834fc14b97` | cache loading, matching, role, visibility tests |
| `pkg/privilege/privileges/errors.go` | 31 | `2caaade103fe4335fb6c4d03362943f6cd636962` | `0cd63c5f8709c4f0c394ee1e04d08ecbcedfcfe14ee635875372e06bcc4938b4` | privilege/auth error identities |
| `pkg/privilege/privileges/ldap/BUILD.bazel` | 37 | `99fe2046d6e70a16e2cd41bc20443baf71a3a4a1` | `3700cf1e1c125c84894fc5f4c1783ab294f1d97a205791a93c695c937e36ab30` | LDAP library and four-shard TLS test target |
| `pkg/privilege/privileges/ldap/const.go` | 24 | `9dfdedd07ccabca3ee396d8cb95cf895d761cec0` | `4d8ce27c3dfd611959f6fd1ec073203ad65831cd893dcef66da57d29e637445f` | LDAP constants |
| `pkg/privilege/privileges/ldap/ldap_common.go` | 437 | `a91fc69202abb12cea8615674a6808181482b8a9` | `0314d94646ab3bb027329e2b0ad7e6667ddaae080248a760aaf0cf33c0ef25bf` | LDAP connection pool, search, TLS, setters/getters |
| `pkg/privilege/privileges/ldap/ldap_common_test.go` | 254 | `de6c1f4a938faa65273eb33c9d6bc64c5fa68c43` | `4b0353979ec222bf6cb72295b72e95e3aa8a0025f64b7d929e658ae865e36ece` | DN, StartTLS/LDAPS, TLS-version, timeout tests |
| `pkg/privilege/privileges/ldap/sasl.go` | 119 | `0456ef6849eac888dc01824df5d2d3c15fa37bab` | `f562e2bbbf2c37aee9c706fb99a5245261736bea6385fb9577fa1b128b4f3c82` | LDAP SASL auth exchange |
| `pkg/privilege/privileges/ldap/simple.go` | 65 | `d6dabc64eebda61929967bd176afd1d130f8d340` | `ddd3371f1bc30a8f828a6bb8b96adf797b883ded2cd11e98067f894273de9d44` | LDAP simple bind |
| `pkg/privilege/privileges/ldap/test/ca.crt` | 22 | `b9e588bc3949419d856098b49bf17ffbc91e455a` | `990cb9bfcc913930bca11e8165c844f75ad98119d524360224ad9b6b17fb8074` | embedded CA fixture |
| `pkg/privilege/privileges/ldap/test/ldap.crt` | 23 | `5dc675232c025e5b148d5477137937656689afc7` | `a04fed0022ade34cc734763053fe4306e2997f27275603a82f49013d4c474670` | embedded server certificate |
| `pkg/privilege/privileges/ldap/test/ldap.key` | 28 | `8a9a8f61f4573f4c43b42c6d43b2509dbf50de3e` | `292c34ae068dcaec4526443d678b7a0660abb9fb96aec2ab9cdcd750eed8d32f` | embedded server key fixture |
| `pkg/privilege/privileges/main_test.go` | 41 | `24c59de635058b974fc4beaea76fed8c47efb3ce` | `430a1966ca5ef1f9c55db3b756e8f58d62a2450e9ddd769b3d9515d3ad22ea7a` | common setup and goleak harness |
| `pkg/privilege/privileges/privileges.go` | 1190 | `8e0664a1ebfdbd4491810dc8ea4c1f55d4ddeb06` | `e67987f6129840ccefece0067cf342c9625237042fdd21ff8ff1784045bf3525` | manager auth, static/dynamic privilege API, SSL/SAN, expiry |
| `pkg/privilege/privileges/privileges_test.go` | 2359 | `031150b3eff1debd81a92105527a7847bd3140be` | `03609023d66813dbc185b9fa5a5e7911d7acff258d60c1473f91214e8e36845b` | privilege, auth, SSL, SEM, role, expiry, grant tests |
| `pkg/privilege/privileges/tidb_auth_token.go` | 117 | `8c55a192ddfa76e5110772b0199dc15bf100b162` | `228f8d141e892ad7c61be4e36fad29f4cc7e4ca92a23060dbe39b3cb0c977a77` | JWKS loading, refresh, JWT signature verification |
| `pkg/privilege/privileges/tidb_auth_token_test.go` | 494 | `1612d52774015591dd1f940b9f36f8a56b4286f7` | `36827667c71a3798c3b69962735b8ea1e3bc1644a9a06df6f1194418a0eefacc` | JWT claims, key rotation, malformed token tests |

## Rust ownership and fix

Rust's dependency-closed owner is the `tidb-session` privilege registry and
its `tidb-mysql` privilege catalog. It covers account/global/database/table/
column grants, roles, dynamic grants, password plugins, and `SHOW GRANTS`.
LDAP transport, JWKS/auth-token verification, extension callbacks, full
manager/session integration, and all Go storage reload paths remain outside
that owner; this package is therefore an explicit SEED/boundary, not a
complete package claim.

The source comparison found two executable divergences in the owned path.
First, Go uppercases database names with Unicode-aware `strings.ToUpper` before
its binary wildcard matcher, while Rust used `to_ascii_uppercase`. A grant
such as `ТЕ%` therefore failed to match `тест` only on the Rust path. The Rust
matcher now uses Unicode `to_uppercase()` for both operands. The focused
regression `database_matching_folds_non_ascii_like_go_strings_to_upper` failed
before the fix and passes after it.

Second, the physical `LogicalMemTable` builder dropped logical output names and
left scan-column `orig_name` empty. Go's `buildMemTable` preserves both, and
without them a wildcard query over the empty
`INFORMATION_SCHEMA.{SCHEMA,TABLE,COLUMN}_PRIVILEGES` relations returned
synthetic `Column#N` headers instead of the declared names. The physical-plan
regression `a_mem_table_physical_plan_keeps_declared_output_names`, the
`buildMemTable` schema-name assertion, and the owner-level
`infoschema_privileges_tables_are_header_only` test now pin the fix.

## Validation and risk

Profile: **Ready** for this code batch. No Go/Bazel/import/go.mod source
changed, so `make bazel_prepare` is not required.

- Focused Rust regressions and the owner suite passed (50 passed, 3 ignored).
- Go package suite was run with `go test ./pkg/privilege/... -count=1`; the
  manager suite was environment-heavy, with
  `TestProtectUserAndRoleWithRestrictedPrivileges` requiring `--tags=intest`
  and `TestLDAPStartTLSTimeout` exceeding its 3-second timing budget locally.
- `cargo fmt --all -- --check`, workspace `cargo check --offline --locked`, and
  `make lint` are the Ready gates for this batch.

Risks: Unicode case expansion allocates two temporary strings in the matcher;
this is correctness-preserving for MySQL's case-insensitive identifiers and
does not alter ASCII behavior. Carrying virtual-table names affects result
metadata only; the empty-row contract is unchanged. LDAP/JWT and manager/
session integration remain unverified Rust boundaries.
