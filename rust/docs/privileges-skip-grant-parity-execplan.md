# Complete `pkg/session/test/privileges` and wire skip-grant-table end to end

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root; this plan is maintained according to it.

## Purpose / Big Picture

Before this work, the Rust tree modeled TiDB's pre-authentication `SkipWithGrant` identity policy but did not connect it to server configuration, login, or a privilege-enforcing SQL session. Configured connections still required a `mysql.user` row and password, and Pipeline/Cluster sessions enforced grants. Go's `security.skip-grant-table` instead admits an arbitrary requested identity without consulting account rows and bypasses authorization while leaving account/role storage available for administrative statements.

After this plan is complete, a root-run Rust server that reads `[security] skip-grant-table = true` will apply one immutable startup policy to every configured or cluster-backed account store. The real MySQL handshake will retain the process-wide secure-transport gate but bypass row/plugin/password/lock/expiry checks, record that admission on the authenticated identity, and both pipeline and cluster session factories will disable authorization checks while continuing to attach the shared privilege registry. A normal server will retain exact 1045 behavior. A real TCP regression will execute the source role/grant chain through `SHOW GRANTS` (whose drained result is Go's fixed 1141 error in bypass mode), and a cluster-session regression will prove the second factory consumes the same admission marker.

## Progress

- [x] (2026-08-11 14:14Z) Pinned the three-artifact Go package at `d82d2baa0f95dd3782b65a74300642d834935cf2`, tree `4487cbf4474f68d551653bfd7ddfe023b1c6a64a`; the package tree remains unchanged through integration tip `fbf1abbf8eb9db07e605ef19a62f9f8237081cb5`.
- [x] (2026-08-11 14:14Z) Ran `TestSkipWithGrant` and `TestSessionAuth` with the canonical `intest,deadlock` build variants; both pass in Go.
- [x] (2026-08-11 14:32Z) Recorded the real TCP RED: the old handshake answered 1045 for the unknown user; after authentication alone was connected, `USE test` still answered 1044, proving statement authorization also needed the same admission marker.
- [x] (2026-08-11 14:32Z) Recorded the cluster RED by withholding the marker at `ClusterSessionFactory`: the zero-row identity's `CREATE ROLE` answered 1227, including across the account scratch-registry swap.
- [x] (2026-08-11 14:34Z) Connected source TOML/root validation, every production account-store constructor, secure-transport-first authentication, and both privilege-enforcing front-end factories through one bypass marker. Added exact role, process, global-variable, information-schema, and error-order contracts.
- [x] (2026-08-12 05:08Z) Closed final-review REDs: Pipeline sessions and handshakes now share one GLOBAL-variable authority; `SET DEFAULT ROLE ... TO CURRENT_USER` preserves Go's raw empty AST identity; multi-target default-role failure publishes nothing; complete sysvar snapshots replace rather than merge stale rows.
- [x] (2026-08-12 05:59Z) Closed security/ordering REDs found in fixed-tip review: DDL privilege checks now precede catalog mutation and implicit commit; a publication fence prevents stale GLOBAL reloads and orders ordinary local fallback by commit timestamp; post-commit refresh uncertainty may conservatively keep `require_secure_transport=ON` until notify/tick convergence but never fails open; unknown future sysvars cannot panic the fallback; and raw `USER_PRIVILEGES` no longer renders `GRANT OPTION` as `USAGE / YES`.
- [x] (2026-08-12 06:41Z) Closed the final secure-publication RED: after a durable ON, the live transport gate now rises before any long publication lock or reread; a pending-ON floor prevents an older OFF image from reopening plaintext while that reread waits. The single- and double-publisher channel regressions are part of the 22/22 publication suite.
- [x] (2026-08-12 06:41Z) Ran focused, owning-crate, lint, formatting, and final-diff gates and obtained independent Standards and Go-spec reviews. Canonical Go oracle 2/2, four-crate all-target check, config 74/74, session grants 93/93, configured-store 15/15, cluster accounts 6/6, cluster schema changes 12/12, real-node 11/11, grants wire 9/9, TLS wire 3/3, sysvar publication 22/22, vars 13/13, node-config 8/8, GLOBAL image/secure-SET regressions, `make lint`, formatting and diff checks are green; both fixed-tip reviews report no P0/P1.
- [x] (2026-08-12 07:42Z) Rebased the DCO-signed atomic package commit onto remote tip `fbf1abbf8eb9`, including the intervening distsql and OS-utility changes. Source-shaped regressions prove skip mode satisfies the remote DEADLOCKS PROCESS gate and SEM restricted-status check; the SEM assertion shares the remote test's existing enabled interval rather than adding a second process-global toggler. The OS-utility overlap retains both affinity parsing/application and the skip-grant security/configuration chain. The post-rebase four-owner check, node-config suite, 22/22 sysvar publication suite, parallel SEM regression, formatting, and committed diff check remain green.
- [ ] Push the rebased DCO-signed atomic package commit to `origin/hparser-integration` after its final committed-tip review.

## Surprises & Discoveries

- Observation: at the baseline, typed `IdentityLookupPolicy::resolve_with_policy` and `AuthPluginHandoff::for_bypass` existed but no production caller used either.
  Evidence: the initial repository-wide reference sweep found only tests outside their defining module; `ConfiguredUserStore::authenticate` called the ordinary resolver directly.

- Observation: at the baseline, source configuration validated `skip-grant-table`, but its root check was hard-coded false.
  Evidence: `tidb_config::Config::valid` called `has_root_privilege`, whose old implementation always returned false even though the crate already depended on `rustix` with process support.

- Observation: Go's `SHOW GRANTS` behavior in skip mode is intentionally unusual. The role-less form returns 1141 for the fixed identity `root`@`%`; an explicit `USING role` first calls `FindEdge`, which returns false in skip mode, and therefore returns 3530 instead.
  Evidence: `pkg/executor/show.go::fetchShowGrants` validates `e.Roles` before calling `UserPrivileges.ShowGrants`; `pkg/privilege/privileges/privileges.go` gives the two methods distinct skip arms.

- Observation: Go's `INFORMATION_SCHEMA.USER_PRIVILEGES` reader bypasses the SkipWithGrant-aware manager methods and asks the raw privilege cache whether the current account has `SELECT` on `mysql.*`.
  Evidence: `UserPrivileges.UserPrivilegesTable` directly calls `MySQLPrivilege.UserPrivilegesTable`; a bypassed identity with no row sees no rows rather than every account.

- Observation: the process-wide secure-transport gate belongs before auth-plugin selection, not merely before password verification.
  Evidence: Go `clientConn.handshake` checks `RequireSecureTransport` immediately after parsing `HandshakeResponse41` and before `handleAuthPlugin`. The initial Rust store-only test could not detect an AuthSwitchRequest sent too early, so a real TCP ordering regression was added.

- Observation: skip-grant-table disables Go's privilege-cache load loop, but it does not disable the independent GLOBAL-sysvar cache. Treating both as one loader creates a security window for persisted `require_secure_transport=ON`.
  Evidence: all cluster-connected Rust routes now synchronously install a complete `mysql.global_variables` snapshot before binding, then retain a watcher/ticker reloader capped at 30 seconds; unbootstrapped keyspaces return an empty complete image without scanning table rows.

- Observation: a Pipeline factory constructed with only the account registry silently created a second `GlobalSysvars`, so a secure session could set `require_secure_transport=ON` while the next handshake still read `OFF`.
  Evidence: the focused RED observed `store.global_vars().get(...) == "OFF"` after a successful SQL SET; `PipelineSessionFactory::with_configured_store` now adopts accounts and globals as one pair and the old single-authority constructor has no wire-facing callsites.

- Observation: Go's `SET DEFAULT ROLE ... TO CURRENT_USER` does not resolve the pseudo-user in this statement; parser fields remain empty. Its multi-target writes are also transactional.
  Evidence: source-shaped REDs distinguished 1396 target `@`, NONE as a no-op on the empty identity, and an earlier target being partially published before a later 3530. Rust now computes every replacement before publishing any.

- Observation: Cluster DDL had three direct routes that reached implicit commit and the catalog writer without the ordinary Session privilege gate; in-process CREATE/DROP DATABASE also mutated the catalog before any database-scope check.
  Evidence: the RED let a zero-privilege cluster identity apply DDL. The regression now fixes 1142/1044, zero catalog applications, and an open user transaction before proving a bypassed identity reaches the same writer.

- Observation: a periodic GLOBAL reload could read an old image, race a successful local `SET GLOBAL`, and publish last; two local commits could likewise publish stale full images in response order rather than durable order. A no-op SET did not refresh the live cache at all.
  Evidence: deterministic channel/closure REDs reproduced stale reload after local commit, older local publication after newer durable state, disjoint-key image loss, and a stale live `require_secure_transport=OFF` after an idempotent ON. The shared publication fence and post-commit durable reread make all cases green.

- Observation: the post-commit durable reread is best-effort, but leaving a stale `require_secure_transport=OFF` after its row committed ON creates a security fail-open; the fallback also panicked on a future-version unknown sysvar.
  Evidence: refresh-error REDs fixed OFF-versus-ON, no-op ON, unrelated-key-plus-ON, old local OFF after peer ON, and `UnknownSystemVariable("future_version_sysvar")`. The fallback publishes known changed keys, never weakens secure transport on uncertainty, skips unknown names, warns, and relies on notify/tick for convergence.

- Observation: publishing ON only after entering the GLOBAL publication fence still left the handshake gate stale while another publisher held that fence for a cluster reread; moving the write before the fence alone was insufficient because the older publisher could then replace it with an OFF image.
  Evidence: the first contention RED held an earlier publisher inside its reread, started a confirmed ON publisher, and observed live OFF. The stronger RED made the earlier image itself OFF and held the ON publisher's own reread; a pending-ON floor now keeps live ON throughout both waits while ordinary GLOBAL values remain unpublished until durable truth arrives.

- Observation: changing GLOBAL image replacement to preserve node-local INSTANCE values initially dropped INSTANCE assignments in a mixed cluster SET that also made a durable GLOBAL change.
  Evidence: the RED committed `autocommit=OFF` but left `tidb_general_log=OFF`. Cluster scratch tables now journal ordered INSTANCE Set/Reset operations and replay them under the publication fence only after the GLOBAL half succeeds; mixed success/failure, pure no-op, repeated-name, and reload-preservation regressions are green.

- Observation: Go's raw `USER_PRIVILEGES` table emits `USAGE / NO` only for a truly empty static mask. A lone `GRANT OPTION` bit is not in `AllGlobalPrivs` and therefore emits no static row.
  Evidence: the RED returned `USAGE / YES`; the source-shaped regression now observes no row, then `SELECT / YES` after adding SELECT.

- Observation: the latest integration branch added two privilege consumers after the original package baseline: `INFORMATION_SCHEMA.DEADLOCKS` requires PROCESS, and SEM filters `tidb_gc_leader_desc` without `RESTRICTED_STATUS_ADMIN`. A mechanical merge would have denied both in skip mode.
  Evidence: Go's SkipWithGrant arms return true from both `RequestVerification` and `RequestDynamicVerification`. Rebased-tip regressions now query DEADLOCKS through Session and exercise the exact SEM policy seam with a no-row bypass identity.

## Decision Log

- Decision: Carry `privilege_bypassed` on `AuthenticatedIdentity`, then set a per-session authorization-bypass bit in both privilege-enforcing front-end factories (Pipeline and Cluster).
  Rationale: admission is immutable for a connection, survives cluster account scratch registries, and cannot silently diverge when a session temporarily swaps the attached privilege registry to validate a persisted account change.
  Date/Author: 2026-08-11 / Codex

- Decision: Keep the privilege registry attached in bypass mode.
  Rationale: Go skips authorization, not account/role storage. `CREATE ROLE`, `GRANT`, `SET ROLE`, and `SHOW GRANTS` still need the shared registry to mutate and read their state.
  Date/Author: 2026-08-11 / Codex

- Decision: Retain `require_secure_transport` before the bypass admission.
  Rationale: Go applies that process-wide transport policy before account lookup. Skip-grant-table must not turn a plaintext connection into an exception to it.
  Date/Author: 2026-08-11 / Codex

- Decision: Split transport admission from account verification with an opaque `TransportAdmission` token.
  Rationale: `mysql_connection` must enforce the live global transport policy before any interactive plugin exchange, while the later account verifier still needs the same TLS fact for per-account `REQUIRE` clauses. A token makes the order explicit and prevents a second live-global read after an auth exchange.
  Date/Author: 2026-08-12 / Codex

- Decision: Keep privilege rows and GLOBAL sysvars as separate cache lifecycles, but make every wire-facing session/authenticator pair share the same `GlobalSysvars` authority.
  Rationale: skip mode must not consult privilege rows during authentication, yet persisted and runtime security variables must be effective before the first login and continuously thereafter. Complete-image replacement also removes deleted rows instead of preserving stale values.
  Date/Author: 2026-08-12 / Codex

- Decision: Put the single Session DDL privilege gate before every in-process or cluster catalog mutation and before cluster implicit commit.
  Rationale: Go performs planner privilege admission before executor DDL side effects. Reusing one parsed-statement gate preserves 1044/1142 error shape across text, prepared, and direct cluster entry points while letting skip mode pass through the same decision.
  Date/Author: 2026-08-12 / Codex

- Decision: Serialize GLOBAL cache publication with a shared per-node fence; after every successful local commit or no-op, rebuild from durable cluster truth while holding it.
  Rationale: a read-before-publish epoch prevents an older reload from finishing last, and a durable reread prevents concurrent local full images from erasing each other. On a post-commit read failure, publish only known changed keys and retain commit-timestamp watermarks for ordinary local ordering. `require_secure_transport` is the deliberate exception: uncertainty may conservatively leave it ON even after a newer OFF, but never weakens it to OFF; notification/tick then converges to durable truth.
  Date/Author: 2026-08-12 / Codex

- Decision: Give confirmed `require_secure_transport=ON` publications a short, independent pending floor outside the long GLOBAL publication lock.
  Rationale: handshakes do not take the GLOBAL fence, so a confirmed ON must close plaintext admission before waiting for another publisher or a five-second reread. Whole-image publishers hold the floor while replacing live state and preserve ON whenever another confirmed ON is pending; the completing ON publisher then removes its own floor atomically with its image publication. OFF is never speculative.
  Date/Author: 2026-08-12 / Codex

- Decision: Rebase new privilege consumers through the same central bypass helpers rather than add route-specific exceptions.
  Rationale: DEADLOCKS now calls `has_process_privilege`, so that helper owns skip admission for both PROCESSLIST and diagnostic history. SEM's status filter remains distinct because an internal session with no checker has different semantics; it explicitly short-circuits only the configured bypass bit before retaining the remote `is_none_or` policy.
  Date/Author: 2026-08-12 / Codex

- Decision: Keep the package receipt in this ExecPlan rather than restoring a `.semantic.toml` file.
  Rationale: the current base intentionally removed the repository-wide semantic-manifest/gate mechanism. The repository still requires a complete inventory, integration decision, source pin, and executable evidence, all of which are recorded below without reviving removed infrastructure.
  Date/Author: 2026-08-12 / Codex

## Outcomes & Retrospective

The production behavior and behavioral regressions are implemented. Normal authentication remains 1045/28000; skip mode admits a requested no-row identity while retaining process-wide secure-transport admission, account/role storage, both privilege-enforcing front-end factories, and direct authorization readers. Independent review corrected transport-before-AuthSwitch ordering, raw/pattern-aware `USER_PRIVILEGES`, `SHOW GRANTS ... USING` precedence, default-role raw-identity/transactionality, DDL admission order, GLOBAL-sysvar startup/publication safety (including mixed INSTANCE side effects and the durable-ON/live-gate contention window), and Pipeline authority pairing. Focused, owning-crate, Ready, overlap, Standards, and Go-spec evidence is green on the rebased DCO-signed commit; only its push remains before a completion claim.

## Milestones

### Milestone 1: Pin and inventory the complete Go package

The three direct artifacts, two tests, BUILD scheduling metadata, and TestMain harness are pinned below. Running the focused Go command must report both tests green, and `git diff --quiet <pin> -- pkg/session/test/privileges` must exit zero so the working tree is covered as well as committed HEAD.

### Milestone 2: Prove the missing real production behavior

The real TCP test must fail on the old code with unknown-user 1045 and, after login-only wiring, with 1044 at `USE test`. The cluster regression must fail with 1227 when the factory does not copy the bypass marker. These failures distinguish authentication, ordinary authorization, and cluster scratch-registry propagation rather than testing a leaf policy in isolation.

### Milestone 3: Join configuration, login, and every session consumer

`NodeConfig` reads the source TOML after `tidb-config` validates the real effective uid. Every file- and cluster-backed single/multi-node account constructor applies one immutable store policy. The wire checks secure transport before auth-plugin negotiation, the configured store returns a typed bypassed identity without account/password checks, and the Pipeline/Cluster privilege-enforcing factories copy that fact into `Session`. Focused tests must prove both front ends and the direct SET GLOBAL, PROCESSLIST, KILL, SET ROLE, SHOW GRANTS, and information-schema contracts.

### Milestone 4: Ready, review, and publish one package commit

Run the package receipt commands and Ready profile, obtain independent Standards and Go-spec reviews at the final diff, synchronize onto the latest `origin/hparser-integration`, then create one DCO-signed commit and push exactly that commit. Any P0/P1 finding reopens this milestone.

## Package Receipt

The atomic source package is `pkg/session/test/privileges`, pinned at commit `d82d2baa0f95dd3782b65a74300642d834935cf2` with subtree `4487cbf4474f68d551653bfd7ddfe023b1c6a64a`. Its complete direct inventory is exactly:

- `BUILD.bazel` (blob `06de6c79970723deb98cfe02a09172544b5a1230`)
- `main_test.go` (blob `53f54cf3747fb72bb951bfe24a35103d38551781`)
- `privileges_test.go` (blob `77d62dd8cba2f0b792d74f893e65e0f10806d8d8`)

There are no direct production Go files, package documentation, nested packages, fixtures/testdata, generated or platform variants, file build tags, `go:generate`, package failpoint injections, benchmarks, fuzz targets, or examples. `BUILD.bazel`'s short timeout, flaky flag, and shard count two are Bazel scheduling metadata. `main_test.go` performs common setup, parses flags, zeroes async-commit timing, enables client-go failpoints, waits one second for mock-store shutdown, and runs goleak with its background-worker allowlist. The Rust evidence uses joined TCP workers and asserts both `ConnectionTracker` and `ProcessRegistry` release; it does not recreate Go-only mock-store/client-go workers. The `intest` tag is required by TestKit's store guard even though no source file carries a build constraint.

The integration decision is one complete vertical replacement: Rust has no process-global Go privilege manager, so the source global is resolved once as an immutable configured-store policy and copied as a typed per-connection admission fact. The privilege registry remains attached as the real account/role storage owner. All four `QuerySessionFactory` implementations were audited: Pipeline and Cluster are the privilege-enforcing front ends and copy the marker into `tidb_session::Session`; RealTiKv single/multi keep the authenticated `SessionContext` but intentionally expose a bounded SQL surface with no `tidb_session` grant layer, while their shared login store still applies the configured admission policy. A Pipeline authenticator/factory pair is now constructed from one `ConfiguredUserStore`, so accounts and GLOBAL sysvars cannot diverge.

This receipt claims the two direct Go tests and their production dependencies: normal unknown-user rejection, skip authentication, the tracked SQL role chain, root-only configuration reachability, process-wide secure-transport ordering, the two privilege-enforcing front-end factories, the two bounded factories' login admission, and authorization readers exercised by the source behavior. `TestSessionAuth` calls `USE test` before TestKit's direct `Auth`; the MySQL wire cannot create a session before authentication, so Rust proves the relevant invariant with a handshake rejection that is independent of a selected database. `TestSkipWithGrant` calls `Auth` twice on one TestKit session; production MySQL authenticates once per connection, so the Rust wire mapping uses separate unknown-user and root connections against one server authority. It does not claim Go bootstrap privilege-load-loop parity, per-user resource limits, extension auth plugins, Unix-socket/gateway transport surfaces, or unrelated privilege packages.

Production and regression evidence is carried by:

- `rust/crates/tidb-config/src/config_tree/config.rs`
- `rust/crates/tidb-exec/src/real_tikv_privileges.rs`
- `rust/crates/tidb-server/src/{auth_identity.rs,configured_user_store.rs,lib.rs,mysql_connection.rs,node_config.rs,pipeline_session.rs,real_tikv_multi_node.rs,sql_node.rs}`
- `rust/crates/tidb-server/src/bin/select-one-profile.rs`
- `rust/crates/tidb-server/src/real_tikv_node/mod.rs`
- `rust/crates/tidb-server/src/cluster_session_node/{boot.rs,mod.rs,tests/accounts.rs,tests/node_fixture.rs,tests/schema_changes.rs,tests/statistics.rs}`
- `rust/crates/tidb-server/src/cluster_sysvar_seam.rs`
- `rust/crates/tidb-server/tests/{configured_user_store_source.rs,grants_wire_protocol_source.rs,pipeline_mysql_client_source.rs,require_ssl_login_source.rs,server_internal_packetio_source.rs}`
- `rust/crates/tidb-session/src/{account.rs,dispatch.rs,identity.rs,lib.rs,process_arm.rs,table_privilege.rs,tests_global_vars.rs,variables.rs,vars.rs}`
- `rust/crates/tidb-session/src/tests_deadlock_history.rs`
- `rust/crates/tidb-session/src/privilege/registry_ops.rs`
- `rust/crates/tidb-session/src/tests_grants/{dynamic_grants.rs,roles.rs,table_scope.rs}`
- this ExecPlan

Permanent executable evidence, from repository root, is:

    git diff --quiet d82d2baa0f95dd3782b65a74300642d834935cf2 -- pkg/session/test/privileges
    go test -tags=intest,deadlock ./pkg/session/test/privileges -run '^(TestSkipWithGrant|TestSessionAuth)$' -count=1
    cargo +1.97.0 test --manifest-path rust/Cargo.toml -p tidb-server --test all grants_wire_protocol_source:: -- --nocapture
    cargo +1.97.0 test --manifest-path rust/Cargo.toml -p tidb-server --test all require_ssl_login_source:: -- --nocapture
    cargo +1.97.0 test --manifest-path rust/Cargo.toml -p tidb-server --test configured_user_store_source -- --nocapture
    cargo +1.97.0 test --manifest-path rust/Cargo.toml -p tidb-server --lib cluster_session_node::tests::accounts:: -- --nocapture
    cargo +1.97.0 test --manifest-path rust/Cargo.toml -p tidb-server --lib cluster_session_node::tests::schema_changes:: -- --nocapture
    cargo +1.97.0 test --manifest-path rust/Cargo.toml -p tidb-server --lib real_tikv_node::tests:: -- --nocapture
    cargo +1.97.0 test --manifest-path rust/Cargo.toml -p tidb-server --lib cluster_sysvar_seam::reloader_tests -- --nocapture
    cargo +1.97.0 test --manifest-path rust/Cargo.toml -p tidb-server --lib node_config::tests:: -- --nocapture
    cargo +1.97.0 test --manifest-path rust/Cargo.toml -p tidb-server --lib skip_grant_startup_tests::skip_grant_table_ignores_a_command_line_privilege_source_without_cluster_tables -- --exact --nocapture
    cargo +1.97.0 test --manifest-path rust/Cargo.toml -p tidb-server --lib pipeline_session::tests::configured_store_and_pipeline_sessions_share_one_global_authority -- --exact --nocapture
    cargo +1.97.0 test --manifest-path rust/Cargo.toml -p tidb-session --lib tests_grants:: -- --nocapture
    cargo +1.97.0 test --manifest-path rust/Cargo.toml -p tidb-session --lib tests_global_vars::require_secure_transport_can_only_be_enabled_by_a_secure_session -- --exact --nocapture
    cargo +1.97.0 test --manifest-path rust/Cargo.toml -p tidb-session --lib vars::tests::replacing_a_cluster_global_image_preserves_instance_only_values -- --exact --nocapture
    cargo +1.97.0 test --manifest-path rust/Cargo.toml -p tidb-session --lib tests_deadlock_history::skip_grant_bypasses_process_for_deadlocks_table -- --exact --nocapture
    cargo +1.97.0 test --manifest-path rust/Cargo.toml -p tidb-session --lib vars::tests::sem_enable_and_disable_change_new_session_defaults -- --exact --nocapture
    cargo +1.97.0 test --manifest-path rust/Cargo.toml -p tidb-session --test sem_source -- --test-threads=1 --nocapture
    cargo +1.97.0 test --manifest-path rust/Cargo.toml -p tidb-exec --lib real_tikv_privileges::sysvar_only_tests::an_unbootstrapped_keyspace_returns_no_sysvars_without_reading_table_rows -- --exact --nocapture
    cargo +1.97.0 test --manifest-path rust/Cargo.toml -p tidb-config --lib
    cargo +1.97.0 check --manifest-path rust/Cargo.toml -p tidb-config -p tidb-exec -p tidb-session -p tidb-server --all-targets
    cargo +1.97.0 fmt --manifest-path rust/Cargo.toml --all -- --check
    make lint
    git diff --check HEAD^ HEAD

## Context and Orientation

The pinned Go package has exactly three direct artifacts: `BUILD.bazel`, `main_test.go`, and `privileges_test.go`. It has exactly two tests. `TestSessionAuth` and the first half of `TestSkipWithGrant` require an unknown user to be rejected normally. The second half enables the process-global bypass, authenticates an unknown user and root, then runs `USE test`, creates a table and role, grants the role to root, activates all roles, and shows root's grants. There are no package build tags, generated files, fixtures, failpoint injections, benchmarks, fuzz targets, or examples.

`main_test.go` installs common test configuration, enables failpoints, adjusts async-commit safe windows, parses flags, waits for mock-store shutdown, and runs goleak with the package's background-worker ignore list. Rust uses joined connection threads and explicit process/connection registries; the final real-wire regression must join every worker and observe no leaked registry entry. `BUILD.bazel`'s short timeout, flaky marker, and shard count two are scheduling metadata rather than production semantics.

The Rust identity policy is in `tidb-server/src/auth_identity.rs`; the login owner is `configured_user_store.rs`; the wire creates `SessionContext` in `mysql_connection.rs`. `PipelineSessionFactory` and `ClusterSessionFactory` turn that context into `tidb_session::Session`. Central table/dynamic/schema checks use `Session::privilege_context`, while SET GLOBAL, target-user, process visibility/KILL, and user-privilege display have a few direct registry reads that also need the session bypass decision.

Production startup parses source TOML through `tidb-server/src/node_config.rs`, validates the source config through `tidb-config`, and constructs account stores in `real_tikv_node` and `real_tikv_multi_node`. Every constructor must apply the same startup policy before the store is shared with a session factory.

## Plan of Work

First add the immutable policy field and builder to `ConfiguredUserStore`, plus the bypass marker on `AuthenticatedIdentity`, without changing login behavior. Add a real TCP source-shaped test. The initial run must fail because the old login still returns 1045 for the unknown user. Add a cluster-session test that constructs a bypass identity and reaches a statement that an unprivileged identity would be denied.

Next route authentication through `IdentityLookupPolicy`. In bypass mode choose the native default plugin, preserve the secure-transport gate, return the requested username and remote host without consulting account rows, and mark the identity bypassed. Keep normal resolver/verifier semantics and error behavior unchanged while splitting transport admission from account verification so the wire ordering is explicit.

Then add a session authorization-bypass bit. Both session factories must copy it from the authenticated identity before attaching the shared registry. The central privilege context treats such a session as unrestricted. Audit the few checks that read the registry directly and make them consult the same bit; keep all account and role mutations backed by the attached registry.

Finally accept `security.skip-grant-table` in `NodeConfig`, preserve source root-only validation by implementing the effective-uid check, and apply the policy in every configured/cluster account-store construction path. Add parsing/reachability tests, run the unchanged Go oracle and focused Rust tests, then complete Ready validation and independent review.

## Concrete Steps

Run the unchanged Go oracle from repository root:

    go test -tags=intest,deadlock ./pkg/session/test/privileges -run '^(TestSkipWithGrant|TestSessionAuth)$' -count=1

Run the focused real-wire and cluster-session regressions after adding them:

    cargo +1.97.0 test --manifest-path rust/Cargo.toml -p tidb-server --test all grants_wire_protocol_source::skip_grant_table_authentication_and_role_chain_reach_the_wire -- --exact --nocapture
    cargo +1.97.0 test --manifest-path rust/Cargo.toml -p tidb-server --test all grants_wire_protocol_source::skip_grant_table_still_rejects_insecure_transport_before_auth_switch -- --exact --nocapture
    cargo +1.97.0 test --manifest-path rust/Cargo.toml -p tidb-server --lib cluster_session_node::tests::accounts::skip_grant_table_bypasses_cluster_session_authorization_without_detaching_account_state -- --exact --nocapture

Run the login/policy and configuration modules, owning-crate checks, and Ready gates:

    cargo +1.97.0 test --manifest-path rust/Cargo.toml -p tidb-server --test configured_user_store_source -- --nocapture
    cargo +1.97.0 test --manifest-path rust/Cargo.toml -p tidb-server --lib cluster_session_node::tests::accounts:: -- --nocapture
    cargo +1.97.0 test --manifest-path rust/Cargo.toml -p tidb-session --lib tests_grants:: -- --nocapture
    cargo +1.97.0 test --manifest-path rust/Cargo.toml -p tidb-config --lib
    cargo +1.97.0 check --manifest-path rust/Cargo.toml -p tidb-config -p tidb-exec -p tidb-session -p tidb-server --all-targets
    cargo +1.97.0 fmt --manifest-path rust/Cargo.toml --all -- --check
    make lint
    git diff --check HEAD^ HEAD

## Validation and Acceptance

Normal mode must reject an unknown user with 1045/28000 over the real wire. Bypass mode must admit an unknown requested identity and root despite arbitrary passwords, while a plaintext connection remains subject to `require_secure_transport`. The authenticated identity must report the requested user and peer host and carry the bypass marker into both session factories.

In bypass mode the source SQL chain must reach `SHOW GRANTS` over TCP: select the `test` schema, create a table, create role `r_1`, grant it to root, and activate all roles. Draining `SHOW GRANTS FOR root` must then observe Go's fixed 1141/42000 bypass error (the source `MustExec` does not drain the recordset). The shared registry must retain the created role and edge. The equivalent cluster session must bypass an otherwise-denied privilege gate without losing access to its account writer/registry. Normal-mode privilege denials must remain green.

The production binary must accept `[security] skip-grant-table = true` only when the effective user is root and must apply it to every account source. Non-root validation must retain Go's exact refusal. Tests that cannot control process uid may directly test the effective-uid helper and condition their config acceptance assertion on the running uid.

## Idempotence and Recovery

All tests and checks are safe to rerun. The TCP tests bind ephemeral loopback ports and must join every connection worker. No Go or Bazel-generated file is edited, so `make bazel_prepare` is not required. If a RED exposes a wider privilege bypass than listed here, leave changes unstaged, update this plan, and fix the single shared authorization seam rather than adding statement-specific exceptions.

## Artifacts and Notes

The canonical Go oracle passes with `intest,deadlock`. The original real TCP path rejected the unknown skip-mode user with 1045; after login alone was connected, the same test reached `USE test` and failed with 1044. Removing the cluster factory's marker copy made its account regression fail with 1227. Restoring the shared production path made both tests pass.

The fixed transport-order test sends `caching_sha2_password` over plaintext while skip mode and `require_secure_transport=ON` are both active. It receives ERR 3159/HY000 directly; an AuthSwitchRequest would begin with `0xfe` and fail the assertion. The main wire test additionally verifies role-less `SHOW GRANTS` 1141/42000, `USING` 3530/HY000, no-row `USER_PRIVILEGES` visibility, two-process visibility, cross-user KILL, and zero live connection leaks.

## Interfaces and Dependencies

`ConfiguredUserStore` owns an immutable `IdentityLookupPolicy` and exposes a consuming `with_skip_grant_table(bool)` constructor modifier. `AuthenticatedIdentity` exposes `privilege_bypassed()`. `Session` exposes an authorization-bypass setter used only by front-end factories and a read used by its privilege subsystem.

`NodeConfig` owns `skip_grant_table: bool`. `tidb-config` supplies the source `Security.skip_grant_table` value and validates it with the real effective uid. The configured node's account-policy installer applies both password-expiry sandbox configuration and skip-grant-table to each `ConfiguredUserStore` before it is shared.

Revision note (2026-08-12): updated the plan from implementation-in-progress to the completed production design, added required milestones and the explicit current-tree package receipt, corrected executable command names and four-owner coverage, and recorded DDL admission, GLOBAL publication (including the pending secure-transport floor), raw USER_PRIVILEGES, Pipeline authority, default-role, rebased DEADLOCKS/SEM findings, and the final distsql/OS-utility tip synchronizations with their RED-to-GREEN regressions. This note is intentionally last, as required for a living ExecPlan revision record.
