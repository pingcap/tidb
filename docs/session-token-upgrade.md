# Upgrade session-token authentication without changing TiProxy

Session migration uses `SHOW SESSION_STATES` to export state and a signed token. TiProxy forwards the token unchanged to a destination TiDB using the `tidb_session_token` authentication plugin, then restores the state after authentication. This rollout does not require a TiProxy binary or protocol change.

`tidb_session_token_mode` is a persisted, GLOBAL-only system variable:

| Value | Tokens issued | Tokens accepted |
| --- | --- | --- |
| `COMPATIBLE` (default) | Legacy | Legacy and V2 |
| `TRANSITION` | V2 | Legacy and V2 |
| `STRICT` | V2 | V2 |

Legacy tokens preserve the original wire encoding, account checks, and ECDSA signing behavior so unmodified old TiDB can participate in a rolling upgrade. V2 signs an explicit version and the exact matched account username and host, and authenticates the complete payload for ECDSA. A malformed, unsupported, or invalid V2 token is rejected; it is never retried as legacy.

## Security during compatibility

Legacy tokens do not bind the source account host. Their username comparison is case insensitive, and legacy ECDSA signatures cover only a prefix of the JSON payload. These weaknesses remain wherever legacy tokens are accepted. `COMPATIBLE` and `TRANSITION` are temporary upgrade phases, not complete enforcement of the security fix. A token's unsigned or incompletely authenticated timestamps cannot determine when to end the compatibility phase.

The phase is read for every token export and token authentication. Existing client sessions therefore follow phase changes; session defaults and migrated session state do not select the phase. The policy contains no secrets. Setting it requires the usual privilege to change global system variables.

## Rolling upgrade

1. Leave the phase at `COMPATIBLE` while upgrading TiDB. Upgrade or fence every process that can issue tokens or receive migrated sessions. This includes dormant instances and processes that could rejoin after a network partition. A source may be shut down only after its sessions migrate successfully. Failed migration can leave the source usable, but a later forced shutdown can still disconnect its clients.
2. Confirm that every possible destination understands V2. Fence rollback and admission paths so they cannot introduce an unmodified old TiDB after V2 issuance starts. A server-version list alone is not proof: leases can expire while a process remains alive and may later re-register.
3. Enable V2 issuance:

       SET GLOBAL tidb_session_token_mode = 'TRANSITION';

4. Connect directly to each eligible TiDB SQL endpoint, bypassing load-balanced routing, and check:

       SELECT @@global.tidb_session_token_mode;

   Every eligible issuer must report `TRANSITION`. Fence an issuer if its state cannot be confirmed. Also confirm that operational routing cannot select an old or otherwise unsupported destination.
5. Record the latest successful issuer confirmation. From that point, wait at least one minute for ordinary deployments or eight hours for Starter, plus allowances for the deployment's maximum clock skew and in-flight migration delay. Starter can retain tokens during idle zero-backend windows. Do not use a universal one-minute wait. Do not measure the wait from the `SET GLOBAL` statement or trust timestamps supplied in legacy ECDSA tokens.
6. Disable legacy acceptance:

       SET GLOBAL tidb_session_token_mode = 'STRICT';

7. Check the phase directly on every eligible destination. Only declare V2-only enforcement complete when all report `STRICT`; fence any unconfirmed destination.

The persisted setting uses the existing global-variable cache and notifications. Refresh is eventual, and failed refreshes may keep a previous value. The ordinary refresh interval is not a convergence guarantee. A successful `SET GLOBAL` or one successful load-balanced query does not acknowledge every server.

These commands express an operator-controlled transition. TiDB does not automatically prove membership, admission fencing, successful draining, or the grace interval, and it does not infer finalization from a snapshot of server versions.

## Rollback and recovery

After V2 issuance starts, every rollback target must understand V2 because new tokens may already be in flight. Keep a V2-capable bridge build as the rollback floor. Re-enabling `COMPATIBLE` accepts V2 tokens already in flight but does not make those tokens consumable by an unmodified old destination.

Any rollback that re-enables legacy issuance invalidates the previous retirement schedule. Repeat the issuer confirmation and full grace procedure before setting `STRICT` again. Returning to a legacy-accepting phase also restores the legacy security exposure.

Unknown policy values and policy-read failures reject token operations instead of selecting a weaker phase. An absent persisted row for this newly introduced variable uses its registered `COMPATIBLE` default during initial upgrade. Repair malformed persisted policy data through the normal validated `SET GLOBAL` path.

## Connection-continuity validation

Use the deployed TiProxy binary unchanged and pin old/new TiDB binaries and the signing certificate. With client reconnect disabled, keep one frontend connection open and migrate old to new and new to old while the phase is `COMPATIBLE`. Verify the backend actually changes, queries continue, and the current database, user/session variables, text and binary prepared statements survive. Keep the source available until destination authentication and state restoration succeed.

After every eligible endpoint supports V2, repeat V2 migrations in `TRANSITION` and `STRICT`, and verify that strict destinations reject legacy tokens. Include failed-target authentication and confirm the original frontend connection remains usable while its source is alive. Unit tests using a frozen old codec establish wire compatibility but do not replace this transport-level test.
