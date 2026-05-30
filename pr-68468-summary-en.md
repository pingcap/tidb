# PR #68468 Summary

PR: `*: Meta service group basic`

1. This PR introduces a unified abstraction layer in `pkg/metaservice` to manage PD/etcd-related metadata access. It consolidates several responsibilities that were previously handled separately: discovering PD addresses, resolving the meta service group for a keyspace, and deciding which meta service nodes the current keyspace should connect to.

2. `pkg/store/driver/tikv_driver.go` now computes `MetaServiceInfo` when creating a TiKV store based on keyspace metadata. The flow is: fetch PD addresses from the PD client, read `meta_service_group_id` and `meta_service_group_addrs` from keyspace metadata, and fall back to the global meta service if the keyspace does not have a dedicated group. After that, logic that needs etcd/meta service access, such as GC safepoint KV and the AutoID service, consistently uses the resolved address set.

3. Upper-layer callers are switched to this new abstraction instead of directly inferring PD/etcd addresses on their own. This includes AutoID service registration in `server`, etcd client acquisition in `lightning`, PD information queries in `infoschema` / `executor` / `store helper`, and meta service client initialization in `infosync`. As a result, in keyspace scenarios these components automatically route to the correct meta service group, and tests have been added for the new resolution logic and key error paths.
