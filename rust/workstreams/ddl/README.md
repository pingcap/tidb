# DDL workstreams

DDL leaves are split by source-owned metadata boundary so agents can work in
parallel without sharing one coordinator file. `domains/*.toml` is the durable
ownership queue; each domain names exact Go selectors, Rust paths, evidence,
and the commands required before integration.

The current dependency-closed partition metadata leaf is
`ddl_partition_metadata`: it owns normalized partition names, add/reorganize
validation, source-order physical IDs, `Definitions`/`AddingDefinitions`
staging, and the ADD PARTITION `StateNone -> StateReplicaOnly -> StatePublic`
order. Partition expression evaluation, ID allocation, TiKV/tablecodec and PD
placement, catalog mutation, and DDL worker coordination stay explicit follow-up
domains rather than being hidden behind a Rust-only compatibility path.
