# Starter txn-file Docker Compose tests

This fixture builds the current TiDB checkout and runs the two external
starter-mode txn-file SQL tests against a private NextGen cluster. It is an
alternative local entry point for these focused cases; it does not replace the
binary-based NextGen RealTiKV workflow.

## Topology and lifecycle

All services use one private Compose network with no published host ports. The
runner starts and verifies each stage explicitly:

1. PD and MinIO become healthy.
2. `minio-init` creates and validates the `tidbcloud-local-dfs` bucket.
3. Three API-v2 TiKV stores become healthy.
4. The local `tidb` image is built once from the current checkout. It contains
   `tidb-server` and the compiled `startertest` test binary.
5. `bootstrap-tidb` bootstraps the `SYSTEM` keyspace and exits successfully.
6. `create-keyspace` creates and validates the enabled `startertest` keyspace
   with `gc_management_type=keyspace_level`.
7. `tikv-worker` becomes healthy.
8. The starter TiDB starts in standby mode and becomes healthy.
9. `activate-tidb` activates `startertest` and exits successfully.
10. The attached `test` service runs both selected cases in-network.

Every health and completion wait inspects the actual container state, health,
and exit code. Waits are bounded at 300 one-second attempts by default. For a
slow development host, `COMPOSE_WAIT_ATTEMPTS` can set another positive number;
do not lower it for normal runs.

## Prerequisites

- Docker Engine and permission to use its daemon.
- Docker Compose v2 or newer through the `docker compose` plugin. The legacy
  `docker-compose` executable is unsupported.
- Pull access to the private PingCAP PD and TiKV registries, plus the MinIO
  server and client registries. Authenticate before running the fixture.
- Capacity for PD, three TiKVs, one worker, two TiDB lifecycle containers,
  MinIO, image compilation, and their volumes. Plan for at least 4 CPUs, 12 GiB
  RAM, and 30 GiB free Docker disk; more capacity reduces build and bootstrap
  time.

The default external images are pinned by digest in `docker-compose.yml`:

- PD: `release-nextgen-202603@sha256:2af376596238fb9c6350ba962780cb45cf4284e4d8c90574a697b45222b6049a`
- TiKV and TiKV worker: `release-nextgen-202603@sha256:be56383f85979aaf45e15a59201b5f41d1345dd96a19b9fd8c41d2226556408f`
- MinIO: `sha256:d5c7b30d2e49f3886d7da679ffddd8fc327508b4ee564053da619c86b71ac0ba`
- MinIO client: `sha256:a7fe349ef4bd8521fb8497f55c6042871b2ae640607cf99d9bede5e9bdf11727`
- Go build image: `golang:1.25.12@sha256:fe5d57d3b718e7a4986bae156c2d73f44973bfd313073aed08a4de6692bb6161`

The fixed `minioadmin` credentials are local fixture credentials only. Do not
replace them with real credentials or publish this network.

## Run

From any working directory, invoke the repository path to the runner. From the
repository root, the exact command is:

```sh
tests/realtikvtest/startertest/docker-compose/run.sh
```

The runner resolves its own location and repository build context. Unless
`COMPOSE_PROJECT_NAME` is already set, it creates a safe unique project named
`tidb-txn-file-<user>-<timestamp>-<random>`. A fixed project is useful for
correlating a manual QA run:

```sh
COMPOSE_PROJECT_NAME=tidb-txn-file-e2e \
  tests/realtikvtest/startertest/docker-compose/run.sh
```

To use a compatible TiKV/CSE image for all three TiKV services and the worker:

```sh
TIKV_COMPOSE_IMAGE=<compatible-tikv-image> \
  tests/realtikvtest/startertest/docker-compose/run.sh
```

The override does not change PD, MinIO, or the locally built TiDB image. TiDB
and the test binary always come from the current checkout, including committed
and uncommitted source included by the Docker build context.

## Tests and expected evidence

The test container selects only these cases:

- `TestExternalStarterTxnFileCommitAcrossChunksAndRegions` reports three
  distinct regions, payload above 1 MiB spanning at least four 256 KiB chunks,
  txn-file metric delta `ok=1 err=0`, and 24 exact committed rows.
- `TestExternalStarterTxnFileWriteConflictRollsBack` reports MySQL error 9007,
  metric delta `ok=0 err=1`, 23 baseline rows plus one winner and no losing
  rows, then a successful pessimistic `FOR UPDATE NOWAIT` lock probe.

The client output streams to the caller and is retained in a runner-owned host
temporary file until teardown, so an ephemeral or stopped test container does
not hide its result. The test runs inside the Compose network because PD and
TiKV advertise Compose service names; host-side execution against this topology
is unsupported.

## Diagnostics, exit status, and cleanup

On any startup, build, activation, test, or signal failure, the runner prints a
named diagnostics section before teardown. It includes `compose ps`, all
container logs, PD's file log, all three TiKV file logs, the worker file log,
SYSTEM bootstrap output and file logs, starter TiDB output and file log,
activation output, and captured test output/client log attempts. Active
containers are read with `compose exec`; stopped containers fall back to
`docker cp`.

The exit trap always runs exactly:

```sh
docker compose ... down -v --remove-orphans --rmi local
```

This removes the project's containers, volumes, network, orphans, and local
TiDB image. The runner also removes its host temporary directory. An original
nonzero status always wins over cleanup failure; cleanup failure replaces only
an otherwise successful status. `HUP`, `INT`, and `TERM` produce statuses 129,
130, and 143 respectively, after the same diagnostics and cleanup. Cleanup is
scoped to the selected project and never targets another Compose project.

## Common failures

| Symptom | Check or resolution |
| --- | --- |
| Compose v2 requirement fails | Install or select the modern `docker compose` plugin and verify `docker compose version --short` reports major version 2 or newer. |
| Image pull is denied | Authenticate to the private PingCAP registries and verify access to the pinned MinIO images. |
| Build fails or uses unexpected source | Inspect the Docker build output and `/opt/tidb/source-status` in a retained debugging image; the normal runner removes local images during cleanup. |
| `minio-init` fails | Check MinIO health and bucket-bootstrap output. The fixture deliberately disables DFS fallback. |
| TiKV or worker is unhealthy | Inspect the named service output and its file log in automatic diagnostics; verify Docker memory and disk capacity and image compatibility. |
| SYSTEM bootstrap or keyspace creation fails | Inspect `bootstrap-tidb` and `create-keyspace` output. Both `SYSTEM` and `startertest` must be enabled, and `startertest` must use keyspace-level GC management. |
| Starter TiDB does not activate | Inspect `tidb` and `activate-tidb` output and confirm worker readiness and the exact `startertest` keyspace. |
| Test cannot reach advertised TiKV addresses | Use the in-network runner. Do not add host ports or run this test binary on the host. |

## Non-goals

This fixture proves successful txn-file SQL commits and determinate
write-conflict rollback with lock release. It does not prove DFS orphan
cleanup, deletion of partially accepted chunks, recovery of an undetermined
commit, or probe-dependent shared-lock TTL behavior. It must not be used to
make recovery or orphan-cleanup guarantees.
