# BR coarse scatter playground verification

Date: 2026-06-12 UTC

Purpose: verify with a real TiUP playground that `br restore ... --corase-scatter=true` only scatters rough split regions.

## Step 0. Build the local BR binary

Command:

```bash
go build -o /tmp/br-coarse-scatter ./br/cmd/br
/tmp/br-coarse-scatter --version
/tmp/br-coarse-scatter restore db --help | grep -- --corase-scatter
```

Output:

```text
Release Version: nightly-dirty
Git Commit Hash: None
Git Branch: None
Go Version: go1.25.10
UTC Build Time: None
Race Enabled: false
Kernel Type: Classic
      --corase-scatter                         only scatter regions from rough split during snapshot restore
```

Note: I first tried `tiup playground v8.5.6`, but the locally built BR uses a newer PD client and backup repeatedly failed with `rpc error: code = Unimplemented desc = unknown method QueryRegion for service pdpb.PD`. The successful verification below uses the already installed `v9.0.0-beta.2.pre-nightly` playground components.

## Step 1. Start playground

Command:

```bash
tiup playground v9.0.0-beta.2.pre-nightly --db 1 --pd 1 --kv 3 --tiflash 0 --without-monitor --tag br-coarse-scatter-verify
curl -sf http://127.0.0.1:2379/pd/api/v1/version
mysql -h 127.0.0.1 -P 4000 -u root -e 'select version();'
curl -s http://127.0.0.1:2379/pd/api/v1/config/replicate
```

Output:

```text
playground pid: 2367090
{
  "version": "v9.0.0-beta.2.pre-335-g5e71b3a",
  "build_time": "",
  "hash": "",
  "branch": ""
}
version()
8.0.11-TiDB-v9.0.0-beta.2.pre-1504-ga5ad421c49
up stores: 3
{
  "max-replicas": 3,
  "location-labels": "",
  "strictly-match-label": "false",
  "enable-placement-rules": "true",
  "enable-placement-rules-cache": "false",
  "isolation-level": ""
}
```

Why this matters: BR only performs scatter when `store-count >= max-replicas` and `store-count > 1`. This playground has 3 Up stores and `max-replicas = 3`, so scatter is active.

## Step 2. Create split source databases

Command:

```sql
DROP DATABASE IF EXISTS br_cs_default;
DROP DATABASE IF EXISTS br_cs_coarse;
CREATE DATABASE br_cs_default;
CREATE DATABASE br_cs_coarse;
CREATE TABLE br_cs_default.t (id BIGINT PRIMARY KEY, v VARCHAR(32));
CREATE TABLE br_cs_coarse.t (id BIGINT PRIMARY KEY, v VARCHAR(32));
SPLIT TABLE br_cs_default.t BETWEEN (0) AND (80000) REGIONS 8;
SPLIT TABLE br_cs_coarse.t BETWEEN (0) AND (80000) REGIONS 8;
INSERT INTO br_cs_default.t VALUES (5000,'v5000'),(15000,'v15000'),(25000,'v25000'),(35000,'v35000'),(45000,'v45000'),(55000,'v55000'),(65000,'v65000'),(75000,'v75000');
INSERT INTO br_cs_coarse.t VALUES (5000,'v5000'),(15000,'v15000'),(25000,'v25000'),(35000,'v35000'),(45000,'v45000'),(55000,'v55000'),(65000,'v65000'),(75000,'v75000');
SELECT 'br_cs_default' AS db_name, COUNT(*) AS row_count FROM br_cs_default.t;
SELECT 'br_cs_coarse' AS db_name, COUNT(*) AS row_count FROM br_cs_coarse.t;
```

Output:

```text
SPLIT TABLE br_cs_default.t BETWEEN (0) AND (80000) REGIONS 8
+--------------------+----------------------+
| TOTAL_SPLIT_REGION | SCATTER_FINISH_RATIO |
+--------------------+----------------------+
|                  7 |                    1 |
+--------------------+----------------------+

SPLIT TABLE br_cs_coarse.t BETWEEN (0) AND (80000) REGIONS 8
+--------------------+----------------------+
| TOTAL_SPLIT_REGION | SCATTER_FINISH_RATIO |
+--------------------+----------------------+
|                  7 |                    1 |
+--------------------+----------------------+

INSERT INTO br_cs_default.t ...
Query OK, 8 rows affected

INSERT INTO br_cs_coarse.t ...
Query OK, 8 rows affected

+---------------+-----------+
| db_name       | row_count |
+---------------+-----------+
| br_cs_default |         8 |
+---------------+-----------+

+--------------+-----------+
| db_name      | row_count |
+--------------+-----------+
| br_cs_coarse |         8 |
+--------------+-----------+
```

## Step 3. Back up both databases

Command:

```bash
/tmp/br-coarse-scatter backup db --db br_cs_default --pd 127.0.0.1:2379 -s local:///tmp/br-coarse-scatter-verify/backup-default --log-file /tmp/br-coarse-scatter-verify/backup-default.log --checksum=false --check-requirements=false
/tmp/br-coarse-scatter backup db --db br_cs_coarse --pd 127.0.0.1:2379 -s local:///tmp/br-coarse-scatter-verify/backup-coarse --log-file /tmp/br-coarse-scatter-verify/backup-coarse.log --checksum=false --check-requirements=false
find /tmp/br-coarse-scatter-verify/backup-default -maxdepth 1 -type f | wc -l
find /tmp/br-coarse-scatter-verify/backup-coarse -maxdepth 1 -type f | wc -l
```

Output:

```text
Detail BR log in /tmp/br-coarse-scatter-verify/backup-default.log
[2026/06/12 09:19:21.804 +00:00] [INFO] [collector.go:77] ["Database Backup success summary"] [total-ranges=8] [ranges-succeed=8] [ranges-failed=0] [backup-checksum=3.717806ms] [write-CF-files=8] [backup-total-ranges=1] [backup-total-regions=8] [total-take=3.493531474s] [backup-data-size(after-compressed)=14.08kB] [Size=14079] [BackupTS=466945561939935258] [total-kv=8] [total-kv-size=271B] [average-speed=77.57B/s]
Detail BR log in /tmp/br-coarse-scatter-verify/backup-coarse.log
[2026/06/12 09:19:25.329 +00:00] [INFO] [collector.go:77] ["Database Backup success summary"] [total-ranges=8] [ranges-succeed=8] [ranges-failed=0] [backup-checksum=1.6606ms] [backup-total-regions=8] [write-CF-files=8] [backup-total-ranges=1] [total-take=3.479387406s] [BackupTS=466945562870546436] [total-kv=8] [total-kv-size=271B] [average-speed=77.89B/s] [backup-data-size(after-compressed)=14.08kB] [Size=14079]
backup-default files: 4
backup-coarse files: 4
```

## Step 4. Drop source databases before restore

Command:

```sql
DROP DATABASE br_cs_default;
DROP DATABASE br_cs_coarse;
SHOW DATABASES LIKE 'br_cs_%';
```

Output:

```text
DROP DATABASE br_cs_default
Query OK, 0 rows affected

DROP DATABASE br_cs_coarse
Query OK, 0 rows affected

SHOW DATABASES LIKE 'br_cs_%'
Empty set
```

## Step 5. Restore with default scatter behavior

Command:

```bash
/tmp/br-coarse-scatter restore db --db br_cs_default --pd 127.0.0.1:2379 -s local:///tmp/br-coarse-scatter-verify/backup-default --log-file /tmp/br-coarse-scatter-verify/restore-default.log --checksum=false --check-requirements=false --load-stats=false --split-region-index-step=2 --merge-region-size-bytes=1 --merge-region-key-count=1
mysql -h 127.0.0.1 -P 4000 -u root -e "SELECT COUNT(*) AS default_rows FROM br_cs_default.t;"
```

Output:

```text
Detail BR log in /tmp/br-coarse-scatter-verify/restore-default.log
[2026/06/12 09:19:28.754 +00:00] [INFO] [collector.go:77] ["DataBase Restore success summary"] [total-ranges=8] [ranges-succeed=8] [ranges-failed=0] [merge-ranges=491.94us] [split-regions=41.509104ms] [restore-files=16.351669ms] [restore-pipeline=63.655us] [default-CF-files=0] [write-CF-files=8] [split-keys=8] [total-take=3.266305201s] [restore-data-size(after-compressed)=14.08kB] [Size=14079] [BackupTS=466945561939935258] [RestoreTS=466945563840479237] [total-kv=8] [total-kv-size=271B] [average-speed=82.97B/s]
default_rows
8
```

## Step 6. Restore with coarse scatter enabled

Command:

```bash
/tmp/br-coarse-scatter restore db --db br_cs_coarse --pd 127.0.0.1:2379 -s local:///tmp/br-coarse-scatter-verify/backup-coarse --log-file /tmp/br-coarse-scatter-verify/restore-coarse.log --checksum=false --check-requirements=false --load-stats=false --split-region-index-step=2 --merge-region-size-bytes=1 --merge-region-key-count=1 --corase-scatter=true
mysql -h 127.0.0.1 -P 4000 -u root -e "SELECT COUNT(*) AS coarse_rows FROM br_cs_coarse.t;"
```

Output:

```text
Detail BR log in /tmp/br-coarse-scatter-verify/restore-coarse.log
[2026/06/12 09:19:34.109 +00:00] [INFO] [collector.go:77] ["DataBase Restore success summary"] [total-ranges=8] [ranges-succeed=8] [ranges-failed=0] [merge-ranges=151.758us] [split-regions=43.057546ms] [restore-files=6.990495ms] [restore-pipeline=78.206us] [split-keys=8] [default-CF-files=0] [write-CF-files=8] [total-take=3.254340464s] [Size=14079] [BackupTS=466945562870546436] [RestoreTS=466945565242949700] [total-kv=8] [total-kv-size=271B] [average-speed=83.27B/s] [restore-data-size(after-compressed)=14.08kB]
coarse_rows
8
```

## Step 7. Proof from BR restore logs

Command:

```bash
grep -c '"scatter regions"' /tmp/br-coarse-scatter-verify/restore-default.log
grep -c '"scatter regions"' /tmp/br-coarse-scatter-verify/restore-coarse.log
grep -nE 'coarse scatter|execute split sorted keys|"scatter regions"|finish spliting regions roughly|finish spliting and scattering regions|finish splitting regions\.' /tmp/br-coarse-scatter-verify/restore-default.log
grep -nE 'coarse scatter|execute split sorted keys|"scatter regions"|finish spliting regions roughly|finish spliting and scattering regions|finish splitting regions\.' /tmp/br-coarse-scatter-verify/restore-coarse.log
```

Output:

```text
default restore scatter log count: 5
coarse restore scatter log count: 1

-- default log key lines --
92:[2026/06/12 09:19:25.550 +00:00] [INFO] [client.go:336] ["coarse scatter"] [enabled=false]
181:[2026/06/12 09:19:28.675 +00:00] [INFO] [split.go:97] ["execute split sorted keys"] ["keys count"=8]
184:[2026/06/12 09:19:28.690 +00:00] [INFO] [client.go:240] ["scatter regions"] [regions=3]
189:[2026/06/12 09:19:28.702 +00:00] [INFO] [split.go:121] ["finish spliting regions roughly"] [take=26.271794ms]
192:[2026/06/12 09:19:28.714 +00:00] [INFO] [client.go:240] ["scatter regions"] [regions=1]
193:[2026/06/12 09:19:28.714 +00:00] [INFO] [client.go:240] ["scatter regions"] [regions=1]
194:[2026/06/12 09:19:28.715 +00:00] [INFO] [client.go:240] ["scatter regions"] [regions=1]
195:[2026/06/12 09:19:28.715 +00:00] [INFO] [client.go:240] ["scatter regions"] [regions=2]
199:[2026/06/12 09:19:28.717 +00:00] [INFO] [split.go:128] ["finish spliting and scattering regions"] [take=41.467275ms]

-- coarse log key lines --
92:[2026/06/12 09:19:30.918 +00:00] [INFO] [client.go:336] ["coarse scatter"] [enabled=true]
181:[2026/06/12 09:19:34.039 +00:00] [INFO] [split.go:97] ["execute split sorted keys"] ["keys count"=8]
184:[2026/06/12 09:19:34.053 +00:00] [INFO] [client.go:240] ["scatter regions"] [regions=3]
189:[2026/06/12 09:19:34.066 +00:00] [INFO] [split.go:121] ["finish spliting regions roughly"] [take=26.895772ms]
192:[2026/06/12 09:19:34.082 +00:00] [INFO] [split.go:151] ["finish splitting regions."] [take=16.065814ms]
193:[2026/06/12 09:19:34.082 +00:00] [INFO] [split.go:128] ["finish spliting and scattering regions"] [take=43.022482ms]
```

Interpretation:

- Both restores used `split-keys=8`.
- Default restore logged `coarse scatter enabled=false` and 5 `scatter regions` calls. One call happened before rough split completed, and four calls happened after rough split completed, which are fine split scatter calls.
- Coarse restore logged `coarse scatter enabled=true` and only 1 `scatter regions` call. That call happened before `finish spliting regions roughly`.
- After rough split completed in the coarse restore log, BR logged `finish splitting regions.` for the fine split pass and did not log any more `scatter regions` calls.

This proves `--corase-scatter=true` preserved rough split scatter and skipped fine split scatter in a real TiUP playground restore.

## Step 8. Cleanup

Command:

```bash
kill "$PLAYGROUND_PID"
rm -rf "$HOME/.tiup/data/br-coarse-scatter-verify"
curl -sf http://127.0.0.1:2379/pd/api/v1/version || echo cleaned
```

Output:

```text
PD endpoint unreachable after cleanup
```

Artifacts left intentionally for inspection:

```text
/tmp/br-coarse-scatter-verify/restore-default.log
/tmp/br-coarse-scatter-verify/restore-coarse.log
/tmp/br-coarse-scatter-verify/playground.log
```
