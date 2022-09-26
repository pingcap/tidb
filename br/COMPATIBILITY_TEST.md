# Compatibility test

## Background

We had some incompatibility issues in the past, which made BR cannot restore backed up data in some situations.
So we need a test workflow to check the compatiblity.

## Goal

- Ensure backward compatibility for restoring data from the previous 3 minor versions

## Workflow

### Data Preparation

This workflow needs previous backup data. To get this data. we perform the following steps

- Run a TiDB cluster with previous version.
- Run backup jobs with corresponding BR version, with different storages (s3, gcs).

Given we test for the previous 3 versions, and there are 2 different storage systems, we will produce 6 backup archives for 6 separate compatibility tests.

### Test Content

- Start TiDB cluster with nightly version.
- Build BR binary with current directory.
- Use BR to restore different version backup data one by one.
- Make sure restore data is expected.

### Running tests

Start a cluster with docker-compose and Build br with latest version.

```sh
docker-compose -f docker-compose.yaml rm -s -v && \
docker-compose -f docker-compose.yaml build && \
docker-compose -f docker-compose.yaml up --remove-orphans
```

```sh
docker-compose -f docker-compose.yaml control make compatibility_test
```
