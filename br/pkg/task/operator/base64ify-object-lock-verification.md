# base64ify Object Lock Manual Verification

Date: 2026-05-20

This records the manual verification for `br operator base64ify` object-lock encoding. No unit test was added because the check depends on a live MinIO/S3-compatible service.

## Environment

- Reused the existing MinIO instance at `http://127.0.0.1:9000`.
- Used `/workspace/ext-bin/mc`.
- Access key and secret key were both `minioadmin`.

## Steps

1. Configure a MinIO alias:

   ```bash
   /workspace/ext-bin/mc alias set base64ify-local http://127.0.0.1:9000 minioadmin minioadmin
   ```

2. Create a fresh bucket with object lock enabled:

   ```bash
   bucket=br-base64ify-objlock-1779287829
   /workspace/ext-bin/mc mb --with-lock "base64ify-local/${bucket}"
   /workspace/ext-bin/mc stat --json "base64ify-local/${bucket}"
   ```

   The `mc stat --json` output included:

   ```json
   "ObjectLock":{"enabled":"Enabled","mode":"","validity":""}
   ```

3. Generate the base64 storage backend without `--load-creds`:

   ```bash
   go run ./br/cmd/br operator base64ify \
     -s "s3://${bucket}/prefix?access-key=minioadmin&secret-access-key=minioadmin&endpoint=http%3A%2F%2F127.0.0.1%3A9000&force-path-style=true&provider=minio&region=us-east-1"
   ```

4. Decode the generated base64 into `brpb.StorageBackend`. The decoded S3 backend contained:

   ```text
   bucket=br-base64ify-objlock-1779287829 prefix=prefix endpoint=http://127.0.0.1:9000 provider=minio object_lock_enabled=true access_key="" secret_access_key=""
   ```

5. Repeat with `--load-creds`. The decoded S3 backend contained:

   ```text
   bucket=br-base64ify-objlock-1779287829 prefix=prefix endpoint=http://127.0.0.1:9000 provider=minio object_lock_enabled=true access_key="minioadmin" secret_access_key="minioadmin"
   ```

6. Remove the temporary bucket:

   ```bash
   /workspace/ext-bin/mc rb --force "base64ify-local/${bucket}"
   ```
