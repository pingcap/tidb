# `pkg/objstore/s3store` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The root package contains 16 tracked artifacts and 5,619 lines. Every
production source, test source, and BUILD target was read in full before this
receipt was written. The package includes AWS S3, GCS-compatible signing, KS3,
Alibaba fallback credentials, and Tencent COS credential variants. It has no
fixture directories or platform-specific files. The nested generated mock
package is a separate Go package and is covered by its own receipt.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 98 | `a1c44601d585aebe6398d70e63246d2c46635aaf` | `729a64dcbff1cb0849df79491d43340a15c8bc6f5ddc845a7b25ad87804ad1ce` | AWS/compatible S3 library and 50-shard flaky test target |
| `client.go` | 479 | `e78108fc35069195ab818af65696bb7fa5807d80` | `fc7f815fa6c67a2fd1e8db0eec5231919b5de9c0adb0709a168ea3733dbbd5d0` | AWS S3 `PrefixClient`, content-MD5 compatibility, multipart and presign |
| `client_test.go` | 436 | `31cbd607c0cd0060a21695d361f7553f882e77f4` | `48af2f92d5ee5f89fa40ed9b95e33f59c52ee46a23b910ab89214003fb6cd01b` | mocked AWS permissions/CRUD/ranges/MD5 and metrics tests |
| `gcs_s3_signer.go` | 59 | `047f9fe5f1c6b686ee594f636f68b5d032d87fa2` | `f65c3143d1b479dcde0747e6d6742befc5031225d80578db517c10a702cd8943` | GCS V4 signer excluding `Accept-Encoding` from canonical headers |
| `gcs_s3_test.go` | 214 | `25ea7f37953a26e948139141c1662e777770e96a` | `c9ba506d34868cb991fa6783828b267e3306d5e36ee618ff200a83b42479bfbe` | GCS endpoint detection and signed-header HTTP-server tests |
| `interface.go` | 41 | `c0f1ab0c25024ff202927aca6874fbae4b19b141` | `8cc0c34b61cd8b6babae25e2a9f6bf817508b31dfcb6aa34f7c301c83163cd3c` | AWS S3 API subset contract |
| `ks3.go` | 848 | `d51030da80c7fc534a00bbded04f2c2fc2bc486e` | `e1db53a6a4b47c59a7ed4b0d2d71244a45103b4469ee8f28fed9003faee010d3` | Kingsoft SDK backend, permission checks, range reader, multipart/copy |
| `logger.go` | 47 | `76b2de6c8253e01d1aca25522ce7e32a7e135e14` | `ddc84d310f6d5d0ded1bea340575de11832f514d3a5072775c51776f27799389` | AWS Smithy logger bridge |
| `main_test.go` | 232 | `f4d923f0d6f3505919fa9d990db40da2f135fe38` | `bb01c3b38605d036ce4be8834de36161749a74ec8b8e46f7a5381250be7fb837` | AWS/Aliyun fallback credential and shared test-suite helpers |
| `retry.go` | 87 | `eadc5df0ab052e25a03d4607cb2325944aa1853d` | `3f20a7149816e893e45212006e76f0681ff3a1c2a7c5a18a5bf2cc068da0b223` | AWS retryer and bucket-region redirect classifier |
| `retry_test.go` | 105 | `dd2bd40d6c10241abd7129ad2ec4bf1bbc870812` | `ef31f8ee662d1930114ed6c0316e193bfaa5583ef4dee616ae341a10131ebcd7` | retry budget, metadata errors, redirect suppression tests |
| `s3_flags_test.go` | 382 | `5cf064928a1aa3026330a326ee0d2b45f56d7044` | `c178d43f1768f80f12d6d7a5936619f7c40b8138ee86c7d70ee464e39fd88241` | profile/credential/flag parsing tests |
| `s3_test.go` | 1,961 | `0a28a7c882aad407b7776ec73ba6bbdae854d881` | `8ce5ee33ef1b37c0fe5449cd5bf5c99347f8ff0fb70da9c6a0b8c8c29e2dba6b` | local HTTP, multipart, ranges/retries, walks, locking, object-lock, and failpoint tests |
| `store.go` | 451 | `ab43a52ddecd6b4db056beeb38856a47e30e68d3` | `3cee4095c2add8cec0f0772862370d90bc75d9d7c0aaf435fd35e92d15126532` | AWS configuration, endpoint/region detection, role/fallback credentials, storage construction |
| `tencent_cos.go` | 66 | `a78af9e61c2474534a936e8e5ea33f3a476c007a` | `39e5bc36ad11b317ebfed8e9f72ce84df8213adce9ee4ef5c27d87c60dae7249` | Tencent CVM role credential provider |
| `tencent_cos_test.go` | 113 | `d441f68c1cccd060fe3007afb780440d9f3fa9f0` | `c1f7996ce3fef69a288b8f182467b8ae729913b5db8bcab459bb4cc7cb2f117b` | Tencent endpoint/precedence/provider-refresh tests |

The production sources contain 71 functions/methods. The seven test/support
sources contain 72 top-level tests plus local HTTP/readers, suite builders,
and assertion helpers. The complete test surface covers options and profile
precedence, AWS region discovery and FIPS endpoints, GCS canonical signing,
Tencent and Alibaba credential chains, object-lock detection, CRUD/list/copy,
multipart limits and content-MD5 compatibility, buffered and ranged reads with
retry/cancellation, walk pagination/start-after/empty prefixes, lock behavior,
and failpoint-driven retry paths.

The current Go-master delta from the earlier pinned source
`e2788410d8d696605e8cb002585877a063ccc909` was read in full. BUILD adds the
GCS/Tencent sources and SDK dependencies and expands the test target from ten
to fifty shards. AWS client multipart writes now enforce the shared
`MaxUploadParts` sentinel and map uploader overflow errors; client tests add
content-MD5 and metric coverage. Go master adds a GCS-specific V4 signer and
tests, Tencent COS CVM-role credentials and tests, AWS retry suppression for
expected region redirects, Alibaba ECS RAM fallback credentials, GCS region
discovery bypass, profile/role/configuration handling, and expanded local HTTP
regressions. KS3 drops its obsolete forced upload size. The generated mock has
no source delta.

## Rust ownership and explicit boundary

Rust has no dependency-closed owner for AWS S3, GCS XML interoperability,
Kingsoft KS3, Tencent COS, Alibaba fallback credentials, cloud retry policy,
multipart/ranged object I/O, or the shared provider-option surface. Existing
Rust storage traits are plan-replayer-specific and cannot substitute for these
cloud SDK contracts.

No Rust-only behavior was found to remove, and implementing any single backend
without the complete object-store stack and provider SDK lifecycle would be
speculative. This package remains an explicit parity boundary.

## Validation and risk

Profile: **WIP** for this documentation-only boundary record. No Go, Bazel,
module, or Rust source changed. The package uses failpoints; the canonical
wrapper was run against exact Go `origin/master`.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh pkg/objstore/s3store -count=1
# exact Go origin/master source: PASS, 8.005s
```

Not verified here: live AWS/GCS/KS3/Tencent services, Bazel's 50-shard target,
Windows execution, or full-workspace tests. No Rust validation was applicable
because no Rust source changed.
