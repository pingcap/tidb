# `pkg/inference` — complete Go-master parity boundary receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01). The package landed
in Go commits `b36863fbed` (foundation), `c11efc57f0` (provider adapters), and
`f8848b3cf1` (runtime and `EMBED_TEXT`).

## Complete inventory

The package contains 43 tracked artifacts and 7,368 lines. Every production
source, provider protocol model, test, shared test helper, mock, and Bazel
target was read in full before comparing the Rust workspace. There is no
`doc.go`, fixture file, generated output, benchmark, fuzz target, or
platform/build-tag variant. The `embedding/mock` and
`embedding/internal/testutil` directories are Go test-support packages and
are included in the atomic inventory.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 46 | `9731a8c118626f67ccf047bef74ef35dc7a03341` | `ff8525b5774751bd8000378665e1060d93eba4def58cca351f2470df706a5427` | inference runtime and root tests |
| `domainadaptor/BUILD.bazel` | 12 | `0d6a846251fc947d057ccb115b990e59f98533ed` | `b90f07015d168fd330d20f7e64fa9ed7185d9a27d8c71b5de2e1718963135fe4` | domain adaptor target |
| `domainadaptor/adaptor.go` | 46 | `b9f8f3c36ef234de916531e868018a3c6d33dfce` | `3e21ea9b71533ee44f937d6292ff8efa52ca566462e2b3a1c7afa21829441fbb` | Domain-owned embed-function bridge |
| `embedding/base/BUILD.bazel` | 28 | `e6064b6dad910f783221654db5411fe7eb81382e` | `ba72253c766e15d3a699776f2e5094cb69b3ab09b152efdd72f0ddff4b1fba29` | shared provider base target |
| `embedding/base/base.go` | 441 | `0e78eaa06886c4179f72951c5134299c9f2e1cc9` | `33712be2203b9ad25350bcf0b53c236ebb20bdbac055b2dfcc332ceef0159126` | HTTP lifecycle, decoding, redaction, and provider contracts |
| `embedding/base/base_test.go` | 448 | `19e8f481b6f67d2faa0e1e3956eb6141506bdc09` | `eb21928f5b20479cf04d9af016a3cd8b915dc3e82867b3691b9d8ae0aa23e234` | shared HTTP/codec/redaction tests |
| `embedding/batcher/BUILD.bazel` | 26 | `447be9ca49d8db8e2c30ec82f6e2839917d9db63` | `a14591f57f5dc37b9c8bcdbf704f656df9a4800a92608799c5c60208709b2af3` | batching target |
| `embedding/batcher/batcher.go` | 445 | `b080ae605b46c8613433afa0f950e988844fd67d` | `b5a5b90cf4af612899c8ecaca674c03d6cbae1f9fa6d30c903a007c1a97b64f8` | keyed batching, cancellation, registration, and dispatch |
| `embedding/batcher/batcher_test.go` | 1042 | `34169003e3a9a062577fedac443ce30b273d13eb` | `cb1bd1a94cf04c7f6b61ae71752c406275d9cd42dd2ea46828a8d075bfc7e410` | batching, cancellation, ordering, and panic tests |
| `embedding/cohere/BUILD.bazel` | 27 | `0fa70976ad901bbb6a9c6788f8ab7008fe294e7f` | `8ba229650f96bc5adb4f1d684ccbde877708f8b088f48fe4d1d79f0e1f256afe` | Cohere provider target |
| `embedding/cohere/cohere.go` | 187 | `46a2a39f48c32dcc1ec3c42205dba97d942b0d3a` | `9f642c3eeeca964082ed8a24026b40ad3ab2dc1a7f7b29de5a43827583b251d2` | Cohere request/response adapter |
| `embedding/cohere/cohere_test.go` | 243 | `e484e7f6ad5b538353a4a786764f92e6b393d8a3` | `470ed1befe7d618df8b3095656ca0170f6739fb082009ce7e64e83321ddab356` | Cohere wire, validation, and contract tests |
| `embedding/cohere/protocol.go` | 28 | `659b788dfee3ef6d45549fdaeddc7652a139a984` | `2f59d1fb7087af6830287dcf162e3a0e9f7a13daab03f1819882a374deadbf2f` | Cohere protocol models |
| `embedding/gemini/BUILD.bazel` | 27 | `abb3d21de8c2973fc793d3b91bace15416aa8d5e` | `6e56ebca19a947294ecf8fa94ff9c4eed1bb925fb8d93aa5f661ddd4bc02ecfc` | Gemini provider target |
| `embedding/gemini/gemini.go` | 137 | `67ea22437a8f81a9c19e045e1bb799a023dd9124` | `3e829a10f043bd02b447f187449832707b1bffd3b2056124849068ee3a66ccee` | Gemini batch request/response adapter |
| `embedding/gemini/gemini_test.go` | 337 | `e35e9c9e738cd61ed5f2df9e5121334e3216f47a` | `b48f0ba72fb13976805368ebe78c352e8ec523d28da6086fd56c55b39b5e60ef` | Gemini URL, wire, and contract tests |
| `embedding/gemini/protocol.go` | 32 | `a4051a7b23682ad75f9fd71d0d56ec4d135b39d6` | `2e02a85af7dd60c1b0a45a2493904f63c139f6ba1bbb4913edbffe3623b9d659` | Gemini protocol models |
| `embedding/huggingface/BUILD.bazel` | 27 | `06cb4e3c223e5c0eb88900866e38cf946fb53ce0` | `78b42a879b7655639b12d90ab992539d895331a2080e941343b7eebbb3aa931a` | Hugging Face provider target |
| `embedding/huggingface/huggingface.go` | 126 | `b426ace39ea46fb79a4f7f9694203b02aa2f9e74` | `03392607b4133fa8d2b3eaddcce47e15148d0e37e58c982b8dacd792bc7935b7` | Hugging Face feature-extraction adapter |
| `embedding/huggingface/huggingface_test.go` | 238 | `736f14f88e12a17b9dbf3e69efd2e0b2c23e2367` | `641d35b8cfb4abdb4b0c7f803c3f01748b6bfadce2181473e83e556c2c534452` | Hugging Face wire, URL, and contract tests |
| `embedding/huggingface/protocol.go` | 30 | `17e3f18ade849904b4cb3610600d7eb98b843150` | `f5e8658a2b4f94943fafa9453dbef95d9435329bcae392c1b4bc029f9ade0478` | Hugging Face protocol models |
| `embedding/internal/testutil/BUILD.bazel` | 9 | `6ddf242700cc9f4f257c02167e8af155c8b2fa6a` | `b92f5cac6e836b8dd856a2785e70850f68a44d0c15c6e6a0ca577808fca47205` | shared provider-test target |
| `embedding/internal/testutil/testutil.go` | 197 | `1ec7fe4be9fa32a1ecbd21ea5326e3ddd3a4ec7f` | `fee4f90d34229d04be5dc275b98bde191dfa36044bcb618e6ed2a67f4ffc6427` | contract runner and HTTP fixtures |
| `embedding/jina/BUILD.bazel` | 27 | `00614483484b02492da6f2f216a96d19ebde8855` | `1ae9a9a60cf560c5eb5c74ac003b728e1b67fa0c9f88a8a56af936cc8689bae4` | Jina provider target |
| `embedding/jina/jina.go` | 123 | `6a3ddd429e87707dd1164863eb18c83ac53beaba` | `f10c14ce15e81c765c7b1cc818b462c4b3d546ae17e8b29955cb291a805200b4` | Jina base64/indexed adapter |
| `embedding/jina/jina_test.go` | 300 | `1749041e1f1b6f980d4d5e386bffd311172ec6d6` | `627ac542622b2d8f890f0c927416c9ae364aa18bb75490b9044147f4552c8ccf` | Jina wire, index, option, and contract tests |
| `embedding/jina/protocol.go` | 36 | `d09e4b4d76e61701e16939990ac9c34e07adc1da` | `648c857906eec52a9e912d3894702b985776846ce8cddd8a7ddbacf3979afe57` | Jina protocol models |
| `embedding/mock/BUILD.bazel` | 9 | `f37e758e81a636b6d22db8b60690b2c7ad994dc6` | `aef272c2a3c3d80ea2a91e73f7e37e77ded3a95f4deaef31e38b318aa289971b` | deterministic mock provider target |
| `embedding/mock/mock.go` | 102 | `1e3302c5263b4f2580d7582af4132df3e354b969` | `5d336d9ff39873b441b7d170def9545c248898c905129ce83da16e95a3e3a729` | JSON-vector mock embedder |
| `embedding/nvidia/BUILD.bazel` | 27 | `47e94f0ac1d172c7a53c1aa2c25b0533b53deb4b` | `b04034587074fa08443b659688a20a02309f5223a2c77d7b45f71d5e96a58a2e` | NVIDIA NIM provider target |
| `embedding/nvidia/nvidia.go` | 138 | `8496298cc035f9df8c5845cf648da96b7297f918` | `cfa9aa6062e8383eeef2555a0383b079804e69b124b1b82f18dffd738fa4d028` | NVIDIA NIM request/response adapter |
| `embedding/nvidia/nvidia_test.go` | 331 | `5d82e55a3418e8551c14b3148519f4cb8cff9a0d` | `3407fde2c1bb06e27bc2d437bf317a084ce23ce9ca4189af4b3cedd1cbfdbbc9` | NVIDIA wire, validation, and contract tests |
| `embedding/nvidia/protocol.go` | 46 | `644fc3837978c39f8cc3f3dd94744e9acb5987c9` | `e9747e53e3ce3f6eb88ba9fa3fa0536f4ccc239cc37843c0f21bdff35ad41318` | NVIDIA protocol models |
| `embedding/openai/BUILD.bazel` | 29 | `6c3895d554e018b361f56836d189d29b281ce397` | `efa7c44d6d1ccf2d1c0b9c20267433a39fb6a3fcbf531b2f5cbb35d43507c896` | OpenAI provider target |
| `embedding/openai/openai.go` | 125 | `3971d89439ef69cdd8e786be24f6a2d7d9263458` | `ed6ddebc8f1508e6b1d1d98adb35148757c432b24eb7692760f71eb4a4a61335` | OpenAI-compatible base64 adapter |
| `embedding/openai/openai_test.go` | 415 | `9ae03fa0f67da3bdf4a6a1c8c2dd87f0c2e6ada4` | `51ff9ce8fbf4ab049c4830a25b032aab55095f01c8068db1aee2aafa4af93614` | OpenAI URL, timeout, wire, and contract tests |
| `embedding/openai/protocol.go` | 40 | `53afc79c6bb1c9c48679a84cb2f9eb6a67d1f705` | `add9949479c6880657f697a47b7a708becfb0d28f2bb5563fd9f5a9cc4f30e27` | OpenAI protocol models |
| `embedding/tidbcloud/BUILD.bazel` | 26 | `6cb9617f75604962b413c822889306bc7c2f2675` | `e759655b1c58fdf549053b8b8c16064880c7a19fdc4f27366d40808bbfcf5f62` | TiDB Cloud provider target |
| `embedding/tidbcloud/protocol.go` | 25 | `6f974b8d3b9d3518532496cc67dd01fa29583903` | `b070cddf9903a3550740b4e46bc2c0bc8435f584cd1fcfb87c76dc98340b59e5` | TiDB Cloud protocol models |
| `embedding/tidbcloud/tidbcloud_free.go` | 157 | `d698ed4eccd0352ae4510976c4d187c6bc0dc530` | `dac682ef48376dc3515958cd65e3066d9639c315aa13ec23aac81dfb02433f4f` | hosted/free inference adapter |
| `embedding/tidbcloud/tidbcloud_free_test.go` | 243 | `52aa53ff9cf786b2aa138cd24aec72afc2514382` | `6ace425e393aca22dcd5989c7002c507e7a39aea5d2a0f01cc3327fe5af3177b` | hosted adapter and contract tests |
| `sqlembed.go` | 559 | `7dc8b7d1aeaa536d4bd2f684e8b5a6b3852a58a2` | `40009c33376fd0af621cdd652c5d013d3c36730472b72deb710ac259c766baf0` | Domain embed function, cache, cancellation, and provider registry |
| `sqlembed_test.go` | 431 | `c9c75fa19340bf8e278e38f2a6bd5500efd038b1` | `bf5b0da28fdb322b0681c388b8c8923c16ffc5aae533c5f89f6764b26811fa32` | runtime cache, cancellation, config, and error tests |

The 29 production/support Go files cover the provider-independent HTTP
contract (bounded bodies, URL escaping, JSON merges, error redaction, indexed
base64 decoding), keyed batching with caller-isolated cancellation, seven
provider adapters (OpenAI, Jina, Cohere, Hugging Face, NVIDIA NIM, Gemini, and
TiDB Cloud), a deterministic JSON-vector mock, Domain ownership indirection,
and the SQL-facing cache/in-flight lifecycle. The 10 test files contain 122
top-level `Test*` declarations, including provider contract suites and
regressions for cancellation, option snapshots, URL traversal, response
ordering, credential redaction, and vector dimension limits. All 238
function/method declarations and all 122 tests were checked individually.

## Rust ownership and explicit boundary

Rust has no dependency-closed inference owner. `tidb-vardef` contains the
embedding system-variable names and defaults, while `tidb-expr` and
`tidb-ast` retain explicit ignored `EMBED_TEXT`/embedding-key redaction gap
tests. No Rust crate implements the `EmbedFn` registry, Domain adaptor,
provider HTTP protocols, API-key/error handling, batching/cache/cancellation,
or vector-returning `EMBED_TEXT` execution. These existing constants and gap
tests are provenance, not a transcreated runtime.

No Rust-only behavior was found to remove. Implementing only a parser or a
single provider would leave the package-atomic contract violated and would
invent an integration boundary for external credentials and network clients.
The complete Go package is therefore recorded as an explicit SEED/boundary;
future Rust inference work must establish the session/expression/vector and
configuration owners together before enabling the ignored behavior tests.

## Validation and risk

Profile: **WIP** for this documentation-only boundary record. The package is
new on Go `origin/master` and is absent from the integration branch checkout,
so its tests ran from an exact detached Go-master worktree. No Go source,
imports, test declarations, Bazel metadata, or module files changed in this
batch; `make bazel_prepare`, Rust compilation gates, and the Ready lint gate
were not required.

```text
(cd <detached-origin/master-worktree> && \
 PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
 GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
 go test ./pkg/inference/... -count=1)
# passed: root inference, all provider packages, mock, and test utilities
```

The initial command from the integration checkout correctly reported that
`pkg/inference` is absent there; no source was copied into the branch. No Rust
code changed, so no Rust owner test was applicable. Not verified here: Bazel
execution, external provider calls with real credentials, full workspace tests,
or a future Rust `EMBED_TEXT` runtime owner.

This receipt certifies the bounded `pkg/inference` inventory and ownership
decision; it is not a repository-wide transcreation claim.
