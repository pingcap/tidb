# Types package source inventory

Go package `github.com/pingcap/tidb/pkg/types` is **unaccepted**.
Pinned source revision: `aba629bb455dc09d6a5d98b3c39a542bb1189b9d`.
The native value implementation is primarily `tidb-datatype`; its expression,
planner, executor and session consumers remain part of the integration audit.
One atomic package claim must account for every production, test, support and
build artifact below. No individual row is a completion claim.

The package has 56 direct artifacts: 29 production Go files,
26 Go test/benchmark/support files, and 1 build/support files.
There is no doc.go, package-local testdata directory, go:embed directive,
go:generate directive, generated-code header or build-tag variant in this
pinned root package. Test fixtures are inline in the inventoried test sources.
The `parser_driver` child is a separate Go package; its dependency/integration
role does not make it implicitly accepted. Referenced Go/external dependencies
remain separate package units. The checked-in BUILD.bazel records original
source lists, imports, visibility and test dependencies; the workspace Cargo
manifests and crate build inputs must also be validated before native acceptance.

## Complete direct artifact inventory

Every hash was checked against both the pinned Git object and the current
checkout on 2026-09-21. All rows remain pending complete native mapping and
package acceptance gates. The ongoing dependency audit is recorded in
[the physicalop ExecPlan](planner/physicalop-package-parity-execplan.md).

| Artifact | Kind | SHA-256 | Disposition |
| --- | --- | --- | --- |
| `pkg/types/BUILD.bazel` | build/ownership/support | `558878acb2d3721d384d0591b4651e2cf2f4c62f36e384e2ebeffdc0b568157d` | Inventoried; whole-package audit open. |
| `pkg/types/benchmark_test.go` | test/benchmark/support | `a84fc6703fc979647fd5056e54c99702f5e88882d0c18a77c858ebb83a2becaf` | Inventoried; whole-package audit open. |
| `pkg/types/binary_literal.go` | production | `44f9a34b1639c5d454c9b1c726ef26af85f952fb40103ded708e79d83b407850` | Inventoried; whole-package audit open. |
| `pkg/types/binary_literal_test.go` | test/benchmark/support | `5ad4d597b8e25711566a54022d7913bb87bf52aa78c7d1bae19adebad3fd8312` | Inventoried; whole-package audit open. |
| `pkg/types/compare.go` | production | `077756458a730820cfb3deb2bbb0fda8bdf3600dbaf1dce59f8c2bb593f36d73` | Inventoried; whole-package audit open. |
| `pkg/types/compare_test.go` | test/benchmark/support | `ce4e2878358e57814e3acee95449353e461dfa6d7a1345f7c185808b3f1b8247` | Inventoried; whole-package audit open. |
| `pkg/types/const_test.go` | test/benchmark/support | `0cd67f9292c3b9757fa1a498d32a620b90e3a957584b6bda1d980afabdb77581` | Inventoried; whole-package audit open. |
| `pkg/types/context.go` | production | `4f95c649cecf84cfdb3378b156fb4577762b53d8f2c4ade1600ef66ff237606a` | Inventoried; whole-package audit open. |
| `pkg/types/context_test.go` | test/benchmark/support | `0da44ffcfd5fd95d18c0db3e46ee6ac82b76f94f0ab2bda612adf178133c3976` | Inventoried; whole-package audit open. |
| `pkg/types/convert.go` | production | `256d7882460e5e2ec9705cbad7238c7e0d08de2a732519e3075eeffff3b501dd` | Inventoried; whole-package audit open. |
| `pkg/types/convert_test.go` | test/benchmark/support | `a140f20a025d116825021bd749dd94c6ed030167c244256991e837d1562e6512` | Inventoried; whole-package audit open. |
| `pkg/types/core_time.go` | production | `12b6aacf7dfabec52469298e64570bace891261cece35228e17d9150b5789de9` | Inventoried; whole-package audit open. |
| `pkg/types/core_time_test.go` | test/benchmark/support | `da68a780cba365a0995052db91cfe61f33330ca24fc49b38cec49a57d236fbd4` | Inventoried; whole-package audit open. |
| `pkg/types/datum.go` | production | `a7e97bcf032c6abd2e5c8d59516212cb6ead164dc3ec826bfd87392bc66475fc` | Inventoried; whole-package audit open. |
| `pkg/types/datum_eval.go` | production | `23780b98e20b22749c13469abb3012c69144a18805b83424bd78b9afd5107704` | Inventoried; whole-package audit open. |
| `pkg/types/datum_test.go` | test/benchmark/support | `8b8d78130b76b9c5e0232e546c3ca819d88a0a0feffed33401dcdce44264234d` | Inventoried; whole-package audit open. |
| `pkg/types/enum.go` | production | `0c6c53aae4b0916a33f64cc27507b9f98158af0f53fce145ce3de1e2ba83f6c0` | Inventoried; whole-package audit open. |
| `pkg/types/enum_test.go` | test/benchmark/support | `a48b847dbd6c8c27bb56db1fd3937e48a5b4d91c9517cce47a02fcbdac9bb21f` | Inventoried; whole-package audit open. |
| `pkg/types/errors.go` | production | `662f3bf673f1ac432654d7b0c7350ecc48d61d4c4b73e0a0b2fbde443cd39b72` | Inventoried; whole-package audit open. |
| `pkg/types/errors_test.go` | test/benchmark/support | `ea12ebee6a592baf97584b847da0b25123aef04e81aee8c379cc6c6e8c48c9b2` | Inventoried; whole-package audit open. |
| `pkg/types/etc.go` | production | `f8fe3b7caa4b52eba7284959bce2b3992b079c0477dcb1e6895a71ce9bbd5d7e` | Inventoried; whole-package audit open. |
| `pkg/types/etc_test.go` | test/benchmark/support | `d0df53257289665478e400a0f2c69778428979a878b6033d390380b27262f568` | Inventoried; whole-package audit open. |
| `pkg/types/eval_type.go` | production | `8c7d2f7eec5e9ecb9d84d3779c798f9c9de9d7bd683646c108da388fcd657d46` | Inventoried; whole-package audit open. |
| `pkg/types/explain_format.go` | production | `886eb80ef2c85a46c5c92e5236e95fba168bd33ab0dae60eb2a6dbdc4ed41f71` | Inventoried; whole-package audit open. |
| `pkg/types/export_test.go` | test/benchmark/support | `3cab79480ead86fba4f8dc1179888fa8072139e99eaf753723a20913867f81ef` | Inventoried; whole-package audit open. |
| `pkg/types/field_name.go` | production | `78d5c8a03648343403f1dda918968a63e889442ec86ca907777338746e10a8e9` | Inventoried; whole-package audit open. |
| `pkg/types/field_type.go` | production | `13b017f7a87b5bd1addae80574b5d10038a46988163af0796f79f595762e80d9` | Inventoried; whole-package audit open. |
| `pkg/types/field_type_builder.go` | production | `01aef13bc6a41796c8a0df5f6def01953d5d055365daeea4cc60fd5a65b4de2d` | Inventoried; whole-package audit open. |
| `pkg/types/field_type_test.go` | test/benchmark/support | `7c706b034aa83d8fd55359a0828039d5be36698664c1020b1fbcce43eb39c89c` | Inventoried; whole-package audit open. |
| `pkg/types/format_test.go` | test/benchmark/support | `1cf0aaa612bd51f03c341e0cbc591b2667d28ce399946835dc27a13e46645310` | Inventoried; whole-package audit open. |
| `pkg/types/fsp.go` | production | `a4a2e05c8b53a6bc035b1d7cf6a6f080c4a68945cc4e84f3e1392640e0bbfa5c` | Inventoried; whole-package audit open. |
| `pkg/types/fsp_test.go` | test/benchmark/support | `3f4026173c89ee9333d8a524941513adbbf3ed8a5996c9235626a9ec4cc0605c` | Inventoried; whole-package audit open. |
| `pkg/types/helper.go` | production | `560a5028bc5703db96b2ba7fa95288c5faaa4a9ecfdead8006dfbbef5d5c3aaf` | Inventoried; whole-package audit open. |
| `pkg/types/helper_test.go` | test/benchmark/support | `361e1ba6202f2f790273f65b46c34e5278ffc866f0f56ca048087bca2cd98a1f` | Inventoried; whole-package audit open. |
| `pkg/types/json_binary.go` | production | `aa86fce5fbaaa7225eac4d2c45fe445f88c3e7a4bb7097634e93129385577541` | Inventoried; whole-package audit open. |
| `pkg/types/json_binary_functions.go` | production | `578522e49701af013a1f91a3947f1c4d3231f49cbbb2d60d9edd4bcd24ae082b` | Inventoried; whole-package audit open. |
| `pkg/types/json_binary_functions_test.go` | test/benchmark/support | `da605aa2d45db00a2736fa420ea93397db22578b76d5666b3ca9de34a6383989` | Inventoried; whole-package audit open. |
| `pkg/types/json_binary_test.go` | test/benchmark/support | `aa71d912c99dc9be59df8285dee0521e65d225bd5c97bd9dc9a44c32bb293d39` | Inventoried; whole-package audit open. |
| `pkg/types/json_constants.go` | production | `50b5fb966e4dcbf71b99b843f2fbb371592a6ddd0987ca0b1d59c662c6b78497` | Inventoried; whole-package audit open. |
| `pkg/types/json_path_expr.go` | production | `96cb4c1f104530138d4b8cbd363132e8f66b4de0c7c0266b5f9467f387a7e44a` | Inventoried; whole-package audit open. |
| `pkg/types/json_path_expr_test.go` | test/benchmark/support | `23c9fa28059a32e08fb4d75ba563c078cd02b4a901b7cb37652eebb683f5f311` | Inventoried; whole-package audit open. |
| `pkg/types/main_test.go` | test/benchmark/support | `801e622052aaccc59aca944919a109fe42badbbbaa02b566c847604efbab94d8` | Inventoried; whole-package audit open. |
| `pkg/types/mydecimal.go` | production | `7cac77d01f7fd8447962b794d2c397cb280a44b0da345edc63193260bdc1f1d7` | Inventoried; whole-package audit open. |
| `pkg/types/mydecimal_benchmark_test.go` | test/benchmark/support | `ac3b94f13cc2097467b3264ee04b8662bb20f748431543d636a216138fe96351` | Inventoried; whole-package audit open. |
| `pkg/types/mydecimal_test.go` | test/benchmark/support | `a7e0706001aebbc7c3740c04084930cc22c5f57ae7d0f4097d99fe33cb71ecfa` | Inventoried; whole-package audit open. |
| `pkg/types/overflow.go` | production | `f1c645a9fe679b6a0bf07c5d9770a505e4e2979d6b7af3a38ac265b67892b359` | Inventoried; whole-package audit open. |
| `pkg/types/overflow_test.go` | test/benchmark/support | `6d7c56f677d4b5d6c56ec522ea0c3e54a244eb1f896fc1b907240299f3a22673` | Inventoried; whole-package audit open. |
| `pkg/types/set.go` | production | `591b6284a4925b45e7f95996ebefe72a7075ac17fdd69cca8b45e854e6f26b00` | Inventoried; whole-package audit open. |
| `pkg/types/set_test.go` | test/benchmark/support | `36f9f1d9a7904aaa58896d953d419929827b6a7c4a18af5f37e8245fd81cfd3d` | Inventoried; whole-package audit open. |
| `pkg/types/string.go` | production | `ec6585c98adfc174c057cc47788422c811132f16691f284cf68be6206d628b18` | Inventoried; whole-package audit open. |
| `pkg/types/time.go` | production | `e4f3da22a90a6271f86e11f073c7afb6b6fc25ee32bb95e59904ccb861e0c425` | Inventoried; whole-package audit open. |
| `pkg/types/time_test.go` | test/benchmark/support | `b0871c17e7aac503dbcbfa27f7bda57b528ac897a8c0d5fb7333d3b49433f3f8` | Inventoried; whole-package audit open. |
| `pkg/types/truncate.go` | production | `aad3ea2ab9b0e07fce84a60895a3090be6312be11f45804dc00abfe9532e16a6` | Inventoried; whole-package audit open. |
| `pkg/types/vector.go` | production | `ce574a82bec1ef880087f511f09fca46b26992eb48ff97ddec1fd476ab3f75b5` | Inventoried; whole-package audit open. |
| `pkg/types/vector_functions.go` | production | `a379639b2f3dce2ec6655962f998b83ff386de9b2046cf1a3230e3891d9f853b` | Inventoried; whole-package audit open. |
| `pkg/types/vector_test.go` | test/benchmark/support | `2580059b14bc49bf562a6bc279e1d98a5fbcda4ba96138b03791a2991e661dba` | Inventoried; whole-package audit open. |

## Reference validation and remaining gates

The complete original Go unit-test selection passes on darwin/arm64 using
go.mod's minimum Go 1.25.12, race detection, intest/deadlock tags, and count=1.
From the isolated checkout /private/tmp/tidb-parity-publish-aba629bb:

    GOTOOLCHAIN=go1.25.12 GOPROXY=off go test -race -tags=intest,deadlock -count=1 ./pkg/types

Result: ok github.com/pingcap/tidb/pkg/types 1.106s. Log:
/tmp/tidb-types-original-go12512.log. Checks for failpoint., testfailpoint. and
the Bazel failpoint dependency return no matches, so no failpoint activation
is required for this package. No upstream Go source or dependency is changed.
This run does not execute benchmarks or establish Linux/Bazel/native parity.

Before acceptance, map every production symbol and original test/support
artifact to the native implementation, compare values/types/errors/warnings,
NULL, charset/collation, numeric boundaries and temporal behavior, account for
original inline fixtures and all build inputs, run mapped native tests and
consumer integration checks, and run the repository's required lint gate.
Existing narrow conversion oracles and the passing Go reference suite do not
replace those gates. Any platform or generated variants in dependencies must
be handled by their owning complete package claims. Rust representation and
ownership choices must preserve source behavior without inventing features.

Performance acceptance still requires comparable sysbench, TPC-C, TPC-H and
YCSB runs against matching Go source under equivalent configuration. Component
benchmarks alone do not satisfy that requirement. This inventory and reference
test receipt do not accept the package or any subset of it.
