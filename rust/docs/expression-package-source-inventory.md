# Expression package source inventory

Go package `github.com/pingcap/tidb/pkg/expression` is **unaccepted**.
Pinned source revision: `aba629bb455dc09d6a5d98b3c39a542bb1189b9d`.
All hashes below were checked against the pinned Git objects as well as the
current checkout. One atomic package claim must account for every artifact;
no individual row is a completion claim. Native code spans tidb-expr and its
datatype, chunk, collation, planner and executor dependencies.

The root package contains 133 direct artifacts. There is no doc.go,
package-local testdata directory, go:embed directive or root build-tag variant
in this revision. Generated outputs and tests are included explicitly. The
BUILD.bazel testdata glob currently matches no files. Child aggregation,
exprctx, expropt, exprstatic, sessionexpr, integration_test and test directories
are separate Go packages, not silently included as accepted dependencies.

## Complete direct artifact inventory

Every row remains pending full source/test mapping and required package gates.
The ongoing source comparisons and fixes are documented in
[the physicalop dependency ExecPlan](planner/physicalop-package-parity-execplan.md).

| Artifact | Kind | SHA-256 | Disposition |
| --- | --- | --- | --- |
| `pkg/expression/BUILD.bazel` | build/ownership/support | `fa7feb1fa8e8c1bcbda4c9785e7fbb724ffcc2612bac9bc77b3b5b2b8c5e4528` | Inventoried; whole-package audit open. |
| `pkg/expression/OWNERS` | build/ownership/support | `8996b42b6e49642563d90f9d6585212aa67320f61cbd5c20a2802e017e46e944` | Inventoried; whole-package audit open. |
| `pkg/expression/bench_test.go` | test/support | `0245bd00a9eb3688da9dda6a9569338bfe42465b4832b4ab6eadf35e4d86244f` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin.go` | production | `496f0b345019f0af073dab6cf7b6608087135728ca96ffe1323708d4cba13b4e` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_arithmetic.go` | production | `1b3802ee5d1066be485b3ee2ae64090ee1b93023f4c69419c630724b2bff770d` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_arithmetic_test.go` | test/support | `167ed03103c14e108216efee063f8a3b574f8e101b174fcd357e0c228746a848` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_arithmetic_vec.go` | production | `2ae5427b83096d26b2320041634668a42c10f0ed9a332cce4f14e4bc5e52ba24` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_arithmetic_vec_test.go` | test/support | `adec4add41c154529c6a6325de9bfd60d4a34664502f5e1fb6ff29e3fb395167` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_cast.go` | production | `12741863129c46a008a9064f94e11bf8be0a20f0b4efd83ef0a9e6b40b731ab5` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_cast_bench_test.go` | test/support | `bfeea12e25ddfc8ae854f2670bb9e44d16e82cbe8167923723153c19a933b7ac` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_cast_test.go` | test/support | `caa0b678d597d60452e2dda8c6a84f55cd21d6f2920f17661e85ae3d055ad6d7` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_cast_vec.go` | production | `bc5556808e300885467d8af4422762bb089acaae1d40184ab042dcc611f2a8b7` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_cast_vec_test.go` | test/support | `928ffd53244f44cdeab41b7cf4a98259ab3b8c94d3219f18b1a0b508051233b2` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_compare.go` | production | `b5f819998dd8f8abf6e3352d500154d63aca4b8fffe18278475350272fe91bd2` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_compare_test.go` | test/support | `bc6d55170b6b6cfe42abde6517adfe2ea0b400baa56cf3812bd0b92e81f6ce41` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_compare_vec.go` | production | `d8e3c8a02b2ce632a0403233c3a19121b9a3fc6d09043dc0437fec435833d3b1` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_compare_vec_generated.go` | generated production | `08fdfb7836e34edc080a750cba561e749169c93ef3a363a020046be1900c8cfd` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_compare_vec_generated_test.go` | generated test/support | `748119cb1eecec5177d973e63a23d94225c0c01262e8933f872053c212c63915` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_compare_vec_test.go` | test/support | `777dbf1b196cce09e5cb46eec53791b3a0c07b7f0bcc8c0d9514a8a2eb6392a7` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_control.go` | production | `1f9ae65cdd75b5b0b52f12dcf831a425fe9580be7abfdbde0981445108f583d8` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_control_test.go` | test/support | `2522707c9ac0f3fde34be8df519b66e7467e75b04f9d1c3b86bcd5d1ae784441` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_control_vec_generated.go` | generated production | `c24457256ce222e0539efc193d2392244168ca8541aadf310e0718cc9f3d18db` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_control_vec_generated_test.go` | generated test/support | `964b1966733156ef4c23bdab1a675f4e34a77cb38e9dad816185aa6bd308cfd9` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_convert_charset.go` | production | `e30f76a031f0b6d1eeeb566183c0439c1702f6e080b413e9b59acf1f96f4d4a5` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_encryption.go` | production | `3af4a2be5d0a275d0a5f1248673bc7d022735fdb195b3e4c6ce495cba407384e` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_encryption_test.go` | test/support | `85582635c18f0404ee5ae10cfcb44e7669099374683665b83afbd9dbead74362` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_encryption_vec.go` | production | `ef09bf3a19bab18401ea716a038d715fa136df08c6f51d41e84d03da2ddd0550` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_encryption_vec_test.go` | test/support | `4bb5bae5c770bf3e4d3315369e7a57999ef3bb1d081d324cc7541b086d06c396` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_fts.go` | production | `2bb518f48918c9f0b11ef81b5ca70e884e23a0f66e52137deccdcdec85b0f2dc` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_func_param.go` | production | `308c8814e6ee80aecdc6f549dc95112cf3143077ad01f3e52ad8fa71abb36ff0` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_grouping.go` | production | `7e0bc987ae31221380ed12d49eee907aa737e84496bac9468007dda0400f37af` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_grouping_test.go` | test/support | `8e6b55c4b5640e09c7db50265be3734df442e8d9ef18a97528a7184908bc977a` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_ilike.go` | production | `a49d6cf9560bffa4b48de34e1513b2aa555c2c187526506f367a42c899ba7f87` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_ilike_test.go` | test/support | `4431880082669c24fac3a405adfa5343e35c061d0a8400ce9684e47cf9ad01d2` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_ilike_vec.go` | production | `e0f1c29336a340c6899e2322217c7ac53f5a29988c6738c16f8c6920a674af46` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_info.go` | production | `fa624614856a5b84c9e6e1ed237138820df11c515584daf8065bcffd4bf4a4df` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_info_test.go` | test/support | `a1f1e5a4dd025440e92830f809a8881305bce5f4c5bb52992a4f90fc0aabf584` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_info_vec.go` | production | `83ccc746a1702d90b6d38bfe8f5d3c3129d7dfbcbdcb6c9a516693de81edc437` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_info_vec_test.go` | test/support | `f0189e4c015393c8179bf321d59f08c89c1ee277c7ec1181396800b82206cf22` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_json.go` | production | `f8e5b1e3f5e2e3c58f559e74fecc6322c077c874a3e384e7d3062f4f5eca6edd` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_json_test.go` | test/support | `775598466662eb69c91ae4ebdc38256fade97741485821105c726fbcf4e83f5c` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_json_vec.go` | production | `7fa30c8dedec85cc0f902049507454f3b4118e0241a707c5321fff3c3fecf96b` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_json_vec_test.go` | test/support | `75bc58a7603118d127e2552234f5030334fb91e3c9e89034a8a3ed2ddd425a7c` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_like.go` | production | `81a2cacd2313c6db0bbc192fb6d475d7bd4bf3c40c242fa2dcb8341b452dfa8a` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_like_test.go` | test/support | `5a839e974a9f700d49d8e508364c6d7797acd9cfa99f5f3a5af56f63a9aeed5b` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_like_vec.go` | production | `776c84e4ce3b9c2fe86707345c4338f223684270587f1fb812e0e36f7c0aade7` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_like_vec_test.go` | test/support | `18b4db247dca8ef244616e47b1dda1fed3634f80e8257dec9fa4fff86aa5b368` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_math.go` | production | `bbeac380c7142e00a4f24eb95089057a0874e08596d2aef64ca80df0781a8af0` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_math_test.go` | test/support | `2b6d27cf087408c904e906b08c2e7c2d04e67772dfb6f7f4ceba51331c64ba0a` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_math_vec.go` | production | `8c87f06d9a32501891c94522d46bd9df6c5ea91f9384c1fd846773a4bc3a2e03` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_math_vec_test.go` | test/support | `6f4b179ac5ecdfe8ad71bd7642de46d854b405c5b9c11ac4edb64b6a151f4d8b` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_miscellaneous.go` | production | `9ccda3fa097903b41f8264df2baff590d0c771af9c34161f12331b1f1a6a6b0a` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_miscellaneous_test.go` | test/support | `0aeaa25d19335e1075452a997c2f1b4420048a0840da4ee95c2e2195ed58dacc` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_miscellaneous_vec.go` | production | `fc3dd4dfefb2b6230d10896605a48c87e7d78b37cc2ee99d213daad49244e7d0` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_miscellaneous_vec_test.go` | test/support | `dc711d426066a49a58a55e45e6ecea4836bc80c2a92a5306b167a1672175528b` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_op.go` | production | `5c9abd133fddefeb53fbde8b9447bc5666c2b1aad6aec50da15f7214fb2bb154` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_op_test.go` | test/support | `889b39894da2d0ca77b79dd010c8082b0a832845580351752e5cb7ed042fded9` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_op_vec.go` | production | `e563c2976d6e72df0108177aa445f1a05515d02633b61cb8921d877011e448aa` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_op_vec_test.go` | test/support | `1e8f17480d49e47ebf49fc22ab86f0884d999ba02dbfa3e41a370d2c96fd94ee` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_other.go` | production | `19efa794b6cf1d967090031d70a6224519e7d808921bf7face23bbc4c6a11891` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_other_test.go` | test/support | `e63a2bf0e0005ef610379f436217412ffd8a7689c93a338e551360e06faca57c` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_other_vec.go` | production | `0a89906fb36e250e455584673e985633c9d9086b187af64434c1326209b0ee12` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_other_vec_generated.go` | generated production | `7f7b3bb510b5b7f28661ad97f3928811034b255478506f7e1ee310a7a1fb48ca` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_other_vec_generated_test.go` | generated test/support | `d267a69990ca3afd60da348f9cfe2e501228e99c8678888d546a22daa44ab09d` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_other_vec_test.go` | test/support | `52d07af7755f79a740cb9397147fc11e4709040a0606ad88e9a1de2a8b7be985` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_regexp.go` | production | `2e9e7d271c01354617e11feaf0338baa1733905febfc0f8cf1b8cbb63c4b8f9d` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_regexp_test.go` | test/support | `fd29f992d3710369c6ef89e37b16652b7f08b315bcbf9ea4b725d68f43ef14b5` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_regexp_util.go` | production | `d16cb62449cddee5047396779b7c6be75b80c6f761f0ebf28fb007dc97be2319` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_regexp_vec_const_test.go` | test/support | `a1e227d67d88d09c5925460a5d02aa159bfbfed9c77f49a342563400992f0d9f` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_registry.go` | production | `d19b53caba82cb4751715001a619e9c56a6da10c625e4a50c649ba9e44206988` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_string.go` | production | `8ad90c15d286a845172e8a4aca41c4d0e7752bcf9117ff41cc0794c1b67712d9` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_string_test.go` | test/support | `1f79bc3afafb9fd4ab84d0f2630091670c1ec4fdb6b91dcb761d58f89014a989` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_string_vec.go` | production | `8e4b9ca265e34f995f36b2d4788a534d0b9751a67968ab0117ae166b2e86675c` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_string_vec_generated.go` | generated production | `dd6c1cbced4c9a80c597ab14dfdb0b47912fd25d06f104f0af9cfb36fb58f30b` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_string_vec_generated_test.go` | generated test/support | `dfccfbf7b1b4eeabf4be4bdab05202e268cacebded3187c900ebb568b3f77c55` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_string_vec_test.go` | test/support | `5adcc265c8c66d6c8819f637f4cef755c1e9bc57a5d26407b0be99e161065a1a` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_test.go` | test/support | `1ff2a71da96d6ef83f3286d31257504c562622d8f7bfd632308c1721280962a3` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_threadsafe_generated.go` | generated production | `589e0395d5c32ea689be64a70b9cbb34165e64af01bca507088ba0ed44d2b1fa` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_threadunsafe_generated.go` | generated production | `b01a671f9cc40c66298d465e3f14eaad6b6095a75eab28170630e13a58a663ed` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_time.go` | production | `f11999a897166acf30c428da28c5daeb2924e1e26166664873d367b19f8a16e8` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_time_test.go` | test/support | `4b217a4e6b13e85024311cd1384bc54a58fad882cbfc3dc10080e78de2566d19` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_time_vec.go` | production | `8f0715d58ca72bdc8cb007bc778edfb557b2775a59f77cb09b3d3f60c5abd81f` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_time_vec_generated.go` | generated production | `ecf49005963c45d0d862000b57549e95f758b1b0172d8eef7579167be78fdef6` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_time_vec_generated_test.go` | generated test/support | `91fd76d35c7ffb5269d8ff635dc7521ef62848f59d3f19464a062c3a2c0a1dae` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_time_vec_test.go` | test/support | `1cc407af3bd45c508259840d00febd65090b266a411c9571c192c5c8a48bcceb` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_vec.go` | production | `16cb47277e28ba845cf72acd9ba4b33df3c4b2f73a434f5cf57908e69b6dc3a8` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_vec_vec.go` | generated production | `dd3a6aab9c79371db455d6eaaf71b16cc4c77bc9c385366b52a2d30c90a49da6` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_vec_vec_test.go` | test/support | `593d84fb4a8f7c336d09ad3f296834b07099503fd6f58859cdd83f1817021c6d` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_vectorized.go` | production | `521883b38fc1116ccd4770b46fb639e25bce4341dda937aa4a68e49412fc6b91` | Inventoried; whole-package audit open. |
| `pkg/expression/builtin_vectorized_test.go` | test/support | `e555642084b575f74c372c1ee03d8cd150eaf8c2776c2727302ad670256b0ad9` | Inventoried; whole-package audit open. |
| `pkg/expression/chunk_executor.go` | production | `8e460d41b8daa8da17a62dcbdf5429230599ef9803b7584a7bc04bc99e649f3f` | Inventoried; whole-package audit open. |
| `pkg/expression/collation.go` | production | `b0be069d8c4f3500f513a1430dadef59e661e0cb3f325e4d3a3c2cb14f3ba7a4` | Inventoried; whole-package audit open. |
| `pkg/expression/collation_test.go` | test/support | `2fd59237904f900ad8487985b0e43a3ed1e2c4126b158eb49df8372b3413de6a` | Inventoried; whole-package audit open. |
| `pkg/expression/column.go` | production | `639baae431869ee2cc6bb67f74429b2b72adeb1687d87aa421a5753b343bcf69` | Inventoried; whole-package audit open. |
| `pkg/expression/column_test.go` | test/support | `e57e168ef27ce90b985b6f8a33f7d264d58e39b98110ad931398ad3e95d9cd67` | Inventoried; whole-package audit open. |
| `pkg/expression/constant.go` | production | `e40ac69d175855a2f2ffb1f48af929425657972dcf6f47b964e54fa24e31be25` | Inventoried; whole-package audit open. |
| `pkg/expression/constant_fold.go` | production | `e4dfb72d3f0c64dbcc80e0c4bf116fe060f4964ce2421b6feea4af6e1058b276` | Inventoried; whole-package audit open. |
| `pkg/expression/constant_propagation.go` | production | `4581436be2a1fab166a01275174ee23e5d46b9c51a4f5840f923aecabc22d0b2` | Inventoried; whole-package audit open. |
| `pkg/expression/constant_test.go` | test/support | `6348cf14f940176cade2708c07de6a61ac14584715b5bc7f0445ea7b9ba9bb51` | Inventoried; whole-package audit open. |
| `pkg/expression/context.go` | production | `6443f614bf958d4c816c4f58b3b678650027534a100ec97f80daed1dec580d4a` | Inventoried; whole-package audit open. |
| `pkg/expression/distsql_builtin.go` | production | `2dfa1a31195a2db2f0cc4e0b3f463813116945eb148c06f035b8ad9e62ccfc45` | Inventoried; whole-package audit open. |
| `pkg/expression/distsql_builtin_test.go` | test/support | `8cf99adf0ede33319c55727fcc23ee75398f3cbf49ff0b117e171f38f6ef322c` | Inventoried; whole-package audit open. |
| `pkg/expression/errors.go` | production | `20b780015eb49f627b6a5b580ae554d5e3fb7fcac7ba89429f3c90f58d18dad7` | Inventoried; whole-package audit open. |
| `pkg/expression/evaluator.go` | production | `2cf1c14fcabcde3a0f28ad1cf7f107cae8361a4745e65314a819c33061808a43` | Inventoried; whole-package audit open. |
| `pkg/expression/evaluator_test.go` | test/support | `b9131f5ceda8c9bfa7a7495b9b8fff4b9635dbcfbfa1e86f0f79cd54b66cb385` | Inventoried; whole-package audit open. |
| `pkg/expression/explain.go` | production | `08ae29229c3aa09678a318ea56f6530ff4e4c51db353467ec9e7f61fb9153861` | Inventoried; whole-package audit open. |
| `pkg/expression/expr_to_pb.go` | production | `5f9aea8ebabfc1ce993a64817d9d43c01c5d80fc7ceba630a3623d76b2229c42` | Inventoried; whole-package audit open. |
| `pkg/expression/expr_to_pb_test.go` | test/support | `e03e21e0175014d938be6205a8a7b6f8b66333b36c2c06c2ab6a1d80a44e89d4` | Inventoried; whole-package audit open. |
| `pkg/expression/expression.go` | production | `63250c653a19851e54d3f3b70714c8ee63f65993ecad7cb9c1fdbbbcf93403dd` | Inventoried; whole-package audit open. |
| `pkg/expression/expression_test.go` | test/support | `f1d606a2fcf77690c68b913db2cca648b255e369db7310333bb7b6211f1ada4e` | Inventoried; whole-package audit open. |
| `pkg/expression/extension.go` | production | `631173513200151ee69b8648e93b9a4d743de45ff29769b1e66d96536f68c78a` | Inventoried; whole-package audit open. |
| `pkg/expression/fts_helper.go` | production | `284e1bcd4db9cb89d0b949720d63958185d971f73bb0a519e77a61acc30e62de` | Inventoried; whole-package audit open. |
| `pkg/expression/fts_to_like.go` | production | `afdbc4ded34beb43f8769ad27927a523eb72569bb41c6a66e90eb1ec464877ac` | Inventoried; whole-package audit open. |
| `pkg/expression/fts_to_like_test.go` | test/support | `aca2e0d0e1e5f7a4487209d225e65b4afad0828bbeb4b22efa0114c645b4ad0c` | Inventoried; whole-package audit open. |
| `pkg/expression/function_traits.go` | production | `71dca71f659cba18d816ffcaa5d79a7f9429a01f51da902f315b5c355935defc` | Inventoried; whole-package audit open. |
| `pkg/expression/function_traits_test.go` | test/support | `aacb0752e1933e650b3ca978f956d7aea590014fd0f04485f0921faae74036d9` | Inventoried; whole-package audit open. |
| `pkg/expression/grouping_sets.go` | production | `0ad9d728d3c337a7f779af4148321cb873c32d0dfc73255f8c77d5901d33890c` | Inventoried; whole-package audit open. |
| `pkg/expression/grouping_sets_test.go` | test/support | `613ead4c0cccc82555511a099b5dd61b47a513bd32f960bf555b0202352813eb` | Inventoried; whole-package audit open. |
| `pkg/expression/helper.go` | production | `29f07112cf35161e0c032f0ab08c153a4dd1c2830fe27c9991a4a5fe75a1e47f` | Inventoried; whole-package audit open. |
| `pkg/expression/helper_test.go` | test/support | `70f3eead9e6a83a2cc89832ccd6c8ed8bbac737e5b06baf0cf696090c5675166` | Inventoried; whole-package audit open. |
| `pkg/expression/infer_pushdown.go` | production | `f5f9c97ee653aca12116249131affd467a26ded1b9b24d98754e59cac4f570eb` | Inventoried; whole-package audit open. |
| `pkg/expression/main_test.go` | test/support | `1726b4fc074317ed91770be8b8e6613cb7682c566ad30484a32749b31910c116` | Inventoried; whole-package audit open. |
| `pkg/expression/scalar_function.go` | production | `5a677d0b2a4e2457c25948aceb20932c7a92e4722a02fc8196e6e21943e3ff9a` | Inventoried; whole-package audit open. |
| `pkg/expression/scalar_function_test.go` | test/support | `1712f4df758514aa42e0710ef8562c0ca8c0c43cec308337ada1971e09f8ce15` | Inventoried; whole-package audit open. |
| `pkg/expression/schema.go` | production | `faee921c27f0dd2dcbd22dbee738e74c51793ce001d12b45dd89257ea7e1a05d` | Inventoried; whole-package audit open. |
| `pkg/expression/schema_test.go` | test/support | `22474db3fe0c6cff1de225b7461f9083c43e6c0b60504b5c8ab14ab36f801743` | Inventoried; whole-package audit open. |
| `pkg/expression/simple_rewriter.go` | production | `8edbeb36933737cd6291ed5bcf98f530e10a7f75e0d3e328ec1f768b42502a7e` | Inventoried; whole-package audit open. |
| `pkg/expression/simple_rewriter_test.go` | test/support | `3890ba6af71f9843c6f7a3f28ea0e9411b8df64d66a47ab6a82d46f7f42da879` | Inventoried; whole-package audit open. |
| `pkg/expression/typeinfer_test.go` | test/support | `d993df8af68897162244202a825395f9f3c99a262dc30bd46ce66e8474611a21` | Inventoried; whole-package audit open. |
| `pkg/expression/util.go` | production | `16a7b11d8aa0be4b68c262378671ac1b3d675609205bf207f30ff3c32db0835b` | Inventoried; whole-package audit open. |
| `pkg/expression/util_test.go` | test/support | `eff9e21388ae027379183c754d34d16a383a1b68c253961e2ddf55578783310d` | Inventoried; whole-package audit open. |
| `pkg/expression/vectorized.go` | production | `a868fde67a93a9b74585712d829ecded8e95ef0635acc0ac40b41d7c35bef272` | Inventoried; whole-package audit open. |
| `pkg/expression/vs_helper.go` | production | `c8403a00548b70098db1e3077cfa5e123c952c96393875aafaff4b54203b7763` | Inventoried; whole-package audit open. |

Artifact counts: 2 build/ownership/support, 8 generated production, 5 generated test/support, 65 production, 53 test/support.

## Generation and module inputs

`builtin.go` invokes compare_vec.go, control_vec.go, other_vec.go,
string_vec.go, time_vec.go and builtin_threadsafe.go through go:generate.
Generator templates live in those sources and share generator/helper. The
ignore build tags on standalone generator programs are retained as inputs.
The existing builtin_vec_vec.go generated marker has no root go:generate
entry in this revision; its provenance/regeneration remains an explicit open
validation obligation. Imported packages and Bazel toolchain dependencies
require their own atomic audits; this table is not a transitive source claim.

| Input | SHA-256 |
| --- | --- |
| `pkg/expression/generator/BUILD.bazel` | `d4ac9b1dd48dc2c625db6b7b1a8ec93f0f9e55cbb7ea557e36836f9cb58666d0` |
| `pkg/expression/generator/builtin_threadsafe.go` | `059f20a49ba151f2634cf09963e84d8f1dda009ffaec4fa40b6209bad4b6463f` |
| `pkg/expression/generator/compare_vec.go` | `69f7753bcffa7aba62a708501f944b884031e84cf8554e2442250a7bfdefb217` |
| `pkg/expression/generator/control_vec.go` | `d6fc0ef6614445c6aff26dcfbaaec4b63c8216881bc23bec6a152d065d3e9dc0` |
| `pkg/expression/generator/helper/BUILD.bazel` | `1532fec1f57a3d3f342c45299e54e23d8b2b2a0219403e6fa736c86a2135956d` |
| `pkg/expression/generator/helper/helper.go` | `086696c249752e8aedbdcd5647d698a4b7fd1a6d47737997ab7fcad6f21a64b8` |
| `pkg/expression/generator/other_vec.go` | `15992d021f6350297195a2a4a9f9ce731547eba7b3b231e11ad0f922b1020023` |
| `pkg/expression/generator/string_vec.go` | `e54f761f267cfe798d609045a00570cbe520d6b2aa3cfd4421ce12a8ce1eda99` |
| `pkg/expression/generator/time_vec.go` | `5771b87a2b6d251a55211b5c777bc04ffcbee89ada22e0990913ab66f597bdd8` |
| `go.mod` | `a2f0229f01a3156b8ff95ef39b557236125435d91200429856ff81ed10c7b7ac` |
| `go.sum` | `833b6f2127500f40eb2d9e76bcd472869221c4ac93fdc1065f176c99568a879b` |

## Required acceptance evidence

Reconcile every production API, signature, context/default, scalar/vector
branch, platform/build variant and generated contract against native Rust.
Map every original test, benchmark and support artifact to runnable native
coverage without replacing source semantics with test-only substitutes.
The currently ignored native tests are unresolved obligations.

Before package acceptance, run original Go package tests with failpoints
enabled according to repository testing policy (the package imports failpoint),
verify generated outputs against their original inputs, run mapped native
package tests, and validate planner/executor/session consumers including SQL
values, types, errors, warnings, collation, NULL and vector evaluation order.
Run make lint for code changes. Preserve the pinned build manifests and record
exact commands, configurations and receipts. Separate mock harness probes are
useful evidence but do not replace these whole-package gates.

Performance acceptance additionally requires comparable sysbench, TPC-C, TPC-H
and YCSB runs against matching Go source and equivalent configuration. Existing
component benchmarks do not satisfy that requirement. Historical equal/fixed
rows in expr-builtin-divergence-inventory.md remain scoped evidence, not package
acceptance or permission to skip revalidation.


## Original Go reference validation, 2026-09-21


All 133 direct artifacts were rechecked against both the pinned Git objects
and the main checkout using SHA-256; no direct package file is omitted. The
complete original expression unit-test selection passes on darwin/arm64 with
Go 1.25.12, race detection and required failpoints. From the isolated checkout
root /private/tmp/tidb-parity-publish-aba629bb:

    GOTOOLCHAIN=go1.25.12 GOPROXY=off ./tools/check/failpoint-go-test.sh pkg/expression -race -count=1

The runner supplies -tags=intest,deadlock and cleans up failpoints on exit.
Result: PASS; package time 109.022s. Log:
/tmp/tidb-expression-original-go12512.log. Cleanup returns refcount zero and
leaves no Go-source diff. The cached Go 1.26.0 toolchain failed before test
execution in its ARM64 linker; the go.mod minimum version avoids that failure
without source changes. Earlier failed attempts and native/cache evidence are
recorded in the linked ExecPlan.

This closes this platform's original-Go reference test gate only. Native
production/test mapping, ignored native cases, generated/build/platform and
support-artifact gates, benchmarks and the four full workloads remain open.
The package is still unaccepted; no inventory row is promoted by this result.
