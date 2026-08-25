// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

fn main() {
    let mut prost = prost_build::Config::new();
    prost.enable_type_names();

    tonic_prost_build::configure()
        .emit_rerun_if_changed(false)
        .build_server(true)
        .server_mod_attribute(".", "#[allow(non_camel_case_types)]")
        .include_file("mod.rs")
        .out_dir("src/generated")
        .file_descriptor_set_path("src/generated/file_descriptor_set.bin")
        .compile_with_config(
            prost,
            &glob::glob("proto/*.proto")
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            &["proto/include".into(), "proto".into()],
        )
        .unwrap();
}
