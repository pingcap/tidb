// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::path::PathBuf;

use clap::crate_version;
use clap::App;
use clap::Arg;

pub struct CommandArgs {
    pub pd: Vec<String>,
    pub ca: Option<PathBuf>,
    pub cert: Option<PathBuf>,
    pub key: Option<PathBuf>,
}

pub fn parse_args(app_name: &str) -> CommandArgs {
    let matches = App::new(app_name)
        .version(crate_version!())
        .author("The TiKV Project Authors")
        .arg(
            Arg::with_name("pd")
                .long("pd")
                .aliases(&["pd-endpoint", "pd-endpoints"])
                .value_name("PD_URL")
                .help("Sets PD endpoints")
                .long_help("Sets PD endpoints. Uses `,` to separate multiple PDs")
                .takes_value(true)
                .multiple(true)
                .value_delimiter(",")
                .required(false)
                .default_value("localhost:2379"),
        )
        // A cyclic dependency between CA, cert and key is made
        // to ensure that no security options are missing.
        .arg(
            Arg::with_name("ca")
                .long("ca")
                .value_name("CA_PATH")
                .help("Sets the CA")
                .long_help("Sets the CA. Must be used with --cert and --key")
                .takes_value(true)
                .requires("cert"),
        )
        .arg(
            Arg::with_name("cert")
                .long("cert")
                .value_name("CERT_PATH")
                .help("Sets the certificate")
                .long_help("Sets the certificate. Must be used with --ca and --key")
                .takes_value(true)
                .requires("key"),
        )
        .arg(
            Arg::with_name("key")
                .long("key")
                .alias("private-key")
                .value_name("KEY_PATH")
                .help("Sets the private key")
                .long_help("Sets the private key. Must be used with --ca and --cert")
                .takes_value(true)
                .requires("ca"),
        )
        .get_matches();

    CommandArgs {
        pd: matches.values_of("pd").unwrap().map(String::from).collect(),
        ca: matches.value_of("ca").map(PathBuf::from),
        cert: matches.value_of("cert").map(PathBuf::from),
        key: matches.value_of("key").map(PathBuf::from),
    }
}
