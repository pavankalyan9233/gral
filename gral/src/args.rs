#![allow(dead_code)]

use log::{info, warn};
use std::convert::Infallible;
use std::env::VarError;
use std::fs::File;
use std::io::prelude::*;
use std::sync::{Arc, Mutex};
use warp::Filter;

const HELP: &str = "\
gral

USAGE:
  gral [OPTIONS]

OPTIONS:
  -h, --help            Prints help information
  --use-tls BOOL        Use TLS or not [default: true]
  --use-auth BOOL       Use TLS client cert authentification [default: false]
  --cert PATH           Path to server certificate [default: 'tls/cert.pem']
  --key PATH            Path to server secret key [default: 'tls/key.pem']
  --authca PATH         Path to CA certificate for client authentication
                        [default: 'tls/authca.pem']
  --bind-address ADDR   Network address for bind [default: '0.0.0.0']
  --bind-port PORT      Network port for bind [default: 9999]
  --arangodb-endpoints  Network endpoints for ArangoDB deployment (multiple,
                        separated by commas are possible)
                        [default: 'https://localhost:8529']
  --arangodb-username   Username for access to ArangoDB [default: 'root']
  --arangodb-pw-path    Path for file with password for access to ArangoDB
                        [default: 'secret.password']
  --arangodb-jwt-secret Path name with jwt secret [default: 'secret.jwt']
";

#[derive(Debug, Clone)]
pub struct GralArgs {
    pub use_tls: bool,
    pub use_auth: bool,
    pub keyfile: std::path::PathBuf,
    pub cert: std::path::PathBuf,
    pub key: std::path::PathBuf,
    pub authca: std::path::PathBuf,
    pub bind_addr: String,
    pub port: u16,
    pub arangodb_endpoints: String,
    pub arangodb_username: String,
    pub arangodb_password: String,
    pub arangodb_jwt_secret: String,
}

pub fn parse_args() -> Result<GralArgs, pico_args::Error> {
    let mut pargs = pico_args::Arguments::from_env();

    // Help has a higher priority and should be handled separately.
    if pargs.contains(["-h", "--help"]) {
        print!("{}", HELP);
        std::process::exit(0);
    }

    let my_get_env = |name: &str, default: &str| -> String {
        let from_env = std::env::var(name);
        match from_env {
            Err(VarError::NotPresent) => default.to_string(),
            Err(VarError::NotUnicode(e)) => {
                warn!(
                    "{} environment variable does not contain unicode: {:?}, using {}",
                    name, e, default
                );
                default.to_string()
            }
            Ok(s) => {
                info!("Using value {} from environment variable {}.", s, name);
                s
            }
        }
    };

    // First get some default values from the environment:
    let def_port: u16 = 9999;
    let default_port_str = my_get_env("HTTP_PORT", "9999");
    let default_port_res = str::parse::<u16>(&default_port_str);
    let default_port = match default_port_res {
        Ok(n) => n,
        Err(e) => {
            warn!(
                "HTTP_PORT environment variable {} cannot be parsed as integer: {}, using {}",
                default_port_str, e, def_port
            );
            def_port
        }
    };
    let default_user = my_get_env("ARANGODB_USER", "root");
    let default_endpoint = my_get_env("ARANGODB_ENDPOINT", "https://localhost:8529");
    let default_jwt_path = my_get_env("ARANGODB_JWT", "");
    let default_passwd_path = my_get_env("ARANGODB_PASSWORD_FILE", "secret.password");
    let default_keyfile = my_get_env("ARANGODB_CA_CERTS", "tls/keyfile.pem");

    let mut args = GralArgs {
        use_tls: pargs.opt_value_from_str("--use-tls")?.unwrap_or(true),
        use_auth: pargs.opt_value_from_str("--use-auth")?.unwrap_or(false),
        keyfile: pargs
            .opt_value_from_str("--keyfile")?
            .unwrap_or(default_keyfile.into()),
        cert: pargs
            .opt_value_from_str("--cert")?
            .unwrap_or("tls/cert.pem".into()),
        key: pargs
            .opt_value_from_str("--key")?
            .unwrap_or("tls/key.pem".into()),
        authca: pargs
            .opt_value_from_str("--authca")?
            .unwrap_or("tls/authca.pem".into()),
        bind_addr: pargs
            .opt_value_from_str("--bind-address")?
            .unwrap_or("0.0.0.0".into()),
        port: pargs
            .opt_value_from_str("--bind-port")?
            .unwrap_or(default_port),
        arangodb_endpoints: pargs
            .opt_value_from_str("--arangodb-endpoints")?
            .unwrap_or(default_endpoint),
        arangodb_username: pargs
            .opt_value_from_str("--arangodb-username")?
            .unwrap_or(default_user),
        arangodb_password: pargs
            .opt_value_from_str("--arangodb-password")?
            .unwrap_or(default_passwd_path),
        arangodb_jwt_secret: pargs
            .opt_value_from_str("--arangodb-jwt-secret")?
            .unwrap_or(default_jwt_path),
    };

    // Now read the password from file, if it exists:
    let file = File::open(args.arangodb_password.clone());
    if let Err(e) = file {
        warn!(
            "Could not read password file {}: {:?}",
            args.arangodb_password, e
        );
        args.arangodb_password = "".to_string();
    } else {
        let mut file = file.unwrap();
        let mut content: String = "".to_string();
        let err = file.read_to_string(&mut content);
        if let Err(e) = err {
            warn!(
                "Could not read password file {}: {:?}",
                &args.arangodb_password, e
            );
            args.arangodb_password = "".to_string();
        } else {
            args.arangodb_password = content;
        }
    }

    // It's up to the caller what to do with the remaining arguments.
    let remaining = pargs.finish();
    if !remaining.is_empty() {
        eprintln!("Warning: unused arguments left: {:?}.", remaining);
    }

    Ok(args)
}

pub fn with_args(
    args: Arc<Mutex<GralArgs>>,
) -> impl Filter<Extract = (Arc<Mutex<GralArgs>>,), Error = Infallible> + Clone {
    warp::any().map(move || args.clone())
}
