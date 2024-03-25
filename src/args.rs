#![allow(dead_code)]

use log::{info, warn};
use std::convert::Infallible;
use std::env::VarError;
use std::ffi::OsString;
use std::fs::File;
use std::io::prelude::*;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use warp::Filter;

const HELP: &str = "\
gral

USAGE:
  gral [OPTIONS]

OPTIONS:
  -h, --help             Prints help information
  --use-tls BOOL         Use TLS or not [default: true]
  --use-tls-auth BOOL    Use TLS client cert authentification [default: false]
  --cert PATH            Path to server certificate [default: 'tls/cert.pem']
  --key PATH             Path to server secret key [default: 'tls/key.pem']
  --authca PATH          Path to CA certificate for client authentication
                         [default: 'tls/authca.pem']
  --bind-address ADDR    Network address for bind [default: '0.0.0.0']
  --bind-port PORT       Network port for bind [default: 9999]
  --arangodb-endpoints   Network endpoints for ArangoDB deployment (multiple,
                         separated by commas are possible)
                         [default: 'https://localhost:8529']
  --authentication BOOL  Check authentication [default: true]
  --arangodb-user USER   ArangoDB user to fall back to without authentication
  --arangodb-jwt-secrets Path name with jwt secrets [default: 'secrets.jwt']
  --auth-service ADDR    Hostname and port of authentication service
                         [default: '']
  --warp-trace BOOL      Switch on warp tracing [default: false]

The following environment variables can set defaults for the above
options (command line options have higher precedence!):

  HTTP_PORT                   Sets the default for --bind-port
  ARANGODB_ENDPOINT           Sets the default for --arangodb-endpoints
  ARANGODB_JWT                Sets the default path for --arangodb-jwt-secrets
  ARANGODB_CA_CERTS           Sets the path with 'ca.crt' for --cert and
                              sets the path with 'ca.key' for --key
  ARANGODB_USER               Sets user in --arangodb-user
  INTEGRATION_SERVICE_ADDRESS Sets the address for --auth-service
";

#[derive(Debug, Clone)]
pub struct GralArgs {
    pub use_tls: bool,
    pub use_auth: bool,
    pub cert: PathBuf,
    pub key: std::path::PathBuf,
    pub authca: std::path::PathBuf,
    pub bind_addr: String,
    pub port: u16,
    pub arangodb_endpoints: String,
    pub authentication: bool,
    pub arangodb_user: String,
    pub auth_service: String,
    pub arangodb_jwt_secrets: Vec<Vec<u8>>, // the first used for signing
    // all for signature verification
    pub warp_trace: bool,
}

fn read_jwt_secrets(jwt_path: &String) -> Vec<Vec<u8>> {
    let mut path: PathBuf = jwt_path.into();
    let e = std::fs::read_dir(&path);
    if let Err(e) = e {
        warn!("Path to JWT secrets is not readable: {jwt_path}, error: {e:?}!");
        return vec![];
    }
    let rd = e.unwrap(); // Unwrap ReadDir struct
    let mut secrets: Vec<Vec<u8>> = Vec::new();
    let mut use_to_sign: usize = 0;
    for de in rd {
        if let Ok(de) = de {
            path.push(de.file_name());
            match File::open(&path) {
                Err(e) => {
                    warn!("Could not read JWT secret from file '{path:?}, error: {e:?}");
                }
                Ok(mut file) => {
                    let mut buf: Vec<u8> = Vec::with_capacity(256);
                    match file.read_to_end(&mut buf) {
                        Err(e) => {
                            warn!("Could not read JWT secret from file '{path:?}, error: {e:?}");
                        }
                        Ok(len) => {
                            if len != 0 {
                                if de.file_name() == *"token" {
                                    use_to_sign = secrets.len();
                                }
                                secrets.push(buf);
                            }
                        }
                    }
                }
            }
            path.pop();
        }
    }
    if use_to_sign != 0 && !secrets.is_empty() {
        secrets.swap(0, use_to_sign);
    }
    secrets
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
    let default_endpoint = my_get_env("ARANGODB_ENDPOINT", "https://localhost:8529");
    let default_jwt_path = my_get_env("ARANGODB_JWT", "./secrets.jwt");
    let default_keypath = my_get_env("ARANGODB_CA_CERTS", "./tls");
    let jwt_path = pargs
        .opt_value_from_str("--arangodb-jwt-secrets")?
        .unwrap_or(default_jwt_path);
    let default_arangodb_user = my_get_env("ARANGODB_USER", "root");
    let default_auth_service = my_get_env("INTEGRATION_SERVICE_ADDRESS", "");

    // Read the JWT secrets from files, empty results if this fails:
    let jwt_secrets: Vec<Vec<u8>> = read_jwt_secrets(&jwt_path);

    let args = GralArgs {
        use_tls: pargs.opt_value_from_str("--use-tls")?.unwrap_or(false),
        use_auth: pargs.opt_value_from_str("--use-auth")?.unwrap_or(false),
        cert: pargs
            .opt_value_from_str("--cert")?
            .unwrap_or((default_keypath.clone() + "/ca.crt").into()),
        key: pargs
            .opt_value_from_str("--key")?
            .unwrap_or((default_keypath.clone() + "/ca.key").into()),
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
        authentication: pargs
            .opt_value_from_str("--authentication")?
            .unwrap_or(true),
        arangodb_user: pargs
            .opt_value_from_str("--arangodb-user")?
            .unwrap_or(default_arangodb_user),
        arangodb_jwt_secrets: jwt_secrets,
        auth_service: pargs
            .opt_value_from_str("--auth-service")?
            .unwrap_or(default_auth_service),
        warp_trace: pargs.opt_value_from_str("--warp-trace")?.unwrap_or(false),
    };

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
