#![allow(dead_code)]

use std::convert::Infallible;
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
  --arangodb-password   Password for access to ArangoDB [default: '']
  --arangodb-jwt-secret File name with jwt secret [default: 'secret.jwt']
";

#[derive(Debug, Clone)]
pub struct GralArgs {
    pub use_tls: bool,
    pub use_auth: bool,
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

    let args = GralArgs {
        use_tls: pargs.opt_value_from_str("--use-tls")?.unwrap_or(true),
        use_auth: pargs.opt_value_from_str("--use-auth")?.unwrap_or(false),
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
        port: pargs.opt_value_from_str("--bind-port")?.unwrap_or(9999),
        arangodb_endpoints: pargs
            .opt_value_from_str("--arangodb-endpoints")?
            .unwrap_or("https://localhost:8529".into()),
        arangodb_username: pargs
            .opt_value_from_str("--arangodb-username")?
            .unwrap_or("root".into()),
        arangodb_password: pargs
            .opt_value_from_str("--arangodb-password")?
            .unwrap_or("".into()),
        arangodb_jwt_secret: pargs
            .opt_value_from_str("--arangodb-jwt-secret")?
            .unwrap_or("secret.jwt".into()),
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
