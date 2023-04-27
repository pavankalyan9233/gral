#![allow(dead_code)]

const HELP: &str = "\
gral

USAGE:
  gral [OPTIONS]

FLAGS:
  -h, --help            Prints help information

OPTIONS:
  --bind-address ADDR   Network address for bind [default: '0.0.0.0']
  --bind-port    PORT   Network port foro bind [default: 9999]
";

#[derive(Debug)]
pub struct GralArgs {
    pub bind_addr: String,
    pub port: u16,
}

pub fn parse_args() -> Result<GralArgs, pico_args::Error> {
    let mut pargs = pico_args::Arguments::from_env();

    // Help has a higher priority and should be handled separately.
    if pargs.contains(["-h", "--help"]) {
        print!("{}", HELP);
        std::process::exit(0);
    }

    let args = GralArgs {
        bind_addr: pargs
            .opt_value_from_str("--bind-address")?
            .unwrap_or("0.0.0.0".into()),
        port: pargs.opt_value_from_str("--bind-port")?.unwrap_or(9999),
    };

    // It's up to the caller what to do with the remaining arguments.
    let remaining = pargs.finish();
    if !remaining.is_empty() {
        eprintln!("Warning: unused arguments left: {:?}.", remaining);
    }

    Ok(args)
}
