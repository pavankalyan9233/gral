#![allow(dead_code)]

use reqwest::{Certificate, Identity};
use std::cmp::min;
use std::fs::File;
use std::io::Read;
use std::sync::Arc;

mod commands;

const HELP: &str = "\
grupload

USAGE:
  grupload COMMAND [OPTIONS]

COMMANDS:                 
  create                   create a graph
  vertices                 upload vertices and seal them
  edges                    upload edges and seal them
  dropGraph                drop a graph
  upload                   create, upload and seal, return number
  randomize                create a random graph with max-vertices vertices
                           and max-edges edges
  compute                  trigger a computation
  progress                 get progress information
  vertexresults            dump results of a computation for all vertices
  dropcomp                 drops a computation
  shutdown                 shut down server
  version                  ask for server version
                          
OPTIONS:                  
  -h, --help               Prints help information
  --max-vertices NR        Maximal number of vertices (only for create)
                           [default: 1000000]
  --max-edges NR           Maximal number of edges (only for create)
                           [default: 1000000]
  --store-keys BOOL        Flag if gral should store the keys [default: true]
  --graph GRAPHNUMBER      Number of graph to use [default: 0]
  --vertices FILENAME      Vertex input file (jsonl)
                           [default: 'vertices.jsonl']
  --edges FILENAME         Edge input file (jsonl) [default: 'edges.jsonl']
  --endpoint ENDPOINT      gral endpoint to send data to
                           [default: 'https://localhost:9999']
  --key-size NR            Size of keys in bytes in `randomize` [default: 32]
  --vertex-coll-name NAME  Name of the vertex collection (relevant for 
                           `randomize`) [default: 'V']
  --threads NR             Number of threads to use [default: 1]
  --algorithm NAME         Name of algorithm to trigger [default: 'wcc']
  --comp-id ID             Computation id [default: 0]
  --output FILENAME        Output file for data dump [default: 'output.jsonl']
  --index-edges INTEGER    Flags, if gral should index the edges when they are
                           sealed. 1-bit is indexing by from, 2-bit is indexing
                           by to. If not, this is done lazily later when a
                           computation needs the edge index [default: false]
  --use-tls BOOL           Flag if TLS should be used [default: true]
  --cacert PATH            Path to CA certificate for TLS
                           [default: 'tls/ca.pem']
  --client-identity PATH   Path to client certificate and private key as PEM
                           [default: 'tls/client-keyfile.pem']
";

#[derive(Debug)]
pub struct TLSClientInfo {
    cacert: Vec<u8>,
    client_identity: Vec<u8>,
}

#[derive(Debug)]
pub struct GruploadArgs {
    command: String,
    store_keys: bool,
    max_vertices: u64,
    max_edges: u64,
    graph_number: u32,
    vertex_file: std::path::PathBuf,
    edge_file: std::path::PathBuf,
    endpoint: String,
    key_size: u32,
    vertex_coll_name: String,
    nr_threads: u32,
    algorithm: String,
    comp_id: u64,
    output_file: std::path::PathBuf,
    index_edges: u32,
    use_tls: bool,
    cacert_filename: std::path::PathBuf,
    client_identity_filename: std::path::PathBuf,
    identity: Arc<TLSClientInfo>,
}

fn upload(args: &mut GruploadArgs) -> Result<(), String> {
    crate::commands::create(args)?;
    crate::commands::vertices(args)?;
    crate::commands::seal_vertices(args)?;
    crate::commands::edges(args)?;
    crate::commands::seal_edges(args)?;
    Ok(())
}

fn main() {
    let mut args = match parse_args() {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Error: {}.", e);
            std::process::exit(1);
        }
    };

    if args.command == "empty" {
        eprintln!("Error: no command given, see --help for a list of commands!!");
        std::process::exit(2);
    }

    let r = match args.command.as_str() {
        "create" => crate::commands::create(&mut args),
        "vertices" => crate::commands::vertices(&args),
        "sealVertices" => crate::commands::seal_vertices(&args),
        "edges" => crate::commands::edges(&args),
        "sealEdges" => crate::commands::seal_edges(&args),
        "dropGraph" => crate::commands::drop_graph(&args),
        "upload" => upload(&mut args),
        "randomize" => crate::commands::randomize(&args),
        "compute" => crate::commands::compute(&args),
        "progress" => crate::commands::progress(&args),
        "vertexresults" => crate::commands::vertex_results(&args),
        "dropcomp" => crate::commands::drop_computation(&args),
        "shutdown" => crate::commands::shutdown(&args),
        "version" => crate::commands::version(&args),
        _ => Err(format!("Command {} not implemented.", args.command)),
    };
    if let Err(s) = r {
        eprintln!("Error: {}", s);
    }
}

fn parse_args() -> Result<GruploadArgs, pico_args::Error> {
    let mut pargs = pico_args::Arguments::from_env();

    // Help has a higher priority and should be handled separately.
    if pargs.contains(["-h", "--help"]) {
        print!("{}", HELP);
        std::process::exit(0);
    }

    let mut args = GruploadArgs {
        store_keys: pargs.opt_value_from_str("--store-keys")?.unwrap_or(true),
        max_vertices: pargs
            .opt_value_from_str("--max-vertices")?
            .unwrap_or(1000000),
        max_edges: pargs.opt_value_from_str("--max-edges")?.unwrap_or(1000000),
        graph_number: pargs.opt_value_from_str("--graph")?.unwrap_or(0),
        vertex_file: pargs
            .opt_value_from_str("--vertices")?
            .unwrap_or("vertices.jsonl".into()),
        edge_file: pargs
            .opt_value_from_str("--edges")?
            .unwrap_or("edges.jsonl".into()),
        endpoint: pargs
            .opt_value_from_str("--endpoint")?
            .unwrap_or("https://localhost:9999".into()),
        key_size: pargs.opt_value_from_str("--key-size")?.unwrap_or(32),
        vertex_coll_name: pargs
            .opt_value_from_str("--vertex-coll-name")?
            .unwrap_or("V".into()),
        nr_threads: pargs.opt_value_from_str("--threads")?.unwrap_or(1),
        algorithm: pargs
            .opt_value_from_str("--algorithm")?
            .unwrap_or("wcc".into()),
        comp_id: pargs.opt_value_from_str("--comp-id")?.unwrap_or(0),
        output_file: pargs
            .opt_value_from_str("--output")?
            .unwrap_or("output_jsonl".into()),
        index_edges: pargs.opt_value_from_str("--index-edges")?.unwrap_or(0),
        use_tls: pargs.opt_value_from_str("--use-tls")?.unwrap_or(true),
        cacert_filename: pargs
            .opt_value_from_str("--cacert")?
            .unwrap_or("tls/ca.pem".into()),
        client_identity_filename: pargs
            .opt_value_from_str("--client-identity")?
            .unwrap_or("tls/client-keyfile.pem".into()),
        identity: Arc::new(TLSClientInfo {
            cacert: vec![],
            client_identity: vec![],
        }),
        command: pargs.opt_free_from_str()?.unwrap_or("empty".into()),
    };

    args.key_size = min(args.key_size, 64);

    // It's up to the caller what to do with the remaining arguments.
    let remaining = pargs.finish();
    if !remaining.is_empty() {
        eprintln!("Warning: unused arguments left: {:?}.", remaining);
    }

    if args.use_tls {
        let mut certbuf: Vec<u8> = vec![];
        let cafile = File::open(&args.cacert_filename);
        match cafile {
            Err(err) => {
                eprintln!(
                    "Cannot open cacert file {}: {:?}",
                    args.cacert_filename.to_string_lossy(),
                    err
                );
                std::process::exit(2);
            }
            Ok(mut f) => {
                let r = f.read_to_end(&mut certbuf);
                if let Err(err) = r {
                    eprintln!(
                        "Cannot read cacert file {}: {:?}",
                        args.cacert_filename.to_string_lossy(),
                        err
                    );
                    std::process::exit(3);
                }
            }
        }

        let certificate = Certificate::from_pem(&certbuf);
        if let Err(err) = certificate {
            eprintln!(
                "Cannot parse cacert file {}: {:?}",
                args.cacert_filename.to_string_lossy(),
                err
            );
        }
        // TLS clients will reparse the cacert file and thus rebuild the
        // certificate object.

        let mut identitybuf: Vec<u8> = vec![];
        let keyfile = File::open(&args.client_identity_filename);
        match keyfile {
            Err(err) => {
                eprintln!(
                    "Cannot open client identity file {}: {:?}",
                    args.client_identity_filename.to_string_lossy(),
                    err
                );
                std::process::exit(4);
            }
            Ok(mut f) => {
                let r = f.read_to_end(&mut identitybuf);
                if let Err(err) = r {
                    eprintln!(
                        "Cannot read client identity file {}: {:?}",
                        args.client_identity_filename.to_string_lossy(),
                        err
                    );
                    std::process::exit(5);
                }
            }
        }

        let id = Identity::from_pem(&identitybuf);
        if let Err(err) = id {
            eprintln!(
                "Cannot parse client cert and key from file {}: {:?}",
                args.client_identity_filename.to_string_lossy(),
                err
            );
            std::process::exit(6);
        }
        // TLS clients will reparse the identity file and thus rebuild the identity
        args.identity = Arc::new(TLSClientInfo {
            cacert: certbuf,
            client_identity: identitybuf,
        });
    }
    Ok(args)
}
