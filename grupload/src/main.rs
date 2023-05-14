#![allow(dead_code)]

use reqwest::{Certificate, Identity};
use std::cmp::min;
use std::fs::File;
use std::io::Read;

mod commands;

const HELP: &str = "\
grupload

USAGE:
  grupload COMMAND [OPTIONS]

COMMANDS:                 
  create                   create a graph
  vertices                 upload vertices and seal them
  edges                    upload edges and seal them
  drop                     drop a graph
  upload                   create, upload and seal, return number
  randomize                create a random graph with max-vertices vertices
                           and max-edges edges
  compute                  trigger a computation
  progress                 get progress information
  vertexresults            dump results of a computation for all vertices
  dropcomp                 drops a computation
                          
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
                           [default: 'http://localhost:9999']
  --key-size NR            Size of keys in bytes in `randomize` [default: 32]
  --vertex-coll-name NAME  Name of the vertex collection (relevant for 
                           `randomize`) [default: 'V']
  --threads NR             Number of threads to use [default: 1]
  --algorithm NAME         Name of algorithm to trigger [default: 'wcc']
  --comp-id ID             Computation id [default: 0]
  --output FILENAME        Output file for data dump [default: 'output.jsonl']
  --index-edges BOOL       Flag, if gral should index the edges when they
                           are sealed. If not, this is done lazily later when
                           a computation needs the edge index [default: false]
  --use-tls BOOL           Flag if TLS should be used [default: true]
  --cacert PATH            Path to CA certificate for TLS
                           [default: 'tls/ca.pem']
  --client-keyfile PATH    Path to client certificate for authentication
                           [default: 'tls/client-keyfile.pem']
";

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
    index_edges: bool,
    use_tls: bool,
    cacert_filename: std::path::PathBuf,
    cacert: Vec<u8>,
    client_keyfile_filename: std::path::PathBuf,
    client_keyfile: Vec<u8>,
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

    println!("{:#?}", args);

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
            .unwrap_or("http://localhost:9999".into()),
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
        index_edges: pargs.opt_value_from_str("--index-edges")?.unwrap_or(false),
        use_tls: pargs.opt_value_from_str("--use-tls")?.unwrap_or(true),
        cacert_filename: pargs
            .opt_value_from_str("--cacert")?
            .unwrap_or("tls/ca.pem".into()),
        cacert: vec![],
        client_keyfile_filename: pargs
            .opt_value_from_str("--client-keyfile")?
            .unwrap_or("tls/client-keyfile.pem".into()),
        client_keyfile: vec![],
        command: pargs.opt_free_from_str()?.unwrap_or("empty".into()),
    };

    args.key_size = min(args.key_size, 64);

    // It's up to the caller what to do with the remaining arguments.
    let remaining = pargs.finish();
    if !remaining.is_empty() {
        eprintln!("Warning: unused arguments left: {:?}.", remaining);
    }

    if args.use_tls {
        let file = File::open(&args.cacert_filename);
        match file {
            Err(err) => {
                eprintln!(
                    "Cannot open cacert file {}: {:?}",
                    args.cacert_filename.to_string_lossy(),
                    err
                );
                std::process::exit(2);
            }
            Ok(mut f) => {
                let r = f.read_to_end(&mut args.cacert);
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

        let certificate = Certificate::from_pem(&args.cacert);
        if let Err(err) = certificate {
            eprintln!(
                "Cannot parse cacert file {}: {:?}",
                args.cacert_filename.to_string_lossy(),
                err
            );
        }
        // TLS clients will reparse the cacert file and thus rebuild the
        // certificate object.

        let file2 = File::open(&args.client_keyfile_filename);
        match file2 {
            Err(err) => {
                eprintln!(
                    "Cannot open client keyfile {}: {:?}",
                    args.client_keyfile_filename.to_string_lossy(),
                    err
                );
                std::process::exit(4);
            }
            Ok(mut f) => {
                let r = f.read_to_end(&mut args.client_keyfile);
                if let Err(err) = r {
                    eprintln!(
                        "Cannot read client keyfile {}: {:?}",
                        args.client_keyfile_filename.to_string_lossy(),
                        err
                    );
                    std::process::exit(3);
                }
            }
        }

        let id = Identity::from_pem(&args.client_keyfile);
        if let Err(err) = id {
            eprintln!(
                "Cannot parse client keyfile {}: {:?}",
                args.client_keyfile_filename.to_string_lossy(),
                err
            );
        }
        // TLS clients will reparse the keyfile and thus rebuild the identity
    }
    Ok(args)
}
