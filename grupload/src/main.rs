#![allow(dead_code)]

use std::cmp::min;

mod commands;

const HELP: &str = "\
grupload

USAGE:
  grupload [OPTIONS] COMMAND

FLAGS:
  -h, --help               Prints help information
                          
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
                          
OPTIONS:                  
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
        command: pargs.opt_free_from_str()?.unwrap_or("empty".into()),
    };

    args.key_size = min(args.key_size, 64);

    // It's up to the caller what to do with the remaining arguments.
    let remaining = pargs.finish();
    if !remaining.is_empty() {
        eprintln!("Warning: unused arguments left: {:?}.", remaining);
    }

    Ok(args)
}
