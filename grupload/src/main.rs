#![allow(dead_code)]

mod commands;

const HELP: &str = "\
grupload

USAGE:
  grupload [OPTIONS] COMMAND

FLAGS:
  -h, --help            Prints help information

COMMANDS:
  create                create a graph
  vertices              upload vertices and seal them
  edges                 upload edges and seal them
  drop                  drop a graph
  upload                create, upload and seal, return number

OPTIONS:
  --max-vertices NR     Maximal number of vertices (only for create) [default: 1000000]
  --max-edges NR        Maximal number of edges (only for create) [default: 1000000]
  --store-keys BOOL     Flag if gral should store the keys [default: true]
  --graph GRAPHNUMBER   Number of graph to use [default: 0]
  --vertices FILENAME   Vertex input file (jsonl) [default: 'vertices.jsonl']
  --edges FILENAME      Edge input file (jsonl) [default: 'edges.jsonl']
  --endpoint ENDPOINT   gral endpoint to send data to
                        [default: 'http://localhost:9999']
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
}

fn main() {
    let args = match parse_args() {
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

    match args.command.as_str() {
        "create" => crate::commands::create(&args),
        _ => {
            eprintln!("Error: command {} not yet implemented!", args.command);
        }
    }
}

fn parse_args() -> Result<GruploadArgs, pico_args::Error> {
    let mut pargs = pico_args::Arguments::from_env();

    // Help has a higher priority and should be handled separately.
    if pargs.contains(["-h", "--help"]) {
        print!("{}", HELP);
        std::process::exit(0);
    }

    let args = GruploadArgs {
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
        command: pargs.opt_free_from_str()?.unwrap_or("empty".into()),
    };

    // It's up to the caller what to do with the remaining arguments.
    let remaining = pargs.finish();
    if !remaining.is_empty() {
        eprintln!("Warning: unused arguments left: {:?}.", remaining);
    }

    Ok(args)
}
