use crate::arangodb::fetch_graph_from_arangodb;
use crate::args::{with_args, GralArgs};
use crate::computations::{with_computations, Computation, Computations};
use crate::conncomp::{strongly_connected_components, weakly_connected_components};
use crate::graphs::{with_graphs, Graph, Graphs, KeyOrHash, VertexHash, VertexIndex};
use crate::VERSION;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use bytes::Bytes;
use graphanalyticsengine::*;
use http::Error;
use log::info;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::io::{BufRead, Cursor};
use std::ops::Deref;
use std::str;
use std::sync::{Arc, Mutex, RwLock};
use warp::{http::Response, http::StatusCode, reject, Filter, Rejection};
use xxhash_rust::xxh3::xxh3_64_with_seed;

pub mod graphanalyticsengine {
    include!(concat!(
        env!("OUT_DIR"),
        "/arangodb.cloud.internal.graphanalytics.v1.rs"
    ));
}

/// The following function puts together the filters for the API.
/// To this end, it relies on the following async functions below.
pub fn api_filter(
    graphs: Arc<Mutex<Graphs>>,
    computations: Arc<Mutex<Computations>>,
    args: Arc<Mutex<GralArgs>>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    let version_bin = warp::path!("v1" / "versionBinary")
        .and(warp::get())
        .map(version_bin);
    let version = warp::path!("v1" / "version")
        .and(warp::get())
        .map(version_json);
    let create = warp::path!("v1" / "create")
        .and(warp::post())
        .and(with_graphs(graphs.clone()))
        .and(warp::body::bytes())
        .and_then(api_create);
    let drop_bin = warp::path!("v1" / "dropGraphBinary")
        .and(warp::put())
        .and(with_graphs(graphs.clone()))
        .and(warp::body::bytes())
        .and_then(api_drop_bin);
    let vertices = warp::path!("v1" / "vertices")
        .and(warp::post())
        .and(with_graphs(graphs.clone()))
        .and(warp::body::bytes())
        .and_then(api_vertices);
    let seal_vertices = warp::path!("v1" / "sealVertices")
        .and(warp::post())
        .and(with_graphs(graphs.clone()))
        .and(warp::body::bytes())
        .and_then(api_seal_vertices);
    let edges = warp::path!("v1" / "edges")
        .and(warp::post())
        .and(with_graphs(graphs.clone()))
        .and(warp::body::bytes())
        .and_then(api_edges);
    let seal_edges = warp::path!("v1" / "sealEdges")
        .and(warp::post())
        .and(with_graphs(graphs.clone()))
        .and(warp::body::bytes())
        .and_then(api_seal_edges);
    let get_progress_bin = warp::path!("v1" / "getProgress")
        .and(warp::put())
        .and(with_computations(computations.clone()))
        .and(warp::body::bytes())
        .and_then(api_get_progress_bin);
    let get_progress =
        warp::path!("api" / "graphanalytics" / "v1" / "engines" / String / "jobs" / String)
            .and(warp::get())
            .and(with_computations(computations.clone()))
            .and_then(api_get_progress);
    let get_results_by_vertices = warp::path!("v1" / "getResultsByVertices")
        .and(warp::put())
        .and(with_computations(computations.clone()))
        .and(warp::body::bytes())
        .and_then(api_get_results_by_vertices);
    let drop_computation_bin = warp::path!("v1" / "dropComputationBinary")
        .and(warp::put())
        .and(with_computations(computations.clone()))
        .and(warp::body::bytes())
        .and_then(api_drop_computation_bin);
    let drop_computation =
        warp::path!("api" / "graphanalytics" / "v1" / "engines" / String / "jobs" / String)
            .and(warp::delete())
            .and(with_computations(computations.clone()))
            .and_then(api_drop_computation);
    let compute_bin = warp::path!("v1" / "compute-binary")
        .and(warp::post())
        .and(with_graphs(graphs.clone()))
        .and(with_computations(computations.clone()))
        .and(warp::body::bytes())
        .and_then(api_compute_bin);
    let compute = warp::path!("api" / "graphanalytics" / "v1" / "engines" / String / "process")
        .and(warp::post())
        .and(with_graphs(graphs.clone()))
        .and(with_computations(computations.clone()))
        .and(warp::body::bytes())
        .and_then(api_compute);
    let get_arangodb_graph =
        warp::path!("api" / "graphanalytics" / "v1" / "engines" / String / "loaddata")
            .and(warp::post())
            .and(with_graphs(graphs.clone()))
            .and(with_args(args.clone()))
            .and(warp::body::bytes())
            .and_then(api_get_arangodb_graph);
    let write_result_back_arangodb =
        warp::path!("api" / "graphanalytics" / "v1" / "engines" / String / "storeresults")
            .and(warp::post())
            .and(with_graphs(graphs.clone()))
            .and(with_computations(computations.clone()))
            .and(warp::body::bytes())
            .and_then(api_write_result_back_arangodb);
    let get_arangodb_graph_aql =
        warp::path!("api" / "graphanalytics" / "v1" / "engines" / String / "loaddataaql")
            .and(warp::post())
            .and(with_graphs(graphs.clone()))
            .and(with_computations(computations.clone()))
            .and(warp::body::bytes())
            .and_then(api_get_arangodb_graph_aql);

    version_bin
        .or(version)
        .or(create)
        .or(drop_bin)
        .or(vertices)
        .or(seal_vertices)
        .or(edges)
        .or(seal_edges)
        .or(get_progress_bin)
        .or(get_progress)
        .or(get_results_by_vertices)
        .or(drop_computation_bin)
        .or(drop_computation)
        .or(compute_bin)
        .or(compute)
        .or(get_arangodb_graph)
        .or(write_result_back_arangodb)
        .or(get_arangodb_graph_aql)
}

fn version_bin() -> Result<Response<Vec<u8>>, Error> {
    let mut v = Vec::new();
    v.write_u32::<BigEndian>(VERSION as u32).unwrap();
    v.write_u32::<BigEndian>(1 as u32).unwrap();
    v.write_u32::<BigEndian>(2 as u32).unwrap();

    Response::builder()
        .header("Content-Type", "x-application-gral")
        .body(v)
}

fn version_json() -> Result<Response<Vec<u8>>, Error> {
    let version_str = format!(
        "{}.{}.{}",
        VERSION >> 16,
        (VERSION >> 8) & 0xff,
        VERSION & 0xff
    );
    let body = serde_json::json!({
        "version": version_str,
        "apiMinVersion": 1,
        "apiMaxVersion": 2
    });
    let v = serde_json::to_vec(&body).expect("Should be serializable");
    Response::builder()
        .header("Content-Type", "application/json")
        .body(v)
}

/// An error object, which is used when the body cannot be parsed as JSON.
#[derive(Debug)]
struct CannotParseJSON {
    pub msg: String,
}
impl reject::Reject for CannotParseJSON {}

/// An error object, which is used when the fetching of a graph from
/// ArangoDB did not work
#[derive(Debug)]
struct GetFromArangoDBFailed {
    pub msg: String,
}
impl reject::Reject for GetFromArangoDBFailed {}

/// An error object, which is used when the starting of a computation
/// did not work.
#[derive(Debug)]
struct ComputeFailed {
    pub msg: String,
}
impl reject::Reject for ComputeFailed {}

/// An error object, which is used when the body size is unexpected.
#[derive(Debug)]
struct WrongBodyLength {
    pub found: usize,
    pub expected: usize,
}
impl reject::Reject for WrongBodyLength {}

/// An error object, which is used when the body size is too short.
#[derive(Debug)]
struct TooShortBodyLength {
    pub found: usize,
    pub expected_at_least: usize,
}
impl reject::Reject for TooShortBodyLength {}

/// An error object, which is used when a (numbered) graph is not found.
#[derive(Debug)]
struct GraphNotFound {
    pub number: u32,
}
impl reject::Reject for GraphNotFound {}

/// An error object, which is used when a graph's vertices are already
/// sealed and the client wants to add more vertices.
#[derive(Debug)]
struct GraphVerticesSealed {
    pub number: u32,
}
impl reject::Reject for GraphVerticesSealed {}

/// An error object, which is used when a graph's vertices are not yet
/// sealed and the client wants to seal the edges.
#[derive(Debug)]
struct GraphVerticesNotSealed {
    pub number: u32,
}
impl reject::Reject for GraphVerticesNotSealed {}

/// An error object, which is used when a graph's edges are already
/// sealed and the client wants to seal them again.
#[derive(Debug)]
struct GraphEdgesSealed {
    pub number: u32,
}
impl reject::Reject for GraphEdgesSealed {}

/// An error object, which is used when a graph's edges are not yet
/// sealed and the client wants to do something for which this is needed.
#[derive(Debug)]
struct GraphEdgesNotSealed {
    pub number: u32,
}
impl reject::Reject for GraphEdgesNotSealed {}

/// An error object, which is used if a vertex with an empty key was
/// presented. The whole batch is rejected in this case.
#[derive(Debug)]
struct KeyMustNotBeEmpty {}
impl reject::Reject for KeyMustNotBeEmpty {}

/// An error object, which is used if a computation is not found.
#[derive(Debug)]
struct ComputationNotFound {
    pub comp_id: u64,
}
impl reject::Reject for ComputationNotFound {}

/// An error object, which is used if a computation is not yet finished.
#[derive(Debug)]
struct ComputationNotYetFinished {
    pub comp_id: u64,
}
impl reject::Reject for ComputationNotYetFinished {}

/// A general error type for internal errors like out of memory:
#[derive(Debug)]
struct InternalError {
    pub msg: String,
}
impl reject::Reject for InternalError {}

/// An error object, which is used if an unknown algorithm is requested.
#[derive(Debug)]
struct UnknownAlgorithm {
    pub algorithm: u32,
}
impl reject::Reject for UnknownAlgorithm {}

/// This async function implements the "create graph" API call.
async fn api_create(graphs: Arc<Mutex<Graphs>>, bytes: Bytes) -> Result<Vec<u8>, Rejection> {
    // Handle wrong length:
    if bytes.len() != 26 {
        return Err(warp::reject::custom(WrongBodyLength {
            found: bytes.len(),
            expected: 26,
        }));
    }

    // Parse body and extract integers:
    // (Note that we have checked the buffer length and so these cannot
    // fail! Therefore unwrap() is OK here.)
    let mut reader = Cursor::new(bytes.to_vec());
    let client_id: u64 = reader.read_u64::<BigEndian>().unwrap();
    // let max_number_vertices: u64 =
    reader.read_u64::<BigEndian>().unwrap();
    // let max_number_edges: u64 =
    reader.read_u64::<BigEndian>().unwrap();
    // let mut bits_for_hash: u8 =
    reader.read_u8().unwrap(); // ignored for now!
    let store_keys: u8 = reader.read_u8().unwrap();

    let bits_for_hash = 64; // any other value ignored for now!

    // Lock list of graphs via their mutex:
    let mut graphs = graphs.lock().unwrap();

    // First try to find an empty spot:
    let mut index: u32 = 0;
    let mut found: bool = false;
    for g in graphs.list.iter_mut() {
        // Lock graph mutex:
        let dropped: bool;
        {
            let gg = g.read().unwrap();
            dropped = gg.dropped;
        }
        if dropped {
            *g = Graph::new(store_keys != 0, 64, index);
            found = true;
            break;
        }
        index += 1;
    }
    // or else append to the end:
    if !found {
        index = graphs.list.len() as u32;
        let graph = Graph::new(store_keys != 0, 64, index);
        graphs.list.push(graph);
    }
    // By now, index is always set to some sensible value!

    info!("Have created graph with number {}!", index);

    // Write response:
    let mut v = Vec::new();
    v.write_u64::<BigEndian>(client_id).unwrap();
    v.write_u32::<BigEndian>(index).unwrap();
    v.write_u8(bits_for_hash).unwrap();
    Ok(v)
}

fn get_graph(
    graphs: &Arc<Mutex<Graphs>>,
    graph_number: u32,
) -> Result<Arc<RwLock<Graph>>, Rejection> {
    // Lock list of graphs via their mutex:
    let graphs = graphs.lock().unwrap();
    if graph_number as usize >= graphs.list.len() {
        return Err(warp::reject::custom(GraphNotFound {
            number: graph_number,
        }));
    }
    Ok(graphs.list[graph_number as usize].clone())
}

fn get_computation(
    computations: &Arc<Mutex<Computations>>,
    comp_id: u64,
) -> Result<Arc<Mutex<dyn Computation + Send + 'static>>, Rejection> {
    let comps = computations.lock().unwrap();
    let comp = comps.list.get(&comp_id);
    match comp {
        None => {
            return Err(warp::reject::custom(ComputationNotFound { comp_id }));
        }
        Some(c) => {
            return Ok(c.clone());
        }
    }
}

/// This async function implements the "drop graph" API call.
async fn api_drop_bin(graphs: Arc<Mutex<Graphs>>, bytes: Bytes) -> Result<Vec<u8>, Rejection> {
    // Handle wrong length:
    if bytes.len() < 12 {
        return Err(warp::reject::custom(TooShortBodyLength {
            found: bytes.len(),
            expected_at_least: 12,
        }));
    }

    // Parse body and extract integers:
    // (Note that we have checked the buffer length and so these cannot
    // fail! Therefore unwrap() is OK here.)
    let mut reader = Cursor::new(bytes.to_vec());
    let client_id = reader.read_u64::<BigEndian>().unwrap();
    let graph_number = reader.read_u32::<BigEndian>().unwrap();

    let mut graphs = graphs.lock().unwrap();
    if graph_number as usize >= graphs.list.len() {
        return Err(warp::reject::custom(GraphNotFound {
            number: graph_number,
        }));
    }

    {
        // Lock graph:
        let graph = graphs.list[graph_number as usize].write().unwrap();

        if graph.dropped {
            return Err(warp::reject::custom(GraphNotFound {
                number: graph_number as u32,
            }));
        }
    }

    // The following will automatically free graph if no longer used by
    // a computation:
    if graph_number as usize + 1 == graphs.list.len() {
        graphs.list.pop();
    } else {
        graphs.list[graph_number as usize] = Graph::new(false, 64, graph_number);
        let mut graph = graphs.list[graph_number as usize].write().unwrap();
        graph.dropped = true; // Mark unused
    }

    info!("Have dropped graph {}!", graph_number);

    // Write response:
    let mut v = Vec::new();
    v.write_u64::<BigEndian>(client_id).unwrap();
    v.write_u32::<BigEndian>(graph_number).unwrap();
    Ok(v)
}

/// A varlen is a length marker which can either be
///  - 0 to indicate something special (zero length or something else)
///  - between 1 and 0x7f to indicate this length in one byte
///  - be a u32 BigEndian with high bit set (so that the first byte is
///    in the range 0x80..0xff and then indicates the length).
/// This function extracts a varlen from the cursor c.
///
fn get_varlen(c: &mut Cursor<Vec<u8>>) -> Result<u32, std::io::Error> {
    let mut b = c.read_u8()?;
    match b {
        0 => Ok(0),
        1..=0x7f => Ok(b as u32),
        _ => {
            let mut r = (b & 0x7f) as u32;
            for _i in 1..=3 {
                b = c.read_u8()?;
                r = (r << 8) | (b as u32);
            }
            Ok(r)
        }
    }
}

fn put_varlen(v: &mut Vec<u8>, l: u32) {
    if l <= 0x7f {
        v.write_u8(l as u8).unwrap();
    } else {
        v.write_u32::<BigEndian>(l | 0x80000000).unwrap();
    };
}

fn read_bytes_or_fail(reader: &mut Cursor<Vec<u8>>, l: u32) -> Result<&[u8], Rejection> {
    let v = reader.get_ref();
    if (v.len() as u64) - reader.position() < l as u64 {
        return Err(warp::reject::custom(TooShortBodyLength {
            found: reader.position() as usize,
            expected_at_least: reader.position() as usize + l as usize,
        }));
    }
    Ok(&v[(reader.position() as usize)..((reader.position() + l as u64) as usize)])
}

fn put_key_or_hash(out: &mut Vec<u8>, koh: &KeyOrHash) {
    match koh {
        KeyOrHash::Hash(h) => {
            put_varlen(out, 0);
            out.write_u64::<BigEndian>(h.to_u64()).unwrap();
        }
        KeyOrHash::Key(k) => {
            put_varlen(out, k.len() as u32);
            out.extend_from_slice(&k);
        }
    }
}

fn parse_vertex(reader: &mut Cursor<Vec<u8>>) -> Result<(VertexHash, Vec<u8>, Vec<u8>), Rejection> {
    let l = get_varlen(reader);
    if l.is_err() {
        return Err(warp::reject::custom(TooShortBodyLength {
            found: reader.position() as usize,
            expected_at_least: reader.position() as usize + 2,
        }));
    }
    let l = l.unwrap();

    if l == 0 {
        return Err(warp::reject::custom(KeyMustNotBeEmpty {}));
    }

    let k = read_bytes_or_fail(reader, l)?;
    let mut key: Vec<u8> = vec![];
    key.extend_from_slice(k);
    let hash = VertexHash::new(xxh3_64_with_seed(k, 0xdeadbeefdeadbeef));
    reader.consume(l as usize);

    // Before we move on with this, let's get the optional data:
    let ld = get_varlen(reader);
    if ld.is_err() {
        return Err(warp::reject::custom(TooShortBodyLength {
            found: reader.position() as usize,
            expected_at_least: reader.position() as usize + 2,
        }));
    }
    let ld = ld.unwrap(); // Length of data, 0 means none

    let mut data: Vec<u8> = vec![];
    if ld > 0 {
        let k = read_bytes_or_fail(reader, ld)?;
        data.extend_from_slice(k);
    }
    reader.consume(ld as usize);

    return Ok((hash, key, data));
}

// parse_key reads a varlen and a key, or a hash if the length was 0.
// If a key was read, the hash is computed.
fn parse_key_or_hash(reader: &mut Cursor<Vec<u8>>) -> Result<KeyOrHash, Rejection> {
    let l = get_varlen(reader);
    if l.is_err() {
        return Err(warp::reject::custom(TooShortBodyLength {
            found: reader.position() as usize,
            expected_at_least: reader.position() as usize + 2,
        }));
    }
    let l = l.unwrap();
    if l == 0 {
        let u = reader.read_u64::<BigEndian>();
        match u {
            Err(_) => {
                return Err(warp::reject::custom(TooShortBodyLength {
                    found: reader.position() as usize,
                    expected_at_least: reader.position() as usize + 8,
                }));
            }
            Ok(uu) => {
                return Ok(KeyOrHash::Hash(VertexHash::new(uu)));
            }
        }
    } else {
        let k = read_bytes_or_fail(reader, l)?;
        let key = k.to_vec();
        reader.consume(l as usize);
        return Ok(KeyOrHash::Key(key));
    }
}

async fn api_vertices(graphs: Arc<Mutex<Graphs>>, bytes: Bytes) -> Result<Vec<u8>, Rejection> {
    // Handle wrong length:
    if bytes.len() < 16 {
        return Err(warp::reject::custom(TooShortBodyLength {
            found: bytes.len(),
            expected_at_least: 16,
        }));
    }

    // Parse body and extract integers:
    // (Note that we have checked the buffer length and so these cannot
    // fail! Therefore unwrap() is OK here.)
    let mut reader = Cursor::new(bytes.to_vec());
    let client_id = reader.read_u64::<BigEndian>().unwrap();
    let graph_number = reader.read_u32::<BigEndian>().unwrap();
    let number = reader.read_u32::<BigEndian>().unwrap();

    let graph_arc = get_graph(&graphs, graph_number)?;

    // Lock graph:
    let mut graph = graph_arc.write().unwrap();

    if graph.dropped {
        return Err(warp::reject::custom(GraphNotFound {
            number: graph_number,
        }));
    }
    if graph.vertices_sealed {
        return Err(warp::reject::custom(GraphVerticesSealed {
            number: graph_number,
        }));
    }

    // Collect exceptions:
    let mut exceptional: Vec<(u32, VertexHash)> = vec![];
    let mut exceptional_keys: Vec<Vec<u8>> = vec![];

    for i in 0..number {
        let (hash, key, data) = parse_vertex(&mut reader)?;
        // Whole batch is rejected, if there is an empty key or the input
        // is too short! Note that previous vertices might already have
        // been inserted! This is allowed according to the API!
        graph.insert_vertex(i, hash, key, data, &mut exceptional, &mut exceptional_keys)
    }

    // Write response:
    let mut v = Vec::new();
    // TODO: handle errors!

    // Compute sum of length of keys:
    let mut total_sum = 0;
    for k in exceptional_keys.iter() {
        total_sum += k.len();
    }

    assert_eq!(exceptional.len(), exceptional_keys.len());

    v.reserve(12 + exceptional.len() * (12 + 4) + total_sum);
    v.write_u64::<BigEndian>(client_id).unwrap();
    v.write_u32::<BigEndian>(exceptional.len() as u32).unwrap();
    for (index, hash) in exceptional.iter() {
        v.write_u32::<BigEndian>(*index).unwrap();
        v.write_u64::<BigEndian>((*hash).to_u64()).unwrap();
        put_varlen(&mut v, exceptional_keys[*index as usize].len() as u32);
        v.extend_from_slice(&exceptional_keys[*index as usize]);
    }
    Ok(v)
}

/// This function seals the vertices of a graph.
async fn api_seal_vertices(graphs: Arc<Mutex<Graphs>>, bytes: Bytes) -> Result<Vec<u8>, Rejection> {
    // Handle wrong length:
    if bytes.len() != 12 {
        return Err(warp::reject::custom(WrongBodyLength {
            found: bytes.len(),
            expected: 12,
        }));
    }

    // Parse body and extract integers:
    // (Note that we have checked the buffer length and so these cannot
    // fail! Therefore unwrap() is OK here.)
    let mut reader = Cursor::new(bytes.to_vec());
    let client_id = reader.read_u64::<BigEndian>().unwrap();
    let graph_number = reader.read_u32::<BigEndian>().unwrap();

    let graph_arc = get_graph(&graphs, graph_number)?;

    // Write lock graph:
    let mut graph = graph_arc.write().unwrap();

    if graph.dropped {
        return Err(warp::reject::custom(GraphNotFound {
            number: graph_number,
        }));
    }
    if graph.vertices_sealed {
        return Err(warp::reject::custom(GraphVerticesSealed {
            number: graph_number,
        }));
    }

    graph.seal_vertices();

    // Write response:
    let mut v = Vec::new();
    // TODO: handle errors!
    v.reserve(20);
    v.write_u64::<BigEndian>(client_id).unwrap();
    v.write_u32::<BigEndian>(graph_number).unwrap();
    v.write_u64::<BigEndian>(graph.number_of_vertices())
        .unwrap();
    Ok(v)
}

/// This function reads from a Vec<u8> Cursor and tries to parse an
/// edge.
fn parse_edge(
    graph: &Graph,
    reader: &mut Cursor<Vec<u8>>,
) -> Result<(Option<(VertexIndex, VertexIndex)>, Vec<u8>), Rejection> {
    let l = get_varlen(reader);
    if l.is_err() {
        return Err(warp::reject::custom(TooShortBodyLength {
            found: reader.position() as usize,
            expected_at_least: reader.position() as usize + 2,
        }));
    }
    let l = l.unwrap();

    let from_index: Option<VertexIndex>;
    match l {
        0 => {
            let val = reader.read_u64::<BigEndian>();
            if val.is_err() {
                return Err(warp::reject::custom(TooShortBodyLength {
                    found: reader.position() as usize,
                    expected_at_least: reader.position() as usize + 2,
                }));
            }
            let hash = VertexHash::new(val.unwrap());
            let index = graph.hash_to_index.get(&hash);
            from_index = match index {
                None => None,
                Some(index) => Some(*index),
            }
        }
        _ => {
            {
                let k = read_bytes_or_fail(reader, l)?;
                from_index = graph.index_from_vertex_key(k);
            }
            reader.consume(l as usize);
        }
    }

    let l = get_varlen(reader);
    if l.is_err() {
        return Err(warp::reject::custom(TooShortBodyLength {
            found: reader.position() as usize,
            expected_at_least: reader.position() as usize + 2,
        }));
    }
    let l = l.unwrap();
    let to_index: Option<VertexIndex>;
    match l {
        0 => {
            let val = reader.read_u64::<BigEndian>();
            if val.is_err() {
                return Err(warp::reject::custom(TooShortBodyLength {
                    found: reader.position() as usize,
                    expected_at_least: reader.position() as usize + 2,
                }));
            }
            let hash = VertexHash::new(val.unwrap());
            let index = graph.hash_to_index.get(&hash);
            to_index = match index {
                None => None,
                Some(index) => Some(*index),
            }
        }
        _ => {
            {
                let k = read_bytes_or_fail(reader, l)?;
                to_index = graph.index_from_vertex_key(k);
            }
            reader.consume(l as usize);
        }
    }

    // Before we move on with this, let's get the optional data:
    let ld = get_varlen(reader);
    if ld.is_err() {
        return Err(warp::reject::custom(TooShortBodyLength {
            found: reader.position() as usize,
            expected_at_least: reader.position() as usize + 2,
        }));
    }
    let ld = ld.unwrap(); // Length of data, 0 means none
    let mut data: Vec<u8> = vec![];
    if ld > 0 {
        let v = reader.get_ref();
        if (v.len() as u64) - reader.position() < ld as u64 {
            return Err(warp::reject::custom(TooShortBodyLength {
                found: reader.position() as usize,
                expected_at_least: reader.position() as usize + ld as usize,
            }));
        }
        data.extend_from_slice(
            &v[(reader.position() as usize)..((reader.position() + l as u64) as usize)],
        );
    }
    reader.consume(ld as usize);

    if from_index.is_some() && to_index.is_some() {
        Ok((Some((from_index.unwrap(), to_index.unwrap())), data))
    } else {
        Ok((None, data))
    }
}

/// This function implements the API to insert edges.
async fn api_edges(graphs: Arc<Mutex<Graphs>>, bytes: Bytes) -> Result<Vec<u8>, Rejection> {
    // Handle wrong length:
    if bytes.len() < 16 {
        return Err(warp::reject::custom(TooShortBodyLength {
            found: bytes.len(),
            expected_at_least: 16,
        }));
    }

    // Parse body and extract integers:
    // (Note that we have checked the buffer length and so these cannot
    // fail! Therefore unwrap() is OK here.)
    let mut reader = Cursor::new(bytes.to_vec());
    let client_id = reader.read_u64::<BigEndian>().unwrap();
    let graph_number = reader.read_u32::<BigEndian>().unwrap();
    let number = reader.read_u32::<BigEndian>().unwrap();

    let graph_arc = get_graph(&graphs, graph_number)?;

    // Write lock graph:
    let mut graph = graph_arc.write().unwrap();
    check_graph(graph.deref(), graph_number, false)?;

    // Collect rejections:
    let mut rejected: Vec<u32> = vec![];

    for i in 0..number {
        let (fromto, data) = parse_edge(&graph, &mut reader)?;
        match fromto {
            None => rejected.push(i),
            Some((from, to)) => graph.insert_edge(from, to, data),
        }
    }

    // Write response:
    let mut v = Vec::new();
    // TODO: handle errors!
    v.reserve(16 + rejected.len() * 8);
    v.write_u64::<BigEndian>(client_id).unwrap();
    v.write_u32::<BigEndian>(rejected.len() as u32).unwrap();
    for r in rejected.iter() {
        v.write_u32::<BigEndian>(*r).unwrap();
        v.write_u32::<BigEndian>(3).unwrap();
        v.write_u8(0).unwrap();
    }
    Ok(v)
}

/// This function seals the edges of a graph.
async fn api_seal_edges(graphs: Arc<Mutex<Graphs>>, bytes: Bytes) -> Result<Vec<u8>, Rejection> {
    // Handle wrong length:
    if bytes.len() != 16 {
        return Err(warp::reject::custom(WrongBodyLength {
            found: bytes.len(),
            expected: 16,
        }));
    }

    // Parse body and extract integers:
    // (Note that we have checked the buffer length and so these cannot
    // fail! Therefore unwrap() is OK here.)
    let mut reader = Cursor::new(bytes.to_vec());
    let client_id = reader.read_u64::<BigEndian>().unwrap();
    let graph_number = reader.read_u32::<BigEndian>().unwrap();
    let index_edges = reader.read_u32::<BigEndian>().unwrap();

    let graph_arc = get_graph(&graphs, graph_number)?;

    // Write lock graph:
    let mut graph = graph_arc.write().unwrap();
    check_graph(graph.deref(), graph_number, false)?;

    graph.seal_edges();
    if index_edges != 0 {
        graph.index_edges(index_edges & 1 != 0, index_edges & 2 != 0);
    }

    // Write response:
    let mut v = Vec::new();
    // TODO: handle errors!
    v.reserve(20);
    v.write_u64::<BigEndian>(client_id).unwrap();
    v.write_u32::<BigEndian>(graph_number).unwrap();
    v.write_u64::<BigEndian>(graph.number_of_vertices())
        .unwrap();
    v.write_u64::<BigEndian>(graph.number_of_edges()).unwrap();
    Ok(v)
}

pub struct ConcreteComputation {
    pub algorithm: u32,
    pub graph: Arc<RwLock<Graph>>,
    pub components: Option<Vec<u64>>,
    pub shall_stop: bool,
    pub number: u64,
}

impl Computation for ConcreteComputation {
    fn is_ready(&self) -> bool {
        self.components.is_some()
    }
    fn cancel(&mut self) {
        self.shall_stop = true;
    }
    fn algorithm_id(&self) -> u32 {
        return self.algorithm;
    }
    fn get_graph(&self) -> Arc<RwLock<Graph>> {
        return self.graph.clone();
    }
    fn dump_result(&self, out: &mut Vec<u8>) -> Result<(), String> {
        out.write_u8(8).unwrap();
        out.write_u64::<BigEndian>(self.number).unwrap();
        Ok(())
    }
    fn get_result(&self) -> u64 {
        self.number
    }
    fn dump_vertex_results(
        &self,
        comp_id: u64,
        kohs: &Vec<KeyOrHash>,
        out: &mut Vec<u8>,
    ) -> Result<(), Rejection> {
        let comps = self.components.as_ref();
        match comps {
            None => {
                return Err(warp::reject::custom(ComputationNotYetFinished { comp_id }));
            }
            Some(result) => {
                let g = self.graph.read().unwrap();
                for koh in kohs.iter() {
                    let index = g.index_from_key_or_hash(koh);
                    match index {
                        None => {
                            put_key_or_hash(out, koh);
                            out.write_u8(0).unwrap();
                        }
                        Some(i) => {
                            put_key_or_hash(out, koh);
                            out.write_u8(8).unwrap();
                            out.write_u64::<BigEndian>(result[i.to_u64() as usize])
                                .unwrap();
                        }
                    }
                }
                return Ok(());
            }
        }
    }
}

/// This function triggers a computation:
async fn api_compute_bin(
    graphs: Arc<Mutex<Graphs>>,
    computations: Arc<Mutex<Computations>>,
    bytes: Bytes,
) -> Result<Vec<u8>, Rejection> {
    // Handle wrong length:
    if bytes.len() != 16 {
        return Err(warp::reject::custom(WrongBodyLength {
            found: bytes.len(),
            expected: 16,
        }));
    }

    // Parse body and extract integers:
    // (Note that we have checked the buffer length and so these cannot
    // fail! Therefore unwrap() is OK here.)
    let mut reader = Cursor::new(bytes.to_vec());
    let client_id = reader.read_u64::<BigEndian>().unwrap();
    let graph_number = reader.read_u32::<BigEndian>().unwrap();
    let algorithm = reader.read_u32::<BigEndian>().unwrap();

    let graph_arc = get_graph(&graphs, graph_number)?;

    {
        // Check graph:
        let graph = graph_arc.read().unwrap();
        check_graph(graph.deref(), graph_number, true)?;
    }

    if algorithm < 1 || algorithm > 2 {
        return Err(warp::reject::custom(UnknownAlgorithm { algorithm }));
    }

    let comp_arc = Arc::new(Mutex::new(ConcreteComputation {
        algorithm,
        graph: graph_arc.clone(),
        components: None,
        shall_stop: false,
        number: 0,
    }));

    let mut rng = rand::thread_rng();
    let mut comp_id: u64;
    {
        let mut comps = computations.lock().unwrap();
        loop {
            comp_id = rng.gen::<u64>();
            if !comps.list.contains_key(&comp_id) {
                break;
            }
        }
        comps.list.insert(comp_id, comp_arc.clone());
    }
    let _join_handle = std::thread::spawn(move || {
        let (nr, components) = match algorithm {
            1 => {
                let graph = graph_arc.read().unwrap();
                weakly_connected_components(&graph)
            }
            2 => {
                {
                    // Make sure we have an edge index:
                    let mut graph = graph_arc.write().unwrap();
                    if !graph.edges_indexed_from {
                        info!("Indexing edges by from...");
                        graph.index_edges(true, false);
                    }
                }
                let graph = graph_arc.read().unwrap();
                strongly_connected_components(&graph)
            }
            _ => std::unreachable!(),
        };
        info!("Found {} connected components.", nr);
        let mut comp = comp_arc.lock().unwrap();
        comp.components = Some(components);
        comp.number = nr;
    });

    // Write response:
    let mut v = Vec::new();
    // TODO: handle errors!
    v.reserve(20);
    v.write_u64::<BigEndian>(client_id).unwrap();
    v.write_u32::<BigEndian>(graph_number).unwrap();
    v.write_u32::<BigEndian>(algorithm).unwrap();
    v.write_u64::<BigEndian>(comp_id).unwrap();
    Ok(v)
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ComputeRequestResponse {
    client_id: String,
    graph_id: String,
    algorithm: String,
    job_id: Option<String>,
}

/// This function triggers a computation:
async fn api_compute(
    _engine_id: String,
    graphs: Arc<Mutex<Graphs>>,
    computations: Arc<Mutex<Computations>>,
    bytes: Bytes,
) -> Result<Vec<u8>, Rejection> {
    // Parse body and extract integers:
    let parsed: serde_json::Result<GraphAnalyticsEngineProcessRequest> =
        serde_json::from_slice(&bytes[..]);
    if let Err(e) = parsed {
        return Err(warp::reject::custom(CannotParseJSON { msg: e.to_string() }));
    }
    let body = parsed.unwrap();

    let client_id = u64::from_str_radix(&body.client_id, 16);
    if let Err(_) = client_id {
        return Err(warp::reject::custom(ComputeFailed {
            msg: "Could not read clientId as 64bit hex value".to_string(),
        }));
    }
    let _client_id = client_id.unwrap();
    let graph_number = u32::from_str_radix(&body.graph_id, 16);
    if let Err(_) = graph_number {
        return Err(warp::reject::custom(ComputeFailed {
            msg: "Could not read graphId as 32bit hex value".to_string(),
        }));
    }
    let graph_number = graph_number.unwrap();

    let graph_arc = get_graph(&graphs, graph_number)?;

    {
        // Check graph:
        let graph = graph_arc.read().unwrap();
        check_graph(graph.deref(), graph_number, true)?;
    }

    let algorithm: u32 = match body.algorithm.as_ref() {
        "wcc" => 1,
        "scc" => 2,
        _ => 0,
    };

    if algorithm == 0 {
        return Err(warp::reject::custom(ComputeFailed {
            msg: format!("Unknown algorithm: {}", body.algorithm),
        }));
    }

    let comp_arc = Arc::new(Mutex::new(ConcreteComputation {
        algorithm,
        graph: graph_arc.clone(),
        components: None,
        shall_stop: false,
        number: 0,
    }));

    let mut rng = rand::thread_rng();
    let mut comp_id: u64;
    {
        let mut comps = computations.lock().unwrap();
        loop {
            comp_id = rng.gen::<u64>();
            if !comps.list.contains_key(&comp_id) {
                break;
            }
        }
        comps.list.insert(comp_id, comp_arc.clone());
    }
    let _join_handle = std::thread::spawn(move || {
        let (nr, components) = match algorithm {
            1 => {
                let graph = graph_arc.read().unwrap();
                weakly_connected_components(&graph)
            }
            2 => {
                {
                    // Make sure we have an edge index:
                    let mut graph = graph_arc.write().unwrap();
                    if !graph.edges_indexed_from {
                        info!("Indexing edges by from...");
                        graph.index_edges(true, false);
                    }
                }
                let graph = graph_arc.read().unwrap();
                strongly_connected_components(&graph)
            }
            _ => std::unreachable!(),
        };
        info!("Found {} connected components.", nr);
        let mut comp = comp_arc.lock().unwrap();
        comp.components = Some(components);
        comp.number = nr;
    });
    let response = GraphAnalyticsEngineProcessResponse {
        job_id: format!("{:08x}", comp_id),
        client_id: body.client_id,
        error: false,
        error_code: 0,
        error_message: "".to_string(),
    };

    // Write response:
    let v = serde_json::to_vec(&response).expect("Should be serializable!");
    Ok(v)
}

/// This function writes a computation result back to ArangoDB:
async fn api_write_result_back_arangodb(
    _engine_id: String,
    _graphs: Arc<Mutex<Graphs>>,
    _computations: Arc<Mutex<Computations>>,
    bytes: Bytes,
) -> Result<Vec<u8>, Rejection> {
    // Parse body and extract integers:
    let parsed: serde_json::Result<GraphAnalyticsEngineStoreResultsRequest> =
        serde_json::from_slice(&bytes[..]);
    if let Err(e) = parsed {
        return Err(warp::reject::custom(CannotParseJSON { msg: e.to_string() }));
    }
    let body = parsed.unwrap();

    let client_id = u64::from_str_radix(&body.client_id, 16);
    if let Err(_) = client_id {
        return Err(warp::reject::custom(ComputeFailed {
            msg: "Could not read clientId as 64bit hex value".to_string(),
        }));
    }
    let _client_id = client_id.unwrap();
    let job_id = u32::from_str_radix(&body.job_id, 16);
    if let Err(_) = job_id {
        return Err(warp::reject::custom(ComputeFailed {
            msg: "Could not read jobId as 32bit hex value".to_string(),
        }));
    }
    let job_id = job_id.unwrap();

    // TO BE IMPLEMENTED

    let response = GraphAnalyticsEngineStoreResultsResponse {
        job_id: format!("{:08x}", job_id),
        client_id: body.client_id,
        error: true,
        error_code: 1,
        error_message: "NOT_YET_IMPLEMENTED".to_string(),
    };

    // Write response:
    let v = serde_json::to_vec(&response).expect("Should be serializable!");
    Ok(v)
}

/// This function writes a computation result back to ArangoDB:
async fn api_get_arangodb_graph_aql(
    _engine_id: String,
    _graphs: Arc<Mutex<Graphs>>,
    _computations: Arc<Mutex<Computations>>,
    bytes: Bytes,
) -> Result<Vec<u8>, Rejection> {
    // Parse body and extract integers:
    let parsed: serde_json::Result<GraphAnalyticsEngineLoadDataAqlRequest> =
        serde_json::from_slice(&bytes[..]);
    if let Err(e) = parsed {
        return Err(warp::reject::custom(CannotParseJSON { msg: e.to_string() }));
    }
    let body = parsed.unwrap();

    let client_id = u64::from_str_radix(&body.client_id, 16);
    if let Err(_) = client_id {
        return Err(warp::reject::custom(ComputeFailed {
            msg: "Could not read clientId as 64bit hex value".to_string(),
        }));
    }
    let _client_id = client_id.unwrap();
    let job_id = u32::from_str_radix(&body.job_id, 16);
    if let Err(_) = job_id {
        return Err(warp::reject::custom(ComputeFailed {
            msg: "Could not read jobId as 32bit hex value".to_string(),
        }));
    }
    let job_id = job_id.unwrap();

    // TO BE IMPLEMENTED

    let response = GraphAnalyticsEngineLoadDataResponse {
        job_id: format!("{:08x}", job_id),
        client_id: body.client_id,
        graph_id: "bla".to_string(),
        error: true,
        error_code: 1,
        error_message: "NOT_YET_IMPLEMENTED".to_string(),
    };

    // Write response:
    let v = serde_json::to_vec(&response).expect("Should be serializable!");
    Ok(v)
}

/// This function gets progress of a computation.
async fn api_get_progress_bin(
    computations: Arc<Mutex<Computations>>,
    bytes: Bytes,
) -> Result<Vec<u8>, Rejection> {
    // Handle wrong length:
    if bytes.len() != 8 {
        return Err(warp::reject::custom(WrongBodyLength {
            found: bytes.len(),
            expected: 8,
        }));
    }

    // Parse body and extract integers:
    // (Note that we have checked the buffer length and so these cannot
    // fail! Therefore unwrap() is OK here.)
    let mut reader = Cursor::new(bytes.to_vec());
    let comp_id = reader.read_u64::<BigEndian>().unwrap();

    let comps = computations.lock().unwrap();
    let comp_arc = comps.list.get(&comp_id);
    match comp_arc {
        None => {
            return Err(warp::reject::custom(ComputationNotFound { comp_id }));
        }
        Some(comp_arc) => {
            let comp = comp_arc.lock().unwrap();

            // Write response:
            let mut v = Vec::new();
            // TODO: handle errors!
            v.reserve(256);
            v.write_u64::<BigEndian>(comp_id).unwrap();
            v.write_u32::<BigEndian>(1).unwrap();
            if comp.is_ready() {
                v.write_u32::<BigEndian>(1).unwrap();
                comp.dump_result(&mut v).unwrap();
            } else {
                v.write_u32::<BigEndian>(0).unwrap();
                v.write_u8(0).unwrap();
            }
            Ok(v)
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProgressResponse {
    job_id: String,
    total: u32,
    progress: u32,
    result: Option<String>,
}

/// This function gets progress of a computation.
async fn api_get_progress(
    _engine_id: String,
    job_id: String,
    computations: Arc<Mutex<Computations>>,
) -> Result<Vec<u8>, Rejection> {
    let comp_id = u64::from_str_radix(&job_id, 16);
    if let Err(_) = comp_id {
        return Err(warp::reject::custom(ComputationNotFound { comp_id: 0 }));
    }
    let comp_id = comp_id.unwrap();

    let comps = computations.lock().unwrap();
    let comp_arc = comps.list.get(&comp_id);
    match comp_arc {
        None => {
            return Err(warp::reject::custom(ComputationNotFound { comp_id }));
        }
        Some(comp_arc) => {
            let comp = comp_arc.lock().unwrap();
            let graph_arc = comp.get_graph();
            let graph = graph_arc.read().unwrap();

            // Write response:
            let response = GraphAnalyticsEngineJob {
                job_id,
                graph_id: format!("{:x}", graph.graph_id),
                total: 1,
                progress: if comp.is_ready() { 1 } else { 0 },
                result: if comp.is_ready() {
                    comp.get_result() as i64
                } else {
                    0
                },
            };
            Ok(serde_json::to_vec(&response).expect("Should be serializable"))
        }
    }
}

/// This function deletes a computation.
async fn api_drop_computation(
    _engine_id: String,
    job_id: String,
    computations: Arc<Mutex<Computations>>,
) -> Result<Vec<u8>, Rejection> {
    let comp_id = u64::from_str_radix(&job_id, 16);
    if let Err(_) = comp_id {
        return Err(warp::reject::custom(ComputationNotFound { comp_id: 0 }));
    }
    let comp_id = comp_id.unwrap();

    let mut comps = computations.lock().unwrap();
    let comp_arc = comps.list.get(&comp_id);
    match comp_arc {
        None => {
            return Err(warp::reject::custom(ComputationNotFound { comp_id }));
        }
        Some(comp_arc) => {
            {
                let mut comp = comp_arc.lock().unwrap();
                comp.cancel();
            }
            comps.list.remove(&comp_id);

            // Write response:
            let response = GraphAnalyticsEngineDeleteJobResponse {
                job_id,
                error: false,
                error_code: 0,
                error_message: "".to_string(),
            };
            Ok(serde_json::to_vec(&response).expect("Should be serializable"))
        }
    }
}

// The following function implements
async fn api_get_results_by_vertices(
    computations: Arc<Mutex<Computations>>,
    bytes: Bytes,
) -> Result<Vec<u8>, Rejection> {
    // Handle wrong length:
    if bytes.len() < 12 {
        return Err(warp::reject::custom(TooShortBodyLength {
            found: bytes.len(),
            expected_at_least: 12,
        }));
    }

    // Parse body and extract integers:
    // (Note that we have checked the buffer length and so these cannot
    // fail! Therefore unwrap() is OK here.)
    let mut reader = Cursor::new(bytes.to_vec());
    let comp_id = reader.read_u64::<BigEndian>().unwrap();
    let number = reader.read_u32::<BigEndian>().unwrap();

    let computation_arc = get_computation(&computations, comp_id)?;

    let mut input: Vec<KeyOrHash> = vec![];
    input.reserve(number as usize);
    for _i in 0..number {
        let key_or_hash = parse_key_or_hash(&mut reader)?;
        input.push(key_or_hash);
    }

    // Write response:
    let mut v = Vec::new();
    // TODO: handle errors!
    v.reserve(1024 * 1024);
    v.write_u64::<BigEndian>(comp_id).unwrap();
    v.write_u32::<BigEndian>(number).unwrap();

    // Now lock computation:
    let computation = computation_arc.lock().unwrap();
    v.write_u32::<BigEndian>(computation.algorithm_id())
        .unwrap();
    computation.dump_vertex_results(comp_id, &input, &mut v)?;
    Ok(v)
}

/// This function drops a computation.
async fn api_drop_computation_bin(
    computations: Arc<Mutex<Computations>>,
    bytes: Bytes,
) -> Result<Vec<u8>, Rejection> {
    // Handle wrong length:
    if bytes.len() != 16 {
        return Err(warp::reject::custom(WrongBodyLength {
            found: bytes.len(),
            expected: 16,
        }));
    }

    // Parse body and extract integers:
    // (Note that we have checked the buffer length and so these cannot
    // fail! Therefore unwrap() is OK here.)
    let mut reader = Cursor::new(bytes.to_vec());
    let client_id = reader.read_u64::<BigEndian>().unwrap();
    let comp_id = reader.read_u64::<BigEndian>().unwrap();

    let mut comps = computations.lock().unwrap();
    let comp_arc = comps.list.get(&comp_id).clone();
    match comp_arc {
        None => {
            return Err(warp::reject::custom(ComputationNotFound { comp_id }));
        }
        Some(comp_arc) => {
            {
                let mut comp = comp_arc.lock().unwrap();
                comp.cancel();
            }
            comps.list.remove(&comp_id);
        }
    }

    // Write response:
    let mut v = Vec::new();
    // TODO: handle errors!
    v.reserve(256);
    v.write_u64::<BigEndian>(client_id).unwrap();
    v.write_u64::<BigEndian>(comp_id).unwrap();
    Ok(v)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CollectionInfo {
    pub name: String,
    pub fields: Vec<String>,
}

async fn api_get_arangodb_graph(
    _engine_id: String,
    graphs: Arc<Mutex<Graphs>>,
    args: Arc<Mutex<GralArgs>>,
    bytes: Bytes,
) -> Result<warp::reply::Json, Rejection> {
    let parsed: serde_json::Result<GraphAnalyticsEngineLoadDataRequest> =
        serde_json::from_slice(&bytes[..]);
    //let parsed: serde_json::Result<GetArangoDBGraphRequest> = serde_json::from_slice(&bytes[..]);
    if let Err(e) = parsed {
        return Err(warp::reject::custom(CannotParseJSON { msg: e.to_string() }));
    }
    let mut body = parsed.unwrap();
    // Set a few sensible defaults:
    if body.batch_size == 0 {
        body.batch_size = 400000;
    }
    if body.parallelism == 0 {
        body.parallelism = 5;
    }

    // Fetch from ArangoDB:
    let graph = fetch_graph_from_arangodb(&body, args).await;
    if let Err(e) = graph {
        return Err(warp::reject::custom(GetFromArangoDBFailed {
            msg: e.to_string(),
        }));
    }
    let graph = graph.unwrap();

    // And store it amongst the graphs:
    let mut graphs = graphs.lock().unwrap();
    // First try to find an empty spot:
    let mut index: u32 = 0;
    let mut found: bool = false;
    for g in graphs.list.iter_mut() {
        // Lock graph mutex:
        let dropped: bool;
        {
            let gg = g.read().unwrap();
            dropped = gg.dropped;
        }
        if dropped {
            found = true;
            break;
        }
        index += 1;
    }
    // or else append to the end:
    if !found {
        index = graphs.list.len() as u32;
        {
            let mut graph = graph.write().unwrap();
            graph.graph_id = index;
        }
        graphs.list.push(graph);
    } else {
        {
            let mut graph = graph.write().unwrap();
            graph.graph_id = index;
        }
        graphs.list[index as usize] = graph;
    }
    // By now, index is always set to some sensible value!

    info!("Have created graph with number {}!", index);

    // Write response:
    let response = GraphAnalyticsEngineLoadDataResponse {
        job_id: "bla".to_string(), // will be ID of computation when this is async
        client_id: body.client_id,
        graph_id: format!("{:x}", index),
        error: false,
        error_code: 0,
        error_message: "".to_string(),
    };
    Ok(warp::reply::json(&response))
}

// This function receives a `Rejection` and is responsible to convert
// this into a proper HTTP error with a body as designed.
pub async fn handle_errors(err: Rejection) -> Result<impl warp::Reply, Infallible> {
    let code;
    let message: String;
    let mut output_json = false;

    if err.is_not_found() {
        code = StatusCode::NOT_FOUND;
        message = "NOT_FOUND".to_string();
    } else if let Some(_) = err.find::<warp::reject::MethodNotAllowed>() {
        // We can handle a specific error, here METHOD_NOT_ALLOWED,
        // and render it however we want
        code = StatusCode::METHOD_NOT_ALLOWED;
        message = "METHOD_NOT_ALLOWED".to_string();
    } else if let Some(wrong) = err.find::<WrongBodyLength>() {
        code = StatusCode::BAD_REQUEST;
        message = format!(
            "Expected body size {} but found {}",
            wrong.expected, wrong.found
        );
    } else if let Some(wrong) = err.find::<CannotParseJSON>() {
        code = StatusCode::BAD_REQUEST;
        message = format!("Cannot parse JSON body of request: {}", wrong.msg);
        output_json = true;
    } else if let Some(wrong) = err.find::<GetFromArangoDBFailed>() {
        code = StatusCode::BAD_REQUEST;
        message = format!("Could not fetch graph from ArangoDB: {}", wrong.msg);
        output_json = true;
    } else if let Some(wrong) = err.find::<ComputeFailed>() {
        code = StatusCode::BAD_REQUEST;
        message = format!("Could not start computation: {}", wrong.msg);
        output_json = true;
    } else if let Some(wrong) = err.find::<TooShortBodyLength>() {
        code = StatusCode::BAD_REQUEST;
        message = format!(
            "Expected body size of at least {} but found {}",
            wrong.expected_at_least, wrong.found
        );
    } else if let Some(wrong) = err.find::<GraphNotFound>() {
        code = StatusCode::NOT_FOUND;
        message = format!(
            "Graph with number {} not found or already deleted",
            wrong.number
        );
    } else if let Some(wrong) = err.find::<GraphVerticesSealed>() {
        code = StatusCode::FORBIDDEN;
        message = format!(
            "Graph with number {} has its vertices already sealed",
            wrong.number
        );
    } else if let Some(wrong) = err.find::<GraphVerticesNotSealed>() {
        code = StatusCode::FORBIDDEN;
        message = format!(
            "Graph with number {} does not yet have its vertices sealed",
            wrong.number
        );
    } else if let Some(wrong) = err.find::<GraphEdgesSealed>() {
        code = StatusCode::FORBIDDEN;
        message = format!(
            "Graph with number {} has its edges already sealed",
            wrong.number
        );
    } else if let Some(wrong) = err.find::<GraphEdgesNotSealed>() {
        code = StatusCode::FORBIDDEN;
        message = format!(
            "Graph with number {} does not yet have its edges sealed",
            wrong.number
        );
    } else if let Some(_) = err.find::<KeyMustNotBeEmpty>() {
        code = StatusCode::BAD_REQUEST;
        message = "Key must not be empty, whole batch rejected".to_string();
    } else if let Some(wrong) = err.find::<ComputationNotFound>() {
        code = StatusCode::NOT_FOUND;
        message = format!("Computation with id {} does not exist", wrong.comp_id);
        output_json = true;
    } else if let Some(wrong) = err.find::<ComputationNotYetFinished>() {
        code = StatusCode::SERVICE_UNAVAILABLE;
        message = format!("Computation with id {} does not exist", wrong.comp_id);
        output_json = true;
    } else if let Some(wrong) = err.find::<InternalError>() {
        code = StatusCode::INTERNAL_SERVER_ERROR;
        message = wrong.msg.clone();
    } else if let Some(wrong) = err.find::<UnknownAlgorithm>() {
        code = StatusCode::BAD_REQUEST;
        message = format!("Unknown algorithm with id {}", wrong.algorithm);
    } else {
        // We should have expected this... Just log and say its a 500
        eprintln!("unhandled rejection: {:?}", err);
        code = StatusCode::INTERNAL_SERVER_ERROR;
        message = "UNHANDLED_REJECTION".to_string();
    }

    if !output_json {
        let mut v = Vec::new();
        v.write_u32::<BigEndian>(code.as_u16() as u32).unwrap();
        put_varlen(&mut v, message.len() as u32);
        v.extend_from_slice(message.as_bytes());
        Ok(warp::reply::with_status(v, code))
    } else {
        let body = serde_json::json!({
            "error": true,
            "errorCode": code.as_u16(),
            "errorMessage": message
        });
        let v = serde_json::to_vec(&body).expect("Should be serializable");
        Ok(warp::reply::with_status(v, code))
    }
}

fn check_graph(
    graph: &Graph,
    graph_number: u32,
    edges_must_be_sealed: bool,
) -> Result<(), Rejection> {
    if graph.dropped {
        return Err(warp::reject::custom(GraphNotFound {
            number: graph_number,
        }));
    }
    if !graph.vertices_sealed {
        return Err(warp::reject::custom(GraphVerticesNotSealed {
            number: graph_number,
        }));
    }
    if edges_must_be_sealed {
        if !graph.edges_sealed {
            return Err(warp::reject::custom(GraphEdgesNotSealed {
                number: graph_number,
            }));
        }
    } else {
        if graph.edges_sealed {
            return Err(warp::reject::custom(GraphEdgesSealed {
                number: graph_number,
            }));
        }
    }
    Ok(())
}
