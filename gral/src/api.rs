use crate::computations::{with_computations, Computation, Computations};
use crate::conncomp::{strongly_connected_components, weakly_connected_components};
use crate::graphs::{with_graphs, Graph, Graphs, KeyOrHash, VertexHash, VertexIndex};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use bytes::Bytes;
use log::info;
use rand::Rng;
use serde::{Deserialize, Serialize};
//use serde_json::Value;
use std::collections::HashMap;
use std::convert::Infallible;
use std::io::{BufRead, Cursor};
use std::ops::Deref;
use std::str;
use std::sync::{Arc, Mutex, RwLock};
use std::time::SystemTime;
use tokio::task::JoinSet;
use warp::{http::StatusCode, reject, Filter, Rejection};
use xxhash_rust::xxh3::xxh3_64_with_seed;

/// The following function puts together the filters for the API.
/// To this end, it relies on the following async functions below.
pub fn api_filter(
    graphs: Arc<Mutex<Graphs>>,
    computations: Arc<Mutex<Computations>>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    let create = warp::path!("v1" / "create")
        .and(warp::post())
        .and(with_graphs(graphs.clone()))
        .and(warp::body::bytes())
        .and_then(api_create);
    let drop = warp::path!("v1" / "dropGraph")
        .and(warp::put())
        .and(with_graphs(graphs.clone()))
        .and(warp::body::bytes())
        .and_then(api_drop);
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
    let get_progress = warp::path!("v1" / "getProgress")
        .and(warp::put())
        .and(with_computations(computations.clone()))
        .and(warp::body::bytes())
        .and_then(api_get_progress);
    let get_results_by_vertices = warp::path!("v1" / "getResultsByVertices")
        .and(warp::put())
        .and(with_computations(computations.clone()))
        .and(warp::body::bytes())
        .and_then(api_get_results_by_vertices);
    let drop_computation = warp::path!("v1" / "dropComputation")
        .and(warp::put())
        .and(with_computations(computations.clone()))
        .and(warp::body::bytes())
        .and_then(api_drop_computation);
    let compute = warp::path!("v1" / "compute")
        .and(warp::post())
        .and(with_graphs(graphs.clone()))
        .and(with_computations(computations.clone()))
        .and(warp::body::bytes())
        .and_then(api_compute);
    let get_arangodb_graph = warp::path!("v1" / "getArangoDBGraph")
        .and(warp::post())
        .and(with_graphs(graphs.clone()))
        .and(warp::body::bytes())
        .and_then(api_get_arangodb_graph);

    create
        .or(drop)
        .or(vertices)
        .or(seal_vertices)
        .or(edges)
        .or(seal_edges)
        .or(get_progress)
        .or(get_results_by_vertices)
        .or(drop_computation)
        .or(compute)
        .or(get_arangodb_graph)
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
            *g = Graph::new(store_keys != 0, 64);
            found = true;
            break;
        }
        index += 1;
    }
    // or else append to the end:
    if !found {
        index = graphs.list.len() as u32;
        graphs.list.push(Graph::new(store_keys != 0, 64));
    }
    // By now, index is always set to some sensible value!

    println!("Have created graph with number {}!", index);

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
async fn api_drop(graphs: Arc<Mutex<Graphs>>, bytes: Bytes) -> Result<Vec<u8>, Rejection> {
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
        graphs.list[graph_number as usize] = Graph::new(false, 64);
        let mut graph = graphs.list[graph_number as usize].write().unwrap();
        graph.dropped = true; // Mark unused
    }

    println!("Have dropped graph {}!", graph_number);

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
async fn api_compute(
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
                        println!("Indexing edges by from...");
                        graph.index_edges(true, false);
                    }
                }
                let graph = graph_arc.read().unwrap();
                strongly_connected_components(&graph)
            }
            _ => std::unreachable!(),
        };
        println!("Found {} connected components.", nr);
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

/// This function gets progress of a computation.
async fn api_get_progress(
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
async fn api_drop_computation(
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
struct CollectionInfo {
    name: String,
    fields: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GetArangoDBGraphRequest {
    client_id: String,
    endpoints: Vec<String>,
    use_tls: bool,
    database: String,
    vertex_collections: Vec<CollectionInfo>,
    edge_collections: Vec<CollectionInfo>,
    username: String,
    password: String,
    jwt: String,
    parallelism: u32,
    index_edges: u32,
    bits_for_hash: u32,
    store_keys: bool,
    batch_size: u32,
    prefetch_count: u32,
    dbserver_parallelism: u32,
}

async fn api_get_arangodb_graph(
    _graphs: Arc<Mutex<Graphs>>,
    bytes: Bytes,
) -> Result<Vec<u8>, Rejection> {
    let parsed: serde_json::Result<GetArangoDBGraphRequest> = serde_json::from_slice(&bytes[..]);
    if let Err(e) = parsed {
        return Err(warp::reject::custom(CannotParseJSON { msg: e.to_string() }));
    }
    let mut body = parsed.unwrap();
    if body.endpoints.is_empty() {
        return Err(warp::reject::custom(GetFromArangoDBFailed {
            msg: "no endpoints given".to_string(),
        }));
    }
    // Set a few sensible defaults:
    if body.batch_size == 0 {
        body.batch_size = 400000;
    }
    if body.prefetch_count == 0 {
        body.prefetch_count = 5;
    }
    if body.dbserver_parallelism == 0 {
        body.dbserver_parallelism = 5;
    }

    // Fetch from ArangoDB:
    let graph = fetch_graph_from_arangodb(&body).await;
    match graph {
        Err(e) => Err(warp::reject::custom(GetFromArangoDBFailed {
            msg: e.to_string(),
        })),
        Ok(_graph) => {
            let v: Vec<u8> = vec![];
            Ok(v)
        }
    }
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
    } else if let Some(wrong) = err.find::<ComputationNotYetFinished>() {
        code = StatusCode::SERVICE_UNAVAILABLE;
        message = format!("Computation with id {} does not exist", wrong.comp_id);
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

#[derive(Debug, Serialize, Deserialize)]
struct ShardLocation {
    leader: String,
    followers: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct CollectionDistribution {
    plan: HashMap<String, ShardLocation>,
    current: HashMap<String, ShardLocation>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ShardDistribution {
    error: bool,
    code: i32,
    results: HashMap<String, CollectionDistribution>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ArangoDBError {
    error: bool,
    error_num: i32,
    error_message: String,
    code: i32,
}

fn build_client(use_tls: bool) -> Result<reqwest::Client, String> {
    let builder = reqwest::Client::builder();
    if use_tls {
        let client = builder
            .use_rustls_tls()
            .min_tls_version(reqwest::tls::Version::TLS_1_2)
            .danger_accept_invalid_certs(true)
            .https_only(true)
            .build();
        if let Err(err) = client {
            return Err(format!("Error message from request builder: {:?}", err));
        }
        Ok(client.unwrap())
    } else {
        let client = builder
            //.connection_verbose(true)
            //.http2_prior_knowledge()
            .build();
        if let Err(err) = client {
            return Err(format!("Error message from request builder: {:?}", err));
        }
        Ok(client.unwrap())
    }
}

// This function handles an HTTP response from ArangoDB, including
// connection errors, bad status codes and body parsing. The template
// type is the type of the expected body in the good case.
async fn handle_arangodb_response_with_parsed_body<T>(
    resp: reqwest::Result<reqwest::Response>,
    expected_code: reqwest::StatusCode,
) -> Result<T, String>
where
    T: serde::de::DeserializeOwned,
{
    if let Err(err) = resp {
        return Err(err.to_string());
    }
    let resp = resp.unwrap();
    let status = resp.status();
    if status != expected_code {
        let err = resp.json::<ArangoDBError>().await;
        match err {
            Err(e) => {
                return Err(format!(
                    "Could not parse error body, error: {}, status code: {:?}",
                    e.to_string(),
                    status,
                ));
            }
            Ok(e) => {
                return Err(format!(
                    "Error code: {}, message: {}, HTTP code: {}",
                    e.error_num, e.error_message, e.code
                ));
            }
        }
    }
    let body = resp.json::<T>().await;
    body.map_err(|e| format!("Could not parse response body, error: {}", e.to_string()))
}

// This function handles an empty HTTP response from ArangoDB, including
// connection errors and bad status codes.
async fn handle_arangodb_response(
    resp: reqwest::Result<reqwest::Response>,
    code_test: fn(code: reqwest::StatusCode) -> bool,
) -> Result<reqwest::Response, String> {
    if let Err(err) = resp {
        return Err(err.to_string());
    }
    let resp = resp.unwrap();
    let status = resp.status();
    if !code_test(status) {
        let err = resp.json::<ArangoDBError>().await;
        match err {
            Err(e) => {
                return Err(format!(
                    "Could not parse error body, error: {}, status code: {:?}",
                    e.to_string(),
                    status,
                ));
            }
            Ok(e) => {
                return Err(format!(
                    "Error code: {}, message: {}, HTTP code: {}",
                    e.error_num, e.error_message, e.code
                ));
            }
        }
    }
    Ok(resp)
}

// A ShardMap maps dbserver names to lists of shards for which these dbservers
// are leaders. We will have one for the vertices and one for the edges.

type ShardMap = HashMap<String, Vec<String>>;

fn compute_shard_map(sd: &ShardDistribution, coll_list: &Vec<String>) -> Result<ShardMap, String> {
    let mut result: ShardMap = HashMap::new();
    for c in coll_list.into_iter() {
        match sd.results.get(c) {
            None => {
                return Err(format!("collection {} not found in shard distribution", c));
            }
            Some(coll_dist) => {
                // Keys of coll_dist are the shards, value has leader:
                for (shard, location) in &(coll_dist.plan) {
                    let leader = &(location.leader);
                    match result.get_mut(leader) {
                        None => {
                            result.insert(leader.clone(), vec![shard.clone()]);
                        }
                        Some(list) => {
                            list.push(shard.clone());
                        }
                    }
                }
            }
        }
    }
    Ok(result)
}

#[derive(Debug, Clone)]
struct DBServerInfo {
    dbserver: String,
    dump_id: String,
}

async fn get_all_shard_data(
    req: &GetArangoDBGraphRequest,
    shard_map: &ShardMap,
    result_channel: std::sync::mpsc::Sender<Bytes>,
) -> Result<(), String> {
    let begin = SystemTime::now();

    let client = build_client(req.use_tls)?;

    let make_url =
        |path: &str| -> String { req.endpoints[0].clone() + "/_db/" + &req.database + path };

    // Start a single dump context on all involved dbservers, we can do
    // this sequentially, since it is not performance critical, we can
    // also use the same HTTP client and the same first endpoint:
    let mut dbservers: Vec<DBServerInfo> = vec![];
    let mut error_happened = false;
    let mut error: String = "".into();
    for (server, shard_list) in shard_map.iter() {
        let url = make_url(&format!("/_api/dump/start?dbserver={}", server));
        let body = DumpStartBody {
            batch_size: req.batch_size,
            prefetch_count: req.prefetch_count,
            parallelism: req.dbserver_parallelism,
            shards: shard_list.clone(),
        };
        let body_v =
            serde_json::to_vec::<DumpStartBody>(&body).expect("could not serialize DumpStartBody");
        let resp = client.post(url).body(body_v).send().await;
        let r = handle_arangodb_response(resp, |c| c == StatusCode::NO_CONTENT).await;
        if let Err(rr) = r {
            error = rr;
            error_happened = true;
            break;
        }
        let r = r.unwrap();
        let headers = r.headers();
        if let Some(id) = headers.get("X-Arango-Dump-Id") {
            if let Ok(id) = id.to_str() {
                dbservers.push(DBServerInfo {
                    dbserver: server.clone(),
                    dump_id: id.to_owned(),
                });
            }
        }
        println!("Started dbserver {}", server);
    }

    let client_clone_for_cleanup = client.clone();
    let cleanup = |dbservers: Vec<DBServerInfo>| async move {
        println!("Doing cleanup...");
        for dbserver in dbservers.iter() {
            let url = make_url(&format!(
                "/_api/dump/{}?dbserver={}",
                dbserver.dump_id, dbserver.dbserver
            ));
            let resp = client_clone_for_cleanup.delete(url).send().await;
            let r = handle_arangodb_response(resp, |c| c == StatusCode::OK).await;
            if let Err(rr) = r {
                println!(
                    "An error in cancelling a dump context occurred, dbserver: {}, error: {}",
                    dbserver.dbserver, rr
                );
                // Otherwise ignore the error, this is just a cleanup!
            }
        }
    };

    if error_happened {
        // We need to cancel all dump contexts which we did get successfully:
        cleanup(dbservers).await;
        return Err(error);
    }

    // We want to start the same number of tasks for each dbserver, each of
    // them will send next requests until no more data arrives

    #[derive(Debug)]
    struct TaskInfo {
        dbserver: DBServerInfo,
        current_batch_id: u64,
        last_batch_id: Option<u64>,
        id: u64,
    }

    let par_per_dbserver = (req.parallelism as usize + dbservers.len() - 1) / dbservers.len();
    let mut task_set = JoinSet::new();
    let mut endpoints_round_robin: usize = 0;
    for i in 0..par_per_dbserver {
        for dbserver in &dbservers {
            let mut task_info = TaskInfo {
                dbserver: dbserver.clone(),
                current_batch_id: i as u64,
                last_batch_id: None,
                id: i as u64,
            };
            //let client_clone = client.clone(); // the clones will share
            //                                   // the connection pool
            let client_clone = build_client(req.use_tls)?;
            let endpoint_clone = req.endpoints[endpoints_round_robin].clone();
            endpoints_round_robin += 1;
            if endpoints_round_robin >= req.endpoints.len() {
                endpoints_round_robin = 0;
            }
            let database_clone = req.database.clone();
            let result_channel_clone = result_channel.clone();
            task_set.spawn(async move {
                loop {
                    let mut url = format!(
                        "{}/_db/{}/_api/dump/next/{}?dbserver={}&batchId={}",
                        endpoint_clone,
                        database_clone,
                        task_info.dbserver.dump_id,
                        task_info.dbserver.dbserver,
                        task_info.current_batch_id
                    );
                    if let Some(last) = task_info.last_batch_id {
                        url.push_str(&format!("&lastBatch={}", last));
                    }
                    let start = SystemTime::now();
                    info!(
                        "{:?} Sending post request... {} {} {}",
                        start.duration_since(begin),
                        task_info.id,
                        task_info.dbserver.dbserver,
                        task_info.current_batch_id
                    );
                    let resp = client_clone.post(url).send().await;
                    let resp = handle_arangodb_response(resp, |c| {
                        c == StatusCode::OK || c == StatusCode::NO_CONTENT
                    })
                    .await?;
                    let end = SystemTime::now();
                    let dur = end.duration_since(start).unwrap();
                    let h = resp.headers().get("X-Arango-Dump_Block_Counts");
                    if let Some(hh) = h {
                        println!("Dump_block_counts: {:?}", hh);
                    }
                    if resp.status() == StatusCode::NO_CONTENT {
                        // Done, cleanup will be done later
                        info!(
                            "{:?} Received final post response... {} {} {} {:?}",
                            end.duration_since(begin),
                            task_info.id,
                            task_info.dbserver.dbserver,
                            task_info.current_batch_id,
                            dur
                        );
                        return Ok::<(), String>(());
                    }
                    // Now the result was OK and the body is JSONL
                    task_info.last_batch_id = Some(task_info.current_batch_id);
                    task_info.current_batch_id += par_per_dbserver as u64;
                    let body = resp
                        .bytes()
                        .await
                        .map_err(|e| format!("Error in body: {:?}", e))?;
                    result_channel_clone
                        .send(body)
                        .expect("Could not send to channel!");
                }
            });
        }
    }
    while let Some(res) = task_set.join_next().await {
        let r = res.unwrap();
        match r {
            Ok(_x) => {
                println!("Got OK Result");
            }
            Err(msg) => {
                println!("Got error result: {}", msg);
            }
        }
    }
    cleanup(dbservers).await;
    println!("Done cleanup and channel is closed!");
    Ok(())
    // We drop the result_channel when we leave the function.
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct DumpStartBody {
    batch_size: u32,
    prefetch_count: u32,
    parallelism: u32,
    shards: Vec<String>,
}

async fn fetch_graph_from_arangodb(
    req: &GetArangoDBGraphRequest,
) -> Result<Arc<RwLock<Graph>>, String> {
    let client = build_client(req.use_tls)?;

    let make_url =
        |path: &str| -> String { req.endpoints[0].clone() + "/_db/" + &req.database + path };

    // First ask for the shard distribution:
    assert!(!req.endpoints.is_empty()); // checked outside!
    let url = make_url("/_admin/cluster/shardDistribution");
    let resp = client.get(url).send().await;
    let shard_dist =
        handle_arangodb_response_with_parsed_body::<ShardDistribution>(resp, StatusCode::OK)
            .await?;

    // Compute which shard we must get from which dbserver, we do vertices
    // and edges right away to be able to error out early:
    let vertex_coll_list = req
        .vertex_collections
        .iter()
        .map(|ci| -> String { ci.name.clone() })
        .collect();
    let vertex_map = compute_shard_map(&shard_dist, &vertex_coll_list)?;
    let edge_coll_list = req
        .edge_collections
        .iter()
        .map(|ci| -> String { ci.name.clone() })
        .collect();
    let edge_map = compute_shard_map(&shard_dist, &edge_coll_list)?;

    // Generate a graph object:
    let graph_arc = Graph::new(true, 64);

    // Let's first get the vertices:
    {
        let (sender, receiver) = std::sync::mpsc::channel::<Bytes>();
        let consumer = std::thread::spawn(move || {
            while let Ok(body) = receiver.recv() {
                //println!("Processing batch, response size {}...", body.len());
            }
        });
        get_all_shard_data(req, &vertex_map, sender).await?;
        let _guck = consumer.join();
        let mut graph = graph_arc.write().unwrap();
        graph.seal_vertices();
    }

    // And now the edges:
    {
        let (sender, receiver) = std::sync::mpsc::channel::<Bytes>();
        let consumer = std::thread::spawn(move || {
            while let Ok(body) = receiver.recv() {
                //println!("Processing batch, response size {}...", body.len());
            }
        });
        get_all_shard_data(req, &edge_map, sender).await?;
        let _guck = consumer.join();

        let mut graph = graph_arc.write().unwrap();
        graph.seal_edges();
    }

    println!("All successfully transferred...");
    Ok(graph_arc)
}
