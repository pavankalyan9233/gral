use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use bytes::Bytes;
use http::Error;
use log::info;
use std::convert::Infallible;
use std::io::{BufRead, Cursor};
use std::ops::Deref;
use std::sync::{Arc, Mutex, RwLock};
use warp::{http::Response, http::StatusCode, reject, Filter, Rejection};
use xxhash_rust::xxh3::xxh3_64_with_seed;

use crate::args::GralArgs;
use crate::computations::{with_computations, Computation, Computations, ConcreteComputation};
use crate::conncomp::{strongly_connected_components, weakly_connected_components};
use crate::graphs::{encode_id, with_graphs, Graph, Graphs, KeyOrHash, VertexHash, VertexIndex};
use crate::VERSION;

/// The following function puts together the filters for the API.
/// To this end, it relies on the following async functions below.
pub fn api_bin_filter(
    graphs: Arc<Mutex<Graphs>>,
    computations: Arc<Mutex<Computations>>,
    _args: Arc<Mutex<GralArgs>>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    let version_bin = warp::path!("v1" / "versionBinary")
        .and(warp::get())
        .map(version_bin);
    let create_bin = warp::path!("v1" / "create")
        .and(warp::post())
        .and(with_graphs(graphs.clone()))
        .and(warp::body::bytes())
        .and_then(api_create_bin);
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
    let compute_bin = warp::path!("v1" / "compute-binary")
        .and(warp::post())
        .and(with_graphs(graphs.clone()))
        .and(with_computations(computations.clone()))
        .and(warp::body::bytes())
        .and_then(api_compute_bin);
    let get_progress_bin = warp::path!("v1" / "getProgress")
        .and(warp::put())
        .and(with_computations(computations.clone()))
        .and(warp::body::bytes())
        .and_then(api_get_progress_bin);
    let drop_computation_bin = warp::path!("v1" / "dropComputationBinary")
        .and(warp::put())
        .and(with_computations(computations.clone()))
        .and(warp::body::bytes())
        .and_then(api_drop_computation_bin);
    let get_results_by_vertices = warp::path!("v1" / "getResultsByVertices")
        .and(warp::put())
        .and(with_computations(computations.clone()))
        .and(warp::body::bytes())
        .and_then(api_get_results_by_vertices);
    version_bin
        .or(create_bin)
        .or(drop_bin)
        .or(vertices)
        .or(seal_vertices)
        .or(edges)
        .or(seal_edges)
        .or(compute_bin)
        .or(get_progress_bin)
        .or(drop_computation_bin)
        .or(get_results_by_vertices)
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

/// This async function implements the "create graph" API call.
async fn api_create_bin(graphs: Arc<Mutex<Graphs>>, bytes: Bytes) -> Result<Vec<u8>, Rejection> {
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

    let graph_arc = Graph::new(store_keys != 0, 64, 0);
    let graph_id = graphs.register(graph_arc);

    info!("Have created graph with id {}!", encode_id(graph_id));

    // Write response:
    let mut v = Vec::new();
    v.write_u64::<BigEndian>(client_id).unwrap();
    v.write_u64::<BigEndian>(graph_id).unwrap();
    v.write_u8(bits_for_hash).unwrap();
    Ok(v)
}

/// This async function implements the "drop graph" API call.
async fn api_drop_bin(graphs: Arc<Mutex<Graphs>>, bytes: Bytes) -> Result<Vec<u8>, Rejection> {
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
    let graph_id = reader.read_u64::<BigEndian>().unwrap();

    let mut graphs = graphs.lock().unwrap();

    let graph_arc = graphs.list.get(&graph_id);
    if graph_arc.is_none() {
        return Err(warp::reject::custom(GraphNotFound { number: graph_id }));
    }

    // The following will automatically free graph if no longer used by
    // a computation:
    graphs.list.remove(&graph_id);
    info!("Have dropped graph {}!", graph_id);

    // Write response:
    let mut v = Vec::new();
    v.write_u64::<BigEndian>(client_id).unwrap();
    v.write_u64::<BigEndian>(graph_id).unwrap();
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

pub fn put_key_or_hash(out: &mut Vec<u8>, koh: &KeyOrHash) {
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
pub fn parse_key_or_hash(reader: &mut Cursor<Vec<u8>>) -> Result<KeyOrHash, Rejection> {
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

pub fn get_graph(
    graphs: &Arc<Mutex<Graphs>>,
    graph_id: u64,
) -> Result<Arc<RwLock<Graph>>, Rejection> {
    // Lock list of graphs via their mutex:
    let graphs = graphs.lock().unwrap();
    let graph = graphs.list.get(&graph_id);
    if graph.is_none() {
        return Err(warp::reject::custom(GraphNotFound { number: graph_id }));
    }
    Ok(graph.unwrap().clone())
}

pub fn check_graph(
    graph: &Graph,
    graph_id: u64,
    edges_must_be_sealed: bool,
) -> Result<(), Rejection> {
    if !graph.vertices_sealed {
        return Err(warp::reject::custom(GraphVerticesNotSealed {
            number: graph_id,
        }));
    }
    if edges_must_be_sealed {
        if !graph.edges_sealed {
            return Err(warp::reject::custom(GraphEdgesNotSealed {
                number: graph_id,
            }));
        }
    } else {
        if graph.edges_sealed {
            return Err(warp::reject::custom(GraphEdgesSealed { number: graph_id }));
        }
    }
    Ok(())
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
    let graph_id = reader.read_u64::<BigEndian>().unwrap();
    let number = reader.read_u32::<BigEndian>().unwrap();

    let graph_arc = get_graph(&graphs, graph_id)?;

    // Lock graph:
    let mut graph = graph_arc.write().unwrap();

    if graph.vertices_sealed {
        return Err(warp::reject::custom(GraphVerticesSealed {
            number: graph_id,
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
        graph.insert_vertex(
            i,
            hash,
            key,
            data,
            None,
            &mut exceptional,
            &mut exceptional_keys,
        )
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
    let graph_id = reader.read_u64::<BigEndian>().unwrap();

    let graph_arc = get_graph(&graphs, graph_id)?;

    // Write lock graph:
    let mut graph = graph_arc.write().unwrap();

    if graph.vertices_sealed {
        return Err(warp::reject::custom(GraphVerticesSealed {
            number: graph_id,
        }));
    }

    graph.seal_vertices();

    // Write response:
    let mut v = Vec::new();
    // TODO: handle errors!
    v.reserve(20);
    v.write_u64::<BigEndian>(client_id).unwrap();
    v.write_u64::<BigEndian>(graph_id).unwrap();
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
    if bytes.len() < 20 {
        return Err(warp::reject::custom(TooShortBodyLength {
            found: bytes.len(),
            expected_at_least: 20,
        }));
    }

    // Parse body and extract integers:
    // (Note that we have checked the buffer length and so these cannot
    // fail! Therefore unwrap() is OK here.)
    let mut reader = Cursor::new(bytes.to_vec());
    let client_id = reader.read_u64::<BigEndian>().unwrap();
    let graph_id = reader.read_u64::<BigEndian>().unwrap();
    let number = reader.read_u32::<BigEndian>().unwrap();

    let graph_arc = get_graph(&graphs, graph_id)?;

    // Write lock graph:
    let mut graph = graph_arc.write().unwrap();
    check_graph(graph.deref(), graph_id, false)?;

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
    if bytes.len() != 20 {
        return Err(warp::reject::custom(WrongBodyLength {
            found: bytes.len(),
            expected: 20,
        }));
    }

    // Parse body and extract integers:
    // (Note that we have checked the buffer length and so these cannot
    // fail! Therefore unwrap() is OK here.)
    let mut reader = Cursor::new(bytes.to_vec());
    let client_id = reader.read_u64::<BigEndian>().unwrap();
    let graph_id = reader.read_u64::<BigEndian>().unwrap();
    let index_edges = reader.read_u32::<BigEndian>().unwrap();

    let graph_arc = get_graph(&graphs, graph_id)?;

    // Write lock graph:
    let mut graph = graph_arc.write().unwrap();
    check_graph(graph.deref(), graph_id, false)?;

    graph.seal_edges();
    if index_edges != 0 {
        graph.index_edges(index_edges & 1 != 0, index_edges & 2 != 0);
    }

    // Write response:
    let mut v = Vec::new();
    // TODO: handle errors!
    v.reserve(32);
    v.write_u64::<BigEndian>(client_id).unwrap();
    v.write_u64::<BigEndian>(graph_id).unwrap();
    v.write_u64::<BigEndian>(graph.number_of_vertices())
        .unwrap();
    v.write_u64::<BigEndian>(graph.number_of_edges()).unwrap();
    Ok(v)
}

/// This function triggers a computation:
async fn api_compute_bin(
    graphs: Arc<Mutex<Graphs>>,
    computations: Arc<Mutex<Computations>>,
    bytes: Bytes,
) -> Result<Vec<u8>, Rejection> {
    // Handle wrong length:
    if bytes.len() != 20 {
        return Err(warp::reject::custom(WrongBodyLength {
            found: bytes.len(),
            expected: 20,
        }));
    }

    // Parse body and extract integers:
    // (Note that we have checked the buffer length and so these cannot
    // fail! Therefore unwrap() is OK here.)
    let mut reader = Cursor::new(bytes.to_vec());
    let client_id = reader.read_u64::<BigEndian>().unwrap();
    let graph_id = reader.read_u64::<BigEndian>().unwrap();
    let algorithm = reader.read_u32::<BigEndian>().unwrap();

    let graph_arc = get_graph(&graphs, graph_id)?;

    {
        // Check graph:
        let graph = graph_arc.read().unwrap();
        check_graph(graph.deref(), graph_id, true)?;
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

    let comp_id: u64;
    {
        let mut comps = computations.lock().unwrap();
        comp_id = comps.register(comp_arc.clone());
    }
    // Launch background thread for this computation
    std::thread::spawn(move || {
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
    v.reserve(28);
    v.write_u64::<BigEndian>(client_id).unwrap();
    v.write_u64::<BigEndian>(graph_id).unwrap();
    v.write_u64::<BigEndian>(comp_id).unwrap();
    v.write_u32::<BigEndian>(algorithm).unwrap();
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

/// An error object, which is used when the body size is unexpected.
#[derive(Debug)]
pub struct WrongBodyLength {
    pub found: usize,
    pub expected: usize,
}
impl reject::Reject for WrongBodyLength {}

/// An error object, which is used when the body size is too short.
#[derive(Debug)]
pub struct TooShortBodyLength {
    pub found: usize,
    pub expected_at_least: usize,
}
impl reject::Reject for TooShortBodyLength {}

/// An error object, which is used when a (numbered) graph is not found.
#[derive(Debug)]
pub struct GraphNotFound {
    pub number: u64,
}
impl reject::Reject for GraphNotFound {}

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
pub struct ComputationNotYetFinished {
    pub comp_id: u64,
}
impl reject::Reject for ComputationNotYetFinished {}

/// An error object, which is used when a graph's vertices are already
/// sealed and the client wants to add more vertices.
#[derive(Debug)]
struct GraphVerticesSealed {
    pub number: u64,
}
impl reject::Reject for GraphVerticesSealed {}

/// An error object, which is used when a graph's vertices are not yet
/// sealed and the client wants to seal the edges.
#[derive(Debug)]
struct GraphVerticesNotSealed {
    pub number: u64,
}
impl reject::Reject for GraphVerticesNotSealed {}

/// An error object, which is used when a graph's edges are already
/// sealed and the client wants to seal them again.
#[derive(Debug)]
struct GraphEdgesSealed {
    pub number: u64,
}
impl reject::Reject for GraphEdgesSealed {}

/// An error object, which is used when a graph's edges are not yet
/// sealed and the client wants to do something for which this is needed.
#[derive(Debug)]
struct GraphEdgesNotSealed {
    pub number: u64,
}
impl reject::Reject for GraphEdgesNotSealed {}

/// An error object, which is used if a load job cannot dump data.
#[derive(Debug)]
pub struct CannotDumpVertexData {
    pub comp_id: u64,
}
impl reject::Reject for CannotDumpVertexData {}

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
        message = format!("Computation with id {:x} does not exist", wrong.comp_id);
        output_json = true;
    } else if let Some(wrong) = err.find::<ComputationNotYetFinished>() {
        code = StatusCode::SERVICE_UNAVAILABLE;
        message = format!("Computation with id {} does not exist", wrong.comp_id);
        output_json = true;
    } else if let Some(wrong) = err.find::<CannotDumpVertexData>() {
        code = StatusCode::BAD_REQUEST;
        message = format!("Job with id {} cannot dump vertex data", wrong.comp_id);
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
