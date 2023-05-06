use crate::computations::{with_computations, Computation, Computations};
use crate::conncomp::weakly_connected_components;
use crate::graphs::{with_graphs, Graph, Graphs, VertexHash, VertexIndex};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use bytes::Bytes;
use rand::Rng;
use std::convert::Infallible;
use std::io::{BufRead, Cursor};
use std::str;
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
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
    let weakly_connected_components = warp::path!("v1" / "weaklyConnectedComponents")
        .and(warp::post())
        .and(with_graphs(graphs.clone()))
        .and(with_computations(computations.clone()))
        .and(warp::body::bytes())
        .and_then(api_weakly_connected_components);
    let get_progress = warp::path!("v1" / "getProgress")
        .and(warp::put())
        .and(with_computations(computations.clone()))
        .and(warp::body::bytes())
        .and_then(api_get_progress);

    create
        .or(drop)
        .or(vertices)
        .or(seal_vertices)
        .or(edges)
        .or(seal_edges)
        .or(weakly_connected_components)
        .or(get_progress)
}

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
struct ComputationNotFound {}
impl reject::Reject for ComputationNotFound {}

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

/// This async function implements the "drop graph" API call.
async fn api_drop(graphs: Arc<Mutex<Graphs>>, bytes: Bytes) -> Result<Vec<u8>, Rejection> {
    // Handle wrong length:
    if bytes.len() < 10 {
        return Err(warp::reject::custom(TooShortBodyLength {
            found: bytes.len(),
            expected_at_least: 10,
        }));
    }

    // Parse body and extract integers:
    // (Note that we have checked the buffer length and so these cannot
    // fail! Therefore unwrap() is OK here.)
    let mut reader = Cursor::new(bytes.to_vec());
    let client_id = reader.read_u64::<BigEndian>().unwrap();
    let graph_number = reader.read_u32::<BigEndian>().unwrap();

    println!("Dropping graph with number {}!", graph_number);

    let graph_arc = get_graph(&graphs, graph_number)?;

    // Lock graph:
    let mut graph = graph_arc.write().unwrap();

    if graph.dropped {
        return Err(warp::reject::custom(GraphNotFound {
            number: graph_number as u32,
        }));
    }

    graph.clear();
    graph.dropped = true;

    println!("Have dropped graph with number {}!", graph_number);

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

    // Collect exceptions and rejections:
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
    check_write_graph(&graph, graph_number, false)?;

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
    check_write_graph(&graph, graph_number, false)?;

    graph.seal_edges();

    let (nr, _) = weakly_connected_components(&graph);
    println!("Found {} weakly connected components.", nr);

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

pub struct WeaklyConnectedComponentsComputation {
    pub graph: Arc<RwLock<Graph>>,
    pub components: Option<Vec<u64>>,
    pub shall_stop: bool,
    pub number: u64,
}

impl Computation for WeaklyConnectedComponentsComputation {
    fn is_ready(&self) -> bool {
        self.components.is_some()
    }
    fn cancel(&mut self) {
        self.shall_stop = true;
    }
    fn dump_result(&self, out: &mut Vec<u8>) -> Result<(), String> {
        out.write_u8(8).unwrap();
        out.write_u64::<BigEndian>(self.number).unwrap();
        Ok(())
    }
    fn dump_vertex_results(
        &self,
        hashes: &Vec<VertexHash>,
        out: &mut Vec<u8>,
    ) -> Result<(), String> {
        let comps = self.components.as_ref();
        match comps {
            None => Err("Computation not yet finished.".to_string()),
            Some(result) => {
                let g = self.graph.read().unwrap();
                if g.store_keys {
                    for i in hashes.iter() {
                        let index = g.hash_to_index.get(i);
                        if let Some(ind) = index {
                            put_varlen(out, g.index_to_key[ind.to_u64() as usize].len() as u32);
                            out.extend_from_slice(&(g.index_to_key[ind.to_u64() as usize]));
                            put_varlen(out, 8);
                            out.write_u64::<BigEndian>(result[ind.to_u64() as usize])
                                .unwrap();
                        }
                    }
                } else {
                    for i in hashes.iter() {
                        let index = g.hash_to_index.get(i);
                        if let Some(ind) = index {
                            put_varlen(out, std::mem::size_of::<VertexHash>() as u32);
                            out.write_u64::<BigEndian>(i.to_u64()).unwrap();
                            put_varlen(out, 8);
                            out.write_u64::<BigEndian>(result[ind.to_u64() as usize])
                                .unwrap();
                        }
                    }
                }
                Ok(())
            }
        }
    }
}

/// This function triggers the computation of the weakly connected components
async fn api_weakly_connected_components(
    graphs: Arc<Mutex<Graphs>>,
    computations: Arc<Mutex<Computations>>,
    bytes: Bytes,
) -> Result<Vec<u8>, Rejection> {
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

    {
        // Check graph:
        let graph = graph_arc.read().unwrap();
        check_read_graph(&graph, graph_number, true)?;
    }

    let comp_arc = Arc::new(Mutex::new(WeaklyConnectedComponentsComputation {
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
        let graph = graph_arc.read().unwrap();
        let (nr, components) = weakly_connected_components(&graph);
        println!("Found {} weakly connected components.", nr);
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
    v.write_u64::<BigEndian>(comp_id).unwrap();
    Ok(v)
}

/// This function seals the edges of a graph.
async fn api_get_progress(
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
    let graph_number = reader.read_u32::<BigEndian>().unwrap();
    let comp_id = reader.read_u64::<BigEndian>().unwrap();

    let comps = computations.lock().unwrap();
    let comp_arc = comps.list.get(&comp_id);
    match comp_arc {
        None => {
            return Err(warp::reject::custom(ComputationNotFound {}));
        }
        Some(comp_arc) => {
            let comp = comp_arc.lock().unwrap();

            // Write response:
            let mut v = Vec::new();
            // TODO: handle errors!
            v.reserve(20);
            v.write_u64::<BigEndian>(client_id).unwrap();
            v.write_u32::<BigEndian>(graph_number).unwrap();
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

// This function receives a `Rejection` and is responsible to convert
// this into a proper HTTP error with a body as designed.
pub async fn handle_errors(err: Rejection) -> Result<impl warp::Reply, Infallible> {
    let code;
    let message: String;

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
    } else if let Some(_) = err.find::<ComputationNotFound>() {
        code = StatusCode::NOT_FOUND;
        message = "Computation with given id does not exist".to_string();
    } else {
        // We should have expected this... Just log and say its a 500
        eprintln!("unhandled rejection: {:?}", err);
        code = StatusCode::INTERNAL_SERVER_ERROR;
        message = "UNHANDLED_REJECTION".to_string();
    }

    let mut v = Vec::new();
    v.write_u32::<BigEndian>(code.as_u16() as u32).unwrap();
    if message.len() < 128 {
        v.write_u8(message.len() as u8).unwrap();
    } else {
        v.write_u32::<BigEndian>((message.len() as u32) | 0x80000000)
            .unwrap();
    }
    v.reserve(message.len());
    for x in message.bytes() {
        v.push(x);
    }
    Ok(warp::reply::with_status(v, code))
}

fn check_write_graph(
    graph: &RwLockWriteGuard<Graph>,
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

fn check_read_graph(
    graph: &RwLockReadGuard<Graph>,
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
