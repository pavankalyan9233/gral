use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use warp::Filter;

pub struct Edge {
    from: u64,        // index of vertex
    to: u64,          // index of vertex
    data_offset: u64, // offset into edge_data
}

pub struct Graph {
    // key is the hash of the vertex, value is the index, high bit
    // indicates a collision
    vertices: HashMap<u64, u64>,

    // key is the key of the vertex, value is the exceptional hash
    exceptions: HashMap<String, u64>,

    // Maps indices of vertices to their names, not necessarily used:
    keys: Vec<String>,

    // Additional data for vertices:
    vertex_data: Vec<u8>,
    vertex_data_offsets: Vec<u64>,

    // Edges as from/to tuples:
    edges: Vec<Edge>,

    // Additional data for vertices:
    edge_data: Vec<u8>,

    // store keys?
    store_keys: bool,

    // dropped indicates that the graph is no longer there
    pub dropped: bool,

    // sealed?
    vertices_sealed: bool,
    edges_sealed: bool,
}

pub struct Graphs {
    pub list: Vec<Arc<Mutex<Graph>>>,
}

pub fn with_graphs(
    graphs: Arc<Mutex<Graphs>>,
) -> impl Filter<Extract = (Arc<Mutex<Graphs>>,), Error = Infallible> + Clone {
    warp::any().map(move || graphs.clone())
}

impl Graph {
    pub fn new(store_keys: bool, _bits_for_hash: u8) -> Arc<Mutex<Graph>> {
        Arc::new(Mutex::new(Graph {
            vertices: HashMap::new(),
            exceptions: HashMap::new(),
            keys: vec![],
            vertex_data: vec![],
            vertex_data_offsets: vec![],
            edges: vec![],
            edge_data: vec![],
            store_keys,
            dropped: false,
            vertices_sealed: false,
            edges_sealed: false,
        }))
    }
}
