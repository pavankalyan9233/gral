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
    pub vertices: HashMap<u64, u64>,

    // key is the key of the vertex, value is the exceptional hash
    pub exceptions: HashMap<String, u64>,

    // Maps indices of vertices to their names, not necessarily used:
    pub keys: Vec<String>,

    // Additional data for vertices:
    pub vertex_data: Vec<u8>,
    pub vertex_data_offsets: Vec<u64>,

    // Edges as from/to tuples:
    pub edges: Vec<Edge>,

    // Additional data for vertices:
    pub edge_data: Vec<u8>,

    // store keys?
    pub store_keys: bool,

    // dropped indicates that the graph is no longer there
    pub dropped: bool,

    // sealed?
    pub vertices_sealed: bool,
    pub edges_sealed: bool,
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

    pub fn clear(&mut self) {
        // TODO: implement by clearing most structures
        self.vertices.clear();
        self.exceptions.clear();
        self.keys.clear();
        self.vertex_data.clear();
        self.vertex_data.push(0); // use first byte, so that all
                                  // offsets in here are positive!
        self.vertex_data_offsets.clear();
        self.edges.clear();
        self.edge_data.clear();
        self.vertices_sealed = false;
        self.edges_sealed = false;
        self.dropped = true;
    }
}
