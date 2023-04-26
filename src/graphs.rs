use rand::Rng;
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use warp::Filter;
use xxhash_rust::xxh3::xxh3_64_with_seed;

#[derive(Eq, Hash, PartialEq, Clone, Copy)]
pub struct VertexHash(u64);
impl VertexHash {
    pub fn new(x: u64) -> VertexHash {
        VertexHash(x)
    }
    pub fn to_u64(&self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy)]
pub struct VertexIndex(u64);
impl VertexIndex {
    pub fn new(x: u64) -> VertexIndex {
        VertexIndex(x)
    }
    pub fn to_u64(&self) -> u64 {
        self.0
    }
}

pub struct Edge {
    from: VertexIndex, // index of vertex
    to: VertexIndex,   // index of vertex
    data_offset: u64,  // offset into edge_data
}

pub struct Graph {
    // key is the hash of the vertex, value is the index, high bit
    // indicates a collision
    pub hash_to_index: HashMap<VertexHash, VertexIndex>,

    // key is the key of the vertex, value is the exceptional hash
    pub exceptions: HashMap<String, VertexHash>,

    // Maps indices of vertices to their names, not necessarily used:
    pub index_to_key: Vec<String>,

    // Additional data for vertices:
    pub vertex_data: Vec<u8>,
    pub vertex_data_offsets: Vec<u64>,

    // Edges as from/to tuples:
    pub edges: Vec<Edge>,

    // Additional data for vertices:
    pub edge_data: Vec<u8>,

    // Maps indices of vertices to offsets in edges by from:
    pub edge_index_by_from: Vec<u64>,

    // Edge index by from:
    pub edges_by_from: Vec<VertexIndex>,

    // Maps indices of vertices to offsets in edge index by to:
    pub edge_index_by_to: Vec<u64>,

    // Edge index by to:
    pub edges_by_to: Vec<VertexIndex>,

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
            hash_to_index: HashMap::new(),
            exceptions: HashMap::new(),
            index_to_key: vec![],
            vertex_data: vec![0],
            vertex_data_offsets: vec![],
            edges: vec![],
            edge_data: vec![0],
            edges_by_from: vec![],
            edge_index_by_from: vec![],
            edges_by_to: vec![],
            edge_index_by_to: vec![],
            store_keys,
            dropped: false,
            vertices_sealed: false,
            edges_sealed: false,
        }))
    }

    pub fn clear(&mut self) {
        self.hash_to_index.clear();
        self.exceptions.clear();
        self.index_to_key.clear();
        self.vertex_data.clear();
        self.vertex_data.push(0); // use first byte, so that all
                                  // offsets in here are positive!
        self.vertex_data_offsets.clear();
        self.edges.clear();
        self.edge_data.clear();
        self.edge_data.push(0);
        self.edge_index_by_from.clear();
        self.edges_by_from.clear();
        self.edge_index_by_to.clear();
        self.edges_by_to.clear();
        self.vertices_sealed = false;
        self.edges_sealed = false;
        self.dropped = true;
    }

    pub fn insert_vertex(
        &mut self,
        i: u32,
        hash: Option<VertexHash>,
        key: Option<String>,
        data: Option<Vec<u8>>,
        rejected: &mut Vec<u32>,
        exceptional: &mut Vec<(u32, VertexHash)>,
    ) {
        match hash {
            None => rejected.push(i),
            Some(h) => {
                // First detect a collision:
                let index = VertexIndex(self.vertex_data_offsets.len() as u64);
                let mut actual = h;
                if self.hash_to_index.contains_key(&h) {
                    if key.is_some() {
                        // This is a collision, we create a random alternative
                        // hash and report the collision:
                        let mut rng = rand::thread_rng();
                        loop {
                            actual = VertexHash(rng.gen::<u64>());
                            if let Some(_) = self.hash_to_index.get_mut(&actual) {
                                break;
                            }
                        }
                        let oi = self.hash_to_index.get_mut(&h).unwrap();
                        *oi = VertexIndex((*oi).0 | 0x800000000000000);
                        exceptional.push((i, actual));
                        if self.store_keys {
                            self.exceptions.insert(key.clone().unwrap(), actual);
                        }
                    } else {
                        // This is a duplicate hash without key, we must
                        // reject this:
                        rejected.push(i);
                        return;
                    }
                }
                // Will succeed:
                self.hash_to_index.insert(actual, index);
                if let Some(k) = key {
                    if self.store_keys {
                        self.index_to_key.push(k.to_string());
                    }
                }
                if let Some(d) = data {
                    // Insert data:
                    let pos = self.vertex_data.len() as u64;
                    self.vertex_data_offsets.push(pos);
                    for b in d.iter() {
                        self.vertex_data.push(*b);
                    }
                } else {
                    self.vertex_data_offsets.push(0);
                }
            }
        }
    }

    pub fn number_of_vertices(&self) -> u64 {
        self.vertex_data_offsets.len() as u64
    }

    pub fn number_of_edges(&self) -> u64 {
        self.edges.len() as u64
    }

    pub fn seal_vertices(&mut self) {
        self.vertices_sealed = true;
    }

    pub fn seal_edges(&mut self) {
        self.edges_sealed = true;
        // Idea:
        //   Sort edges by from
        //   Build edges_by_from and edge_index_by_from
        //   Sort edges by to
        //   Build edges_by_to and edge_index_by_to
    }

    pub fn hash_from_vertex_key(&self, k: &str) -> Option<VertexHash> {
        let hash = VertexHash(xxh3_64_with_seed(k.as_bytes(), 0xdeadbeefdeadbeef));
        let index = self.hash_to_index.get(&hash);
        match index {
            None => None,
            Some(index) => {
                if index.0 & 0x8000000000000000 != 0 {
                    // collision!
                    let kk = String::from(k);
                    let except = self.exceptions.get(&kk);
                    match except {
                        Some(h) => Some(*h),
                        None => Some(hash),
                    }
                } else {
                    Some(hash)
                }
            }
        }
    }

    pub fn index_from_vertex_key(&self, k: &str) -> Option<VertexIndex> {
        let hash: Option<VertexHash> = self.hash_from_vertex_key(k);
        match hash {
            None => None,
            Some(vh) => {
                let index = self.hash_to_index.get(&vh);
                match index {
                    None => None,
                    Some(index) => Some(*index),
                }
            }
        }
    }

    pub fn insert_edge(&mut self, from: VertexIndex, to: VertexIndex, data: Option<Vec<u8>>) {
        match data {
            None => {
                self.edges.push(Edge {
                    from,
                    to,
                    data_offset: 0,
                });
            }
            Some(v) => {
                let offset = self.edge_data.len();
                for b in v.iter() {
                    self.edge_data.push(*b);
                }
                self.edges.push(Edge {
                    from,
                    to,
                    data_offset: offset as u64,
                });
            }
        }
    }
}
