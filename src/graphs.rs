use rand::Rng;
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use warp::Filter;
use xxhash_rust::xxh3::xxh3_64_with_seed;

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
            vertex_data: vec![0],
            vertex_data_offsets: vec![],
            edges: vec![],
            edge_data: vec![0],
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
        self.edge_data.push(0);
        self.vertices_sealed = false;
        self.edges_sealed = false;
        self.dropped = true;
    }

    pub fn insert_vertex(
        &mut self,
        i: u32,
        hash: Option<u64>,
        key: Option<String>,
        data: Option<Vec<u8>>,
        rejected: &mut Vec<u32>,
        exceptional: &mut Vec<(u32, u64)>,
    ) {
        match hash {
            None => rejected.push(i),
            Some(h) => {
                // First detect a collision:
                let index = self.vertex_data_offsets.len();
                let mut actual = h;
                if self.vertices.contains_key(&h) {
                    if key.is_some() {
                        // This is a collision, we create a random alternative
                        // hash and report the collision:
                        let mut rng = rand::thread_rng();
                        loop {
                            actual = rng.gen::<u64>();
                            if let Some(_) = self.vertices.get_mut(&actual) {
                                break;
                            }
                        }
                        let oi = self.vertices.get_mut(&h).unwrap();
                        *oi = *oi | 0x8000000;
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
                self.vertices.insert(actual, index as u64);
                if let Some(k) = key {
                    if self.store_keys {
                        self.keys.push(k.to_string());
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
    }

    pub fn hash_from_vertex_key(&self, k: &str) -> Option<u64> {
        let hash = xxh3_64_with_seed(k.as_bytes(), 0xdeadbeefdeadbeef);
        let index = self.vertices.get(&hash);
        match index {
            None => None,
            Some(index) => {
                if index & 0x8000000 != 0 {
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

    pub fn insert_edge(&mut self, from: u64, to: u64, data: Option<Vec<u8>>) {
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
