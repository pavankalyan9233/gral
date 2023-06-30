use log::info;
use rand::Rng;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::{Arc, Mutex, RwLock};
use warp::Filter;
use xxhash_rust::xxh3::xxh3_64_with_seed;

#[derive(Eq, Hash, PartialEq, Clone, Copy, Ord, PartialOrd, Debug)]
pub struct VertexHash(u64);
impl VertexHash {
    pub fn new(x: u64) -> VertexHash {
        VertexHash(x)
    }
    pub fn to_u64(&self) -> u64 {
        self.0
    }
}

#[derive(Eq, PartialEq, Clone, Copy, Ord, PartialOrd, Debug)]
pub struct VertexIndex(u64);
impl VertexIndex {
    pub fn new(x: u64) -> VertexIndex {
        VertexIndex(x)
    }
    pub fn to_u64(&self) -> u64 {
        self.0
    }
}

#[derive(Debug)]
pub struct Edge {
    pub from: VertexIndex, // index of vertex
    pub to: VertexIndex,   // index of vertex
}

#[derive(Debug)]
pub struct Graph {
    // List of hashes by index:
    pub index_to_hash: Vec<VertexHash>,

    // key is the hash of the vertex, value is the index, high bit
    // indicates a collision
    pub hash_to_index: HashMap<VertexHash, VertexIndex>,

    // key is the key of the vertex, value is the exceptional hash
    pub exceptions: HashMap<Vec<u8>, VertexHash>,

    // Maps indices of vertices to their names, not necessarily used:
    pub index_to_key: Vec<Vec<u8>>,

    // Additional data for vertices. If all data was empty, it is allowed
    // that both of these are empty! After sealing, the offsets get one
    // more entry to mark the end of the last one:
    pub vertex_data: Vec<u8>,
    pub vertex_data_offsets: Vec<u64>,

    // Edges as from/to tuples:
    pub edges: Vec<Edge>,

    // Additional data for edges. If all data was empty, it is allowed that
    // both of these are empty! After sealing, the offsets get one more
    // entry to mark the end of the last one:
    pub edge_data: Vec<u8>,
    pub edge_data_offsets: Vec<u64>,

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

    // Flag, if edges are already indexed:
    pub edges_indexed_from: bool,
    pub edges_indexed_to: bool,
}

pub struct Graphs {
    pub list: Vec<Arc<RwLock<Graph>>>,
}

pub fn with_graphs(
    graphs: Arc<Mutex<Graphs>>,
) -> impl Filter<Extract = (Arc<Mutex<Graphs>>,), Error = Infallible> + Clone {
    warp::any().map(move || graphs.clone())
}

struct EdgeTemp {
    pub from: VertexIndex,
    pub to: VertexIndex,
}

pub enum KeyOrHash {
    Key(Vec<u8>),
    Hash(VertexHash),
}

impl Graph {
    pub fn new(store_keys: bool, _bits_for_hash: u8) -> Arc<RwLock<Graph>> {
        Arc::new(RwLock::new(Graph {
            index_to_hash: vec![],
            hash_to_index: HashMap::new(),
            exceptions: HashMap::new(),
            index_to_key: vec![],
            vertex_data: vec![],
            vertex_data_offsets: vec![],
            edges: vec![],
            edge_data: vec![],
            edge_data_offsets: vec![],
            edges_by_from: vec![],
            edge_index_by_from: vec![],
            edges_by_to: vec![],
            edge_index_by_to: vec![],
            store_keys,
            dropped: false,
            vertices_sealed: false,
            edges_sealed: false,
            edges_indexed_from: false,
            edges_indexed_to: false,
        }))
    }

    pub fn insert_vertex(
        &mut self,
        i: u32,
        hash: VertexHash,
        key: Vec<u8>,  // cannot be empty
        data: Vec<u8>, // can be empty
        exceptional: &mut Vec<(u32, VertexHash)>,
        exceptional_keys: &mut Vec<Vec<u8>>,
    ) {
        // First detect a collision:
        let index = VertexIndex(self.index_to_hash.len() as u64);
        let mut actual = hash;
        if self.hash_to_index.contains_key(&hash) {
            // This is a collision, we create a random alternative
            // hash and report the collision:
            let mut rng = rand::thread_rng();
            loop {
                actual = VertexHash(rng.gen::<u64>());
                if let Some(_) = self.hash_to_index.get_mut(&actual) {
                    break;
                }
            }
            let oi = self.hash_to_index.get_mut(&hash).unwrap();
            *oi = VertexIndex((*oi).0 | 0x800000000000000);
            exceptional.push((i, actual));
            exceptional_keys.push(key.clone());
            if self.store_keys {
                self.exceptions.insert(key.clone(), actual);
            }
        }
        // Will succeed:
        self.index_to_hash.push(actual);
        self.hash_to_index.insert(actual, index);
        if self.store_keys {
            self.index_to_key.push(key.clone());
        }
        let pos = self.vertex_data.len() as u64;
        if data.is_empty() {
            // We only add things here lazily as soon as some non-empty
            // data has been detected to save memory:
            if !self.vertex_data_offsets.is_empty() {
                self.vertex_data_offsets.push(pos);
            }
        } else {
            // Now we have to pay for our laziness:
            if self.vertex_data_offsets.is_empty() {
                for _i in 0..index.to_u64() {
                    self.vertex_data_offsets.push(0);
                }
            }
            // Insert data:
            self.vertex_data_offsets.push(pos);
            self.vertex_data.extend_from_slice(&data);
        }
    }

    pub fn number_of_vertices(&self) -> u64 {
        self.index_to_hash.len() as u64
    }

    pub fn number_of_edges(&self) -> u64 {
        self.edges.len() as u64
    }

    pub fn seal_vertices(&mut self) {
        if !self.vertex_data_offsets.is_empty() {
            self.vertex_data_offsets.push(self.vertex_data.len() as u64);
        }
        self.vertices_sealed = true;
        info!(
            "Vertices sealed in graph, number of vertices: {}",
            self.index_to_hash.len()
        );
    }

    pub fn index_edges(&mut self, by_from: bool, by_to: bool) {
        let mut tmp: Vec<EdgeTemp> = vec![];
        let number_v = self.number_of_vertices() as usize;
        let number_e = self.number_of_edges() as usize;
        tmp.reserve(number_e);
        for e in self.edges.iter() {
            tmp.push(EdgeTemp {
                from: e.from,
                to: e.to,
            });
        }

        if by_from {
            // Create lookup by from:
            tmp.sort_by(|a: &EdgeTemp, b: &EdgeTemp| -> Ordering {
                a.from.to_u64().cmp(&b.from.to_u64())
            });
            self.edge_index_by_from.clear();
            self.edge_index_by_from.reserve(number_v + 1);
            self.edges_by_from.clear();
            self.edges_by_from.reserve(number_e);
            let mut cur_from = VertexIndex::new(0);
            let mut pos: u64 = 0; // position in self.edges_by_from where
                                  // we currently write
            self.edge_index_by_from.push(0);
            // loop invariant: the start offset for cur_from has already been
            // written into edge_index_by_from.
            // loop invariant: pos == edges_by_from.len()
            for e in tmp.iter() {
                if e.from != cur_from {
                    loop {
                        cur_from = VertexIndex::new(cur_from.to_u64() + 1);
                        self.edge_index_by_from.push(pos);
                        if cur_from == e.from {
                            break;
                        }
                    }
                }
                self.edges_by_from.push(e.to);
                pos = pos + 1;
            }
            while cur_from.to_u64() < number_v as u64 {
                cur_from = VertexIndex::new(cur_from.to_u64() + 1);
                self.edge_index_by_from.push(pos);
            }
            self.edges_indexed_from = true;
        }

        if by_to {
            // Create lookup by to:
            tmp.sort_by(|a: &EdgeTemp, b: &EdgeTemp| -> Ordering {
                a.to.to_u64().cmp(&b.to.to_u64())
            });
            self.edge_index_by_to.clear();
            self.edge_index_by_to.reserve(number_v + 1);
            self.edges_by_to.clear();
            self.edges_by_to.reserve(number_e);
            let mut cur_to = VertexIndex::new(0);
            let mut pos = 0; // position in self.edges_by_to where we currently write
            self.edge_index_by_to.push(0);
            // loop invariant: the start offset for cur_to has already been
            // written into edge_index_by_to.
            // loop invariant: pos == edges_by_to.len()
            for e in tmp.iter() {
                if e.to != cur_to {
                    loop {
                        cur_to = VertexIndex::new(cur_to.to_u64() + 1);
                        self.edge_index_by_to.push(pos);
                        if cur_to == e.to {
                            break;
                        }
                    }
                }
                self.edges_by_to.push(e.from);
                pos = pos + 1;
            }
            while cur_to.to_u64() < number_v as u64 {
                cur_to = VertexIndex::new(cur_to.to_u64() + 1);
                self.edge_index_by_to.push(pos);
            }
            self.edges_indexed_to = true;
        }
    }

    pub fn seal_edges(&mut self) {
        self.edges_sealed = true;
        if !self.edge_data_offsets.is_empty() {
            self.edge_data_offsets.push(self.edge_data.len() as u64);
        }

        info!(
            "Sealed graph with {} vertices and {} edges.",
            self.index_to_hash.len(),
            self.edges.len()
        );
    }

    pub fn hash_from_vertex_key(&self, k: &[u8]) -> Option<VertexHash> {
        let hash = VertexHash(xxh3_64_with_seed(k, 0xdeadbeefdeadbeef));
        let index = self.hash_to_index.get(&hash);
        match index {
            None => None,
            Some(index) => {
                if index.0 & 0x80000000_00000000 != 0 {
                    // collision!
                    let except = self.exceptions.get(k);
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

    pub fn index_from_vertex_key(&self, k: &[u8]) -> Option<VertexIndex> {
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

    pub fn index_from_hash(&self, h: &VertexHash) -> Option<VertexIndex> {
        let index = self.hash_to_index.get(h);
        match index {
            None => None,
            Some(i) => Some(*i),
        }
    }

    pub fn index_from_key_or_hash(&self, key_or_hash: &KeyOrHash) -> Option<VertexIndex> {
        match key_or_hash {
            KeyOrHash::Hash(h) => {
                // Lookup if hash exists, if so, this is the index
                self.index_from_hash(h)
            }
            KeyOrHash::Key(k) => {
                // Hash key, look up hash, check for exception:
                self.index_from_vertex_key(k)
            }
        }
    }

    pub fn insert_edge(&mut self, from: VertexIndex, to: VertexIndex, data: Vec<u8>) {
        self.edges.push(Edge { from, to });
        let offset = self.edge_data.len() as u64;
        if data.is_empty() {
            // We use edge_data_offsets lazily, only if there is some
            // non-empty data!
            if !self.edge_data_offsets.is_empty() {
                self.edge_data_offsets.push(offset);
            }
        } else {
            if self.edge_data_offsets.is_empty() {
                // We have to pay for our laziness now:
                for _i in 0..(self.edges.len() - 1) {
                    self.edge_data_offsets.push(0);
                }
            }
            self.edge_data_offsets.push(offset);
            self.edge_data.extend_from_slice(&data);
        }
    }
}
