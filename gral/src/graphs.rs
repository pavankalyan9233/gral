use rand::Rng;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::convert::Infallible;
//use std::io::Write;
use std::str;
use std::sync::{Arc, Mutex};
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
    pub data_offset: u64,  // offset into edge_data
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

struct EdgeTemp {
    pub from: VertexIndex,
    pub to: VertexIndex,
}

impl Graph {
    pub fn new(store_keys: bool, _bits_for_hash: u8) -> Arc<Mutex<Graph>> {
        Arc::new(Mutex::new(Graph {
            index_to_hash: vec![],
            hash_to_index: HashMap::new(),
            exceptions: HashMap::new(),
            index_to_key: vec![],
            vertex_data: vec![],
            vertex_data_offsets: vec![],
            edges: vec![],
            edge_data: vec![],
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
        self.vertex_data_offsets.clear();
        self.edges.clear();
        self.edge_data.clear();
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
        hash: VertexHash,
        key: Vec<u8>,  // cannot be empty
        data: Vec<u8>, // can be empty
        exceptional: &mut Vec<(u32, VertexHash)>,
        exceptional_keys: &mut Vec<Vec<u8>>,
    ) {
        // First detect a collision:
        let index = VertexIndex(self.vertex_data_offsets.len() as u64);
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
        if !data.is_empty() {
            // Insert data:
            let pos = self.vertex_data.len() as u64;
            self.vertex_data_offsets.push(pos);
            self.vertex_data.extend_from_slice(&data);
        } else {
            self.vertex_data_offsets.push(0);
        }
    }

    pub fn number_of_vertices(&self) -> u64 {
        self.index_to_hash.len() as u64
    }

    pub fn number_of_edges(&self) -> u64 {
        self.edges.len() as u64
    }

    pub fn seal_vertices(&mut self) {
        self.vertex_data_offsets.push(self.vertex_data.len() as u64);
        self.vertices_sealed = true;
    }

    pub fn seal_edges(&mut self) {
        self.edges_sealed = true;
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
        for e in self.edges.iter() {
            if e.from != cur_from {
                while cur_from < e.from {
                    self.edge_index_by_from.push(pos);
                    cur_from = VertexIndex::new(cur_from.to_u64() + 1);
                }
                self.edge_index_by_from.push(pos);
            }
            self.edges_by_from.push(e.to);
            pos = pos + 1;
        }
        while cur_from.to_u64() < number_v as u64 {
            cur_from = VertexIndex::new(cur_from.to_u64() + 1);
            self.edge_index_by_from.push(pos);
        }

        // Create lookup by to:
        tmp.sort_by(|a: &EdgeTemp, b: &EdgeTemp| -> Ordering { a.to.to_u64().cmp(&b.to.to_u64()) });
        self.edge_index_by_to.clear();
        self.edge_index_by_to.reserve(number_v + 1);
        self.edges_by_to.clear();
        self.edges_by_to.reserve(number_e);
        let mut cur_to = VertexIndex::new(0);
        pos = 0; // position in self.edges_by_to where we currently write
        self.edge_index_by_to.push(0);
        // loop invariant: the start offset for cur_to has already been
        // written into edge_index_by_to.
        // loop invariant: pos == edges_by_to.len()
        for e in self.edges.iter() {
            if e.to != cur_to {
                while cur_to < e.to {
                    self.edge_index_by_to.push(pos);
                    cur_to = VertexIndex::new(cur_to.to_u64() + 1);
                }
                self.edge_index_by_to.push(pos);
            }
            self.edges_by_to.push(e.from);
            pos = pos + 1;
        }
        while cur_to.to_u64() < number_v as u64 {
            cur_to = VertexIndex::new(cur_to.to_u64() + 1);
            self.edge_index_by_to.push(pos);
        }
        //let _ = self.dump_graph();
        println!(
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
                if index.0 & 0x8000000000000000 != 0 {
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

    pub fn insert_edge(&mut self, from: VertexIndex, to: VertexIndex, data: Vec<u8>) {
        if data.is_empty() {
            self.edges.push(Edge {
                from,
                to,
                data_offset: 0,
            });
        } else {
            let offset = self.edge_data.len();
            self.edge_data.extend_from_slice(&data);
            self.edges.push(Edge {
                from,
                to,
                data_offset: offset as u64,
            });
        }
    }

    pub fn dump_graph(&self) {
        println!("\nVertices:");
        println!("{:<32} {:<16} {}", "key", "hash", "data size");
        for i in 0..self.number_of_vertices() {
            let k = if self.store_keys {
                &self.index_to_key[i as usize]
            } else {
                "not stored".as_bytes()
            };
            let kkk: &str;
            let kk = str::from_utf8(k);
            if kk.is_err() {
                kkk = "non-UTF8-bytes";
            } else {
                kkk = kk.unwrap();
            }

            println!(
                "{:32} {:016x} {}",
                kkk,
                self.index_to_hash[i as usize].to_u64(),
                self.vertex_data_offsets[i as usize + 1] - self.vertex_data_offsets[i as usize]
            );
        }
        println!("\nEdges:");
        println!(
            "{:<15} {:<16} {:<15} {:16} {}",
            "from index", "from hash", "to index", "to hash", "data size"
        );
        for i in 0..(self.number_of_edges() as usize) {
            let size = if i == (self.number_of_edges() as usize - 1) {
                self.edge_data.len() as u64 - self.edges[i].data_offset
            } else {
                self.edges[i + 1].data_offset - self.edges[i].data_offset
            };
            println!(
                "{:>15} {:016x} {:>15} {:016x} {}",
                self.edges[i].from.to_u64(),
                self.index_to_hash[self.edges[i].from.to_u64() as usize].to_u64(),
                self.edges[i].to.to_u64(),
                self.index_to_hash[self.edges[i].to.to_u64() as usize].to_u64(),
                size
            );
        }
    }
}
