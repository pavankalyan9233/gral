use log::info;
use metrics::{decrement_gauge, increment_counter, increment_gauge};
use rand::Rng;
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::convert::Infallible;
use std::mem::size_of;
use std::sync::{Arc, Mutex, RwLock};
use warp::Filter;
use xxhash_rust::xxh3::xxh3_64_with_seed;

// Got this function from stack overflow:
//  https://stackoverflow.com/questions/76454260/rust-serde-get-runtime-heap-size-of-vecserde-jsonvalue
fn sizeof_val(v: &serde_json::Value) -> usize {
    std::mem::size_of::<serde_json::Value>()
        + match v {
            serde_json::Value::Null => 0,
            serde_json::Value::Bool(_) => 0,
            serde_json::Value::Number(_) => 0, // Incorrect if arbitrary_precision is enabled. oh well
            serde_json::Value::String(s) => s.capacity(),
            serde_json::Value::Array(a) => a.iter().map(sizeof_val).sum(),
            serde_json::Value::Object(o) => o
                .iter()
                .map(|(k, v)| {
                    std::mem::size_of::<String>()
                        + k.capacity()
                        + sizeof_val(v)
                        + std::mem::size_of::<usize>() * 3 // As a crude approximation, I pretend each map entry has 3 words of overhead
                })
                .sum(),
        }
}

#[derive(Eq, Hash, PartialEq, Clone, Copy, Ord, PartialOrd, Debug)]
pub struct VertexHash(u64);
impl VertexHash {
    pub fn new(x: u64) -> VertexHash {
        VertexHash(x)
    }
}

#[derive(Eq, PartialEq, Clone, Copy, Ord, PartialOrd, Debug)]
pub struct VertexIndex(u64);
impl VertexIndex {
    pub fn new(x: u64) -> VertexIndex {
        VertexIndex(x)
    }
    pub fn to_u64(self) -> u64 {
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
    // Index in list of graphs:
    pub graph_id: u64,

    // List of hashes by index:
    pub index_to_hash: Vec<VertexHash>,

    // key is the hash of the vertex, value is the index, high bit
    // indicates a collision
    pub hash_to_index: HashMap<VertexHash, VertexIndex>,

    // key is the key of the vertex, value is the exceptional hash
    pub exceptions: HashMap<Vec<u8>, VertexHash>,

    // Maps indices of vertices to their names, not necessarily used:
    pub index_to_key: Vec<Vec<u8>>,

    // JSON data for vertices. This is essentially a column store, on
    // loading a graph, we are given a list of attributes and we store,
    // for each column, the value of the attribute, in an array. If no
    // attributes are given, the following vector is empty:
    pub vertex_nr_cols: usize,
    pub vertex_json: Vec<Vec<Value>>,
    // These are the column names:
    pub vertex_column_names: Vec<String>,
    // And - potentially - the types: (not yet used)
    pub vertex_column_types: Vec<String>,

    // Edges as from/to tuples:
    pub edges: Vec<Edge>,

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

    // sealed?
    pub vertices_sealed: bool,
    pub edges_sealed: bool,

    // Flag, if edges are already indexed:
    pub edges_indexed_from: bool,
    pub edges_indexed_to: bool,

    // For memory size computations:
    pub vertex_id_size_sum: usize,
    pub vertex_json_size_sum: usize,
}

pub struct Graphs {
    pub list: HashMap<u64, Arc<RwLock<Graph>>>,
    next_id: u64,
}

impl Graphs {
    pub fn new() -> Graphs {
        Graphs {
            list: HashMap::new(),
            next_id: 1,
        }
    }
    pub fn register(&mut self, graph: Arc<RwLock<Graph>>) -> u64 {
        let graph_id = self.next_id;
        self.next_id += 1;
        {
            let mut guard = graph.write().unwrap();
            guard.graph_id = graph_id;
        }
        self.list.insert(graph_id, graph);
        increment_gauge!("number_of_graphs", 1.0);
        graph_id
    }
    pub fn remove(&mut self, id: u64) {
        let found = self.list.remove(&id);
        if found.is_some() {
            decrement_gauge!("number_of_graphs", 1.0);
        }
    }
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

pub struct MemoryUsageGraph {
    pub bytes_total: usize,
    pub bytes_per_vertex: usize,
    pub bytes_per_edge: usize,
}

impl Graph {
    pub fn new(
        store_keys: bool,
        _bits_for_hash: u8,
        id: u64,
        col_names: Vec<String>,
    ) -> Arc<RwLock<Graph>> {
        increment_counter!("gral_mycounter_total");
        Arc::new(RwLock::new(Graph {
            graph_id: id,
            index_to_hash: vec![],
            hash_to_index: HashMap::new(),
            exceptions: HashMap::new(),
            index_to_key: vec![],
            vertex_nr_cols: col_names.len(),
            vertex_json: vec![vec![]; col_names.len()],
            vertex_column_names: col_names,
            vertex_column_types: vec![],
            edges: vec![],
            edges_by_from: vec![],
            edge_index_by_from: vec![],
            edges_by_to: vec![],
            edge_index_by_to: vec![],
            store_keys,
            vertices_sealed: false,
            edges_sealed: false,
            edges_indexed_from: false,
            edges_indexed_to: false,
            vertex_id_size_sum: 0,
            vertex_json_size_sum: 0,
        }))
    }

    pub fn insert_vertex(
        &mut self,
        i: u32,
        hash: VertexHash,
        key: Vec<u8>, // cannot be empty
        mut columns: Vec<Value>,
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
                if self.hash_to_index.get_mut(&actual).is_some() {
                    break;
                }
            }
            let oi = self.hash_to_index.get_mut(&hash).unwrap();
            *oi = VertexIndex(oi.0 | 0x800000000000000);
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
        assert_eq!(self.vertex_nr_cols, columns.len());
        for (j, col) in columns.iter_mut().enumerate() {
            let mut v: Value = Value::Null;
            std::mem::swap(&mut v, col);
            self.vertex_json_size_sum += sizeof_val(&v);
            self.vertex_json[j].push(v);
        }
        self.vertex_id_size_sum += key.len();
    }

    pub fn number_of_vertices(&self) -> u64 {
        self.index_to_hash.len() as u64
    }

    pub fn number_of_edges(&self) -> u64 {
        self.edges.len() as u64
    }

    pub fn seal_vertices(&mut self) {
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
        if (!self.edges_indexed_from && by_from) || (!self.edges_indexed_to && by_to) {
            tmp.reserve(number_e);
            for e in self.edges.iter() {
                tmp.push(EdgeTemp {
                    from: e.from,
                    to: e.to,
                });
            }
        }

        if !self.edges_indexed_from && by_from {
            info!("Graph: {}: Indexing edges by from...", self.graph_id);
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
                pos += 1;
            }
            while cur_from.to_u64() < number_v as u64 {
                cur_from = VertexIndex::new(cur_from.to_u64() + 1);
                self.edge_index_by_from.push(pos);
            }
            self.edges_indexed_from = true;
        }

        if !self.edges_indexed_to && by_to {
            info!("Graph: {}: Indexing edges by to...", self.graph_id);
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
                pos += 1;
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
                index.copied()
            }
        }
    }

    pub fn insert_edge(&mut self, from: VertexIndex, to: VertexIndex) {
        self.edges.push(Edge { from, to });
    }

    pub fn add_vertex_nodata(&mut self, key: &[u8]) {
        let key = key.to_vec();
        let hash = VertexHash::new(xxh3_64_with_seed(&key, 0xdeadbeefdeadbeef));
        self.insert_vertex(0, hash, key, vec![], &mut vec![], &mut vec![]);
    }

    pub fn add_edge_nodata(&mut self, from: &[u8], to: &[u8]) {
        let f = self.index_from_vertex_key(from);
        assert!(f.is_some());
        let t = self.index_from_vertex_key(to);
        assert!(t.is_some());
        self.insert_edge(f.unwrap(), t.unwrap());
    }

    pub fn dump(&self) {
        let nr = self.number_of_vertices();
        println!("Vertex columns: {:?}", self.vertex_column_names);
        println!("Vertices ({}):", nr);
        for i in 0..nr {
            let key = std::str::from_utf8(&self.index_to_key[i as usize][..]).unwrap();
            let mut s = format!("{i:>10} {:<40}", key);
            for j in 0..self.vertex_json.len() {
                if (i as usize) < self.vertex_json[j].len() {
                    s += &format!(" {}", self.vertex_json[j][i as usize]);
                }
            }
            println!("{}", s);
        }
        let nre = self.number_of_edges();
        println!("Edges ({}):", nre);
        for i in 0..nre {
            println!(
                "  {} -> {}",
                self.edges[i as usize].from.to_u64(),
                self.edges[i as usize].to.to_u64()
            );
        }
    }

    // The following is only an estimate, it will never be accurate up to
    // the last byte, but it will be good enough for most purposes. The
    // first result is the total memory usage, the second is the number of
    // bytes per vertex and the second is the number of bytes per edge.
    pub fn memory_usage(&self) -> MemoryUsageGraph {
        let nrv = self.number_of_vertices() as usize;
        let nre = self.number_of_edges() as usize;
        let size_hash = size_of::<VertexHash>();
        let size_index = size_of::<VertexIndex>();

        // First what we always have:
        let mut total_v: usize = nrv * (
                // index_to_hash:
                size_hash +
                // hash_to_index:
                size_hash + size_index
            )  // Heuristics for the (hopefully few) exceptions:
              + self.exceptions.len() * (48 + size_hash)
                // index_to_key:
              + nrv * size_of::<Vec<u8>>() + self.vertex_id_size_sum
                // JSON data:
              + self.vertex_json.len() * nrv * size_of::<Vec<Value>>()
              + self.vertex_json_size_sum;

        let mut total_e: usize = nre
            * (
                // edges:
                size_of::<Edge>()
            );

        // Edge index, if present:
        if self.edges_indexed_from {
            // edge_index_by_to and edge_by_to:
            total_v += nrv * size_of::<u64>();
            total_e += nre * size_index;
        }
        if self.edges_indexed_to {
            total_v += nrv * size_of::<u64>();
            total_e += nre * size_index;
        }
        MemoryUsageGraph {
            bytes_total: total_v + total_e,
            bytes_per_vertex: if nrv == 0 { 0 } else { total_v / nrv },
            bytes_per_edge: if nre == 0 { 0 } else { total_e / nre },
        }
    }
}
