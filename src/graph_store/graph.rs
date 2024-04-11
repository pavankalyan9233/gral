use log::info;
use metrics::increment_counter;
use serde_json::Value;
use std::mem::size_of;
use std::sync::{Arc, RwLock};

use crate::graph_store::neighbour_index::NeighbourIndex;
use crate::graph_store::vertex_key_index::{VertexIndex, VertexKeyIndex};

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

#[derive(Debug, PartialEq, Clone)]
pub struct Edge {
    pub from: VertexIndex, // index of vertex
    pub to: VertexIndex,   // index of vertex
}
impl Edge {
    pub fn create(from: VertexIndex, to: VertexIndex) -> Edge {
        Edge { from, to }
    }
    pub fn from(&self) -> VertexIndex {
        self.from
    }
    pub fn to(&self) -> VertexIndex {
        self.to
    }
}

#[derive(Debug)]
pub struct Graph {
    // Index in list of graphs:
    pub graph_id: u64,

    vertex_key_index: VertexKeyIndex,

    // Maps indices of vertices to their names, not necessarily used:
    pub index_to_key: Vec<Vec<u8>>,
    // store keys?
    pub store_keys: bool,

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

    pub from_index: Option<NeighbourIndex>,
    to_index: Option<NeighbourIndex>,

    // sealed?
    pub vertices_sealed: bool,
    pub edges_sealed: bool,

    // For memory size computations:
    pub vertex_id_size_sum: usize,
    pub vertex_json_size_sum: usize,
}

pub struct MemoryUsageGraph {
    pub bytes_total: usize,
    pub bytes_per_vertex: usize,
    pub bytes_per_edge: usize,
}

impl Graph {
    pub fn new(store_keys: bool, col_names: Vec<String>) -> Arc<RwLock<Graph>> {
        increment_counter!("gral_mycounter_total");
        Arc::new(RwLock::new(Graph {
            graph_id: 0,
            vertex_key_index: VertexKeyIndex::new(),
            index_to_key: vec![],
            vertex_nr_cols: col_names.len(),
            vertex_json: vec![vec![]; col_names.len()],
            vertex_column_names: col_names,
            vertex_column_types: vec![],
            edges: vec![],
            from_index: None,
            to_index: None,
            store_keys,
            vertices_sealed: false,
            edges_sealed: false,
            vertex_id_size_sum: 0,
            vertex_json_size_sum: 0,
        }))
    }

    pub fn index_from_vertex_key(&self, k: &[u8]) -> Option<VertexIndex> {
        self.vertex_key_index.get(k)
    }

    pub fn insert_vertex(
        &mut self,
        key: Vec<u8>, // cannot be empty
        mut columns: Vec<Value>,
    ) -> VertexIndex {
        let index = self.vertex_key_index.add(&key);
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
        index
    }

    pub fn number_of_vertices(&self) -> u64 {
        self.vertex_key_index.count() as u64
    }

    pub fn number_of_edges(&self) -> u64 {
        self.edges.len() as u64
    }

    pub fn seal_vertices(&mut self) {
        self.vertices_sealed = true;
        info!(
            "Vertices sealed in graph, number of vertices: {}",
            self.number_of_vertices()
        );
    }

    pub fn index_edges(&mut self, by_from: bool, by_to: bool) {
        if (self.from_index.is_some() && by_from) && (self.to_index.is_some() && by_to) {
            return;
        }

        let mut tmp: Vec<Edge> = self.edges.to_vec();
        let number_v = self.number_of_vertices() as usize;

        if self.from_index.is_none() && by_from {
            info!("Graph: {}: Indexing edges by from...", self.graph_id);
            self.from_index = Some(NeighbourIndex::create_from(&mut tmp, number_v));
        }

        if self.to_index.is_none() && by_to {
            info!("Graph: {}: Indexing edges by to...", self.graph_id);
            self.to_index = Some(NeighbourIndex::create_to(&mut tmp, number_v));
        }
    }

    pub fn seal_edges(&mut self) {
        self.edges_sealed = true;

        info!(
            "Sealed graph with {} vertices and {} edges.",
            self.number_of_vertices(),
            self.edges.len()
        );
    }

    pub fn insert_edge(&mut self, from: VertexIndex, to: VertexIndex) {
        self.edges.push(Edge { from, to });
    }

    pub fn insert_empty_vertex(&mut self, key: &[u8]) {
        self.insert_vertex(key.to_vec(), vec![]);
    }

    pub fn insert_edge_between_vertices(&mut self, from: &[u8], to: &[u8]) {
        let f = self.index_from_vertex_key(from);
        assert!(f.is_some());
        let t = self.index_from_vertex_key(to);
        assert!(t.is_some());
        self.insert_edge(f.unwrap(), t.unwrap());
    }

    pub fn out_neighbours(&self, source: VertexIndex) -> impl Iterator<Item = &VertexIndex> {
        self.from_index.as_ref().expect("Out vertices cannot be calculated without a from index, which seems to be missing.")
	    .neighbours(source)
    }

    pub fn out_neighbour_count(&self, source: VertexIndex) -> u64 {
        self.from_index.as_ref().expect("Out vertex count cannot be calculated without a from index, which seems to be missing.")
	    .neighbour_count(source)
    }

    pub fn in_neighbours(&self, sink: VertexIndex) -> impl Iterator<Item = &VertexIndex> {
        self.to_index
            .as_ref()
            .expect(
                "In vertices cannot be calculated without a to index, which seems to be missing.",
            )
            .neighbours(sink)
    }

    pub fn in_neighbour_count(&self, sink: VertexIndex) -> u64 {
        self.to_index.as_ref().expect("In vertex count cannot be calculated without a to index, which seems to be missing.")
	    .neighbour_count(sink)
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
        let size_index = size_of::<VertexIndex>();

        // First what we always have:
        let mut total_v: usize = self.vertex_key_index.memory_in_bytes()
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
        if self.from_index.is_some() {
            // edge_index_by_to and edge_by_to:
            total_v += nrv * size_of::<u64>();
            total_e += nre * size_index;
        }
        if self.to_index.is_some() {
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

#[cfg(test)]
mod tests {
    use super::*;

    mod inserts_vertex {
        use super::*;

        #[test]
        #[should_panic]
        fn panicks_when_created_graph_has_different_number_of_columns() {
            let g_arc = Graph::new(true, vec!["first column name".to_string()]);
            let mut g = g_arc.write().unwrap();
            g.insert_vertex(
                vec![],
                vec![
                    serde_json::Value::String("first column entry".to_string()),
                    serde_json::Value::String("second column entry".to_string()),
                ],
            );
        }

        #[test]
        fn inserts_vertex_into_given_graph() {
            let g_arc = Graph::new(
                true,
                vec![
                    "string column name".to_string(),
                    "number column name".to_string(),
                ],
            );
            let mut g = g_arc.write().unwrap();

            // add one vertex
            g.insert_vertex(
                b"V/A".to_vec(),
                vec![
                    serde_json::Value::String("string column entry A".to_string()),
                    serde_json::Value::Number(serde_json::Number::from(645)),
                ],
            );

            assert_eq!(g.vertex_key_index.count(), 1);
            assert_eq!(g.index_to_key, vec![b"V/A"]); // only if graph was created with true
            assert_eq!(
                g.vertex_json,
                vec![
                    vec![serde_json::Value::String(
                        "string column entry A".to_string()
                    )],
                    vec![serde_json::Value::Number(serde_json::Number::from(645))]
                ]
            );

            // add another vertex
            g.insert_vertex(
                b"V/B".to_vec(),
                vec![
                    serde_json::Value::String("string column entry B".to_string()),
                    serde_json::Value::Number(serde_json::Number::from(33)),
                ],
            );

            assert_eq!(g.vertex_key_index.count(), 2);
            assert_eq!(g.index_to_key, vec![b"V/A", b"V/B"]);
            assert_eq!(
                g.vertex_json,
                vec![
                    vec![
                        serde_json::Value::String("string column entry A".to_string()),
                        serde_json::Value::String("string column entry B".to_string())
                    ],
                    vec![
                        serde_json::Value::Number(serde_json::Number::from(645)),
                        serde_json::Value::Number(serde_json::Number::from(33)),
                    ]
                ]
            );
        }

        #[test]
        fn does_not_care_about_duplicate_vertex_key() {
            let g_arc = Graph::new(true, vec![]);
            let mut g = g_arc.write().unwrap();
            g.insert_vertex(b"V/A".to_vec(), vec![]);

            g.insert_vertex(b"V/A".to_vec(), vec![]);

            assert_eq!(g.index_to_key, vec![b"V/A", b"V/A"]);
        }
    }

    mod inserts_edge {
        use super::*;

        #[test]
        fn inserts_dangling_edge_into_given_graph() {
            let g_arc = Graph::new(true, vec![]);
            let mut g = g_arc.write().unwrap();

            g.insert_edge(VertexIndex::new(1), VertexIndex::new(2));

            assert_eq!(
                g.edges,
                vec![Edge {
                    from: VertexIndex::new(1),
                    to: VertexIndex::new(2)
                }]
            );
        }

        #[test]
        fn inserts_edge_between_two_existing_vertices_into_given_graph() {
            let g_arc = Graph::new(true, vec![]);
            let mut g = g_arc.write().unwrap();
            let from = g.insert_vertex(b"V/A".to_vec(), vec![]);
            let to = g.insert_vertex(b"V/B".to_vec(), vec![]);

            g.insert_edge(from, to);

            assert_eq!(g.edges, vec![Edge { from, to }]);
        }
    }

    mod from_index {
        use super::*;

        #[test]
        fn adds_from_index_and_retrieves_out_vertices_via_function() {
            // TODO does not work when edges are dangling (if number of vertices in graph is not correct,
            // because edge_index_by_from should be number of vertices + 1)
            let g_arc = Graph::new(true, vec![]);
            let mut g = g_arc.write().unwrap();
            // add 6 random vertices
            g.insert_empty_vertex(b"V/A");
            g.insert_empty_vertex(b"V/B");
            g.insert_empty_vertex(b"V/C");
            g.insert_empty_vertex(b"V/D");
            g.insert_empty_vertex(b"V/E");
            g.insert_empty_vertex(b"V/F");
            // add edges
            g.insert_edge(VertexIndex::new(4), VertexIndex::new(1));
            g.insert_edge(VertexIndex::new(0), VertexIndex::new(3));
            g.insert_edge(VertexIndex::new(0), VertexIndex::new(2));
            g.insert_edge(VertexIndex::new(1), VertexIndex::new(6));

            g.index_edges(true, false);

            assert!(g.from_index.is_some());

            assert_eq!(
                g.out_neighbours(VertexIndex::new(0)).collect::<Vec<_>>(),
                vec![&VertexIndex::new(3), &VertexIndex::new(2)]
            );
            assert_eq!(
                g.out_neighbours(VertexIndex::new(1)).collect::<Vec<_>>(),
                vec![&VertexIndex::new(6)]
            );
            assert_eq!(g.out_neighbours(VertexIndex::new(2)).count(), 0);
            assert_eq!(g.out_neighbours(VertexIndex::new(3)).count(), 0);
            assert_eq!(
                g.out_neighbours(VertexIndex::new(4)).collect::<Vec<_>>(),
                vec![&VertexIndex::new(1)]
            );
        }

        #[test]
        #[should_panic]
        fn requesting_out_vertices_in_not_properly_indexed_graph_panicks() {
            let g_arc = Graph::new(true, vec![]);
            let mut g = g_arc.write().unwrap();
            g.insert_empty_vertex(b"V/A");
            g.insert_edge(VertexIndex::new(0), VertexIndex::new(0));

            g.out_neighbours(VertexIndex::new(0)).count();
        }

        #[test]
        fn counts_outgoing_vertices() {
            let g_arc = Graph::new(true, vec![]);
            let mut g = g_arc.write().unwrap();
            g.insert_empty_vertex(b"V/A");
            g.insert_empty_vertex(b"V/A");
            g.insert_edge(VertexIndex::new(0), VertexIndex::new(0));
            g.insert_edge(VertexIndex::new(0), VertexIndex::new(1));
            g.index_edges(true, false);

            assert_eq!(g.out_neighbour_count(VertexIndex::new(0)), 2);
        }
    }

    mod to_index {
        use super::*;

        #[test]
        fn adds_to_index() {
            // TODO does not work when edges are dangling (if number of vertices in graph is not correct,
            // because edge_index_by_from should be number of vertices + 1)
            let g_arc = Graph::new(true, vec![]);
            let mut g = g_arc.write().unwrap();
            // add 6 random vertices
            g.insert_empty_vertex(b"V/A");
            g.insert_empty_vertex(b"V/B");
            g.insert_empty_vertex(b"V/C");
            g.insert_empty_vertex(b"V/D");
            g.insert_empty_vertex(b"V/E");
            g.insert_empty_vertex(b"V/F");
            // add edges
            g.insert_edge(VertexIndex::new(1), VertexIndex::new(4));
            g.insert_edge(VertexIndex::new(3), VertexIndex::new(0));
            g.insert_edge(VertexIndex::new(2), VertexIndex::new(0));
            g.insert_edge(VertexIndex::new(6), VertexIndex::new(1));

            g.index_edges(false, true);

            assert!(g.to_index.is_some());

            assert_eq!(
                g.in_neighbours(VertexIndex::new(0)).collect::<Vec<_>>(),
                vec![&VertexIndex::new(3), &VertexIndex::new(2)]
            );
            assert_eq!(
                g.in_neighbours(VertexIndex::new(1)).collect::<Vec<_>>(),
                vec![&VertexIndex::new(6)]
            );
            assert_eq!(g.in_neighbours(VertexIndex::new(2)).count(), 0);
            assert_eq!(g.in_neighbours(VertexIndex::new(3)).count(), 0);
            assert_eq!(
                g.in_neighbours(VertexIndex::new(4)).collect::<Vec<_>>(),
                vec![&VertexIndex::new(1)]
            );
        }

        #[test]
        #[should_panic]
        fn requesting_in_vertices_in_not_properly_indexed_graph_panicks() {
            let g_arc = Graph::new(true, vec![]);
            let mut g = g_arc.write().unwrap();
            g.insert_empty_vertex(b"V/A");
            g.insert_edge(VertexIndex::new(0), VertexIndex::new(0));

            g.in_neighbours(VertexIndex::new(0)).count();
        }

        #[test]
        fn counts_incoming_vertices() {
            let g_arc = Graph::new(true, vec![]);
            let mut g = g_arc.write().unwrap();
            g.insert_empty_vertex(b"V/A");
            g.insert_empty_vertex(b"V/A");
            g.insert_edge(VertexIndex::new(0), VertexIndex::new(0));
            g.insert_edge(VertexIndex::new(1), VertexIndex::new(0));
            g.index_edges(false, true);

            assert_eq!(g.in_neighbour_count(VertexIndex::new(0)), 2);
        }
    }
}
