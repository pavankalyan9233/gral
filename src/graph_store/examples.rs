#![allow(dead_code)]
// Functions in here are only used in tests.
// TODO: Find a proper place for this.

use super::graph::Graph;

pub fn make_cyclic_graph(n: u32) -> Graph {
    let mut g = Graph::new(true, vec![]);
    for i in 0..n {
        let id = format!("V/K{i}");
        g.insert_empty_vertex(id.as_bytes());
    }
    g.seal_vertices();
    for i in 0..n {
        let from = format!("V/K{}", i);
        let to = format!("V/K{}", (i + 1) % 10);
        g.insert_edge_between_vertices(from.as_bytes(), to.as_bytes());
    }
    g.seal_edges();
    g.index_edges(true, false);
    g
}

pub fn make_star_graph(n: u32) -> Graph {
    let mut g = Graph::new(true, vec![]);
    for i in 0..n {
        let id = format!("V/K{i}");
        g.insert_empty_vertex(id.as_bytes());
    }
    g.seal_vertices();
    let to = format!("V/K{}", n - 1);
    for i in 0..(n - 1) {
        let from = format!("V/K{}", i);
        g.insert_edge_between_vertices(from.as_bytes(), to.as_bytes());
    }
    g.seal_edges();
    g.index_edges(true, false);
    g
}
