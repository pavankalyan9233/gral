#![allow(dead_code)]
// Functions in here are only used in tests.
// TODO: Find a proper place for this.

use super::graphs::Graph;
use std::sync::{Arc, RwLock};

pub fn make_cyclic_graph(n: u32) -> Arc<RwLock<Graph>> {
    let g_arc = Graph::new(true, 64, 1, vec![]);
    {
        let mut g = g_arc.write().unwrap();
        for i in 0..n {
            let id = format!("V/K{i}");
            g.add_vertex_nodata(id.as_bytes());
        }
        g.seal_vertices();
        for i in 0..n {
            let from = format!("V/K{}", i);
            let to = format!("V/K{}", (i + 1) % 10);
            g.add_edge_nodata(from.as_bytes(), to.as_bytes());
        }
        g.seal_edges();
        g.index_edges(true, false);
    }
    g_arc
}

pub fn make_star_graph(n: u32) -> Arc<RwLock<Graph>> {
    let g_arc = Graph::new(true, 64, 1, vec![]);
    {
        let mut g = g_arc.write().unwrap();
        for i in 0..n {
            let id = format!("V/K{i}");
            g.add_vertex_nodata(id.as_bytes());
        }
        g.seal_vertices();
        let to = format!("V/K{}", n - 1);
        for i in 0..(n - 1) {
            let from = format!("V/K{}", i);
            g.add_edge_nodata(from.as_bytes(), to.as_bytes());
        }
        g.seal_edges();
        g.index_edges(true, false);
    }
    g_arc
}
