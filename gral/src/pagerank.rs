use crate::graphs::Graph;
use log::info;

pub fn page_rank(g: &Graph, supersteps: u32, damping_factor: f64) -> (Vec<f64>, u32) {
    info!("Running page rank...");
    let start = std::time::SystemTime::now();
    let nr = g.number_of_vertices() as usize;
    let mut rank = vec![1.0 / nr as f64; nr as usize];
    let mut new_rank = vec![1.0 / nr as f64 * (1.0 - damping_factor); nr];
    // Do up to so many supersteps:
    let mut step: u32 = 0;
    while step < supersteps {
        step += 1;
        info!("{:?} Page rank step {step}...", start.elapsed());
        // Go through all vertices and send rank away:
        let mut sink_sum: f64 = 0.0;
        for v in 0..nr {
            let first_edge = g.edge_index_by_from[v] as usize;
            let last_edge = g.edge_index_by_from[v + 1] as usize;
            let edge_nr = last_edge - first_edge;
            if edge_nr > 0 {
                let tosend = damping_factor * rank[v] / edge_nr as f64;
                for wi in first_edge..last_edge {
                    let w = g.edges_by_from[wi].to_u64() as usize;
                    new_rank[w] += tosend;
                }
            } else {
                sink_sum += rank[v] * damping_factor;
            }
        }
        let sink_contribution = sink_sum / nr as f64;
        let mut maxdiff: f64 = 1e64;
        for v in 0..nr {
            new_rank[v] += sink_contribution;
            let diff = (rank[v] - new_rank[v]).abs();
            maxdiff = if diff < maxdiff { diff } else { maxdiff };
            rank[v] = new_rank[v];
            new_rank[v] = 1.0 / nr as f64 * (1.0 - damping_factor);
        }
        if maxdiff < 0.0000001 {
            break;
        }
    }
    let dur = start.elapsed();
    info!("Page rank completed in {dur:?} seconds.");
    (rank, step)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, RwLock};

    fn make_cyclic_graph(n: u32) -> Arc<RwLock<Graph>> {
        let g_arc = Graph::new(true, 64, 1);
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

    fn make_star_graph(n: u32) -> Arc<RwLock<Graph>> {
        let g_arc = Graph::new(true, 64, 1);
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

    #[test]
    fn test_pagerank_cyclic() {
        let g_arc = make_cyclic_graph(10);
        let g = g_arc.read().unwrap();
        let (rank, steps) = page_rank(&g, 5, 0.85);
        assert_eq!(steps, 1);
        for i in 0..10 {
            assert!((rank[i] - 1.0 / 10.0).abs() < 0.000001);
        }
        println!("{:?}", rank);
    }

    #[test]
    fn test_pagerank_star() {
        let g_arc = make_star_graph(10);
        let g = g_arc.read().unwrap();
        let (rank, steps) = page_rank(&g, 100, 0.85);
        assert!(steps > 50 && steps < 60);
        assert!(0.49 < rank[9] && rank[9] < 0.50);
        assert!(0.05 < rank[0] && rank[0] < 0.06);
        println!("{:?}", rank);
    }
}
