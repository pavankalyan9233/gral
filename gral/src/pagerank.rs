use crate::graphs::Graph;
use log::info;

pub fn page_rank(g: &Graph, supersteps: u32, damping_factor: f64) -> Vec<f64> {
    info!("Running page rank...");
    let start = std::time::SystemTime::now();
    let nr = g.number_of_vertices() as usize;
    let mut rank = vec![1.0 / nr as f64; nr as usize];
    // Do so many supersteps:
    for step in 1..=supersteps {
        info!("{:?} Page rank step {step}...", start.elapsed());
        let mut new_rank = vec![1.0 / nr as f64 * (1.0 - damping_factor); nr];
        // Go through all vertices and send rank away:
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
            }
        }
        rank = new_rank;
    }
    let dur = start.elapsed();
    info!("Page rank completed in {dur:?} seconds.");
    rank
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pagerank_simple() {
        let g_arc = Graph::new(true, 64, 1);
        let mut g = g_arc.write().unwrap();
        for i in 0..10 {
            let id = format!("V/K{}", i);
            g.add_vertex_nodata(id.as_bytes());
        }
        g.seal_vertices();
        for i in 0..10 {
            let from = format!("V/K{}", i);
            let to = format!("V/K{}", (i + 1) % 10);
            g.add_edge_nodata(from.as_bytes(), to.as_bytes());
        }
        g.seal_edges();
        g.index_edges(true, false);
        let rank = page_rank(&g, 3, 0.85);
        println!("{:?}", rank);
    }
}
