use crate::graph_store::graph::Graph;
use crate::graph_store::vertex_key_index::VertexIndex;
use log::info;

pub fn page_rank(
    g: &Graph,
    supersteps: u32,
    damping_factor: f64,
) -> Result<(Vec<f64>, u32), String> {
    if !g.is_indexed_by_from() {
        return Err("The graph is missing the from-neighbour index which is required for the page rank algorithm.".to_string());
    }

    info!("Running page rank...");
    let start = std::time::SystemTime::now();

    let nr = g.number_of_vertices() as usize;
    let mut rank = vec![1.0 / nr as f64; nr];
    let mut new_rank = vec![1.0 / nr as f64 * (1.0 - damping_factor); nr];
    // Do up to so many supersteps:
    let mut step: u32 = 0;
    while step < supersteps {
        step += 1;
        info!("{:?} Page rank step {step}...", start.elapsed());
        // Go through all vertices and send rank away:
        let mut sink_sum: f64 = 0.0;
        for (v, rankv) in rank.iter().enumerate() {
            let vi = VertexIndex::new(v as u64);
            let edge_count = g.out_neighbour_count(vi);
            if edge_count > 0 {
                let tosend = damping_factor * rankv / edge_count as f64;
                g.out_neighbours(vi).for_each(|sink| {
                    new_rank[sink.to_u64() as usize] += tosend;
                });
            } else {
                sink_sum += rankv * damping_factor;
            }
        }
        let sink_contribution = sink_sum / nr as f64;
        let mut maxdiff: f64 = 0.0;
        for v in 0..nr {
            new_rank[v] += sink_contribution;
            let diff = (rank[v] - new_rank[v]).abs();
            maxdiff = if diff > maxdiff { diff } else { maxdiff };
            rank[v] = new_rank[v];
            new_rank[v] = 1.0 / nr as f64 * (1.0 - damping_factor);
        }
        info!(
            "{:?} Page rank step {step}, rank maximal difference {maxdiff}",
            start.elapsed()
        );
        if maxdiff < 0.0000001 {
            break;
        }
    }
    let dur = start.elapsed();
    info!("Page rank completed in {dur:?} seconds.");
    Ok((rank, step))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_store::examples::make_cyclic_graph;
    use crate::graph_store::examples::make_star_graph;

    #[test]
    fn test_pagerank_cyclic() {
        let g = make_cyclic_graph(10);
        let (rank, steps) = page_rank(&g, 5, 0.85).unwrap();
        assert_eq!(steps, 1);
        for i in 0..10 {
            assert!((rank[i] - 1.0 / 10.0).abs() < 0.000001);
        }
        println!("{:?}", rank);
    }

    #[test]
    fn test_pagerank_star() {
        let g = make_star_graph(10);
        let (rank, steps) = page_rank(&g, 100, 0.85).unwrap();
        assert!(steps > 50 && steps < 70);
        assert!(0.49 < rank[9] && rank[9] < 0.50);
        assert!(0.05 < rank[0] && rank[0] < 0.06);
        println!("{:?}", rank);
    }

    #[test]
    fn does_not_run_when_graph_has_no_from_neighbour_index() {
        let g = Graph::create(
            vec!["V/A".to_string()],
            vec![("V/A".to_string(), "V/A".to_string())],
        );

        assert!(page_rank(&g, 100, 0.85).is_err());
    }
}
