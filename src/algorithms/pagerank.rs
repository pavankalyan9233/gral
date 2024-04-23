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
    use crate::graph_store::examples::{make_cyclic_graph, make_star_graph};
    use approx::assert_ulps_eq;

    #[test]
    fn does_not_run_when_graph_has_no_from_neighbour_index() {
        let g = Graph::create(
            vec!["V/A".to_string()],
            vec![("V/A".to_string(), "V/A".to_string())],
        );

        assert!(page_rank(&g, 100, 0.85).is_err());
    }

    mod ranks {
        use super::*;

        #[test]
        fn gives_empty_results_on_empty_graph() {
            let mut g = Graph::create(vec![], vec![]);
            g.index_edges(true, false);

            let (rank, steps) = page_rank(&g, 100, 0.85).unwrap();

            assert_eq!(rank, Vec::<f64>::new());
            assert_eq!(steps, 1);
        }

        #[test]
        fn ranks_of_unconnected_graph_are_all_equal() {
            let mut g = Graph::create(vec!["V/A".to_string(), "V/B".to_string()], vec![]);
            g.index_edges(true, false);

            let (rank, steps) = page_rank(&g, 100, 0.85).unwrap();

            assert_ulps_eq!(rank[0], 0.5);
            assert_ulps_eq!(rank[1], 0.5);
            assert_eq!(steps, 1);
        }

        #[test]
        fn rank_depends_on_number_of_incoming_edges() {
            let mut g = Graph::create(
                vec![
                    "V/A".to_string(),
                    "V/B".to_string(),
                    "V/C".to_string(),
                    "V/D".to_string(),
                ],
                vec![
                    ("V/A".to_string(), "V/A".to_string()),
                    ("V/A".to_string(), "V/B".to_string()),
                    ("V/A".to_string(), "V/C".to_string()),
                    ("V/C".to_string(), "V/B".to_string()),
                ],
            );
            g.index_edges(true, false);

            let (rank, _steps) = page_rank(&g, 100, 0.85).unwrap();

            assert_ulps_eq!(rank[0], rank[2]);
            assert!(rank[1] > rank[0]);
            assert!(rank[1] > rank[3]);
            assert!(rank[3] < rank[0]);
        }

        #[test]
        fn cyclic_graph_has_equal_ranks() {
            let g = make_cyclic_graph(10);
            let (rank, steps) = page_rank(&g, 5, 0.85).unwrap();
            assert_eq!(steps, 1);
            for i in 0..10 {
                assert_ulps_eq!(rank[i], 1.0 / 10.0);
            }
        }

        #[test]
        fn star_graph_has_one_large_rank_vertex() {
            let g = make_star_graph(10);
            let (rank, steps) = page_rank(&g, 100, 0.85).unwrap();
            assert!(steps > 50 && steps < 70);
            assert!(0.49 < rank[9] && rank[9] < 0.50);
            assert!(0.05 < rank[0] && rank[0] < 0.06);
            assert_ulps_eq!(rank[1], rank[0]);
            assert_ulps_eq!(rank[2], rank[0]);
            assert_ulps_eq!(rank[3], rank[0]);
            assert_ulps_eq!(rank[4], rank[0]);
            assert_ulps_eq!(rank[5], rank[0]);
            assert_ulps_eq!(rank[6], rank[0]);
            assert_ulps_eq!(rank[7], rank[0]);
            assert_ulps_eq!(rank[8], rank[0]);
        }

        #[test]
        fn sum_of_ranks_is_normalized() {
            let g = make_star_graph(10);

            let (rank, _steps) = page_rank(&g, 100, 0.85).unwrap();

            assert_ulps_eq!(rank.iter().sum::<f64>(), 1.0);
        }
    }

    mod supersteps {
        use super::*;

        #[test]
        fn stops_maximally_after_given_supersteps() {
            let g = make_star_graph(10);

            let max_supersteps = 5;
            let (_rank, steps) = page_rank(&g, max_supersteps, 0.85).unwrap();

            assert_eq!(steps, max_supersteps);
        }

        #[test]
        fn stops_earlier_then_maximal_supersteps_when_converging() {
            let g = make_star_graph(10);

            let max_supersteps = 100;
            let (_rank, steps) = page_rank(&g, max_supersteps, 0.85).unwrap();

            assert!(steps < max_supersteps);
        }
    }

    mod damping_factor {
        use super::*;

        #[test]
        fn damping_factor_determines_impact_of_neighbours() {
            let g = make_star_graph(10);

            let (rank_lower_damping, steps_lower_damping) = dbg!(page_rank(&g, 100, 0.4).unwrap());
            let (rank_larger_damping, steps_larger_damping) =
                dbg!(page_rank(&g, 100, 0.85).unwrap());

            assert!(rank_lower_damping[9] < rank_larger_damping[9]);
            assert!(rank_lower_damping[0] > rank_larger_damping[0]);
            assert!(steps_lower_damping < steps_larger_damping);
        }

        #[test]
        fn damping_factor_zero_gives_equal_ranks() {
            let g = make_star_graph(10);

            let (rank, steps) = page_rank(&g, 100, 0.0).unwrap();

            for i in 0..10 {
                assert_ulps_eq!(rank[i], 1.0 / 10.0);
            }
            assert_eq!(steps, 1);
        }
    }
}
