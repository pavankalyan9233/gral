//! Linerank implementation as described in "Centralities in Large Networks: Algorithms and Observations"
//! by Kang et. al. (https://www.cs.cmu.edu/~ukang/papers/CentralitySDM2011.pdf)

use crate::algorithms::linerank::graph_multiplications::EdgeVector;
use crate::graph_store::graph::Graph;
use std::ops::Add;

pub fn line_rank(
    graph: &Graph,
    max_supersteps: u32,
    damping_factor: f64,
) -> Result<(Vec<f64>, u32), String> {
    let mut edge_rank = EdgeVector(vec![
        1. / graph.number_of_edges() as f64;
        graph.number_of_edges() as usize
    ]);

    let normalization = edge_normalization(graph);
    let mut superstep = 0;
    for step in 0..max_supersteps {
        superstep = step;
        let max_diff = next_superstep(&mut edge_rank, graph, &normalization, damping_factor);
        if max_diff < 0.0000001 {
            break;
        }
    }

    let vertex_rank = edge_rank
        .clone()
        .apply_transposed_source_matrix(graph)
        .add(edge_rank.apply_transposed_target_matrix(graph));

    Ok((vertex_rank.0, superstep))
}

fn next_superstep(
    old_rank: &mut EdgeVector,
    graph: &Graph,
    normalization: &EdgeVector,
    damping: f64,
) -> f64 {
    let sum_over_incoming_ranks = old_rank
        .clone()
        .apply_transposed_source_matrix(graph)
        .apply_target_matrix(graph)
        .normalize_with(normalization);

    let mut max_diff = 0.;
    sum_over_incoming_ranks
        .0
        .into_iter()
        .zip(old_rank.0.iter_mut())
        .map(|(sum, old)| {
            (
                damping * sum + (1. - damping) * 1. / graph.number_of_edges() as f64,
                old,
            )
        })
        .for_each(|(new, old)| {
            let diff = (new - *old).abs();
            max_diff = if diff > max_diff { diff } else { max_diff };
            *old = new;
        });

    max_diff
}

fn edge_normalization(graph: &Graph) -> EdgeVector {
    EdgeVector(vec![1.; graph.number_of_edges() as usize])
        .apply_transposed_source_matrix(graph)
        .apply_target_matrix(graph)
        .invert_elementwise()
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_ulps_eq;

    mod edge_normalization {
        use super::*;

        #[test]
        fn is_empty_for_empty_graph() {
            let graph = Graph::create(vec![], vec![]);
            assert_eq!(edge_normalization(&graph).0.len(), 0);
        }

        #[test]
        fn is_empty_for_unconnected_graph() {
            let graph = Graph::create(vec!["V/A".to_string(), "V/B".to_string()], vec![]);
            assert_eq!(edge_normalization(&graph).0.len(), 0);
        }

        #[test]
        fn give_inverse_count_of_outgoing_edges_in_line_graph() {
            let graph = Graph::create(
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
                    ("V/C".to_string(), "V/A".to_string()),
                ],
            );

            let normalization = edge_normalization(&graph);

            assert_eq!(normalization.0.len() as u64, graph.number_of_edges());
            assert_ulps_eq!(normalization.0[0], 1. / 3.);
            assert_ulps_eq!(normalization.0[1], 0.);
            assert_ulps_eq!(normalization.0[2], 1.);
            assert_ulps_eq!(normalization.0[3], 1. / 3.);
        }
    }

    mod aggregates_edge_importance_for_each_vertex {
        use super::*;

        #[test]
        fn fork() {
            let graph = Graph::create(
                vec![
                    "V/A".to_string(),
                    "V/B".to_string(),
                    "V/C".to_string(),
                    "V/D".to_string(),
                ],
                vec![
                    ("V/A".to_string(), "V/B".to_string()),
                    ("V/B".to_string(), "V/C".to_string()),
                    ("V/B".to_string(), "V/D".to_string()),
                ],
            );
            let mut edge_rank = EdgeVector(vec![1. / 3.; graph.number_of_edges() as usize]);
            let normalization = edge_normalization(&graph);

            next_superstep(&mut edge_rank, &graph, &normalization, 0.85);
            assert_ulps_eq!(edge_rank.0[0], 1. / 3.);
            assert_ulps_eq!(edge_rank.0[1], 0.05);
            assert_ulps_eq!(edge_rank.0[2], 0.05);
            next_superstep(&mut edge_rank, &graph, &normalization, 0.85);
            assert_ulps_eq!(edge_rank.0[0], 0.0925);
            assert_ulps_eq!(edge_rank.0[1], 0.05);
            assert_ulps_eq!(edge_rank.0[2], 0.05);

            let (ranks, superstep) = dbg!(line_rank(&graph, 10, 0.85).unwrap());

            assert_eq!(ranks.len(), 4);
            assert_ulps_eq!(ranks[0], 0.0925);
            assert_ulps_eq!(ranks[1], 0.1925);
            assert_ulps_eq!(ranks[2], 0.05);
            assert_ulps_eq!(ranks[3], 0.05);
            assert_eq!(superstep, 2);
        }

        #[test]
        fn one_line() {
            let graph = Graph::create(
                vec![
                    "V/A".to_string(),
                    "V/B".to_string(),
                    "V/C".to_string(),
                    "V/D".to_string(),
                ],
                vec![
                    ("V/A".to_string(), "V/B".to_string()),
                    ("V/B".to_string(), "V/C".to_string()),
                    ("V/C".to_string(), "V/D".to_string()),
                ],
            );
            let mut edge_rank = EdgeVector(vec![1. / 3.; graph.number_of_edges() as usize]);
            let normalization = edge_normalization(&graph);

            next_superstep(&mut edge_rank, &graph, &normalization, 0.85);
            assert_ulps_eq!(edge_rank.0[0], 1. / 3.);
            assert_ulps_eq!(edge_rank.0[1], 1. / 3.);
            assert_ulps_eq!(edge_rank.0[2], 0.05);
            next_superstep(&mut edge_rank, &graph, &normalization, 0.85);
            assert_ulps_eq!(edge_rank.0[0], 1. / 3.);
            assert_ulps_eq!(edge_rank.0[1], 0.0925);
            assert_ulps_eq!(edge_rank.0[2], 0.05);
            next_superstep(&mut edge_rank, &graph, &normalization, 0.85);
            assert_ulps_eq!(edge_rank.0[0], 0.128625);
            assert_ulps_eq!(edge_rank.0[1], 0.0925);
            assert_ulps_eq!(edge_rank.0[2], 0.05);

            let (ranks, superstep) = dbg!(line_rank(&graph, 10, 0.85)).unwrap();
            assert_eq!(ranks.len(), 4);
            assert_ulps_eq!(ranks[0], 0.128625);
            assert_ulps_eq!(ranks[1], 0.221125);
            assert_ulps_eq!(ranks[2], 0.1425);
            assert_ulps_eq!(ranks[3], 0.05);
            assert_eq!(superstep, 3);
        }

        #[test]
        fn circle() {
            let graph = Graph::create(
                vec!["V/A".to_string(), "V/B".to_string(), "V/C".to_string()],
                vec![
                    ("V/A".to_string(), "V/B".to_string()),
                    ("V/B".to_string(), "V/C".to_string()),
                    ("V/C".to_string(), "V/A".to_string()),
                ],
            );
            let mut edge_rank = EdgeVector(vec![1. / 3.; graph.number_of_edges() as usize]);
            let normalization = edge_normalization(&graph);

            next_superstep(&mut edge_rank, &graph, &normalization, 0.85);
            assert_ulps_eq!(edge_rank.0[0], 1. / 3.);
            assert_ulps_eq!(edge_rank.0[1], 1. / 3.);
            assert_ulps_eq!(edge_rank.0[2], 1. / 3.);

            let (ranks, superstep) = dbg!(line_rank(&graph, 10, 0.85).unwrap());

            assert_eq!(ranks.len(), 3);
            assert_ulps_eq!(ranks[0], 2. / 3.);
            assert_ulps_eq!(ranks[1], 2. / 3.);
            assert_ulps_eq!(ranks[2], 2. / 3.);
            assert_eq!(superstep, 0);
        }
    }
}
