use crate::graph_store::graph::Graph;
use crate::graph_store::vertex_key_index::VertexIndex;
use log::{error, info};
use std::collections::HashMap;

fn find_collection_name_column(g: &Graph) -> Result<usize, String> {
    // First count types of vertices, use column with "@collectionname":
    let pos = g
        .vertex_column_names
        .iter()
        .position(|s| s == "@collectionname");
    match pos {
        None => {
            error!("Need @collectionname as a column in column store for irank!");
            Err("Need @collectionname as a column in column store!".to_string())
        }
        Some(pos) => Ok(pos),
    }
}

fn count_collection_names(g: &Graph, pos: usize) -> Result<HashMap<String, u64>, String> {
    let col = &g.vertex_json[pos];
    let mut res: HashMap<String, u64> = HashMap::with_capacity(101);
    for co in col.iter() {
        let s = co.to_string();
        let count = res.get_mut(&s);
        match count {
            None => {
                res.insert(s, 1);
            }
            Some(count) => {
                *count += 1;
            }
        };
    }
    Ok(res)
}

fn determine_size_table(g: &Graph, pos: usize, sizes: &HashMap<String, u64>) -> Vec<u64> {
    let col = &g.vertex_json[pos];
    let nr = g.number_of_vertices() as usize;
    let mut res: Vec<u64> = Vec::with_capacity(nr);
    for co in col.iter() {
        let s = co.to_string();
        let count = sizes.get(&s);
        assert!(count.is_some());
        if let Some(count) = count {
            res.push(*count);
        }
    }
    res
}

pub fn i_rank(g: &Graph, supersteps: u32, damping_factor: f64) -> Result<(Vec<f64>, u32), String> {
    info!("Running irank...");
    let start = std::time::SystemTime::now();

    if !g.is_indexed_by_from() {
        return Err("The graph is missing the from-neighbour index which is required for the irank algorithm.".to_string());
    }

    let nr = g.number_of_vertices() as usize;
    let pos = find_collection_name_column(g)?;
    info!("Counting collection sizes...");
    let sizes = count_collection_names(g, pos)?;
    info!("Building size table...");
    let size_table = determine_size_table(g, pos, &sizes);

    let mut rank: Vec<f64> = Vec::with_capacity(nr);
    let mut new_rank: Vec<f64> = Vec::with_capacity(nr);
    for st in size_table.iter() {
        rank.push(1.0 / *st as f64);
        new_rank.push(1.0 / *st as f64 * (1.0 - damping_factor));
    }
    // Do up to so many supersteps:
    let mut step: u32 = 0;
    while step < supersteps {
        step += 1;
        info!("{:?} irank step {step}...", start.elapsed());
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
            new_rank[v] = 1.0 / size_table[v] as f64 * (1.0 - damping_factor);
        }
        info!(
            "{:?} irank step {step}, rank maximal difference {maxdiff}",
            start.elapsed()
        );
        if maxdiff < 0.0000001 {
            break;
        }
    }
    let dur = start.elapsed();
    info!("irank completed in {dur:?} seconds.");
    Ok((rank, step))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_store::examples::make_cyclic_graph;
    use crate::graph_store::examples::make_star_graph;
    use approx::assert_ulps_eq;
    use serde_json::json;

    fn give_collectionname_column(g: &mut Graph, collname: &str) {
        g.vertex_column_names = vec!["@collectionname".to_string()];
        g.vertex_json = vec![Vec::new()];
        for _i in 0..g.number_of_vertices() {
            g.vertex_json[0].push(json!(collname));
        }
        g.vertex_column_types = vec!["string".to_string()];
    }

    mod ranks {
        use super::*;

        #[test]
        fn gives_empty_results_on_empty_graph() {
            let mut g = Graph::create(vec![], vec![]);
            g.index_edges(true, false);
            give_collectionname_column(&mut g, "c");

            let (rank, steps) = i_rank(&g, 100, 0.85).unwrap();

            assert_eq!(rank, Vec::<f64>::new());
            assert_eq!(steps, 1);
        }

        #[test]
        fn ranks_of_unconnected_graph_are_all_equal_if_same_collection() {
            let mut g = Graph::create(vec!["V/A".to_string(), "V/B".to_string()], vec![]);
            g.index_edges(true, false);
            give_collectionname_column(&mut g, "V");

            let (rank, steps) = i_rank(&g, 100, 0.85).unwrap();

            assert_ulps_eq!(rank[0], 0.5);
            assert_ulps_eq!(rank[1], 0.5);
            assert_eq!(steps, 1);
        }

        #[test]
        fn ranks_of_unconnected_graph_are_not_all_equal_if_different_collections() {
            let mut g = Graph::create(
                vec!["V/A".to_string(), "V/B".to_string(), "W/C".to_string()],
                vec![],
            );
            g.index_edges(true, false);
            g.vertex_column_names = vec!["@collectionname".to_string()];
            g.vertex_json = vec![vec![json!("V"), json!("V"), json!("W")]];
            g.vertex_column_types = vec!["string".to_string()];

            let (rank, steps) = i_rank(&g, 0, 0.85).unwrap();

            assert_ulps_eq!(rank[0], 0.5);
            assert_ulps_eq!(rank[1], 0.5);
            assert_ulps_eq!(rank[2], 1.0);
            assert_eq!(steps, 0);
        }

        #[test]
        fn test_irank_cyclic() {
            let mut g = make_cyclic_graph(10);
            give_collectionname_column(&mut g, "c");

            let (rank, steps) = i_rank(&g, 5, 0.85).unwrap();
            assert_eq!(steps, 1);
            for i in 0..10 {
                assert!((rank[i] - 1.0 / 10.0).abs() < 0.000001);
            }
            println!("{:?}", rank);
        }

        #[test]
        fn test_irank_star() {
            let mut g = make_star_graph(10);
            give_collectionname_column(&mut g, "c");

            let (rank, steps) = i_rank(&g, 100, 0.85).unwrap();
            assert!(steps > 50 && steps < 70);
            assert!(0.49 < rank[9] && rank[9] < 0.50);
            assert!(0.05 < rank[0] && rank[0] < 0.06);
            println!("{:?}", rank);
        }

        #[test]
        fn sum_of_ranks_is_normalized() {
            let mut g = make_star_graph(10);
            give_collectionname_column(&mut g, "c");

            let (rank, _steps) = i_rank(&g, 100, 0.85).unwrap();

            assert_ulps_eq!(rank.iter().sum::<f64>(), 1.0);
        }

        #[test]
        fn sum_of_ranks_is_normalized_multiple_collections() {
            let mut g = make_star_graph(10);
            g.vertex_column_names = vec!["@collectionname".to_string()];
            g.vertex_json = vec![Vec::new()];
            for _i in 0..5 {
                g.vertex_json[0].push(json!("c"));
            }
            for _i in 5..10 {
                g.vertex_json[0].push(json!("d"));
            }
            g.vertex_column_types = vec!["string".to_string()];

            let (rank, _steps) = i_rank(&g, 100, 0.85).unwrap();

            assert_ulps_eq!(rank.iter().sum::<f64>(), 2.0);
        }
    }

    mod supersteps {
        use super::*;

        #[test]
        fn stops_maximally_after_given_supersteps() {
            let mut g = make_star_graph(10);
            give_collectionname_column(&mut g, "c");

            let max_supersteps = 5;
            let (_rank, steps) = i_rank(&g, max_supersteps, 0.85).unwrap();

            assert_eq!(steps, max_supersteps);
        }

        #[test]
        fn stops_earlier_then_maximal_supersteps_when_converging() {
            let mut g = make_star_graph(10);
            give_collectionname_column(&mut g, "c");

            let max_supersteps = 100;
            let (_rank, steps) = i_rank(&g, max_supersteps, 0.85).unwrap();

            assert!(steps < max_supersteps);
        }
    }

    mod damping_factor {
        use super::*;

        #[test]
        fn damping_factor_determines_impact_of_neighbours() {
            let mut g = make_star_graph(10);
            give_collectionname_column(&mut g, "c");

            let (rank_lower_damping, steps_lower_damping) = dbg!(i_rank(&g, 100, 0.4).unwrap());
            let (rank_larger_damping, steps_larger_damping) = dbg!(i_rank(&g, 100, 0.85).unwrap());

            assert!(rank_lower_damping[9] < rank_larger_damping[9]);
            assert!(rank_lower_damping[0] > rank_larger_damping[0]);
            assert!(steps_lower_damping < steps_larger_damping);
        }

        #[test]
        fn damping_factor_zero_gives_equal_ranks() {
            let mut g = make_star_graph(10);
            give_collectionname_column(&mut g, "c");

            let (rank, steps) = i_rank(&g, 100, 0.0).unwrap();

            for i in 0..10 {
                assert_ulps_eq!(rank[i], 1.0 / 10.0);
            }
            assert_eq!(steps, 1);
        }
    }

    #[test]
    fn does_not_run_when_graph_has_no_collection_name_column() {
        let mut g = Graph::create(
            vec!["V/A".to_string()],
            vec![("V/A".to_string(), "V/A".to_string())],
        );
        g.index_edges(true, false);

        assert!(i_rank(&g, 100, 0.85).is_err());
    }

    #[test]
    fn does_not_run_when_graph_has_no_from_neighbour_index() {
        let g = Graph::create(
            vec!["V/A".to_string()],
            vec![("V/A".to_string(), "V/A".to_string())],
        );

        assert!(i_rank(&g, 100, 0.85).is_err());
    }
}
