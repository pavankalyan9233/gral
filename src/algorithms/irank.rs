use crate::graph_store::graphs::Graph;
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

    let nr = g.number_of_vertices() as usize;
    let pos = find_collection_name_column(g)?;
    info!("Counting collection sizes...");
    let sizes = count_collection_names(g, pos)?;
    info!("Building size table...");
    let size_table = determine_size_table(g, pos, &sizes);

    let mut rank = vec![1.0 / nr as f64; nr];
    let mut new_rank: Vec<f64> = Vec::with_capacity(nr);
    for st in size_table.iter() {
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
            let first_edge = g.edge_index_by_from[v] as usize;
            let last_edge = g.edge_index_by_from[v + 1] as usize;
            let edge_nr = last_edge - first_edge;
            if edge_nr > 0 {
                let tosend = damping_factor * rankv / edge_nr as f64;
                for wi in first_edge..last_edge {
                    let w = g.edges_by_from[wi].to_u64() as usize;
                    new_rank[w] += tosend;
                }
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
    use serde_json::json;

    #[test]
    fn test_irank_cyclic() {
        let g_arc = make_cyclic_graph(10);
        let mut g = g_arc.write().unwrap();
        g.vertex_column_names = vec!["@collectionname".to_string()];
        g.vertex_json = vec![Vec::new()];
        for _i in 0..10 {
            g.vertex_json[0].push(json!("c"));
        }
        g.vertex_column_types = vec!["string".to_string()];
        let (rank, steps) = i_rank(&g, 5, 0.85).unwrap();
        assert_eq!(steps, 1);
        for i in 0..10 {
            assert!((rank[i] - 1.0 / 10.0).abs() < 0.000001);
        }
        println!("{:?}", rank);
    }

    #[test]
    fn test_irank_star() {
        let g_arc = make_star_graph(10);
        let mut g = g_arc.write().unwrap();
        g.vertex_column_names = vec!["@collectionname".to_string()];
        g.vertex_json = vec![Vec::new()];
        for _i in 0..10 {
            g.vertex_json[0].push(json!("c"));
        }
        g.vertex_column_types = vec!["string".to_string()];
        let (rank, steps) = i_rank(&g, 100, 0.85).unwrap();
        assert!(steps > 50 && steps < 70);
        assert!(0.49 < rank[9] && rank[9] < 0.50);
        assert!(0.05 < rank[0] && rank[0] < 0.06);
        println!("{:?}", rank);
    }
}
