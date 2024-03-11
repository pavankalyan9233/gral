use crate::graphs::Graph;
use log::info;
use rand::{seq::SliceRandom, thread_rng, Rng};
use std::collections::HashMap;

fn find_label_name_column(g: &Graph, l: &str) -> Result<usize, String> {
    // First count types of vertices, use column with "@collectionname":
    let pos = g.vertex_column_names.iter().position(|s| s == l);
    match pos {
        None => Err(format!("Need '{l}' as a column name in column store!")),
        Some(pos) => Ok(pos),
    }
}

fn load_labels(g: &Graph, pos: usize) -> Vec<String> {
    let col = &g.vertex_json[pos];
    let nr = g.number_of_vertices() as usize;
    let mut res: Vec<String> = Vec::with_capacity(nr);
    for i in 0..nr as usize {
        let mut s = col[i].to_string();
        if s.starts_with("\"") && s.ends_with("\"") && s.len() >= 2 {
            s = (&s[1..s.len() - 1]).to_string();
        }
        res.push(s);
    }
    res
}

pub fn labelpropagation_sync(
    g: &Graph,
    supersteps: u32,
    labelname: &str,
) -> Result<(Vec<String>, u32), String> {
    info!("Running synchronous label propagation...");
    let start = std::time::SystemTime::now();

    let nr = g.number_of_vertices() as usize;
    let pos = find_label_name_column(g, labelname)?;
    let all_labels: Vec<String> = load_labels(g, pos);
    let mut labels: Vec<&String> = Vec::with_capacity(nr);
    for i in 0..nr {
        labels.push(&all_labels[i]);
    }
    let mut newlabels: Vec<&String> = Vec::with_capacity(nr);

    // Do up to so many supersteps:
    let mut step: u32 = 0;
    let mut rng = thread_rng();
    while step < supersteps {
        step += 1;
        info!(
            "{:?} label propagation (sync)  step {step}...",
            start.elapsed()
        );
        // Go through all vertices and determine new label, need to look at
        // directed edges in both directions!
        for v in 0..nr {
            let mut counts = HashMap::<&String, u64>::with_capacity(101);
            let first_edge = g.edge_index_by_from[v] as usize;
            let last_edge = g.edge_index_by_from[v + 1] as usize;
            let edge_nr = last_edge - first_edge;
            if edge_nr > 0 {
                for wi in first_edge..last_edge {
                    let w = g.edges_by_from[wi].to_u64() as usize;
                    let lab = labels[w];
                    let count = counts.get_mut(lab);
                    match count {
                        Some(countref) => {
                            *countref += 1;
                        }
                        None => {
                            counts.insert(lab, 1);
                        }
                    }
                }
            };
            // Now incoming edges:
            let first_edge = g.edge_index_by_to[v] as usize;
            let last_edge = g.edge_index_by_to[v + 1] as usize;
            let edge_nr = last_edge - first_edge;
            if edge_nr > 0 {
                for wi in first_edge..last_edge {
                    let w = g.edges_by_to[wi].to_u64() as usize;
                    let lab = labels[w];
                    let count = counts.get_mut(lab);
                    match count {
                        Some(countref) => {
                            *countref += 1;
                        }
                        None => {
                            counts.insert(lab, 1);
                        }
                    }
                }
            }
            if !counts.is_empty() {
                // Now count the multiplicities and take the largest one:
                let mut max_mult: u64 = 0;
                let mut max_labels: Vec<&String> = Vec::with_capacity(5);
                for (k, m) in counts.iter() {
                    if *m >= max_mult {
                        if *m > max_mult {
                            max_mult = *m;
                            max_labels.clear();
                            max_labels.push(*k);
                        } else {
                            max_labels.push(*k);
                        }
                    }
                }
                let choice = if max_labels.len() == 1 {
                    max_labels[0]
                } else {
                    max_labels[rng.gen_range(0..max_labels.len())]
                };
                newlabels.push(choice);
            } else {
                newlabels.push(labels[v]);
            }
        }
        let mut diffcount: u64 = 0;
        for v in 0..nr {
            if labels[v] != newlabels[v] {
                diffcount += 1;
            }
            labels[v] = newlabels[v];
        }
        newlabels.clear();
        info!(
            "{:?} label propagation (sync)  step {step}, difference count {diffcount}",
            start.elapsed()
        );
        if diffcount == 0 {
            break;
        }
    }
    let dur = start.elapsed();
    info!("label propagation (sync) completed in {dur:?} seconds.");
    let mut result: Vec<String> = Vec::with_capacity(nr);
    for s in &labels {
        result.push((*s).clone());
    }
    Ok((result, step))
}

pub fn labelpropagation_async(
    g: &Graph,
    supersteps: u32,
    labelname: &str,
) -> Result<(Vec<String>, u32), String> {
    info!("Running asynchronous label propagation...");
    let start = std::time::SystemTime::now();

    let nr = g.number_of_vertices() as usize;
    let pos = find_label_name_column(g, labelname)?;
    let all_labels: Vec<String> = load_labels(g, pos);
    let mut labels: Vec<&String> = Vec::with_capacity(nr);
    for i in 0..nr {
        labels.push(&all_labels[i]);
    }

    // Do up to so many supersteps:
    let mut step: u32 = 0;
    let mut order: Vec<usize> = (0..nr).collect();
    let mut rng = thread_rng();
    while step < supersteps {
        step += 1;
        info!(
            "{:?} label propagation (sync)  step {step}...",
            start.elapsed()
        );
        // Go through all vertices and determine new label, need to look at
        // directed edges in both directions!
        let mut diffcount: u64 = 0;
        order.shuffle(&mut rng);
        for vv in 0..nr {
            let v = order[vv];
            let mut counts = HashMap::<&String, u64>::with_capacity(101);
            let first_edge = g.edge_index_by_from[v] as usize;
            let last_edge = g.edge_index_by_from[v + 1] as usize;
            let edge_nr = last_edge - first_edge;
            if edge_nr > 0 {
                for wi in first_edge..last_edge {
                    let w = g.edges_by_from[wi].to_u64() as usize;
                    let lab = labels[w];
                    let count = counts.get_mut(lab);
                    match count {
                        Some(countref) => {
                            *countref += 1;
                        }
                        None => {
                            counts.insert(lab, 1);
                        }
                    }
                }
            };
            // Now incoming edges:
            let first_edge = g.edge_index_by_to[v] as usize;
            let last_edge = g.edge_index_by_to[v + 1] as usize;
            let edge_nr = last_edge - first_edge;
            if edge_nr > 0 {
                for wi in first_edge..last_edge {
                    let w = g.edges_by_to[wi].to_u64() as usize;
                    let lab = labels[w];
                    let count = counts.get_mut(lab);
                    match count {
                        Some(countref) => {
                            *countref += 1;
                        }
                        None => {
                            counts.insert(lab, 1);
                        }
                    }
                }
            }
            if !counts.is_empty() {
                // Now count the multiplicities and take the largest one:
                let mut max_mult: u64 = 0;
                let mut max_labels: Vec<&String> = Vec::with_capacity(5);
                for (k, m) in counts.iter() {
                    if *m >= max_mult {
                        if *m > max_mult {
                            max_mult = *m;
                            max_labels.clear();
                            max_labels.push(*k);
                        } else {
                            max_labels.push(*k);
                        }
                    }
                }
                let choice = if max_labels.len() == 1 {
                    max_labels[0]
                } else {
                    max_labels[rng.gen_range(0..max_labels.len())]
                };
                if labels[v] != choice {
                    diffcount += 1;
                    labels[v] = choice;
                }
            }
        }
        info!(
            "{:?} label propagation (sync)  step {step}, difference count {diffcount}",
            start.elapsed()
        );
        if diffcount == 0 {
            break;
        }
    }
    let dur = start.elapsed();
    info!("label propagation (sync) completed in {dur:?} seconds.");
    let mut result: Vec<String> = Vec::with_capacity(nr);
    for s in &labels {
        result.push((*s).clone());
    }
    Ok((result, step))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graphs::examples::make_cyclic_graph;
    use crate::graphs::examples::make_star_graph;
    use serde_json::json;

    #[test]
    fn test_label_propagation_sync_cyclic() {
        let g_arc = make_cyclic_graph(10);
        let mut g = g_arc.write().unwrap();
        g.vertex_column_names = vec!["startlabel".to_string()];
        g.vertex_json = vec![Vec::new()];
        for i in 0..10 {
            g.vertex_json[0].push(json!(format!("K{i}")));
        }
        g.vertex_column_types = vec!["string".to_string()];
        g.index_edges(true, true);
        let (labels, _steps) = labelpropagation_sync(&g, 10, "startlabel").unwrap();
        println!("{:?}", labels);
    }

    #[test]
    fn test_label_propagation_sync_star() {
        let g_arc = make_star_graph(10);
        let mut g = g_arc.write().unwrap();
        g.vertex_column_names = vec!["startlabel".to_string()];
        g.vertex_json = vec![Vec::new()];
        for i in 0..10 {
            g.vertex_json[0].push(json!(format!("K{i}")));
        }
        g.vertex_column_types = vec!["string".to_string()];
        g.index_edges(true, true);
        let (labels, _steps) = labelpropagation_sync(&g, 5, "startlabel").unwrap();
        println!("{:?}", labels);
    }
}
