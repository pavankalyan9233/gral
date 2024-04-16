use crate::graph_store::graph::Graph;
use crate::graph_store::vertex_key_index::VertexIndex;
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
    for co in col {
        let mut s = co.to_string();
        if s.starts_with('\"') && s.ends_with('\"') && s.len() >= 2 {
            s = (s[1..s.len() - 1]).to_string();
        }
        res.push(s);
    }
    res
}

pub fn labelpropagation_sync(
    g: &Graph,
    supersteps: u32,
    labelname: &str,
    random_tiebreak: bool,
) -> Result<(Vec<String>, usize, u32), String> {
    info!("Running synchronous label propagation...");
    let start = std::time::SystemTime::now();

    let nr = g.number_of_vertices() as usize;
    let pos = find_label_name_column(g, labelname)?;
    let all_labels: Vec<String> = load_labels(g, pos);
    let mut labels: Vec<&String> = Vec::with_capacity(nr);
    for lab in all_labels.iter() {
        labels.push(lab);
    }
    let mut newlabels: Vec<&String> = Vec::with_capacity(nr);

    // Do up to so many supersteps:
    let mut step: u32 = 0;
    let mut rng = thread_rng(); // in case we break ties randomly!
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
            let vi = VertexIndex::new(v as u64);
            if g.out_neighbour_count(vi) > 0 {
                g.out_neighbours(vi).for_each(|sink| {
                    let lab = labels[sink.to_u64() as usize];
                    let count = counts.get_mut(lab);
                    match count {
                        Some(countref) => {
                            *countref += 1;
                        }
                        None => {
                            counts.insert(lab, 1);
                        }
                    }
                });
            }
            // Now incoming edges:
            if g.in_neighbour_count(vi) > 0 {
                g.in_neighbours(vi).for_each(|source| {
                    let lab = labels[source.to_u64() as usize];
                    let count = counts.get_mut(lab);
                    match count {
                        Some(countref) => {
                            *countref += 1;
                        }
                        None => {
                            counts.insert(lab, 1);
                        }
                    }
                });
            }
            let mut choice: &String = labels[v]; // default to old label!
            if random_tiebreak {
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
                    choice = if max_labels.len() == 1 {
                        max_labels[0]
                    } else {
                        max_labels[rng.gen_range(0..max_labels.len())]
                    }
                }
            } else {
                // deterministic tiebreak:
                if !counts.is_empty() {
                    // Now count the multiplicities and take the largest one,
                    // break the tie by taking the smallest label:
                    let mut max_mult: u64 = 0;
                    let mut max_label: &String = labels[v];
                    for (k, m) in counts.iter() {
                        if *m >= max_mult {
                            if *m > max_mult {
                                max_mult = *m;
                                max_label = *k;
                            } else if *k < max_label {
                                max_label = *k;
                            }
                        }
                    }
                    choice = max_label;
                }
            }
            newlabels.push(choice);
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
    let mut total_label_size: usize = 0;
    for s in &labels {
        total_label_size += s.len();
        result.push((*s).clone());
    }
    Ok((result, total_label_size, step))
}

pub fn labelpropagation_async(
    g: &Graph,
    supersteps: u32,
    labelname: &str,
    random_tiebreak: bool,
) -> Result<(Vec<String>, usize, u32), String> {
    info!("Running asynchronous label propagation...");
    let start = std::time::SystemTime::now();

    let nr = g.number_of_vertices() as usize;
    let pos = find_label_name_column(g, labelname)?;
    let all_labels: Vec<String> = load_labels(g, pos);
    let mut labels: Vec<&String> = Vec::with_capacity(nr);
    for lab in all_labels.iter() {
        labels.push(lab);
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
        for v in order.iter() {
            let mut counts = HashMap::<&String, u64>::with_capacity(101);
            let vi = VertexIndex::new(*v as u64);
            if g.out_neighbour_count(vi) > 0 {
                g.out_neighbours(vi).for_each(|sink| {
                    let lab = labels[sink.to_u64() as usize];
                    let count = counts.get_mut(lab);
                    match count {
                        Some(countref) => {
                            *countref += 1;
                        }
                        None => {
                            counts.insert(lab, 1);
                        }
                    }
                });
            };
            // Now incoming edges:
            if g.in_neighbour_count(vi) > 0 {
                g.in_neighbours(vi).for_each(|source| {
                    let lab = labels[source.to_u64() as usize];
                    let count = counts.get_mut(lab);
                    match count {
                        Some(countref) => {
                            *countref += 1;
                        }
                        None => {
                            counts.insert(lab, 1);
                        }
                    }
                });
            }
            let mut choice: &String = labels[*v];
            if random_tiebreak {
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
                    choice = if max_labels.len() == 1 {
                        max_labels[0]
                    } else {
                        max_labels[rng.gen_range(0..max_labels.len())]
                    }
                }
            } else {
                // deterministic tiebreak:
                if !counts.is_empty() {
                    // Now count the multiplicities and take the largest one,
                    // break the tie by taking the smallest label:
                    let mut max_mult: u64 = 0;
                    let mut max_label: &String = labels[*v];
                    for (k, m) in counts.iter() {
                        if *m >= max_mult {
                            if *m > max_mult {
                                max_mult = *m;
                                max_label = *k;
                            } else if *k < max_label {
                                max_label = *k;
                            }
                        }
                    }
                    choice = max_label;
                }
            }
            if labels[*v] != choice {
                diffcount += 1;
                labels[*v] = choice;
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
    let mut total_label_size: usize = 0;
    for s in &labels {
        total_label_size += s.len();
        result.push((*s).clone());
    }
    Ok((result, total_label_size, step))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_store::examples::make_cyclic_graph;
    use crate::graph_store::examples::make_star_graph;
    use serde_json::json;

    #[test]
    fn test_label_propagation_sync_cyclic() {
        let mut g = make_cyclic_graph(10);
        g.vertex_column_names = vec!["startlabel".to_string()];
        g.vertex_json = vec![Vec::new()];
        for i in 0..10 {
            g.vertex_json[0].push(json!(format!("K{i}")));
        }
        g.vertex_column_types = vec!["string".to_string()];
        g.index_edges(true, true);
        let (labels, _size, _steps) = labelpropagation_sync(&g, 10, "startlabel", false).unwrap();
        println!("{:?}", labels);
    }

    #[test]
    fn test_label_propagation_sync_star() {
        let mut g = make_star_graph(10);
        g.vertex_column_names = vec!["startlabel".to_string()];
        g.vertex_json = vec![Vec::new()];
        for i in 0..10 {
            g.vertex_json[0].push(json!(format!("K{i}")));
        }
        g.vertex_column_types = vec!["string".to_string()];
        g.index_edges(true, true);
        let (labels, _size, _steps) = labelpropagation_sync(&g, 5, "startlabel", false).unwrap();
        println!("{:?}", labels);
    }
}
