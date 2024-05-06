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
    if !g.is_indexed_by_from() {
        return Err("The graph is missing the from-neighbour index which is required for the label propagation (sync) algorithm.".to_string());
    }
    if !g.is_indexed_by_to() {
        return Err("The graph is missing the to-neighbour index which is required for the label propagation (sync) algorithm.".to_string());
    }

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
    if !g.is_indexed_by_from() {
        return Err("The graph is missing the from-neighbour index which is required for the label propagation (async) algorithm.".to_string());
    }
    if !g.is_indexed_by_to() {
        return Err("The graph is missing the to-neighbour index which is required for the label propagation (async) algorithm.".to_string());
    }

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
            "{:?} label propagation (async)  step {step}...",
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
            "{:?} label propagation (async)  step {step}, difference count {diffcount}",
            start.elapsed()
        );
        if diffcount == 0 {
            break;
        }
    }
    let dur = start.elapsed();
    info!("label propagation (async) completed in {dur:?} seconds.");
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

    fn add_vertex_id_as_label_with_prefix(g: &mut Graph, label: String, prefix: String) {
        let vertex_column_position = g.vertex_column_names.len();
        g.vertex_column_names.push(label);
        let nr_vertices = g.number_of_vertices();
        g.vertex_json.push(Vec::new());
        for i in 0..nr_vertices {
            g.vertex_json[vertex_column_position].push(json!(format!("{}{}", prefix, i)));
        }
        g.vertex_column_types.push("string".to_string());
    }
    fn add_vertex_id_as_label(g: &mut Graph, label: String) {
        add_vertex_id_as_label_with_prefix(g, label, "K".to_string());
    }
    fn assert_labels_are_in_same_component(labels: &Vec<String>) {
        if labels.len() > 0 {
            labels
                .iter()
                .for_each(|label| assert_eq!(label, &labels[0]));
        }
    }

    #[test]
    fn does_not_run_when_graph_has_no_from_neighbour_index() {
        let mut g = Graph::create(
            vec!["V/A".to_string()],
            vec![("V/A".to_string(), "V/A".to_string())],
        );
        g.index_edges(false, true);

        assert!(labelpropagation_sync(&g, 10, "startlabel", false).is_err());
        assert!(labelpropagation_async(&g, 10, "startlabel", false).is_err());
    }

    #[test]
    fn does_not_run_when_graph_has_no_to_neighbour_index() {
        let mut g = Graph::create(
            vec!["V/A".to_string()],
            vec![("V/A".to_string(), "V/A".to_string())],
        );
        g.index_edges(true, false);

        assert!(labelpropagation_sync(&g, 10, "startlabel", false).is_err());
        assert!(labelpropagation_async(&g, 10, "startlabel", false).is_err());
    }

    #[test]
    fn requires_graph_with_requested_label_name() {
        let mut g = Graph::create(vec!["V/A".to_string()], vec![]);
        g.index_edges(true, true);
        add_vertex_id_as_label(&mut g, "other_label".to_string());

        assert!(labelpropagation_sync(&g, 100, "start_label", false).is_err());
        assert!(labelpropagation_async(&g, 100, "start_label", false).is_err());
    }

    #[test]
    fn runs_on_given_label() {
        let mut g = Graph::create(vec!["V/A".to_string()], vec![]);
        g.index_edges(true, true);
        add_vertex_id_as_label_with_prefix(&mut g, "other_label".to_string(), "K".to_string());
        add_vertex_id_as_label_with_prefix(&mut g, "start_label".to_string(), "L".to_string());

        let (labels, _label_size, _step) =
            labelpropagation_sync(&g, 100, "start_label", false).unwrap();

        assert_eq!(labels, vec!["L0"]);

        let (labels, _label_size, _step) =
            labelpropagation_async(&g, 100, "start_label", false).unwrap();

        assert_eq!(labels, vec!["L0"]);
    }

    #[test]
    fn gives_empty_results_on_empty_graph() {
        let mut g = Graph::create(vec![], vec![]);
        add_vertex_id_as_label(&mut g, "start_label".to_string());
        g.index_edges(true, true);

        let (labels, _label_size, step) =
            labelpropagation_sync(&g, 100, "start_label", false).unwrap();

        assert_eq!(step, 1);
        assert_eq!(labels, Vec::<String>::new());

        let (labels, _label_size, step) =
            labelpropagation_async(&g, 100, "start_label", false).unwrap();

        assert_eq!(step, 1);
        assert_eq!(labels, Vec::<String>::new());
    }

    #[test]
    fn labels_of_unconnected_graph_are_start_labels() {
        let mut g = Graph::create(vec!["V/A".to_string(), "V/B".to_string()], vec![]);
        add_vertex_id_as_label_with_prefix(&mut g, "start_label".to_string(), "V".to_string());
        g.index_edges(true, true);

        let (labels, _label_size, step) =
            labelpropagation_sync(&g, 100, "start_label", false).unwrap();

        assert_eq!(step, 1);
        assert_eq!(labels, vec!["V0", "V1"]);

        let (labels, _label_size, step) =
            labelpropagation_async(&g, 100, "start_label", false).unwrap();

        assert_eq!(step, 1);
        assert_eq!(labels, vec!["V0", "V1"]);
    }

    mod sync_version {
        use super::*;

        #[test]
        fn does_not_converge_on_alternating_graph() {
            let mut g = Graph::create(
                vec!["V/A".to_string(), "V/B".to_string()],
                vec![("V/A".to_string(), "V/B".to_string())],
            );
            g.index_edges(true, true);
            add_vertex_id_as_label(&mut g, "start_label".to_string());

            let (labels, _label_size, step) =
                labelpropagation_sync(&g, 100, "start_label", false).unwrap();

            assert_eq!(step, 100);
            assert!(labels[0] != labels[1]);
        }

        #[test]
        fn gives_results_of_ldbc_example_directed() {
            let mut g = Graph::create(
                vec![
                    "1".to_string(),
                    "2".to_string(),
                    "3".to_string(),
                    "4".to_string(),
                    "5".to_string(),
                    "6".to_string(),
                    "7".to_string(),
                    "8".to_string(),
                    "9".to_string(),
                    "10".to_string(),
                ],
                vec![
                    ("1".to_string(), "3".to_string()),
                    ("1".to_string(), "5".to_string()),
                    ("2".to_string(), "4".to_string()),
                    ("2".to_string(), "5".to_string()),
                    ("2".to_string(), "10".to_string()),
                    ("3".to_string(), "1".to_string()),
                    ("3".to_string(), "5".to_string()),
                    ("3".to_string(), "8".to_string()),
                    ("3".to_string(), "10".to_string()),
                    ("5".to_string(), "3".to_string()),
                    ("5".to_string(), "4".to_string()),
                    ("5".to_string(), "8".to_string()),
                    ("6".to_string(), "3".to_string()),
                    ("6".to_string(), "4".to_string()),
                    ("7".to_string(), "4".to_string()),
                    ("8".to_string(), "1".to_string()),
                    ("9".to_string(), "4".to_string()),
                ],
            );
            g.vertex_column_names.push("start_label".to_string());
            g.vertex_json = vec![vec![
                json!("A1"),
                json!("B2"),
                json!("C3"),
                json!("D4"),
                json!("E5"),
                json!("F6"),
                json!("G7"),
                json!("H8"),
                json!("I9"),
                json!("J10"),
            ]];
            g.index_edges(true, true);

            let (labels, _label_size, steps) =
                labelpropagation_sync(&g, 2, "start_label", false).unwrap();

            assert_eq!(steps, 2);
            assert_eq!(
                labels,
                vec!["A1", "B2", "C3", "D4", "A1", "A1", "B2", "C3", "B2", "A1"]
            );
        }
    }

    mod async_version {
        use super::*;

        #[test]
        fn converges_on_alternating_graph() {
            let mut g = Graph::create(
                vec!["V/A".to_string(), "V/B".to_string()],
                vec![("V/A".to_string(), "V/B".to_string())],
            );
            g.index_edges(true, true);
            add_vertex_id_as_label(&mut g, "start_label".to_string());

            let (_labels, _label_size, step) =
                labelpropagation_async(&g, 100, "start_label", false).unwrap();

            assert!(step < 100);
        }

        #[test]
        fn detects_community() {
            let mut g = Graph::create(
                vec![
                    "V/A".to_string(),
                    "V/B".to_string(),
                    "V/C".to_string(),
                    "V/D".to_string(),
                    "V/E".to_string(),
                ],
                vec![
                    ("V/A".to_string(), "V/B".to_string()),
                    ("V/C".to_string(), "V/B".to_string()),
                    ("V/E".to_string(), "V/D".to_string()),
                    ("V/D".to_string(), "V/B".to_string()),
                ],
            );
            g.index_edges(true, true);
            add_vertex_id_as_label(&mut g, "start_label".to_string());

            let (labels, _size, _steps) =
                labelpropagation_async(&g, 100, "start_label", false).unwrap();

            assert_labels_are_in_same_component(&labels);
        }

        #[test]
        fn detects_one_community_on_cyclic_graph() {
            let mut g = make_cyclic_graph(10);
            add_vertex_id_as_label(&mut g, "start_label".to_string());
            g.index_edges(true, true);

            let (labels, _size, _steps) =
                labelpropagation_async(&g, 100, "start_label", false).unwrap();

            assert_labels_are_in_same_component(&labels);
        }

        #[test]
        fn detects_one_community_on_star_graph() {
            let mut g = make_star_graph(10);
            add_vertex_id_as_label(&mut g, "start_label".to_string());
            g.index_edges(true, true);

            let (labels, _size, _steps) =
                labelpropagation_async(&g, 100, "start_label", false).unwrap();

            assert_labels_are_in_same_component(&labels);
        }
    }
}
