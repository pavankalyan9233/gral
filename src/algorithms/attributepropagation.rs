use crate::computations::Computation;
use crate::graph_store::graph::Graph;
use crate::graph_store::vertex_key_index::VertexIndex;
use log::info;
use serde_json::{json, Value};
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

pub struct AttributePropagationComputation {
    pub graph: Arc<RwLock<Graph>>,
    pub sync: bool,
    pub backwards: bool,
    pub shall_stop: bool,
    pub total: u32,
    pub progress: u32,
    pub error_code: i32,
    pub error_message: String,
    pub label: Vec<Vec<String>>,
    pub result_position: usize,
    pub label_size_sum: usize,
}

impl Computation for AttributePropagationComputation {
    fn is_ready(&self) -> bool {
        self.progress == self.total
    }
    fn get_error(&self) -> (i32, String) {
        (self.error_code, self.error_message.clone())
    }
    fn cancel(&mut self) {
        self.shall_stop = true;
    }
    fn algorithm_name(&self) -> String {
        "Attribute Propagation".to_string()
    }
    fn get_graph(&self) -> Arc<RwLock<Graph>> {
        self.graph.clone()
    }
    fn get_total(&self) -> u32 {
        self.total
    }
    fn get_progress(&self) -> u32 {
        self.progress
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn nr_results(&self) -> u64 {
        self.label.len() as u64
    }
    fn get_result(&self, which: u64) -> (String, Value) {
        let key;
        {
            let guard = self.graph.read().unwrap();
            key = std::str::from_utf8(&guard.index_to_key[which as usize])
                .unwrap()
                .to_string();
        }
        (key, json!(self.label[which as usize]))
    }
    fn memory_usage(&self) -> usize {
        self.label_size_sum + self.label.len() * std::mem::size_of::<Vec<String>>()
    }
}

fn find_label_name_column(g: &Graph, l: &str) -> Result<usize, String> {
    let pos = g.vertex_column_names.iter().position(|s| s == l);
    match pos {
        None => Err(format!("Need '{l}' as a column name in column store!")),
        Some(pos) => Ok(pos),
    }
}

// Struct to use as result of load_labels, see there for an explanation.
struct LabelList {
    pub list: Vec<String>,       // the actual labels
    pub sets: Vec<HashSet<u64>>, // the set for each vertex, as indices
}

// load_labels uses the column store of the graph to initialize the label
// sets for each vertex. To save memory and allocations, we store each
// occurring label only once in a list of all labels. Then we can store
// the sets as hash sets of integers (indexes into the list). This function
// computes this data structure in form of a struct LabelList above.
fn load_labels(g: &Graph, pos: usize) -> LabelList {
    let column = &g.vertex_json[pos];
    let nr = g.number_of_vertices() as usize;

    // First classify all label strings:
    let mut label_map: HashMap<String, u64> = HashMap::with_capacity(1000);
    let mut label_list = LabelList {
        list: Vec::with_capacity(1000),
        sets: Vec::with_capacity(nr),
    };
    label_list.list.push("".to_string());

    let mut account = |c: &Value| -> u64 {
        if c.is_null() {
            return 0;
        }
        let mut s: String = c.to_string();
        // Get rid of quotes from the JSON deserialization, if present:
        if s.starts_with('\"') && s.ends_with('\"') && s.len() >= 2 {
            s = (s[1..s.len() - 1]).to_string();
        }
        if s.is_empty() {
            return 0;
        }
        let pos = label_map.get(&s);
        if let Some(p) = pos {
            return *p;
        }
        let index = label_list.list.len() as u64;
        label_map.insert(s.clone(), index);
        label_list.list.push(s);
        index
    };

    for col_entry in column {
        let mut hs: HashSet<u64> = HashSet::with_capacity(1);
        if let Some(array) = col_entry.as_array() {
            for array_entry in array {
                let index = account(array_entry);
                if index != 0 {
                    hs.insert(index);
                }
            }
        } else {
            let index = account(col_entry);
            if index != 0 {
                hs.insert(index);
            }
        }
        label_list.sets.push(hs);
    }

    label_list
}

// The following is the main propagation function, it is used twice
// in the algorithm below:
fn do_propagate_work_async(sets: &mut [HashSet<u64>], from: usize, to: usize) -> u64 {
    let mut diff_count: u64 = 0;
    let labvec: Vec<u64> = sets[from].iter().copied().collect();
    for l in labvec {
        if sets[to].insert(l) {
            diff_count += 1;
        }
    }
    diff_count
}

pub fn attribute_propagation_async(
    g: &Graph,
    supersteps: u32,
    labelname: &str,
    backwards: bool,
) -> Result<(Vec<Vec<String>>, usize, u32), String> {
    if backwards && !g.is_indexed_by_from() {
        return Err("The graph is missing the from-neighbour index for backwards operation, which is required for the label propagation (async) algorithm.".to_string());
    }
    if !backwards && !g.is_indexed_by_to() {
        return Err("The graph is missing the to-neighbour index for forwards operation, which is required for the label propagation (async) algorithm.".to_string());
    }

    info!("Running attribute propagation...");
    let start = std::time::SystemTime::now();

    let nr = g.number_of_vertices() as usize;
    let pos = find_label_name_column(g, labelname)?;
    let mut label_list: LabelList = load_labels(g, pos);

    let mut step: u32 = 0;
    while step < supersteps {
        step += 1;
        info!(
            "{:?} attribute propagation (async)  step {step}...",
            start.elapsed()
        );
        // Go through all vertices and determine new label list:
        // Only need to look at edges by to in the reverse direction:
        let mut diff_count: u64 = 0;

        if backwards {
            for v in 0..nr {
                let vi = VertexIndex::new(v as u64);
                g.out_neighbours(vi).for_each(|fromv| {
                    diff_count +=
                        do_propagate_work_async(&mut label_list.sets, fromv.to_u64() as usize, v);
                });
            }
        } else {
            for v in 0..nr {
                let vi = VertexIndex::new(v as u64);
                g.in_neighbours(vi).for_each(|fromv| {
                    diff_count +=
                        do_propagate_work_async(&mut label_list.sets, fromv.to_u64() as usize, v);
                });
            }
        }
        info!(
            "{:?} attribute propagation (async)  step {step}, changed: {diff_count}",
            start.elapsed()
        );
        if diff_count == 0 {
            break;
        }
    }
    let dur = start.elapsed();
    info!("attribute propagation (async) completed in {dur:?} seconds.");
    let mut result: Vec<Vec<String>> = Vec::with_capacity(nr);
    let mut total_label_size: usize = 0;
    for hs in &label_list.sets {
        let mut v = Vec::with_capacity(hs.len());
        for s in hs {
            total_label_size += label_list.list[*s as usize].len();
            v.push(label_list.list[*s as usize].clone());
        }
        result.push(v);
    }
    Ok((result, total_label_size, step))
}

pub fn attribute_propagation_sync(
    g: &Graph,
    supersteps: u32,
    labelname: &str,
    backwards: bool,
) -> Result<(Vec<Vec<String>>, usize, u32), String> {
    if backwards && !g.is_indexed_by_from() {
        return Err("The graph is missing the from-neighbour index for backwards operation, which is required for the label propagation (async) algorithm.".to_string());
    }
    if !backwards && !g.is_indexed_by_to() {
        return Err("The graph is missing the to-neighbour index for forwards operation, which is required for the label propagation (async) algorithm.".to_string());
    }

    info!("Running attribute propagation...");
    let start = std::time::SystemTime::now();

    let nr = g.number_of_vertices() as usize;
    let pos = find_label_name_column(g, labelname)?;
    let mut label_list: LabelList = load_labels(g, pos);

    let mut step: u32 = 0;
    while step < supersteps {
        step += 1;
        let mut new_labels: Vec<HashSet<u64>> = Vec::with_capacity(nr);
        info!(
            "{:?} attribute propagation (sync)  step {step}...",
            start.elapsed()
        );
        // Go through all vertices and determine new label list:
        // Only need to look at edges by to in the reverse direction:
        let mut diff_count: u64 = 0;
        if backwards {
            for v in 0..nr {
                let vi = VertexIndex::new(v as u64);
                let mut hs: HashSet<u64> = HashSet::with_capacity(label_list.sets[v].len() + 1);
                for l in label_list.sets[v].iter() {
                    hs.insert(*l);
                }
                g.out_neighbours(vi).for_each(|fromv| {
                    for l in label_list.sets[fromv.to_u64() as usize].iter() {
                        if hs.insert(*l) {
                            diff_count += 1;
                        }
                    }
                });
                new_labels.push(hs);
            }
        } else {
            for v in 0..nr {
                let vi = VertexIndex::new(v as u64);
                let mut hs: HashSet<u64> = HashSet::with_capacity(label_list.sets[v].len() + 1);
                for l in label_list.sets[v].iter() {
                    hs.insert(*l);
                }
                g.in_neighbours(vi).for_each(|fromv| {
                    for l in label_list.sets[fromv.to_u64() as usize].iter() {
                        if hs.insert(*l) {
                            diff_count += 1;
                        }
                    }
                });
                new_labels.push(hs);
            }
        }
        info!(
            "{:?} attribute propagation (sync)  step {step}, changed: {diff_count}",
            start.elapsed()
        );
        label_list.sets = new_labels;
        if diff_count == 0 {
            break;
        }
    }
    let dur = start.elapsed();
    info!("attribute propagation (sync) completed in {dur:?} seconds.");
    let mut result: Vec<Vec<String>> = Vec::with_capacity(nr);
    let mut total_label_size: usize = 0;
    for hs in &label_list.sets {
        let mut v = Vec::with_capacity(hs.len());
        for s in hs {
            total_label_size += label_list.list[*s as usize].len();
            v.push(label_list.list[*s as usize].clone());
        }
        result.push(v);
    }
    Ok((result, total_label_size, step))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_store::examples::make_btree_graph;
    use crate::graph_store::examples::make_cyclic_graph;
    use crate::graph_store::examples::make_star_graph;
    use serde_json::json;

    fn add_vertex_id_as_label(g: &mut Graph) {
        let nr_vertices = g.number_of_vertices();
        g.vertex_json = vec![Vec::new()];
        for i in 0..nr_vertices {
            g.vertex_json[0].push(json!(format!("K{i}")));
        }
        g.vertex_column_types = vec!["string".to_string()];
    }

    #[test]
    fn propagates_one_label_to_all_vertices_in_cyclic_graph() {
        let mut g = make_cyclic_graph(10);
        g.vertex_column_names = vec!["start_label".to_string()];
        g.vertex_json = vec![Vec::new()];
        g.vertex_json[0].push(json!("X"));
        for _i in 1..10 {
            g.vertex_json[0].push(json!(null));
        }
        g.vertex_column_types = vec!["string".to_string()];
        g.index_edges(false, true);

        // Async:
        let (labels, size, _steps) =
            attribute_propagation_async(&g, 10, "start_label", false).unwrap();
        assert!(size > 0);

        for i in 0..10 {
            assert_eq!(labels[i], vec!["X".to_string()]);
        }

        // Sync:
        let (labels, size, steps) =
            attribute_propagation_sync(&g, 10, "start_label", false).unwrap();
        assert_eq!(steps, 10);
        for i in 0..10 {
            assert_eq!(labels[i], vec!["X".to_string()]);
        }
        assert!(size > 0);
    }

    #[test]
    fn propagates_all_labels_to_center_vertex_in_star_graph() {
        let mut g = make_star_graph(10);
        g.vertex_column_names = vec!["start_label".to_string()];
        add_vertex_id_as_label(&mut g);
        g.index_edges(true, true);

        // Sync:
        let (labels, size, steps) =
            attribute_propagation_sync(&g, 5, "start_label", false).unwrap();
        assert_eq!(steps, 2);
        for i in 0..9 {
            let v = vec![format!("K{i}")];
            assert_eq!(labels[i], v);
        }
        assert_eq!(labels[9].len(), 10);
        assert!(size > 0);

        // Async:
        let (labels, size, steps) =
            attribute_propagation_async(&g, 5, "start_label", false).unwrap();
        assert_eq!(steps, 2);
        for i in 0..9 {
            let v = vec![format!("K{i}")];
            assert_eq!(labels[i], v);
        }
        assert_eq!(labels[9].len(), 10);
        assert!(size > 0);
    }

    #[test]
    fn propagates_labels_down_a_btree() {
        let mut g = make_btree_graph(5);
        g.vertex_column_names = vec!["start_label".to_string()];
        add_vertex_id_as_label(&mut g);
        g.index_edges(false, true);

        // Async:
        let (labels, size, _steps) =
            attribute_propagation_async(&g, 6, "start_label", false).unwrap();
        for i in 0..31 {
            let mut log: usize = 0;
            let mut j = i + 1;
            while j > 1 {
                j >>= 1;
                log += 1;
            }
            assert_eq!(labels[i].len(), log + 1);
        }
        assert!(size > 0);

        // Sync:
        let (labels, size, steps) =
            attribute_propagation_sync(&g, 6, "start_label", false).unwrap();
        assert_eq!(steps, 5);
        for i in 0..31 {
            let mut log: usize = 0;
            let mut j = i + 1;
            while j > 1 {
                j >>= 1;
                log += 1;
            }
            assert_eq!(labels[i].len(), log + 1);
        }
        assert!(size > 0);
    }

    #[test]
    fn propagates_labels_up_in_btree_with_backwards() {
        let mut g = make_btree_graph(5);
        g.vertex_column_names = vec!["start_label".to_string()];
        add_vertex_id_as_label(&mut g);
        g.index_edges(true, false);

        // Async:
        let (labels, size, _steps) =
            attribute_propagation_async(&g, 6, "start_label", true).unwrap();
        for i in 0..31 {
            let mut log: usize = 0;
            let mut j = i + 1;
            while j > 1 {
                j >>= 1;
                log += 1;
            }
            assert_eq!(labels[i].len(), 2usize.pow(5 - log as u32) - 1);
        }
        assert!(size > 0);

        // Sync:
        let (labels, size, steps) = attribute_propagation_sync(&g, 6, "start_label", true).unwrap();
        assert_eq!(steps, 5);
        for i in 0..31 {
            let mut log: usize = 0;
            let mut j = i + 1;
            while j > 1 {
                j >>= 1;
                log += 1;
            }
            assert_eq!(labels[i].len(), 2usize.pow(5 - log as u32) - 1);
        }
        assert!(size > 0);
    }

    #[test]
    fn test_graph_with_lists_and_nulls() {
        let mut g = make_cyclic_graph(10);
        g.vertex_column_names = vec!["start_label".to_string()];
        g.vertex_json = vec![Vec::new()];
        g.vertex_json[0].push(json!("X"));
        for _i in 1..3 {
            g.vertex_json[0].push(json!(null));
        }
        for _i in 3..5 {
            g.vertex_json[0].push(json!([]));
        }
        g.vertex_json[0].push(json!(""));
        for _i in 6..8 {
            g.vertex_json[0].push(json!(["X"]));
        }
        g.vertex_json[0].push(json!(["X", "Y"]));
        g.vertex_json[0].push(json!("Y"));
        g.vertex_column_types = vec!["string".to_string()];
        g.index_edges(false, true);
        // Async:
        let x = "X".to_string();
        let y = "Y".to_string();
        let v_xy = vec![x.clone(), y.clone()];
        let v_yx = vec![y, x];
        let (labels, size, _steps) =
            attribute_propagation_async(&g, 10, "start_label", false).unwrap();
        for i in 0..10 {
            assert!((labels[i] == v_xy) || (labels[i] == v_yx));
        }
        assert!(size > 0);
        // Sync:
        let (labels, size, _steps) =
            attribute_propagation_sync(&g, 10, "start_label", false).unwrap();
        for i in 0..10 {
            assert!((labels[i] == v_xy) || (labels[i] == v_yx));
        }
        assert!(size > 0);
    }

    #[test]
    fn does_not_run_when_graph_has_no_to_neighbour_index() {
        let g = Graph::create(
            vec!["V/A".to_string()],
            vec![("V/A".to_string(), "V/A".to_string())],
        );

        assert!(attribute_propagation_sync(&g, 10, "start_label", false).is_err());
        assert!(attribute_propagation_async(&g, 10, "start_label", false).is_err());
    }

    #[test]
    fn does_not_run_when_graph_has_no_from_neighbour_index() {
        let g = Graph::create(
            vec!["V/A".to_string()],
            vec![("V/A".to_string(), "V/A".to_string())],
        );

        assert!(attribute_propagation_sync(&g, 10, "start_label", true).is_err());
        assert!(attribute_propagation_async(&g, 10, "start_label", true).is_err());
    }

    #[test]
    fn does_not_run_when_column_not_found() {
        let mut g = Graph::create(
            vec!["V/A".to_string()],
            vec![("V/A".to_string(), "V/A".to_string())],
        );
        g.index_edges(false, true);

        assert!(attribute_propagation_sync(&g, 10, "start_label", false).is_err());
        assert!(attribute_propagation_async(&g, 10, "start_label", false).is_err());
    }

    #[test]
    fn computation_object_methods() {
        let mut g = Graph::create(
            vec!["V/A".to_string()],
            vec![("V/A".to_string(), "V/A".to_string())],
        );
        g.index_edges(false, true);
        let mut apc = AttributePropagationComputation {
            graph: Arc::new(RwLock::new(g)),
            sync: false,
            backwards: false,
            shall_stop: false,
            total: 100,
            progress: 100,
            error_code: 0,
            error_message: "".to_string(),
            label: vec![vec!["X".to_string()]],
            result_position: 0,
            label_size_sum: 0,
        };
        assert!(apc.is_ready());
        let (c, m) = apc.get_error();
        assert_eq!(c, 0);
        assert_eq!(m, "");
        apc.cancel();
        assert_eq!(apc.algorithm_name(), "Attribute Propagation");
        let _gg = apc.get_graph();
        assert_eq!(apc.get_total(), 100);
        assert_eq!(apc.get_progress(), 100);
        assert_eq!(apc.nr_results(), 1);
        let (k, v) = apc.get_result(0);
        assert_eq!(k, "V/A");
        assert_eq!(v, json!(["X"]));
        assert!(apc.memory_usage() > 0);
        let _a = apc.as_any();
    }
}
