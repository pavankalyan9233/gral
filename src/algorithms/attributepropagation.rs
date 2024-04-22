use crate::computations::Computation;
use crate::graph_store::graph::Graph;
use crate::graph_store::vertex_key_index::VertexIndex;
use log::info;
use serde_json::Value;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

pub struct AttributePropagationComputation {
    pub graph: Arc<RwLock<Graph>>,
    pub sync: bool,
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
    fn get_result(&self, which: u64) -> (String, String) {
        let key;
        {
            let guard = self.graph.read().unwrap();
            key = std::str::from_utf8(&guard.index_to_key[which as usize])
                .unwrap()
                .to_string();
        }
        let labs = &self.label[which as usize];
        let mut s = String::with_capacity(16 * labs.len());
        s.push('[');
        let mut first = true;
        for l in labs {
            if !first {
                s.push(',');
                first = false;
            }
            s.push('"');
            s.push_str(l);
            s.push('"');
        }
        s.push(']');
        (key, s)
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

fn load_labels(g: &Graph, pos: usize) -> (Vec<String>, Vec<HashSet<u64>>) {
    let col = &g.vertex_json[pos];
    let nr = g.number_of_vertices() as usize;
    // First classify all label strings:
    let mut labelmap: HashMap<String, u64> = HashMap::with_capacity(1000);
    let mut labellist: Vec<String> = Vec::with_capacity(1000);
    labellist.push("".to_string());
    let mut labels: Vec<HashSet<u64>> = Vec::with_capacity(nr);
    let mut account = |c: &Value| -> u64 {
        if c.is_null() {
            return 0;
        }
        let mut s = c.to_string();
        if s.starts_with('\"') && s.ends_with('\"') && s.len() >= 2 {
            s = (s[1..s.len() - 1]).to_string();
        }
        if s.is_empty() {
            return 0;
        }
        let pos = labelmap.get(&s);
        if let Some(p) = pos {
            return *p;
        }
        let index = labellist.len() as u64;
        labelmap.insert(s.clone(), index);
        labellist.push(s);
        index
    };

    for co in col {
        let mut hs: HashSet<u64> = HashSet::with_capacity(1);
        if let Some(coco) = co.as_array() {
            for c in coco {
                let index = account(c);
                if index != 0 {
                    hs.insert(index);
                }
            }
        } else {
            let index = account(co);
            if index != 0 {
                hs.insert(index);
            }
        }
        labels.push(hs);
    }

    (labellist, labels)
}

pub fn attribute_propagation_async(
    g: &Graph,
    supersteps: u32,
    labelname: &str,
) -> Result<(Vec<Vec<String>>, usize, u32), String> {
    if !g.is_indexed_by_to() {
        return Err("The graph is missing the to-neighbour index which is required for the label propagation (async) algorithm.".to_string());
    }

    info!("Running attribute propagation...");
    let start = std::time::SystemTime::now();

    let nr = g.number_of_vertices() as usize;
    let pos = find_label_name_column(g, labelname)?;
    let labellist: Vec<String>;
    let mut labels: Vec<HashSet<u64>>;
    (labellist, labels) = load_labels(g, pos);

    // Do up to so many supersteps:
    let mut step: u32 = 0;
    while step < supersteps {
        step += 1;
        info!(
            "{:?} attribute propagation (async)  step {step}...",
            start.elapsed()
        );
        // Go through all vertices and determine new label list:
        // Only need to look at edges by to in the reverse direction:
        let mut diffcount: u64 = 0;
        for v in 0..nr {
            let vi = VertexIndex::new(v as u64);
            g.in_neighbours(vi).for_each(|fromv| {
                let labvec: Vec<u64> = labels[fromv.to_u64() as usize].iter().copied().collect();
                for l in labvec {
                    if labels[v].insert(l) {
                        diffcount += 1;
                    }
                }
            });
        }
        info!(
            "{:?} attribute propagation (async)  step {step}, changed: {diffcount}",
            start.elapsed()
        );
        if diffcount == 0 {
            break;
        }
    }
    let dur = start.elapsed();
    info!("attribute propagation (async) completed in {dur:?} seconds.");
    let mut result: Vec<Vec<String>> = Vec::with_capacity(nr);
    let mut total_label_size: usize = 0;
    for hs in &labels {
        let mut v = Vec::with_capacity(hs.len());
        for s in hs {
            total_label_size += labellist[*s as usize].len();
            v.push(labellist[*s as usize].clone());
        }
        result.push(v);
    }
    Ok((result, total_label_size, step))
}

pub fn attribute_propagation_sync(
    g: &Graph,
    supersteps: u32,
    labelname: &str,
) -> Result<(Vec<Vec<String>>, usize, u32), String> {
    if !g.is_indexed_by_to() {
        return Err("The graph is missing the to-neighbour index which is required for the label propagation (sync) algorithm.".to_string());
    }

    info!("Running attribute propagation...");
    let start = std::time::SystemTime::now();

    let nr = g.number_of_vertices() as usize;
    let pos = find_label_name_column(g, labelname)?;
    let labellist: Vec<String>;
    let mut labels: Vec<HashSet<u64>>;
    (labellist, labels) = load_labels(g, pos);

    // Do up to so many supersteps:
    let mut step: u32 = 0;
    while step < supersteps {
        step += 1;
        let mut newlabels: Vec<HashSet<u64>> = Vec::with_capacity(nr);
        info!(
            "{:?} attribute propagation (sync)  step {step}...",
            start.elapsed()
        );
        // Go through all vertices and determine new label list:
        // Only need to look at edges by to in the reverse direction:
        let mut diffcount: u64 = 0;
        for v in 0..nr {
            let vi = VertexIndex::new(v as u64);
            let mut hs: HashSet<u64> = HashSet::with_capacity(labels[v].len() + 1);
            for l in labels[v].iter() {
                hs.insert(*l);
            }
            g.in_neighbours(vi).for_each(|fromv| {
                for l in labels[fromv.to_u64() as usize].iter() {
                    if hs.insert(*l) {
                        diffcount += 1;
                    }
                }
            });
            newlabels.push(hs);
        }
        info!(
            "{:?} attribute propagation (sync)  step {step}, changed: {diffcount}",
            start.elapsed()
        );
        labels = newlabels;
        if diffcount == 0 {
            break;
        }
    }
    let dur = start.elapsed();
    info!("attribute propagation (sync) completed in {dur:?} seconds.");
    let mut result: Vec<Vec<String>> = Vec::with_capacity(nr);
    let mut total_label_size: usize = 0;
    for hs in &labels {
        let mut v = Vec::with_capacity(hs.len());
        for s in hs {
            total_label_size += labellist[*s as usize].len();
            v.push(labellist[*s as usize].clone());
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

    #[test]
    fn test_attribute_propagation_cyclic() {
        let mut g = make_cyclic_graph(10);
        g.vertex_column_names = vec!["startlabel".to_string()];
        g.vertex_json = vec![Vec::new()];
        g.vertex_json[0].push(json!("X"));
        for _i in 1..10 {
            g.vertex_json[0].push(json!(null));
        }
        g.vertex_column_types = vec!["string".to_string()];
        g.index_edges(false, true);
        // Async:
        let x = "X".to_string();
        let vx = vec![x];
        let (labels, _size, _steps) = attribute_propagation_async(&g, 10, "startlabel").unwrap();
        for i in 0..10 {
            assert_eq!(labels[i], vx);
        }
        // Sync:
        let (labels, _size, steps) = attribute_propagation_sync(&g, 10, "startlabel").unwrap();
        assert_eq!(steps, 10);
        for i in 0..10 {
            assert_eq!(labels[i], vx);
        }
    }

    #[test]
    fn test_label_propagation_star() {
        let mut g = make_star_graph(10);
        g.vertex_column_names = vec!["startlabel".to_string()];
        g.vertex_json = vec![Vec::new()];
        for i in 0..10 {
            g.vertex_json[0].push(json!(format!("K{i}")));
        }
        g.vertex_column_types = vec!["string".to_string()];
        g.index_edges(true, true);
        // Sync:
        let (labels, _size, steps) = attribute_propagation_sync(&g, 5, "startlabel").unwrap();
        assert_eq!(steps, 2);
        for i in 0..9 {
            let v = vec![format!("K{i}")];
            assert_eq!(labels[i], v);
        }
        assert_eq!(labels[9].len(), 10);
        // Async:
        let (labels, _size, steps) = attribute_propagation_async(&g, 5, "startlabel").unwrap();
        assert_eq!(steps, 2);
        for i in 0..9 {
            let v = vec![format!("K{i}")];
            assert_eq!(labels[i], v);
        }
        assert_eq!(labels[9].len(), 10);
    }

    #[test]
    fn test_attribute_propagation_btree() {
        let mut g = make_btree_graph(5);
        g.vertex_column_names = vec!["startlabel".to_string()];
        g.vertex_json = vec![Vec::new()];
        for i in 0..31 {
            g.vertex_json[0].push(json!(format!("K{i}")));
        }
        g.vertex_column_types = vec!["string".to_string()];
        g.index_edges(false, true);
        // Async:
        let (labels, _size, _steps) = attribute_propagation_async(&g, 6, "startlabel").unwrap();
        for i in 0..31 {
            let mut log: usize = 0;
            let mut j = i + 1;
            while j > 1 {
                j >>= 1;
                log += 1;
            }
            assert_eq!(labels[i].len(), log + 1);
        }
        // Sync:
        let (labels, _size, steps) = attribute_propagation_sync(&g, 6, "startlabel").unwrap();
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
    }

    #[test]
    fn test_graph_with_lists_and_nulls() {
        let mut g = make_cyclic_graph(10);
        g.vertex_column_names = vec!["startlabel".to_string()];
        g.vertex_json = vec![Vec::new()];
        g.vertex_json[0].push(json!("X"));
        for _i in 1..3 {
            g.vertex_json[0].push(json!(null));
        }
        for _i in 3..6 {
            g.vertex_json[0].push(json!([]));
        }
        for _i in 6..9 {
            g.vertex_json[0].push(json!(["X"]));
        }
        g.vertex_json[0].push(json!("Y"));
        g.vertex_column_types = vec!["string".to_string()];
        g.index_edges(false, true);
        // Async:
        let x = "X".to_string();
        let y = "Y".to_string();
        let vx = vec![x.clone(), y.clone()];
        let vy = vec![y, x];
        let (labels, _size, _steps) = attribute_propagation_async(&g, 10, "startlabel").unwrap();
        for i in 0..10 {
            assert!((labels[i] == vx) || (labels[i] == vy));
        }
        // Sync:
        let (labels, _size, _steps) = attribute_propagation_sync(&g, 10, "startlabel").unwrap();
        for i in 0..10 {
            assert!((labels[i] == vx) || (labels[i] == vy));
        }
    }

    #[test]
    fn does_not_run_when_graph_has_no_to_neighbour_index() {
        let g = Graph::create(
            vec!["V/A".to_string()],
            vec![("V/A".to_string(), "V/A".to_string())],
        );

        assert!(attribute_propagation_sync(&g, 10, "startlabel").is_err());
        assert!(attribute_propagation_async(&g, 10, "startlabel").is_err());
    }

    #[test]
    fn does_not_run_when_column_not_found() {
        let mut g = Graph::create(
            vec!["V/A".to_string()],
            vec![("V/A".to_string(), "V/A".to_string())],
        );
        g.index_edges(false, true);

        assert!(attribute_propagation_sync(&g, 10, "startlabel").is_err());
        assert!(attribute_propagation_async(&g, 10, "startlabel").is_err());
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
        assert_eq!(v, "[\"X\"]");
    }
}
