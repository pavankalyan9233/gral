use crate::computations::types::BaseComputation;
use crate::graph_store::graph::Graph;
use std::any::Any;
use std::sync::{Arc, RwLock};

pub struct LabelPropagationComputation {
    pub graph: Arc<RwLock<Graph>>,
    pub sync: bool,
    pub shall_stop: bool,
    pub total: u32,
    pub progress: u32,
    pub error_code: i32,
    pub error_message: String,
    pub label: Vec<String>,
    pub result_position: usize,
    pub label_size_sum: usize,
}

impl BaseComputation for LabelPropagationComputation {
    fn is_ready(&self) -> bool {
        self.progress == self.total
    }
    fn get_error(&self) -> (i32, String) {
        (self.error_code, self.error_message.clone())
    }
    fn cancel(&mut self) {
        self.shall_stop = true;
    }
    fn get_total(&self) -> u32 {
        self.total
    }
    fn get_progress(&self) -> u32 {
        self.progress
    }
    fn get_graph(&self) -> Arc<RwLock<Graph>> {
        self.graph.clone()
    }
    fn algorithm_name(&self) -> String {
        "Label Propagation".to_string()
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
        (key, self.label[which as usize].clone())
    }
    fn memory_usage(&self) -> usize {
        self.label_size_sum + self.label.len() * std::mem::size_of::<String>()
    }
}
