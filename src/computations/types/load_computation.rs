use crate::computations::types::BaseComputation;
use crate::graph_store::graph::Graph;
use std::any::Any;
use std::sync::{Arc, RwLock};

pub struct LoadComputation {
    pub graph: Arc<RwLock<Graph>>,
    pub shall_stop: bool,
    pub total: u32,
    pub progress: u32,
    pub error_code: i32,
    pub error_message: String,
}

impl BaseComputation for LoadComputation {
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
        "".to_string()
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn nr_results(&self) -> u64 {
        0
    }
    fn get_result(&self, _which: u64) -> (String, String) {
        ("".to_string(), "".to_string())
    }
    fn memory_usage(&self) -> usize {
        // Memory for graph accounted for there!
        0
    }
}
