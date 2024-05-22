use crate::computations::Computation;
use crate::computations::JobRuntime;
use crate::graph_store::graph::Graph;
use serde_json::Value;
use std::any::Any;
use std::collections::HashMap;
use std::mem;
use std::sync::{Arc, RwLock};
use std::time::Duration;

pub struct PythonComputation {
    pub graph: Arc<RwLock<Graph>>,
    pub algorithm: String,
    pub total: u32,
    pub progress: u32,
    pub error_code: i32,
    pub error_message: String,
    pub result: HashMap<u64, Value>,
    pub runtime: JobRuntime,
}

impl Computation for PythonComputation {
    fn is_ready(&self) -> bool {
        self.progress == self.total
    }
    fn get_error(&self) -> (i32, String) {
        (self.error_code, self.error_message.clone())
    }
    fn cancel(&mut self) {}
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
        self.algorithm.clone()
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn nr_results(&self) -> u64 {
        self.result.len() as u64
    }
    fn get_result(&self, which: u64) -> (String, Value) {
        let key;
        {
            let guard = self.graph.read().unwrap();
            key = std::str::from_utf8(&guard.index_to_key[which as usize])
                .unwrap()
                .to_string();
        }
        (key, self.result.get(&which).unwrap().clone())
    }
    fn memory_usage(&self) -> usize {
        let mut total_memory = self.result.len() * mem::size_of::<u64>();
        for (_key, v) in self.result.iter() {
            total_memory += mem::size_of_val(v);
        }
        total_memory
    }
    fn get_runtime(&self) -> Duration {
        self.runtime.get()
    }
}
