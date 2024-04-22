use crate::computations::types::{BaseComputation, Component};
use crate::graph_store::graph::Graph;
use std::any::Any;
use std::sync::{Arc, RwLock};

pub struct AggregationComputation {
    pub graph: Arc<RwLock<Graph>>,
    pub compcomp: Arc<RwLock<dyn BaseComputation + Send + Sync>>,
    pub aggregation_attribute: String,
    pub shall_stop: bool,
    pub total: u32,
    pub progress: u32,
    pub error_code: i32,
    pub error_message: String,
    pub result: Vec<Component>,
}

impl BaseComputation for AggregationComputation {
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
        "ComponentsAggregation".to_string()
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn nr_results(&self) -> u64 {
        self.result.len() as u64
    }
    fn get_result(&self, which: u64) -> (String, String) {
        let comp = &self.result[which as usize];
        (
            comp.key.clone(),
            format!(
                r#""representative":"{}","size":{},"aggregation":{}"#,
                comp.representative,
                comp.size,
                serde_json::to_string(&comp.aggregation).unwrap(),
            ),
        )
    }
    fn memory_usage(&self) -> usize {
        self.result.len() * std::mem::size_of::<Component>()
    }
}
