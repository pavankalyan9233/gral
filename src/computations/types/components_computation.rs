use crate::computations::types::BaseComputation;
use crate::graph_store::graph::Graph;

use std::any::Any;
use std::sync::{Arc, RwLock};
pub struct ComponentsComputation {
    pub algorithm: String,
    pub graph: Arc<RwLock<Graph>>,
    pub components: Option<Vec<u64>>,
    pub next_in_component: Option<Vec<i64>>,
    pub shall_stop: bool,
    pub number: Option<u64>,
    pub error_code: i32,
    pub error_message: String,
}

impl BaseComputation for ComponentsComputation {
    fn is_ready(&self) -> bool {
        self.components.is_some()
    }
    fn get_error(&self) -> (i32, String) {
        (self.error_code, self.error_message.clone())
    }
    fn cancel(&mut self) {
        self.shall_stop = true;
    }
    fn get_total(&self) -> u32 {
        1
    }
    fn get_progress(&self) -> u32 {
        if self.components.is_some() {
            1
        } else {
            0
        }
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
        match &self.components {
            None => 0,
            Some(v) => v.len() as u64,
        }
    }
    fn get_result(&self, which: u64) -> (String, String) {
        match &self.components {
            None => ("".to_string(), "".to_string()),
            Some(vs) => {
                let guard = self.graph.read().unwrap();
                let key = std::str::from_utf8(&guard.index_to_key[which as usize])
                    .unwrap()
                    .to_string();
                let comp = std::str::from_utf8(&guard.index_to_key[vs[which as usize] as usize])
                    .unwrap()
                    .to_string();
                (key, comp)
            }
        }
    }
    fn memory_usage(&self) -> usize {
        let mut total: usize = 0;
        if let Some(c) = &self.components {
            total += c.len() * std::mem::size_of::<u64>();
        }
        if let Some(n) = &self.next_in_component {
            total += n.len() * std::mem::size_of::<u64>()
        }
        total
    }
}
