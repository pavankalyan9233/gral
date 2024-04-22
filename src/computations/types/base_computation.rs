use std::any::Any;
use std::sync::{Arc, RwLock};

use crate::graph_store::graph::Graph;

pub trait BaseComputation {
    fn is_ready(&self) -> bool;
    fn get_error(&self) -> (i32, String);
    fn cancel(&mut self);
    fn get_total(&self) -> u32;
    fn get_progress(&self) -> u32;
    fn get_graph(&self) -> Arc<RwLock<Graph>>;
    fn algorithm_name(&self) -> String;
    fn as_any(&self) -> &dyn Any;
    fn nr_results(&self) -> u64;
    fn get_result(&self, which: u64) -> (String, String);
    fn memory_usage(&self) -> usize;
}
