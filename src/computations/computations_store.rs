use crate::computations::types::BaseComputation;
use metrics::{decrement_gauge, increment_gauge};
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::{Arc, Mutex, RwLock};
use warp::Filter;

pub struct ComputationsStore {
    pub list: HashMap<u64, Arc<RwLock<dyn BaseComputation + Send + Sync>>>,
    next_id: u64,
}

impl ComputationsStore {
    pub fn new() -> Self {
        ComputationsStore {
            list: HashMap::new(),
            next_id: 1,
        }
    }
    pub fn register(&mut self, comp: Arc<RwLock<dyn BaseComputation + Send + Sync>>) -> u64 {
        let comp_id = self.next_id;
        self.next_id += 1;
        self.list.insert(comp_id, comp);
        increment_gauge!("number_of_jobs", 1.0);
        comp_id
    }
    pub fn remove(&mut self, id: u64) {
        let found = self.list.remove(&id);
        if found.is_some() {
            decrement_gauge!("number_of_jobs", 1.0);
        }
    }
}

impl Default for ComputationsStore {
    fn default() -> Self {
        ComputationsStore::new()
    }
}

pub fn with_computations(
    computations: Arc<Mutex<ComputationsStore>>,
) -> impl Filter<Extract = (Arc<Mutex<ComputationsStore>>,), Error = Infallible> + Clone {
    warp::any().map(move || computations.clone())
}
