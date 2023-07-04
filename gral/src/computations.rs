use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::{Arc, Mutex, RwLock};
use warp::Filter;

use crate::graphs::{Graph, KeyOrHash};

pub trait Computation {
    fn is_ready(&self) -> bool;
    fn cancel(&mut self);
    fn dump_result(&self, out: &mut Vec<u8>) -> Result<(), String>;
    fn get_result(&self) -> u64;
    fn algorithm_id(&self) -> u32;
    fn dump_vertex_results(
        &self,
        comp_id: u64,
        hashes: &Vec<KeyOrHash>,
        out: &mut Vec<u8>,
    ) -> Result<(), warp::Rejection>;
    fn get_graph(&self) -> Arc<RwLock<Graph>>;
}

pub struct Computations {
    pub list: HashMap<u64, Arc<Mutex<dyn Computation + Send>>>,
}

impl Computations {
    pub fn new() -> Self {
        Computations {
            list: HashMap::new(),
        }
    }
}

pub fn with_computations(
    computations: Arc<Mutex<Computations>>,
) -> impl Filter<Extract = (Arc<Mutex<Computations>>,), Error = Infallible> + Clone {
    warp::any().map(move || computations.clone())
}
