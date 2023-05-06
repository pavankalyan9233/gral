use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use warp::Filter;

use crate::graphs::VertexHash;

pub trait Computation {
    fn is_ready(&self) -> bool;
    fn cancel(&mut self);
    fn dump_result(&self, out: &mut Vec<u8>) -> Result<(), String>;
    fn dump_vertex_results(
        &self,
        hashes: &Vec<VertexHash>,
        out: &mut Vec<u8>,
    ) -> Result<(), String>;
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

    pub fn clear(&mut self) {
        self.list.clear();
    }
}

pub fn with_computations(
    computations: Arc<Mutex<Computations>>,
) -> impl Filter<Extract = (Arc<Mutex<Computations>>,), Error = Infallible> + Clone {
    warp::any().map(move || computations.clone())
}
