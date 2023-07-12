use byteorder::{BigEndian, WriteBytesExt};
use rand::Rng;
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::{Arc, Mutex, RwLock};
use warp::{Filter, Rejection};

use crate::api::{put_key_or_hash, CannotDumpVertexData, ComputationNotYetFinished};
use crate::graphs::{Graph, KeyOrHash};

pub trait Computation {
    fn is_ready(&self) -> bool;
    fn get_error(&self) -> (i32, String);
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
    pub fn register(&mut self, comp: Arc<Mutex<dyn Computation + Send>>) -> u64 {
        let mut rng = rand::thread_rng();
        let mut comp_id: u64;
        loop {
            comp_id = rng.gen::<u64>();
            if !self.list.contains_key(&comp_id) {
                break;
            }
        }
        self.list.insert(comp_id, comp);
        comp_id
    }
}

pub fn with_computations(
    computations: Arc<Mutex<Computations>>,
) -> impl Filter<Extract = (Arc<Mutex<Computations>>,), Error = Infallible> + Clone {
    warp::any().map(move || computations.clone())
}

pub struct ConcreteComputation {
    pub algorithm: u32,
    pub graph: Arc<RwLock<Graph>>,
    pub components: Option<Vec<u64>>,
    pub shall_stop: bool,
    pub number: u64,
}

impl Computation for ConcreteComputation {
    fn is_ready(&self) -> bool {
        self.components.is_some()
    }
    fn get_error(&self) -> (i32, String) {
        return (0, "".to_string());
    }
    fn cancel(&mut self) {
        self.shall_stop = true;
    }
    fn algorithm_id(&self) -> u32 {
        return self.algorithm;
    }
    fn get_graph(&self) -> Arc<RwLock<Graph>> {
        return self.graph.clone();
    }
    fn dump_result(&self, out: &mut Vec<u8>) -> Result<(), String> {
        out.write_u8(8).unwrap();
        out.write_u64::<BigEndian>(self.number).unwrap();
        Ok(())
    }
    fn get_result(&self) -> u64 {
        self.number
    }
    fn dump_vertex_results(
        &self,
        comp_id: u64,
        kohs: &Vec<KeyOrHash>,
        out: &mut Vec<u8>,
    ) -> Result<(), Rejection> {
        let comps = self.components.as_ref();
        match comps {
            None => {
                return Err(warp::reject::custom(ComputationNotYetFinished { comp_id }));
            }
            Some(result) => {
                let g = self.graph.read().unwrap();
                for koh in kohs.iter() {
                    let index = g.index_from_key_or_hash(koh);
                    match index {
                        None => {
                            put_key_or_hash(out, koh);
                            out.write_u8(0).unwrap();
                        }
                        Some(i) => {
                            put_key_or_hash(out, koh);
                            out.write_u8(8).unwrap();
                            out.write_u64::<BigEndian>(result[i.to_u64() as usize])
                                .unwrap();
                        }
                    }
                }
                return Ok(());
            }
        }
    }
}

pub struct LoadComputation {
    pub graph: Arc<RwLock<Graph>>,
    pub shall_stop: bool,
    pub total: u32,
    pub progress: u32,
    pub error_code: i32,
    pub error_message: String,
}

impl Computation for LoadComputation {
    fn is_ready(&self) -> bool {
        self.progress == self.total
    }
    fn get_error(&self) -> (i32, String) {
        (self.error_code, self.error_message.clone())
    }
    fn cancel(&mut self) {
        self.shall_stop = true;
    }
    fn algorithm_id(&self) -> u32 {
        0
    }
    fn get_graph(&self) -> Arc<RwLock<Graph>> {
        return self.graph.clone();
    }
    fn dump_result(&self, _out: &mut Vec<u8>) -> Result<(), String> {
        Ok(())
    }
    fn get_result(&self) -> u64 {
        0
    }
    fn dump_vertex_results(
        &self,
        comp_id: u64,
        _kohs: &Vec<KeyOrHash>,
        _out: &mut Vec<u8>,
    ) -> Result<(), Rejection> {
        Err(warp::reject::custom(CannotDumpVertexData { comp_id }))
    }
}
