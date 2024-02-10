use byteorder::{BigEndian, WriteBytesExt};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::{Arc, Mutex, RwLock};
use warp::{Filter, Rejection};

use crate::graphs::{Graph, KeyOrHash};

/// An error object, which is used if a computation is not yet finished.
#[derive(Debug)]
pub struct ComputationNotYetFinished {
    pub comp_id: u64,
}
impl warp::reject::Reject for ComputationNotYetFinished {}

pub trait Computation {
    fn is_ready(&self) -> bool;
    fn get_error(&self) -> (i32, String);
    fn cancel(&mut self);
    fn get_total(&self) -> u32;
    fn get_progress(&self) -> u32;
    fn get_graph(&self) -> Arc<RwLock<Graph>>;
    fn algorithm_id(&self) -> u32;
    fn as_any(&self) -> &dyn Any;
}

pub trait ComputationWithResultPerVertex {
    fn get_number_of_components(&self) -> u64;
    fn dump_result(&self, out: &mut Vec<u8>) -> Result<(), String>;
    fn dump_vertex_results(
        &self,
        comp_id: u64,
        hashes: &Vec<KeyOrHash>,
        out: &mut Vec<u8>,
    ) -> Result<(), warp::Rejection>;
}

pub trait ComputationWithListResult {
    fn get_batch(&self, out: &mut Vec<u8>) -> Result<(), String>;
}

fn put_varlen(v: &mut Vec<u8>, l: u32) {
    if l <= 0x7f {
        v.write_u8(l as u8).unwrap();
    } else {
        v.write_u32::<BigEndian>(l | 0x80000000).unwrap();
    };
}

pub fn put_key_or_hash(out: &mut Vec<u8>, koh: &KeyOrHash) {
    match koh {
        KeyOrHash::Hash(h) => {
            put_varlen(out, 0);
            out.write_u64::<BigEndian>(h.to_u64()).unwrap();
        }
        KeyOrHash::Key(k) => {
            put_varlen(out, k.len() as u32);
            out.extend_from_slice(&k);
        }
    }
}

pub struct Computations {
    pub list: HashMap<u64, Arc<RwLock<dyn Computation + Send + Sync>>>,
}

impl Computations {
    pub fn new() -> Self {
        Computations {
            list: HashMap::new(),
        }
    }
    pub fn register(&mut self, comp: Arc<RwLock<dyn Computation + Send + Sync>>) -> u64 {
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

pub struct ComponentsComputation {
    pub algorithm: u32,
    pub graph: Arc<RwLock<Graph>>,
    pub components: Option<Vec<u64>>,
    pub next_in_component: Option<Vec<i64>>,
    pub shall_stop: bool,
    pub number: Option<u64>,
}

impl Computation for ComponentsComputation {
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
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl ComputationWithResultPerVertex for ComponentsComputation {
    fn dump_result(&self, out: &mut Vec<u8>) -> Result<(), String> {
        out.write_u8(8).unwrap();
        out.write_u64::<BigEndian>(match self.number {
            None => 0,
            Some(nr) => nr,
        })
        .unwrap();
        Ok(())
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
    fn get_number_of_components(&self) -> u64 {
        match self.number {
            None => 0,
            Some(nr) => nr,
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
    fn get_total(&self) -> u32 {
        self.total
    }
    fn get_progress(&self) -> u32 {
        self.progress
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Component {
    #[serde(rename = "_key")]
    pub key: String,
    pub representative: String,
    pub size: u64,
    pub aggregation: HashMap<String, u64>,
}

pub struct AggregationComputation {
    pub graph: Arc<RwLock<Graph>>,
    pub compcomp: Arc<RwLock<dyn Computation + Send + Sync>>,
    pub aggregation_attribute: String,
    pub shall_stop: bool,
    pub total: u32,
    pub progress: u32,
    pub error_code: i32,
    pub error_message: String,
    pub result: Vec<Component>,
}

impl Computation for AggregationComputation {
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
        3
    }
    fn get_graph(&self) -> Arc<RwLock<Graph>> {
        return self.graph.clone();
    }
    fn get_total(&self) -> u32 {
        self.total
    }
    fn get_progress(&self) -> u32 {
        self.progress
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct StoreComputation {
    pub comp: Arc<RwLock<dyn Computation + Send + Sync>>,
    pub shall_stop: bool,
    pub total: u32,
    pub progress: u32,
    pub error_code: i32,
    pub error_message: String,
}

impl Computation for StoreComputation {
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
        4
    }
    fn get_graph(&self) -> Arc<RwLock<Graph>> {
        let comp = self.comp.read().unwrap();
        return comp.get_graph();
    }
    fn get_total(&self) -> u32 {
        self.total
    }
    fn get_progress(&self) -> u32 {
        self.progress
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}
