use metrics::{decrement_gauge, increment_gauge};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::{Arc, Mutex, RwLock};
use warp::Filter;

use crate::graph_store::graph::Graph;

pub trait Computation {
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

pub struct Computations {
    pub list: HashMap<u64, Arc<RwLock<dyn Computation + Send + Sync>>>,
    next_id: u64,
}

impl Computations {
    pub fn new() -> Self {
        Computations {
            list: HashMap::new(),
            next_id: 1,
        }
    }
    pub fn register(&mut self, comp: Arc<RwLock<dyn Computation + Send + Sync>>) -> u64 {
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

impl Default for Computations {
    fn default() -> Self {
        Computations::new()
    }
}

pub fn with_computations(
    computations: Arc<Mutex<Computations>>,
) -> impl Filter<Extract = (Arc<Mutex<Computations>>,), Error = Infallible> + Clone {
    warp::any().map(move || computations.clone())
}

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

impl Computation for ComponentsComputation {
    fn is_ready(&self) -> bool {
        self.components.is_some()
    }
    fn get_error(&self) -> (i32, String) {
        (0, "".to_string())
    }
    fn cancel(&mut self) {
        self.shall_stop = true;
    }
    fn algorithm_name(&self) -> String {
        self.algorithm.clone()
    }
    fn get_graph(&self) -> Arc<RwLock<Graph>> {
        self.graph.clone()
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
    fn algorithm_name(&self) -> String {
        "".to_string()
    }
    fn get_graph(&self) -> Arc<RwLock<Graph>> {
        self.graph.clone()
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
    fn algorithm_name(&self) -> String {
        "ComponentsAggregation".to_string()
    }
    fn get_graph(&self) -> Arc<RwLock<Graph>> {
        self.graph.clone()
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

pub struct StoreComputation {
    pub comp: Vec<Arc<RwLock<dyn Computation + Send + Sync>>>,
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
    fn algorithm_name(&self) -> String {
        "Store Operation".to_string()
    }
    fn get_graph(&self) -> Arc<RwLock<Graph>> {
        let comp = self.comp[0].read().unwrap();
        comp.get_graph()
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

pub struct PageRankComputation {
    pub graph: Arc<RwLock<Graph>>,
    pub algorithm: String,
    pub shall_stop: bool,
    pub total: u32,
    pub progress: u32,
    pub error_code: i32,
    pub error_message: String,
    pub steps: u32,
    pub rank: Vec<f64>,
    pub result_position: usize,
}

impl Computation for PageRankComputation {
    fn is_ready(&self) -> bool {
        self.progress == self.total
    }
    fn get_error(&self) -> (i32, String) {
        (self.error_code, self.error_message.clone())
    }
    fn cancel(&mut self) {
        self.shall_stop = true;
    }
    fn algorithm_name(&self) -> String {
        self.algorithm.clone()
    }
    fn get_graph(&self) -> Arc<RwLock<Graph>> {
        self.graph.clone()
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
    fn nr_results(&self) -> u64 {
        self.rank.len() as u64
    }
    fn get_result(&self, which: u64) -> (String, String) {
        let key;
        {
            let guard = self.graph.read().unwrap();
            key = std::str::from_utf8(&guard.index_to_key[which as usize])
                .unwrap()
                .to_string();
        }
        (key, format!("{:.8}", self.rank[which as usize]))
    }
    fn memory_usage(&self) -> usize {
        self.rank.len() * std::mem::size_of::<f64>()
    }
}

pub struct LabelPropagationComputation {
    pub graph: Arc<RwLock<Graph>>,
    pub sync: bool,
    pub shall_stop: bool,
    pub total: u32,
    pub progress: u32,
    pub error_code: i32,
    pub error_message: String,
    pub label: Vec<String>,
    pub result_position: usize,
    pub label_size_sum: usize,
}

impl Computation for LabelPropagationComputation {
    fn is_ready(&self) -> bool {
        self.progress == self.total
    }
    fn get_error(&self) -> (i32, String) {
        (self.error_code, self.error_message.clone())
    }
    fn cancel(&mut self) {
        self.shall_stop = true;
    }
    fn algorithm_name(&self) -> String {
        "Label Propagation".to_string()
    }
    fn get_graph(&self) -> Arc<RwLock<Graph>> {
        self.graph.clone()
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
    fn nr_results(&self) -> u64 {
        self.label.len() as u64
    }
    fn get_result(&self, which: u64) -> (String, String) {
        let key;
        {
            let guard = self.graph.read().unwrap();
            key = std::str::from_utf8(&guard.index_to_key[which as usize])
                .unwrap()
                .to_string();
        }
        (key, self.label[which as usize].clone())
    }
    fn memory_usage(&self) -> usize {
        self.label_size_sum + self.label.len() * std::mem::size_of::<String>()
    }
}
