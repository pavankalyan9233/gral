use crate::graph_store::graph::Graph;
use metrics::{decrement_gauge, increment_gauge};
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::{Arc, Mutex, RwLock};
use warp::Filter;

pub struct Graphs {
    pub list: HashMap<u64, Arc<RwLock<Graph>>>,
    next_id: u64,
}

impl Graphs {
    pub fn new() -> Graphs {
        Graphs {
            list: HashMap::new(),
            next_id: 1,
        }
    }
    pub fn register(&mut self, graph: Arc<RwLock<Graph>>) -> u64 {
        let graph_id = self.next_id;
        self.next_id += 1;
        {
            let mut guard = graph.write().unwrap();
            guard.graph_id = graph_id;
        }
        self.list.insert(graph_id, graph);
        increment_gauge!("number_of_graphs", 1.0);
        graph_id
    }
    pub fn remove(&mut self, id: u64) {
        let found = self.list.remove(&id);
        if found.is_some() {
            decrement_gauge!("number_of_graphs", 1.0);
        }
    }
}

impl Default for Graphs {
    fn default() -> Self {
        Graphs::new()
    }
}

pub fn with_graphs(
    graphs: Arc<Mutex<Graphs>>,
) -> impl Filter<Extract = (Arc<Mutex<Graphs>>,), Error = Infallible> + Clone {
    warp::any().map(move || graphs.clone())
}
