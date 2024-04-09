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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn starts_with_graph_id_1() {
        let mut graphs = Graphs::new();
        let new_graph = Graph::new(false, 343, vec![]);

        assert_eq!(graphs.register(new_graph.clone()), 1);
    }

    #[test]
    fn registers_graph_in_graphs_list() {
        let mut graphs = Graphs::new();
        let new_graph = Graph::new(false, 343, vec![]);

        let graph_id = graphs.register(new_graph.clone());

        assert!(graphs.list.contains_key(&graph_id));
    }

    #[test]
    fn updates_graph_id_during_its_registration() {
        let mut graphs = Graphs::new();
        let new_graph = Graph::new(false, 343, vec![]);

        let graph_id = graphs.register(new_graph.clone());

        assert_eq!(new_graph.read().unwrap().graph_id, graph_id);
    }

    #[test]
    fn removes_graph_from_list() {
        let mut graphs = Graphs {
            list: HashMap::from([
                (1, Graph::new(false, 1, vec![])),
                (2, Graph::new(false, 2, vec![])),
            ]),
            next_id: 3,
        };

        graphs.remove(1);

        assert!(!graphs.list.contains_key(&1));
        assert!(graphs.list.contains_key(&2));
    }
}
