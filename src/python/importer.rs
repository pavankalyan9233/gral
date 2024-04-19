use crate::graph_store::graph::Graph;
use std::sync::{Arc, RwLock};

pub struct Importer {
    pub g_arc: Arc<RwLock<Graph>>,
}

impl Importer {
    pub fn new(g_arc: Arc<RwLock<Graph>>) -> Importer {
        Importer { g_arc }
    }

    pub fn import(&self, file_path: &str) -> Result<(), String> {
        Ok(())
    }
}
