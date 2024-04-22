use crate::graph_store::graph::Graph;
use std::sync::{Arc, RwLock};

pub struct Importer {
    pub g_arc: Arc<RwLock<Graph>>,
    pub file_path: String,
}

impl Importer {
    pub fn new(g_arc: Arc<RwLock<Graph>>, file_path: String) -> Importer {
        Importer { g_arc, file_path }
    }

    pub fn import() -> Result<(), String> {
        Ok(())
    }
}
