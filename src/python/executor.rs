use crate::graph_store::graph::Graph;
use crate::python;
use crate::python::exporter::Exporter;
use crate::python::importer::Importer;
use python::Script;
use std::sync::{Arc, RwLock};

pub struct Executor {
    pub g_arc: Arc<RwLock<Graph>>,
    pub script: Script,     // builds the python3 execution script
    pub exporter: Exporter, // exports a graph to a parquet file
    pub importer: Importer, // imports a computed dictionary from a parquet file
}

impl Executor {
    pub fn new(graph: Arc<RwLock<Graph>>, user_script_snippet: String) -> Executor {
        Executor {
            script: Script::new(graph.read().unwrap().graph_id, user_script_snippet),
            g_arc: graph.clone(),
            exporter: Exporter::new(graph.clone()),
            importer: Importer::new(graph.clone()),
        }
    }

    pub fn run(&self) -> String {
        /*let mut output = String::new();
        let mut child = Command::new("python3")
            .arg("-c")
            .arg(&self.user_script)
            .stdout(Stdio::piped())
            .spawn()
            .expect("Failed to execute command");
        child
            .stdout
            .as_mut()
            .unwrap()
            .read_to_string(&mut output)
            .unwrap();
        output*/
        return "output".to_string();
    }
}
