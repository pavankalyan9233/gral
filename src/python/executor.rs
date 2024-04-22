use crate::graph_store::graph::Graph;
use crate::python;
use crate::python::exporter::Exporter;
use crate::python::importer::Importer;
use python::Script;
use std::process::{Command, Stdio};
use std::sync::{Arc, RwLock};
use tempfile::Builder;

pub struct Executor {
    pub g_arc: Arc<RwLock<Graph>>,
    pub result_file: tempfile::NamedTempFile, // file to store the result of the script
    pub exporter: Exporter,                   // exports a graph to a parquet file
    pub importer: Importer,                   // imports a computed dictionary from a parquet file
    pub script: Script,                       // builds the python3 execution script
}

impl Executor {
    pub fn new(graph: Arc<RwLock<Graph>>, user_script_snippet: String) -> Executor {
        let result_file = Builder::new()
            .prefix("gral_computation_result_")
            .suffix(".parquet")
            .tempfile()
            .expect("Failed to create temporary file for computation result");

        let graph_file = Builder::new()
            .prefix("gral_graph_")
            .suffix(".parquet")
            .tempfile()
            .expect("Failed to create temporary file for graph export");

        let exporter = Exporter::new(graph.clone());
        exporter
            .write_parquet_file()
            .expect("Could not write graph export into parquet file");

        Executor {
            g_arc: graph.clone(),
            exporter,
            importer: Importer::new(graph.clone()),
            script: Script::new(
                user_script_snippet,
                result_file.path().to_str().unwrap().to_string(),
                graph_file.path().to_str().unwrap().to_string(),
            ),
            result_file,
        }
    }

    pub fn run(&self) -> Result<(), String> {
        let mut child = Command::new("python3")
            .arg(&self.script.get_file_path())
            .stdout(Stdio::piped())
            .spawn()
            .expect("Failed to execute command");

        if child.wait().unwrap().success() {
            Ok(())
        } else {
            Err("Failed to execute the script".to_string())
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_store::graph::Graph;
    use crate::graph_store::vertex_key_index::VertexIndex;
    use std::thread::sleep;

    #[test]
    fn test_write_into_parquet_file() {
        let g_arc = Graph::new(false, vec![]);
        {
            let mut g = g_arc.write().unwrap();
            g.insert_empty_vertex(b"V/A");
            g.insert_empty_vertex(b"V/B");
            g.insert_empty_vertex(b"V/C");
            g.insert_empty_vertex(b"V/D");
            g.insert_empty_vertex(b"V/E");
            g.insert_empty_vertex(b"V/F");
            // add edges
            g.insert_edge(VertexIndex::new(4), VertexIndex::new(1));
            g.insert_edge(VertexIndex::new(0), VertexIndex::new(3));
            g.insert_edge(VertexIndex::new(0), VertexIndex::new(2));
            g.insert_edge(VertexIndex::new(1), VertexIndex::new(6));
            g.seal_vertices();
            g.seal_edges();
        }

        let user_snippet = "def worker(graph): return {0: '0', 1: '1'}".to_string();
        let mut executor = Executor::new(g_arc.clone(), user_snippet);
        executor
            .exporter
            .write_parquet_file()
            .expect("Could not export Graph");
        executor.script.pretty_print();
        executor.script.write_to_file();
        let result = executor.run();

        assert!(result.is_ok());
    }
}
