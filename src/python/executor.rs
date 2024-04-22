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
    pub graph_file: tempfile::NamedTempFile,  // file to store the result of the script
    pub exporter: Exporter,                   // exports a graph to a parquet file
    pub importer: Importer,                   // imports a computed dictionary from a parquet file
    pub script: Script,                       // builds the python3 execution script
    pub python3_binary_path: String,
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

        let exporter = Exporter::new(
            graph.clone(),
            graph_file.path().to_str().unwrap().to_string(),
        );
        exporter
            .write_parquet_file()
            .expect("Could not write graph export into parquet file");

        Executor {
            g_arc: graph.clone(),
            exporter,
            importer: Importer::new(
                graph.clone(),
                result_file.path().to_str().unwrap().to_string(),
            ),
            script: Script::new(
                user_script_snippet,
                result_file.path().to_str().unwrap().to_string(),
                graph_file.path().to_str().unwrap().to_string(),
            ),
            result_file,
            graph_file,
            python3_binary_path: "python3".to_string(),
        }
    }

    pub fn run(&mut self) -> Result<(), String> {
        // Export generated script to disk
        self.script.write_to_file();

        // Execute generated script
        let mut child = Command::new(self.python3_binary_path.clone())
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

    pub fn set_python3_binary_path(&mut self, path: String) {
        self.python3_binary_path = path;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_store::graph::Graph;
    use crate::graph_store::vertex_key_index::VertexIndex;

    #[test]
    fn test_full_executor_run() {
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

        // Please keep this line for debugging purposes (venv issue on macOS)
        // executor.set_python3_binary_path("/Users/hkernbach/venv/bin/python".to_string());
        // Please keep this line for debugging purposes
        let result = executor.run();
        assert!(result.is_ok());

        // Now expect that the computation result file is being generated and contains data
        assert!(std::path::Path::new(&executor.result_file.path()).exists());
        let result_content =
            std::fs::read(&executor.result_file.path()).expect("Failed to read file");
        assert!(!result_content.is_empty());

        assert!(result.is_ok());

        let result_path_file = executor.result_file.path().to_str().unwrap().to_string();
        let graph_path_file = executor.graph_file.path().to_str().unwrap().to_string();

        drop(executor);

        // assert that all temporary files got deleted
        assert!(!std::path::Path::new(&result_path_file).exists());
        assert!(!std::path::Path::new(&graph_path_file).exists());
    }
}
