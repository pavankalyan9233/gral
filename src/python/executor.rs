use crate::computations::Computations;
use crate::graph_store::graph::Graph;
use crate::python;
use crate::python::graph_exporter::GraphExporter;
use crate::python::result_importer::ResultImporter;
use python::Script;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex, RwLock};
use tempfile::Builder;

pub struct Executor {
    pub g_arc: Arc<RwLock<Graph>>,
    pub result_file: tempfile::NamedTempFile, // file to store the result of the script
    pub graph_file: tempfile::NamedTempFile,  // file to store the result of the script
    pub graph_exporter: GraphExporter,        // exports a graph to a parquet file
    pub result_importer: ResultImporter,      // imports a computed dictionary from a parquet file
    pub script: Script,                       // builds the python3 execution script
    pub python3_binary_path: String,
}

impl Executor {
    pub fn new(
        graph: Arc<RwLock<Graph>>,
        _computations: Arc<Mutex<Computations>>, // will be used in follow up pr
        user_script_snippet: String,
    ) -> Executor {
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

        let graph_exporter = GraphExporter::new(
            graph.clone(),
            graph_file.path().to_str().unwrap().to_string(),
        );
        graph_exporter
            .write_parquet_file()
            .expect("Could not write graph export into parquet file");

        Executor {
            g_arc: graph.clone(),
            graph_exporter,
            result_importer: ResultImporter::new(result_file.path().to_str().unwrap().to_string()),
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
    use crate::computations::Computations;
    use crate::graph_store::graph::Graph;
    use std::sync::Mutex;

    #[cfg(target_os = "macos")]
    fn return_python_environment() -> Result<String, String> {
        return if let Ok(python_path) = std::env::var("PYTHON3_BINARY_PATH") {
            println!("Python 3 binary path: {:?}", python_path);
            Ok(python_path)
        } else {
            Err(
                "Python 3 binary path not provided in PYTHON3_BINARY_PATH environment variable."
                    .to_string(),
            )
        };
    }

    #[cfg(not(target_os = "macos"))]
    fn return_python_environment() -> Result<String, ()> {
        println!("Python 3 binary path: {:?}", "python3".to_string());
        return Ok("python3".to_string());
    }

    #[test]
    fn test_full_executor_run() {
        let mut g = Graph::create(vec![], vec![]);
        {
            g.insert_vertex(b"V/A".to_vec(), vec![]);
            g.insert_vertex(b"V/B".to_vec(), vec![]);
            g.insert_vertex(b"V/C".to_vec(), vec![]);
            g.insert_vertex(b"V/D".to_vec(), vec![]);
            g.insert_vertex(b"V/E".to_vec(), vec![]);
            g.insert_vertex(b"V/F".to_vec(), vec![]);
            g.seal_vertices();

            // add edges
            let _ = g.insert_edge_between_vertices(b"V/D", b"V/B");
            let _ = g.insert_edge_between_vertices(b"V/A", b"V/D");
            let _ = g.insert_edge_between_vertices(b"V/A", b"V/C");
            let _ = g.insert_edge_between_vertices(b"V/B", b"V/F");
            g.seal_edges();
        }

        let g_arc = Arc::new(RwLock::new(g));
        let computations = Arc::new(Mutex::new(Computations::new()));

        let user_snippet = "def worker(graph): return nx.pagerank(graph, 0.85)".to_string();
        let mut executor = Executor::new(g_arc, computations, user_snippet);
        let python_path_res = return_python_environment();
        if python_path_res.is_err() {
            println!("Failed to get python3 binary path: {:?}", python_path_res);
        }
        assert!(python_path_res.is_ok());
        executor.set_python3_binary_path(python_path_res.unwrap());
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
