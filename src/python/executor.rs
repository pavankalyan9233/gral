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
    pub result_file: tempfile::NamedTempFile, // file to store the computation result (as parquet)
    pub graph_file: tempfile::NamedTempFile,  // file to store the graph itself (as parquet)
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
