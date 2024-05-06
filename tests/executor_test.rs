use gral::python::executor;
use std::sync::{Arc, RwLock};

#[cfg(test)]
mod tests {
    use super::*;
    use gral::graph_store::graph::Graph;
    use gral::python::pythoncomputation::PythonComputation;
    use gral::python::script::generate_script;

    #[test]
    fn test_full_executor_run() {
        let g = Graph::create(
            vec![
                "V/A".to_string(),
                "V/B".to_string(),
                "V/C".to_string(),
                "V/D".to_string(),
                "V/E".to_string(),
                "V/F".to_string(),
            ],
            vec![
                ("V/D".to_string(), "V/B".to_string()),
                ("V/A".to_string(), "V/D".to_string()),
                ("V/A".to_string(), "V/C".to_string()),
                ("V/B".to_string(), "V/F".to_string()),
            ],
        );

        let g_arc = Arc::new(RwLock::new(g));
        let user_snippet = "def worker(graph): return nx.pagerank(graph, 0.85)".to_string();

        let comp_arc = Arc::new(RwLock::new(PythonComputation {
            graph: g_arc.clone(),
            algorithm: "Custom".to_string(),
            total: 3, // 1. Write graph to disk, 2. Execute & write computation to disk, 3. Read back
            progress: 0,
            error_code: 0,
            error_message: "".to_string(),
            result: Default::default(),
        }));

        let res = executor::execute_python_script_on_graph(comp_arc, g_arc, user_snippet, false);
        assert!(res.is_ok());
    }

    #[test]
    fn test_script_generation_and_write_to_disk() {
        let user_script_snippet = "def worker(): print('Hello, World!')".to_string();
        let result_path_file = "result.parquet".to_string();
        let graph_path_file = "graph.parquet".to_string();
        let script = generate_script(
            user_script_snippet,
            false,
            result_path_file,
            graph_path_file,
        )
        .unwrap();

        let script_file = script.write_to_file();
        let file_path = script_file
            .as_ref()
            .unwrap()
            .path()
            .to_str()
            .unwrap()
            .to_string();

        // expect that file exists
        assert!(std::path::Path::new(&file_path).exists());

        // expect that the file has content
        let content = std::fs::read_to_string(&file_path).expect("Failed to read file");
        assert!(!content.is_empty());

        // drop script file
        drop(script_file);

        // expect that the temp file automatically is removed during destruction
        assert!(!std::path::Path::new(&file_path).exists());
    }
}
