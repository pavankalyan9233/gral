use gral::python::executor;
use std::sync::{Arc, RwLock};

#[cfg(test)]
mod tests {
    use super::*;
    use gral::computations::Computations;
    use gral::graph_store::graph::Graph;
    use gral::python::script::generate_script;
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

        let python_path_res = return_python_environment();
        if python_path_res.is_err() {
            println!("Failed to get python3 binary path: {:?}", python_path_res);
        }
        // let the_computations = Arc::new(Mutex::new(Computations::new()));
        assert!(python_path_res.is_ok());
        let res = (executor::execute_python_script_on_graph_with_bin(
            g_arc,
            user_snippet,
            python_path_res.unwrap(),
        ));

        assert!(res.is_ok());
    }

    #[test]
    fn test_script_generation_and_write_to_disk() {
        let user_script_snippet = "def worker(): print('Hello, World!')".to_string();
        let result_path_file = "result.parquet".to_string();
        let graph_path_file = "graph.parquet".to_string();
        let script =
            generate_script(user_script_snippet, result_path_file, graph_path_file).unwrap();

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
