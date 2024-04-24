use gral::python::executor::Executor;
use std::sync::{Arc, RwLock};

#[cfg(test)]
mod tests {
    use super::*;
    use gral::computations::Computations;
    use gral::graph_store::graph::Graph;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs::File;
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

        let result_path_file = executor.result_file.path().to_str().unwrap().to_string();
        let graph_path_file = executor.graph_file.path().to_str().unwrap().to_string();

        drop(executor);

        // assert that all temporary files got deleted
        assert!(!std::path::Path::new(&result_path_file).exists());
        assert!(!std::path::Path::new(&graph_path_file).exists());
    }

    #[test]
    fn test_export_graph_into_parquet_file() {
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
        let computations = Arc::new(Mutex::new(Computations::new()));
        let user_snippet = "def worker(graph): return nx.pagerank(graph, 0.85)".to_string();
        let executor = Executor::new(g_arc, computations, user_snippet);
        executor.write_graph_to_file().unwrap();
        let file_path = executor.graph_file.path().to_str().unwrap().to_string();

        let file = File::open(file_path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();

        assert_eq!(builder.schema().field(0).name(), "_from");
        assert_eq!(builder.schema().field(1).name(), "_to");

        let mut reader = builder.build().unwrap();
        let record_batch = reader.next().unwrap().unwrap();
        assert_eq!(record_batch.num_rows(), 4);
    }
}
