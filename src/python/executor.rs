use crate::graph_store::graph::Graph;
use crate::python::pythoncomputation::PythonComputation;
use crate::python::script;
use arrow::array::{ArrayRef, RecordBatch, UInt64Array};
use log::{error, info};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::process::Command;
use std::string::ToString;
use std::sync::{Arc, RwLock};
use tempfile::{Builder, NamedTempFile};

#[derive(Serialize, Deserialize)]
struct ResultValue {
    vertex_id: u64,
    result: serde_json::Value,
}

pub fn execute_python_script_on_graph(
    c_arc: Arc<RwLock<PythonComputation>>,
    g_arc: Arc<RwLock<Graph>>,
    user_script: String,
    use_cugraph: bool,
) -> Result<(), String> {
    let python3_binary_path = get_python_environment()?;
    execute_python_script_on_graph_internal(
        c_arc,
        g_arc,
        user_script,
        use_cugraph,
        Some(python3_binary_path),
    )
}

#[cfg(target_os = "macos")]
fn get_python_environment() -> Result<String, String> {
    match std::env::var("PYTHON3_BINARY_PATH") {
        Ok(python_path) => {
            println!("Python 3 binary path: {:?}", python_path);
            Ok(python_path)
        }
        Err(_) => Err(
            "Python 3 binary path not provided in PYTHON3_BINARY_PATH environment variable."
                .to_string(),
        ),
    }
}

#[cfg(not(target_os = "macos"))]
fn get_python_environment() -> Result<String, String> {
    println!("Python 3 binary path: {:?}", "python3".to_string());
    Ok("python3".to_string())
}

pub fn execute_python_script_on_graph_with_bin(
    c_arc: Arc<RwLock<PythonComputation>>,
    g_arc: Arc<RwLock<Graph>>,
    user_script: String,
    use_cugraph: bool,
    python3_binary_path: String,
) -> Result<(), String> {
    execute_python_script_on_graph_internal(
        c_arc,
        g_arc,
        user_script,
        use_cugraph,
        Some(python3_binary_path),
    )
}

pub(crate) fn create_temporary_file(
    file_prefix: String,
    file_suffix: String,
) -> Result<NamedTempFile, String> {
    Builder::new()
        .prefix(&file_prefix)
        .suffix(&file_suffix)
        .tempfile()
        .map_err(|e| e.to_string())
}

fn execute_python_script_on_graph_internal(
    c_arc: Arc<RwLock<PythonComputation>>,
    g_arc: Arc<RwLock<Graph>>,
    user_script: String,
    use_cugraph: bool,
    python3_binary_path: Option<String>,
) -> Result<(), String> {
    let python3_bin = python3_binary_path.unwrap_or_else(|| "python3".to_string());

    info!("PYC: Starting Step 1: Now writing the graph to disk");

    // Write graph to disk
    let graph_file = write_graph_to_file(g_arc.clone())?;
    let graph_file_path = graph_file.path().to_str().unwrap().to_string();

    {
        let mut computation = c_arc.write().unwrap();
        computation.progress = 1; // Graph has been written to disk
    }

    info!("PYC: Finished Step 1: Wrote graph to disk");

    // Initialize script instance
    let result_file = create_temporary_file(
        "gral_computation_result_".to_string(),
        ".parquet".to_string(),
    )?;
    let result_file_path = result_file.path().to_str().unwrap().to_string();
    let script_res =
        script::generate_script(user_script, use_cugraph, result_file_path, graph_file_path)
            .map_err(|e| e.to_string());
    let script = script_res.unwrap();
    let script_file = script.write_to_file()?;
    let script_file_path = script_file.path().to_str().unwrap().to_string();

    info!("PYC: Starting Step 2: Will now the execute python script.");

    // Execute generated script
    let process = Command::new(python3_bin)
        .arg(&script_file_path)
        .output()
        .map_err(|err| format!("Failed to execute Python script: {}", err))?;

    if !process.status.success() {
        let stderr = String::from_utf8_lossy(&process.stderr);
        error!("Python script error:\n{}", stderr);
        return Err("Failed to execute Python script".to_string());
    }

    {
        let mut computation = c_arc.write().unwrap();
        computation.progress = 2; // Python script has been executed
    }

    info!("PYC: Finished Step 2: Python script executed (This includes store into parquet).");
    info!("PYC: Starting Step 3: Will now read the result and store it in-memory.");
    // Read computation result from disk
    store_computation_result(c_arc, result_file)
}

pub fn write_graph_to_file(g_arc: Arc<RwLock<Graph>>) -> Result<NamedTempFile, String> {
    let file = create_temporary_file("gral_graph_".to_string(), ".parquet".to_string())?;
    let graph = g_arc.read().unwrap();

    let (from_values, to_values): (Vec<u64>, Vec<u64>) = graph
        .edges
        .iter()
        .map(|edge| (edge.from().to_u64(), edge.to().to_u64()))
        .unzip();

    let arrow_from = UInt64Array::from(from_values);
    let arrow_to = UInt64Array::from(to_values);

    let batch = RecordBatch::try_from_iter(vec![
        ("_from", Arc::new(arrow_from) as ArrayRef),
        ("_to", Arc::new(arrow_to) as ArrayRef),
    ])
    .unwrap();

    let io_file = File::create(file.path()).unwrap();
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(io_file, batch.schema(), Some(props)).unwrap();
    writer.write(&batch).expect("Writing batch");
    writer.close().unwrap();

    Ok(file)
}

pub fn store_computation_result(
    c_arc: Arc<RwLock<PythonComputation>>,
    result_file: NamedTempFile,
) -> Result<(), String> {
    let path = result_file.path().to_str().unwrap().to_string();
    if let Ok(file) = File::open(path) {
        match SerializedFileReader::new(file) {
            Ok(reader) => {
                // Currently we only support reading two columns
                // It is expected that the first column is the node id and the second column is the result
                let row_group_reader = reader.get_row_group(0).unwrap();

                if row_group_reader.num_columns() != 2 {
                    return Err("Unexpected parquet format (columns)!".to_string());
                }

                // get write lock on comp arc
                let mut comp_arc = c_arc.write().unwrap();

                reader.get_row_iter(None).unwrap().try_for_each(|row| {
                    let mut vertex_id = 0;
                    let mut result = serde_json::Value::Null;

                    let row_res = row.map_err(|e| e.to_string())?;
                    let row_as_json = row_res.to_json_value();

                    if let Some(v) = row_as_json.get("Node") {
                        vertex_id = v.as_u64().unwrap();
                    }
                    if let Some(v) = row_as_json.get("Result") {
                        result = v.clone();
                    }

                    comp_arc.result.insert(vertex_id, result);
                    Ok::<(), String>(())
                })?;
            }
            Err(err) => {
                // Handle the error when creating the reader.
                eprintln!("Error creating SerializedFileReader: {}", err);
            }
        }
    } else {
        return Err("Failed to open result file".to_string());
    }

    {
        let mut computation = c_arc.write().unwrap();
        computation.progress = 3; // Computation result has been read
    }

    info!("PYC: Finished Step 3: Result stored in-memory.");

    Ok(())
}
