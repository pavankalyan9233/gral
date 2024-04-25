use crate::graph_store::graph::Graph;
use crate::python::pythoncomputation::PythonComputation;
use crate::python::script;
use arrow::array::{ArrayRef, RecordBatch, UInt64Array};
use log::info;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::RowAccessor;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::process::{Command, Stdio};
use std::string::ToString;
use std::sync::{Arc, RwLock};
use tempfile::{Builder, NamedTempFile};

#[derive(Serialize, Deserialize)]
struct ResultValue {
    vertex_id: u64,
    result: serde_json::Value,
}

pub fn execute_python_script_on_graph(
    g_arc: Arc<RwLock<Graph>>,
    user_script: String,
) -> Result<(), String> {
    execute_python_script_on_graph_internal(g_arc, user_script, None)
}

pub fn execute_python_script_on_graph_with_bin(
    g_arc: Arc<RwLock<Graph>>,
    user_script: String,
    python3_binary_path: String,
) -> Result<(), String> {
    execute_python_script_on_graph_internal(g_arc, user_script, Some(python3_binary_path))
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
    g_arc: Arc<RwLock<Graph>>,
    user_script: String,
    python3_binary_path: Option<String>,
) -> Result<(), String> {
    let python3_bin = python3_binary_path.unwrap_or_else(|| "python3".to_string());

    // Write graph to disk
    let graph_file = write_graph_to_file(g_arc.clone())?;
    let graph_file_path = graph_file.path().to_str().unwrap().to_string();

    // Initialize script instance
    let result_file = create_temporary_file(
        "gral_computation_result_".to_string(),
        ".parquet".to_string(),
    )?;
    let result_file_path = result_file.path().to_str().unwrap().to_string();
    let script_res = script::generate_script(user_script, result_file_path, graph_file_path)
        .map_err(|e| e.to_string());
    let script = script_res.unwrap();
    let script_file = script.write_to_file()?;
    let script_file_path = script_file.path().to_str().unwrap().to_string();

    // Execute generated script
    let mut process = Command::new(python3_bin)
        .arg(&script_file_path)
        .stdout(Stdio::piped())
        .spawn()
        .expect("Failed to execute command");

    let process_result = process.wait();
    if process_result.is_err() {
        return Err("Failed to execute Python script".to_string());
    }

    // Read computation result from disk
    store_computation_result(g_arc, result_file)
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
    info!("Finished custom script computation!");

    let mut writer = ArrowWriter::try_new(io_file, batch.schema(), Some(props)).unwrap();
    writer.write(&batch).expect("Writing batch");
    writer.close().unwrap();

    Ok(file)
}

pub fn store_computation_result(
    g_arc: Arc<RwLock<Graph>>,
    result_file: NamedTempFile,
) -> Result<(), String> {
    let comp_arc = Arc::new(RwLock::new(PythonComputation {
        graph: g_arc,
        algorithm: "Custom".to_string(),
        total: 0,
        progress: 0,
        error_code: 0,
        error_message: "".to_string(),
        result: Default::default(),
    }));

    let path = result_file.path().to_str().unwrap().to_string();
    if let Ok(file) = File::open(&path) {
        let reader = SerializedFileReader::new(file).unwrap();

        let parquet_metadata = reader.metadata();
        assert_eq!(parquet_metadata.num_row_groups(), 1);

        // Currently we only support reading two columns
        // It is expected that the first column is the node id and the second column is the result
        let row_group_reader = reader.get_row_group(0).unwrap();
        assert_eq!(row_group_reader.num_columns(), 2);

        // get write lock on comp arc
        let mut comp_arc = comp_arc.write().unwrap();

        reader.get_row_iter(None).unwrap().try_for_each(|row| {
            let mut vertex_id = 0;
            let mut result = serde_json::Value::Null;

            let row_res = row.map_err(|e| e.to_string())?;
            let row_as_json = row_res.to_json_value();

            row_as_json.get("Node").map(|v| {
                vertex_id = v.as_u64().unwrap()?;
            });
            row_as_json.get("Result").map(|v| {
                result = v.clone();
            });

            comp_arc.result.insert(vertex_id, result);
            Ok::<(), String>(())
        })?;
    } else {
        return Err("Failed to open result file".to_string());
    }

    Ok(())
}
