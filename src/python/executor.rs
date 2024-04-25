use crate::graph_store::graph::Graph;
use crate::python::script;
use arrow::array::{ArrayRef, RecordBatch, RecordBatchReader, UInt64Array};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::process::{Command, Stdio};
use std::string::ToString;
use std::sync::{Arc, RwLock};
use tempfile::{Builder, NamedTempFile};

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
    let graph_file = write_graph_to_file(g_arc)?;
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
    let mut child = Command::new(python3_bin)
        .arg(&script_file_path)
        .stdout(Stdio::piped())
        .spawn()
        .expect("Failed to execute command");

    if child.wait().unwrap().success() {
        Ok(())
    } else {
        Err("Failed to execute the script".to_string())
    }
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

pub fn read_computation_result_from_file(result_file: &NamedTempFile) -> Result<(), String> {
    let file = File::open(result_file).expect("Failed to open Parquet file");
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();

    let record_reader: ParquetRecordBatchReader = builder.with_row_groups(vec![0]).build().unwrap();
    println!("Schema: {:?}", record_reader.schema());
    assert_eq!(record_reader.schema().fields().len(), 2);
    assert_eq!(record_reader.schema().field(0).name(), "Node");
    assert_eq!(record_reader.schema().field(1).name(), "Result");

    for batch in record_reader {
        println!("RecordBatch: {:?}", batch);
    }

    Ok(())
}
