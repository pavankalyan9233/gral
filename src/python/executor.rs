use crate::computations::Computations;
use crate::graph_store::graph::Graph;
use crate::python;
use arrow::array::{ArrayRef, RecordBatch, RecordBatchReader, UInt64Array};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use python::Script;
use std::fs::File;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex, RwLock};
use tempfile::Builder;

pub struct Executor {
    pub g_arc: Arc<RwLock<Graph>>,
    pub result_file: tempfile::NamedTempFile, // file to store the computation result (as parquet)
    pub graph_file: tempfile::NamedTempFile,  // file to store the graph itself (as parquet)
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

        Executor {
            g_arc: graph.clone(),
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
        let res = self.script.write_to_file();
        if res.is_err() {
            return Err("Failed to write script to file".to_string());
        }

        // Write graph to disk
        self.write_graph_to_file()?;

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

    pub fn write_graph_to_file(&self) -> Result<(), String> {
        let graph = self.g_arc.read().unwrap();

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

        let file = File::create(&self.graph_file).unwrap();
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
        writer.write(&batch).expect("Writing batch");
        writer.close().unwrap();

        Ok(())
    }

    pub fn read_computation_result_from_file(&self) -> Result<(), String> {
        let file = File::open(&self.result_file).expect("Failed to open Parquet file");
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();

        let record_reader: ParquetRecordBatchReader =
            builder.with_row_groups(vec![0]).build().unwrap();
        println!("Schema: {:?}", record_reader.schema());
        assert_eq!(record_reader.schema().fields().len(), 2);
        assert_eq!(record_reader.schema().field(0).name(), "Node");
        assert_eq!(record_reader.schema().field(1).name(), "Result");

        for batch in record_reader {
            println!("RecordBatch: {:?}", batch);
        }

        Ok(())
    }

    pub fn set_python3_binary_path(&mut self, path: String) {
        self.python3_binary_path = path;
    }
}
