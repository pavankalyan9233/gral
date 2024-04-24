use arrow::array::{ArrayRef, UInt64Array};
use std::fs::File;
use std::sync::{Arc, RwLock};

use crate::graph_store::graph::Graph;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

pub struct GraphExporter {
    g_arc: Arc<RwLock<Graph>>,
    graph_file_path: String,
}

impl GraphExporter {
    pub fn new(g_arc: Arc<RwLock<Graph>>, graph_file_path: String) -> GraphExporter {
        GraphExporter {
            g_arc,
            graph_file_path,
        }
    }

    pub fn write_parquet_file(&self) -> Result<String, String> {
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

        let file = File::create(&self.graph_file_path).unwrap();
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
        writer.write(&batch).expect("Writing batch");
        writer.close().unwrap();

        Ok(self.graph_file_path.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_store::graph::Graph;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs::File;
    use std::sync::RwLock;

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
        let exporter = GraphExporter::new(g_arc, "/tmp/dont_care.parquet".to_string());
        let file_path = exporter.write_parquet_file().unwrap();

        let file = File::open(file_path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();

        assert_eq!(builder.schema().field(0).name(), "_from");
        assert_eq!(builder.schema().field(1).name(), "_to");

        let mut reader = builder.build().unwrap();
        let record_batch = reader.next().unwrap().unwrap();
        assert_eq!(record_batch.num_rows(), 4);
    }
}
