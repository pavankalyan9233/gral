use arrow::array::{ArrayRef, UInt64Array};
use std::sync::{Arc, RwLock};
use tempfile::Builder;

use crate::graph_store::graph::Graph;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

pub struct Exporter {
    pub g_arc: Arc<RwLock<Graph>>,
    pub temp_file: tempfile::NamedTempFile,
}

impl Exporter {
    pub fn new(g_arc: Arc<RwLock<Graph>>) -> Exporter {
        Exporter {
            g_arc,
            temp_file: Builder::new()
                .prefix("gral")
                .suffix(".parquet")
                .tempfile()
                .expect("Failed to create temporary file"),
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

        let file = &self.temp_file;
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
        writer.write(&batch).expect("Writing batch");
        writer.close().unwrap();

        Ok(file.path().to_str().unwrap().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_store::graph::Graph;
    use crate::graph_store::vertex_key_index::VertexIndex;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs::File;

    #[test]
    fn test_write_into_parquet_file() {
        let g_arc = Graph::new(false, vec![]);
        // add 6 random vertices
        {
            let mut g = g_arc.write().unwrap();
            g.insert_empty_vertex(b"V/A");
            g.insert_empty_vertex(b"V/B");
            g.insert_empty_vertex(b"V/C");
            g.insert_empty_vertex(b"V/D");
            g.insert_empty_vertex(b"V/E");
            g.insert_empty_vertex(b"V/F");
            // add edges
            g.insert_edge(VertexIndex::new(4), VertexIndex::new(1));
            g.insert_edge(VertexIndex::new(0), VertexIndex::new(3));
            g.insert_edge(VertexIndex::new(0), VertexIndex::new(2));
            g.insert_edge(VertexIndex::new(1), VertexIndex::new(6));
            g.seal_vertices();
            g.seal_edges();
        }

        let exporter = Exporter::new(g_arc.clone());
        match exporter.write_parquet_file() {
            Ok(file_path) => {
                let file = File::open(file_path).unwrap();
                let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();

                assert_eq!(builder.schema().field(0).name(), "_from");
                assert_eq!(builder.schema().field(1).name(), "_to");

                let mut reader = builder.build().unwrap();
                let record_batch = reader.next().unwrap().unwrap();
                assert_eq!(record_batch.num_rows(), 4);
            }
            Err(e) => {
                assert!(false, "{}", e);
            }
        }
    }
}
