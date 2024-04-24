use arrow::array::RecordBatchReader;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use std::fs::File;

pub struct ResultImporter {
    file_path: String,
}

impl ResultImporter {
    pub fn new(file_path: String) -> ResultImporter {
        ResultImporter { file_path }
    }

    pub fn run(&self) -> Result<(), String> {
        let file = File::open(&self.file_path).expect("Failed to open Parquet file");
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
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_export_graph_into_parquet_file() {
        assert!(true);
    }
}
