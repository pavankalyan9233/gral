use std::io::Write;
use tempfile::Builder;

pub struct Script {
    pub user_script: String,
    pub lines: Vec<String>,
    pub temp_file: tempfile::NamedTempFile,
    pub result_file_path: String,
    pub graph_file_path: String,
}

impl Script {
    pub fn new(user_script: String, result_file_path: String, graph_file_path: String) -> Script {
        let mut script = Script {
            user_script,
            lines: Vec::new(),
            temp_file: Builder::new()
                .prefix("gral_script_")
                .suffix(".py")
                .tempfile()
                .expect("Failed to create temporary file"),
            result_file_path,
            graph_file_path,
        };

        script.generate_python_script();
        script
    }

    pub(crate) fn write_to_file(&mut self) -> String {
        // Create a temporary directory

        for line in &self.lines {
            let line_with_newline = format!("{}\n", line);
            self.temp_file
                .write_all(line_with_newline.as_bytes())
                .expect("Failed to write to file");
        }

        return self.temp_file.path().to_str().unwrap().to_string();
    }

    pub(crate) fn get_file_path(&self) -> String {
        self.temp_file.path().to_str().unwrap().to_string()
    }

    pub(crate) fn pretty_print(&self) {
        println!("Script for graph: {}", self.graph_file_path);
        for line in &self.lines {
            println!("{}", line);
        }
    }

    fn add_line(&mut self, line: &str) {
        self.lines.push(line.to_string());
    }

    fn add_graph_file_path(&mut self) {
        self.add_line(&format!("graph_file_path = \"{}\"", self.graph_file_path));
    }

    fn generate_python_script(&mut self) {
        let script_bytes = include_bytes!(concat!(env!("OUT_DIR"), "/base_functions.py"));
        let script_str = std::str::from_utf8(script_bytes).unwrap();
        for line in script_str.lines() {
            if line.contains("<Placeholder for graph_file_path>") {
                self.add_graph_file_path();
                continue;
            } else if line.contains("<Placeholder for user injected script>") {
                self.add_line(&self.user_script.clone());
                continue;
            } else if line.contains("<Placeholder for result file path>") {
                self.add_line(&format!("result_file_path = \"{}\"", self.result_file_path));
                continue;
            }
            self.add_line(line);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hello_world_script() {
        let user_script_snippet = "def worker(): print('Hello, World!')".to_string();
        let result_path_file = "result.parquet".to_string();
        let graph_path_file = "graph.parquet".to_string();
        let mut script = Script::new(
            user_script_snippet,
            result_path_file,
            graph_path_file.clone(),
        );
        assert_eq!(script.graph_file_path, graph_path_file);

        let file_path = script.write_to_file();

        // expect that file exists
        assert!(std::path::Path::new(&file_path).exists());

        // expect that the file has content
        let content = std::fs::read_to_string(&file_path).expect("Failed to read file");
        assert!(!content.is_empty());

        // destroy script
        drop(script);

        // expect that the temp file automatically is removed during destruction
        assert!(!std::path::Path::new(&file_path).exists());
    }
}
