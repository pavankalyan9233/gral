use std::io::Write;
use tempfile::Builder;

pub struct Script {
    pub graph_id: u64,
    pub user_script: String,
    pub lines: Vec<String>,
    pub temp_file: tempfile::NamedTempFile,
}

impl Script {
    pub fn new(graph_id: u64, user_script: String) -> Script {
        let mut script = Script {
            graph_id,
            user_script,
            lines: Vec::new(),
            temp_file: Builder::new()
                .prefix("gral")
                .suffix(".py")
                .tempfile()
                .expect("Failed to create temporary file"),
        };

        script.generate_python_script();
        script
    }

    fn write_to_file(&mut self) -> String {
        // Create a temporary directory

        for line in &self.lines {
            let line_with_newline = format!("{}\n", line);
            self.temp_file
                .write_all(line_with_newline.as_bytes())
                .expect("Failed to write to file");
        }

        return self.temp_file.path().to_str().unwrap().to_string();
    }

    fn pretty_print(&self) {
        println!("Script for graph: {}", self.graph_id);
        for line in &self.lines {
            println!("{}", line);
        }
    }

    fn add_line(&mut self, line: &str) {
        self.lines.push(line.to_string());
    }

    fn add_graph_name(&mut self) {
        self.add_line(&format!("graph_name = \"{}\"", self.graph_id));
    }

    fn generate_python_script(&mut self) {
        let script_bytes = include_bytes!(concat!(env!("OUT_DIR"), "/base_functions.py"));
        let script_str = std::str::from_utf8(script_bytes).unwrap();
        for line in script_str.lines() {
            if line.contains("<Placeholder graph_name>") {
                self.add_graph_name();
                continue;
            } else if line.contains("<Placeholder for user injected script>") {
                self.add_line(&self.user_script.clone());
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
        let graph_id = 1;
        let user_script_snippet = "def worker(): print('Hello, World!')".to_string();
        let mut script = Script::new(graph_id, user_script_snippet);
        assert_eq!(script.graph_id, 1);

        let file_path = script.write_to_file();

        // expect that file exists
        assert!(std::path::Path::new(&file_path).exists());

        // can be removed later, just for debugging
        script.pretty_print();

        // destroy script
        drop(script);

        // expect that the temp file automatically is removed during destruction
        assert!(!std::path::Path::new(&file_path).exists());
    }
}
