use std::fs::File;
use std::io::Write;

pub struct Script {
    pub graph_name: String,
    pub user_script: String,
    pub lines: Vec<String>,
}

impl Script {
    pub fn new(graph_name: String, user_script: String) -> Script {
        let mut script = Script {
            graph_name,
            user_script,
            lines: Vec::new(),
        };

        script.generate_python_script();
        script
    }

    fn write_to_file(&self) -> String {
        let current_dir = std::env::current_dir().expect("Failed to get current directory");
        let file_path = current_dir.join(format!("tmp_{}.py", self.graph_name));

        let mut file = File::create(file_path.clone()).expect("Failed to create file");
        for line in &self.lines {
            let line_with_newline = format!("{}\n", line);
            file.write_all(line_with_newline.as_bytes())
                .expect("Failed to write to file");
        }

        return file_path.to_str().unwrap().to_string();
    }

    fn pretty_print(&self) {
        println!("Script for graph: {}", self.graph_name);
        for line in &self.lines {
            println!("{}", line);
        }
    }

    fn add_line(&mut self, line: &str) {
        self.lines.push(line.to_string());
    }

    fn add_graph_name(&mut self) {
        self.add_line(&format!("graph_name = \"{}\"", self.graph_name));
    }

    fn generate_python_script(&mut self) {
        let script_bytes = include_bytes!(concat!(env!("OUT_DIR"), "/base_functions.py"));
        let script_str = std::str::from_utf8(script_bytes).unwrap();
        for line in script_str.lines() {
            println!("line: {}", line);
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
        let graph_name = "test_graph".to_string();
        let user_script_snippet = "def worker(): print('Hello, World!')".to_string();
        let script = Script::new(graph_name, user_script_snippet);
        assert_eq!(script.graph_name, "test_graph");

        let file_path = script.write_to_file();
        // expect that file exists
        assert!(std::path::Path::new(&file_path).exists());
        script.pretty_print();

        // remove that file again
        std::fs::remove_file(&file_path).expect("Failed to remove file");
    }
}
