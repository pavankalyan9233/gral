use std::io::Write;
use tempfile::NamedTempFile;

pub struct Script {
    pub lines: Vec<String>,
}

impl Script {
    fn add_line(&mut self, line: String) {
        self.lines.push(line.to_string());
    }

    fn add_graph_file_path(&mut self, graph_file_path: String) {
        self.add_line(format!("graph_file_path = \"{}\"", graph_file_path));
    }

    pub fn write_to_file(&self, file: &mut NamedTempFile) -> Result<(), String> {
        for line in &self.lines {
            let line_with_newline = format!("{}\n", line);
            let res = file.write_all(line_with_newline.as_bytes());
            if res.is_err() {
                return Err("Could not write script to file".to_string());
            }
        }

        Ok(())
    }
}

fn read_base_script() -> Result<String, String> {
    let res = std::str::from_utf8(include_bytes!(concat!(
        env!("OUT_DIR"),
        "/base_functions.py"
    )));
    if res.is_err() {
        return Err("Failed to read base script".to_string());
    }
    Ok(res.unwrap().to_string())
}

pub fn generate_script(
    user_script: String,
    result_file_path: String,
    graph_file_path: String,
) -> Result<Script, String> {
    let mut script = Script { lines: vec![] };
    let base_script_str = read_base_script()?;

    for line in base_script_str.lines() {
        if line.contains("<Placeholder for graph_file_path>") {
            script.add_graph_file_path(graph_file_path.clone());
            continue;
        } else if line.contains("<Placeholder for user injected script>") {
            script.add_line(user_script.clone());
            continue;
        } else if line.contains("<Placeholder for result file path>") {
            script.add_line(format!("result_file_path = \"{}\"", result_file_path));
            continue;
        }
        script.add_line(line.to_string());
    }

    Ok(script)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::Builder;

    #[test]
    fn test_hello_world_script() {
        let user_script_snippet = "def worker(): print('Hello, World!')".to_string();
        let result_path_file = "result.parquet".to_string();
        let graph_path_file = "graph.parquet".to_string();
        let script =
            generate_script(user_script_snippet, result_path_file, graph_path_file).unwrap();

        let mut script_file = Builder::new()
            .prefix("gral_script_")
            .suffix(".py")
            .tempfile()
            .unwrap();

        let res = script.write_to_file(&mut script_file);
        assert!(res.is_ok());

        let file_path = script_file.path().to_str().unwrap().to_string();

        // expect that file exists
        assert!(std::path::Path::new(&file_path).exists());

        // expect that the file has content
        let content = std::fs::read_to_string(&file_path).expect("Failed to read file");
        assert!(!content.is_empty());

        // destroy script
        drop(script_file);

        // expect that the temp file automatically is removed during destruction
        assert!(!std::path::Path::new(&file_path).exists());
    }
}
