use std::io::Write;
use tempfile::NamedTempFile;

pub struct Script {
    lines: Vec<String>,
}

impl Script {
    pub fn write_to_file(&self) -> Result<NamedTempFile, String> {
        let mut script_file = crate::python::executor::create_temporary_file(
            "gral_script_".to_string(),
            ".py".to_string(),
        )?;
        for line in &self.lines {
            let line_with_newline = format!("{}\n", line);
            let res = script_file.write_all(line_with_newline.as_bytes());
            if res.is_err() {
                return Err("Could not write script to file".to_string());
            }
        }

        Ok(script_file)
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
    let lines = vec![];
    let mut script = Script { lines };
    let base_script_str = read_base_script()?;

    for line in base_script_str.lines() {
        if line.contains("<Placeholder for graph_file_path>") {
            script
                .lines
                .push(format!("graph_file_path = \"{}\"", graph_file_path));
            continue;
        } else if line.contains("<Placeholder for user injected script>") {
            script.lines.push(user_script.clone());
            continue;
        } else if line.contains("<Placeholder for result file path>") {
            script
                .lines
                .push(format!("result_file_path = \"{}\"", result_file_path));
            continue;
        }
        script.lines.push(line.to_string());
    }

    Ok(script)
}
