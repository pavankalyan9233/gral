use crate::python;
use python::Script;

pub struct Executor {
    pub graph_id: u64,
    pub script: Script,
}

impl Executor {
    pub fn new(graph_id: u64, user_script_snippet: String) -> Executor {
        Executor {
            graph_id,
            script: Script::new(graph_id, user_script_snippet),
        }
    }

    pub fn run(&self) -> String {
        /*let mut output = String::new();
        let mut child = Command::new("python3")
            .arg("-c")
            .arg(&self.user_script)
            .stdout(Stdio::piped())
            .spawn()
            .expect("Failed to execute command");
        child
            .stdout
            .as_mut()
            .unwrap()
            .read_to_string(&mut output)
            .unwrap();
        output*/
        return "output".to_string();
    }
}
