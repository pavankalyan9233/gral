use crate::http_helper;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Auth {
    username: String,
    password: String,
}

pub fn generate_superuser_bearer() -> String {
    let username = "root";
    let password = "";
    let arangodb_endpoint = "http://localhost:8529/_open/auth";
    let headers = http_helper::build_basic_auth_header(username, password);

    let body = Auth {
        username: username.to_string(),
        password: password.to_string(),
    };

    // Serialize to JSON
    let json_body = serde_json::to_string(&body).unwrap();

    let response = http_helper::post(arangodb_endpoint, &json_body, Some(headers));
    if response.get("jwt").is_some() {
        return response["jwt"].as_str().unwrap().to_string();
    } else {
        panic!("Failed to generate superuser bearer token");
    }
}

pub fn create_example_graph(db_name: String, graph_name: String) -> bool {
    let arangodb_endpoint = format!(
        "http://localhost:8529/_db/{db_name}/_admin/aardvark/graph-examples/create/#{graph_name}"
    );
    let token = generate_superuser_bearer();
    let headers = http_helper::build_bearer_auth_header(&token);
    let body = "".to_string();
    let response = http_helper::post(&arangodb_endpoint, &body, Some(headers));
    if response.get("error").is_some() {
        let has_error = response["error"].as_bool().unwrap();
        has_error
    } else {
        if response.get("errorMessage").is_some() {
            let error_message = response["errorMessage"].as_str().unwrap().to_string();
            panic!(
                "{}",
                format!("Failed to create example graph: {error_message}")
            );
        } else {
            panic!(
                "{}",
                format!("Failed to create example graph: {graph_name} in {db_name}")
            );
        }
    }
}
