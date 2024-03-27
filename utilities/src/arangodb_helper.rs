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
    println!("Response: {}", response);

    return "peter".to_string();
}
