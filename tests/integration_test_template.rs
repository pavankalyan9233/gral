use base64::{engine::general_purpose, Engine as _};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use reqwest::Client;
use serde_json::Value;
use tokio::runtime::Runtime;

use gral::environment::constants;

#[test]
fn the_example_integration_test() {
    // This test almost does nothing.
    // It is still here to demonstrate how to use modules out of the main crate (gral).
    // Any module which is exposed in `src/lib.rs` can be used here.
    // All tests in this directory are integration tests. Most of them will need ArangoDB running.
    assert_eq!(constants::VERSION, 0x00100);
}

#[test]
fn simple_arangodb_version_check() {
    // This only exists to check whether the communication to ArangoDB in CircleCI does work.
    // After we've real tests, this one can be removed.
    let rt = Runtime::new().unwrap();
    let client = Client::new();
    let mut headers = HeaderMap::new();
    let auth = "root:".to_string();
    let post_body = general_purpose::STANDARD.encode(auth.as_bytes());
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Basic {}", post_body)).unwrap(),
    );

    let future = client
        .get("http://localhost:8529/_api/version")
        .headers(headers)
        .send();

    let response = rt.block_on(future).unwrap();
    assert!(response.status().is_success());
    let body = rt.block_on(response.text()).unwrap();
    let json: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json["server"].as_str().unwrap(), "arango");
    assert!(json["version"].as_str().unwrap().starts_with("3.12"));
}
