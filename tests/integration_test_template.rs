use base64::{engine::general_purpose, Engine as _};
use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use serde_json::Value;

use gral::constants;

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
    let client = Client::new();
    let mut headers = HeaderMap::new();
    let auth = "root:".to_string();
    let post_body = general_purpose::STANDARD.encode(auth.as_bytes());
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Basic {}", post_body)).unwrap(),
    );

    let response = client
        .get("http://localhost:8529/_api/version")
        .headers(headers)
        .send()
        .expect("Failed to send request");

    let body = response.text().unwrap();
    let json: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json["server"].as_str().unwrap(), "arango");
    assert!(json["version"].as_str().unwrap().starts_with("3.12"));
}
