use base64::{engine::general_purpose, Engine as _};
use reqwest::blocking::Client;
use reqwest::{header::HeaderMap, Method};
use serde_json::Value;

pub fn encode_base64(input: &str) -> String {
    general_purpose::STANDARD.encode(input.as_bytes())
}

pub fn build_basic_auth_header(username: &str, password: &str) -> HeaderMap {
    let mut headers = reqwest::header::HeaderMap::new();
    let auth = format!("{}:{}", username, password);
    let encoded = encode_base64(&auth);
    headers.insert(
        reqwest::header::AUTHORIZATION,
        reqwest::header::HeaderValue::from_str(&format!("Basic {}", encoded)).unwrap(),
    );
    headers
}

pub fn build_bearer_auth_header(token: &str) -> HeaderMap {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        reqwest::header::AUTHORIZATION,
        reqwest::header::HeaderValue::from_str(&format!("Bearer {}", token)).unwrap(),
    );
    headers
}

pub fn get(endpoint: &str, headers: Option<HeaderMap>) -> Value {
    return execute_request(Method::GET, endpoint, None, headers);
}

pub fn post(endpoint: &str, body: &str, headers: Option<HeaderMap>) -> Value {
    return execute_request(Method::POST, endpoint, Some(body), headers);
}

pub fn put(endpoint: &str, body: &str, headers: Option<HeaderMap>) -> Value {
    return execute_request(Method::PUT, endpoint, Some(body), headers);
}

pub fn delete(endpoint: &str, headers: Option<HeaderMap>) -> Value {
    return execute_request(Method::DELETE, endpoint, None, headers);
}

pub fn execute_request(
    method: Method,
    endpoint: &str,
    body: Option<&str>,
    headers: Option<HeaderMap>,
) -> Value {
    match send_request(method, endpoint, body, headers) {
        Ok(result) => result,
        Err(err) => panic!("Request failed: {}", err),
    }
}

pub fn send_request(
    method: Method,
    endpoint: &str,
    body: Option<&str>,
    headers: Option<HeaderMap>,
) -> Result<Value, Box<dyn std::error::Error>> {
    let client = Client::new();
    let mut request_builder = client.request(method.clone(), endpoint);

    if let Some(headers) = headers {
        request_builder = request_builder.headers(headers);
    }

    let response = match method {
        Method::POST | Method::PUT => request_builder.body(body.unwrap_or("").to_string()).send(),
        _ => request_builder.send(),
    };

    let response_body = response.unwrap().text().unwrap();
    let json: Value = serde_json::from_str(&response_body)?;

    Ok(json)
}
