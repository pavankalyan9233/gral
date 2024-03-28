extern crate utilities;

pub fn start_gral_server() {
    std::thread::spawn(|| {
        gral::server::run();
    });
}

#[test]
fn test_authentication_no_auth_given() {
    start_gral_server();
    let response = utilities::http_helper::get("http://localhost:9999/v1/graphs", None);
    assert_eq!(response["errorCode"].as_u64().unwrap(), 401);
}

#[test]
fn test_authentication_with_invalid_bearer_token() {
    start_gral_server();
    let invalid_token = "invalid_token";
    let headers = utilities::http_helper::build_bearer_auth_header(&invalid_token);
    let response = utilities::http_helper::get("http://localhost:9999/v1/graphs", Some(headers));

    assert_eq!(response["errorCode"].as_u64().unwrap(), 401);
}

#[test]
fn test_authentication_with_bearer_token() {
    start_gral_server();
    let token = utilities::arangodb_helper::generate_superuser_bearer();
    let headers = utilities::http_helper::build_bearer_auth_header(&token);
    let response = utilities::http_helper::get("http://localhost:9999/v1/graphs", Some(headers));

    assert_eq!(response.to_string(), "[]");
}
