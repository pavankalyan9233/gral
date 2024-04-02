extern crate utilities;

#[test]
fn setup() {
    // setup code here
    std::thread::spawn(|| {
        // Start service, but now use the auth-service docker environment for authentication.
        // Auth service is not reachable via Port 1337
        std::env::set_var("INTEGRATION_SERVICE_ADDRESS", "localhost:1337");
        gral::server::run();
    });
}

#[test]
fn test_authentication_no_auth_given() {
    let response = utilities::http_helper::get("http://localhost:9999/v1/graphs", None);
    assert_eq!(response["errorCode"].as_u64().unwrap(), 401);
}

#[test]
fn test_authentication_with_invalid_bearer_token() {
    let invalid_token = "invalid_token";
    let headers = utilities::http_helper::build_bearer_auth_header(&invalid_token);
    let response = utilities::http_helper::get("http://localhost:9999/v1/graphs", Some(headers));

    assert_eq!(response["errorCode"].as_u64().unwrap(), 401);
}

#[test]
fn test_authentication_with_bearer_token() {
    let token = utilities::arangodb_helper::generate_superuser_bearer();
    let headers = utilities::http_helper::build_bearer_auth_header(&token);
    let response = utilities::http_helper::get("http://localhost:9999/v1/graphs", Some(headers));

    assert_eq!(response["errorCode"].as_u64().unwrap(), 401);
}

#[test]
fn teardown() {
    // teardown code here
}
