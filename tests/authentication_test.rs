extern crate utilities;

#[test]
fn test_authentication_via_jwt() {
    let token = utilities::arangodb_helper::generate_superuser_bearer();
    let response = utilities::http_helper::get("http://localhost:999/v1/graphs", None);

    println!("Token: {}", token);
    println!("Response: {}", response);

    //let response = rt.block_on(future).unwrap();
    //assert!(response.status().is_success());
    //let body = rt.block_on(response.text()).unwrap();
    //let json: Value = serde_json::from_str(&body).unwrap();
    //assert_eq!(json["server"].as_str().unwrap(), "arango");
    //assert!(json["version"].as_str().unwrap().starts_with("3.12"));
}
