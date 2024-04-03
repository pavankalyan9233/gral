#[cfg(feature = "test_arangodb_auth")]
mod arangodb_as_authentication_service;
#[cfg(feature = "test_grpc_auth")]
mod auth_grpc_as_authentication_service;
#[cfg(feature = "test_grpc_auth_unavailable")]
mod auth_grpc_as_authentication_service_unavailable;
