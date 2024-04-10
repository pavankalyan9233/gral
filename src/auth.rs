//use chrono::prelude::*;
use hmac::{Hmac, Mac};
use jwt::header::HeaderType;
use jwt::{AlgorithmType, Header, SignWithKey, Token, VerifyWithKey};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::sync::{Arc, Mutex};
use std::time;
use warp::{
    filters::header::headers_cloned,
    http::header::{HeaderMap, HeaderValue, AUTHORIZATION},
    reject, Filter, Rejection,
};

use crate::args::{with_args, GralArgs};

mod authentication {
    tonic::include_proto! {"authentication"}
}

use authentication::authentication_v1_client::AuthenticationV1Client;
use authentication::{CreateTokenRequest, Duration, ValidateRequest};

const BEARER: &str = "bearer ";
const BEARERCAP: &str = "Bearer ";

type WebResult<T> = std::result::Result<T, Rejection>;

#[derive(Debug, Deserialize, Serialize)]
struct Claims {
    preferred_username: Option<String>,
    iss: String,
    exp: Option<u64>,
    server_id: Option<String>,
}

pub fn with_auth(
    gral_args: Arc<Mutex<GralArgs>>,
) -> impl Filter<Extract = (String,), Error = Rejection> + Clone {
    headers_cloned()
        .and(with_args(gral_args))
        .map(
            move |headers: HeaderMap<HeaderValue>, gral_args: Arc<Mutex<GralArgs>>| {
                (gral_args, headers)
            },
        )
        .and_then(authorize)
}

#[derive(Debug, Serialize)]
pub struct Unauthorized {
    pub msg: String,
}

impl reject::Reject for Unauthorized {}

async fn authorize_via_service(token: String, url: String) -> WebResult<String> {
    let channel = tonic::transport::Channel::from_shared(url)
        .unwrap()
        .connect()
        .await;
    match channel {
        // Embed retry loop and write results to log file, store them in LOGS folder
        // around the caller of: authorize_via_service (1000 calls, every 0,1sec approx.)
        Err(e) => Err(warp::reject::custom(Unauthorized {
            msg: format!("Cannot reach authentication service: {}", e),
        })),
        Ok(channel) => {
            let mut client = AuthenticationV1Client::new(channel);
            let request = tonic::Request::new(ValidateRequest {
                token: token.to_string(),
            });
            let response = client.validate(request).await;
            match response {
                Err(e) => Err(warp::reject::custom(Unauthorized {
                    msg: format!("Cannot talk to authentication service: {}", e),
                })),
                Ok(resp) => {
                    let r = resp.get_ref();
                    if r.is_valid {
                        if let Some(det) = &r.details {
                            Ok(det.user.clone())
                        } else {
                            // Authenticate with empty user:
                            Ok("".to_string())
                        }
                    } else {
                        Err(warp::reject::custom(Unauthorized {
                            msg: format!("Token not valid: {:?}", r.message),
                        }))
                    }
                }
            }
        }
    }
}

fn extract_auth_token(headers: &HeaderMap<HeaderValue>) -> WebResult<String> {
    let header = match headers.get(AUTHORIZATION) {
        Some(v) => v,
        None => {
            return Err(warp::reject::custom(Unauthorized {
                msg: "Missing Authorization header".to_string(),
            }));
        }
    };
    let auth_header = match std::str::from_utf8(header.as_bytes()) {
        Ok(v) => v,
        Err(e) => {
            return Err(warp::reject::custom(Unauthorized {
                msg: format!("Non-UTF-8 authorization header: {:?}", e),
            }))
        }
    };
    if !auth_header.starts_with(BEARER) && !auth_header.starts_with(BEARERCAP) {
        return Err(warp::reject::custom(Unauthorized {
            msg: format!("Bad authentication header: {}", auth_header),
        }));
    }
    Ok(auth_header[7..].to_string())
}

async fn authorize(
    (gral_args, headers): (Arc<Mutex<GralArgs>>, HeaderMap<HeaderValue>),
) -> WebResult<String> {
    // Do we do authentication in the first place, if not, we just take the
    // predefined user and move on:
    {
        let args = gral_args.lock().unwrap();
        if !args.authentication {
            return Ok(args.arangodb_user.clone());
        }
    }

    // Now let's extract the auth token, if this already fails, we block
    // the request (note the ?):
    let token = extract_auth_token(&headers)?;

    // Do we authenticate via the auth service?
    let auth_service;
    {
        let args = gral_args.lock().unwrap();
        auth_service = args.auth_service.clone();
    }
    if !auth_service.is_empty() {
        // Use service to authenticate JWT token:
        let url = "http://".to_string() + &auth_service;
        return authorize_via_service(token, url).await;
    }

    // Finally, try all secrets to verify signature:
    let args = gral_args.lock().unwrap();
    for sec in &args.arangodb_jwt_secrets {
        let key: Hmac<Sha256> = Hmac::new_from_slice(sec).unwrap();
        let maybe_claims: Result<Claims, jwt::error::Error> = token.verify_with_key(&key);
        if let Ok(claims) = maybe_claims {
            // Good signature, extract user:
            return Ok(match claims.preferred_username {
                None => "root".to_string(),
                Some(user) => user.clone(),
            });
        }
    }
    Err(warp::reject::custom(Unauthorized {
        msg: format!("Bad signature in JWT token: {}", token),
    }))
}

async fn use_service(
    auth_service: String,
    user: &str,
    expiry_in_seconds: u64,
) -> Result<String, String> {
    let url = "http://".to_string() + &auth_service;
    let channel = tonic::transport::Channel::from_shared(url)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = AuthenticationV1Client::new(channel);
    let request = tonic::Request::new(CreateTokenRequest {
        user: Some(user.to_string()),
        lifetime: Some(Duration {
            seconds: expiry_in_seconds as i64,
            nanos: 0,
        }),
    });
    let response = client.create_token(request).await.unwrap();
    Ok(response.get_ref().token.clone())
}

fn jwt_token_by_service(auth_service: String, user: &str, expiry_in_seconds: u64) -> String {
    let fut = use_service(auth_service, user, expiry_in_seconds);
    futures::executor::block_on(fut).unwrap()
}

pub fn create_jwt_token(gral_args: &GralArgs, user: &str, expiry_in_seconds: u64) -> String {
    if !gral_args.auth_service.is_empty() {
        return jwt_token_by_service(gral_args.auth_service.clone(), user, expiry_in_seconds);
    }
    // set user to empty for a superuser token
    // set expiry_in_seconds to 0 for a permanent token
    let key: Hmac<Sha256> = if !gral_args.arangodb_jwt_secrets.is_empty() {
        Hmac::new_from_slice(&gral_args.arangodb_jwt_secrets[0]).unwrap()
    } else {
        Hmac::new_from_slice(b"abc").unwrap()
    };
    let exp = if expiry_in_seconds == 0 {
        None
    } else {
        Some(
            (time::SystemTime::now() + time::Duration::from_secs(expiry_in_seconds))
                .duration_since(time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        )
    };
    let header = Header {
        algorithm: AlgorithmType::Hs256,
        type_: Some(HeaderType::JsonWebToken),
        ..Default::default()
    };
    let claims = Claims {
        preferred_username: if user.is_empty() {
            None
        } else {
            Some(user.to_string())
        },
        iss: "arangodb".to_string(),
        exp,
        server_id: None,
    };
    let token = Token::new(header, claims).sign_with_key(&key).unwrap();
    token.as_str().to_string()
}
