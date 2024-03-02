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

const BEARER: &str = "bearer ";
const BEARERCAP: &str = "Bearer ";

type WebResult<T> = std::result::Result<T, Rejection>;

#[derive(Debug, Deserialize, Serialize)]
struct Claims {
    preferred_username: String,
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

async fn authorize(
    (gral_args, headers): (Arc<Mutex<GralArgs>>, HeaderMap<HeaderValue>),
) -> WebResult<String> {
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
    let token = &auth_header[7..].to_string();

    // Now try all secrets to verify signature:
    let args = gral_args.lock().unwrap();
    for sec in &args.arangodb_jwt_secrets {
        let key: Hmac<Sha256> = Hmac::new_from_slice(sec).unwrap();
        let maybe_claims: Result<Claims, jwt::error::Error> = token.verify_with_key(&key);
        if let Ok(claims) = maybe_claims {
            // Good signature, extract user:
            return Ok(claims.preferred_username.clone());
        }
    }
    Err(warp::reject::custom(Unauthorized {
        msg: format!("Bad signature in JWT token: {}", token),
    }))
}

pub fn create_jwt_token(gral_args: &GralArgs, user: &String, expiry_in_seconds: u64) -> String {
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
        preferred_username: user.clone(),
        iss: "arangodb".to_string(),
        exp,
        server_id: None,
    };
    let token = Token::new(header, claims).sign_with_key(&key).unwrap();
    token.as_str().to_string()
}
