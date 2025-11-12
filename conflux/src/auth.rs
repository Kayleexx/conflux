use chrono::{Duration, Utc};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::errors::{ConfluxError, Result};

const SECRET_KEY: &str = "super_secret_key_replace_me";

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Claims {
    pub sub: String,
    pub iat: usize,
    pub exp: usize,
    pub sid: String,
}

pub fn generate_token(username: &str) -> String {
    let now = Utc::now();
    let session_id = Uuid::new_v4().to_string();

    let claims = Claims {
        sub: username.to_string(),
        iat: now.timestamp() as usize,
        exp: (now + Duration::hours(24)).timestamp() as usize, // 24-hour expiry
        sid: session_id,
    };

    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(SECRET_KEY.as_ref()),
    )
    .expect("failed to encode JWT")
}

pub fn validate_token(token: &str) -> Result<Claims> {
    let decoded = decode::<Claims>(
        token,
        &DecodingKey::from_secret(SECRET_KEY.as_ref()),
        &Validation::default(),
    )
    .map_err(|e| ConfluxError::AuthError(format!("Invalid token: {e}")))?;

    Ok(decoded.claims)
}
