use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{Duration, Utc};
use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};
use std::{env, sync::LazyLock};
use uuid::Uuid;

use crate::errors::{ConfluxError, Result};

static JWT_SECRET: LazyLock<String> = LazyLock::new(|| {
    env::var("CONFLUX_JWT_SECRET").unwrap_or_else(|_| {
        #[cfg(debug_assertions)]
        {
            eprintln!("[WARN] CONFLUX_JWT_SECRET not set, using default (dev mode only)");
            "dev_secret_do_not_use_in_production".to_string()
        }
        #[cfg(not(debug_assertions))]
        {
            panic!("CONFLUX_JWT_SECRET environment variable must be set in production");
        }
    })
});

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
        exp: (now + Duration::hours(24)).timestamp() as usize,
        sid: session_id,
    };

    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(JWT_SECRET.as_bytes()),
    )
    .expect("failed to encode JWT")
}

pub fn validate_token(token: &str) -> Result<Claims> {
    let decoded = decode::<Claims>(
        token,
        &DecodingKey::from_secret(JWT_SECRET.as_bytes()),
        &Validation::default(),
    )
    .map_err(|e| ConfluxError::AuthError(format!("Invalid token: {e}")))?;

    Ok(decoded.claims)
}

/// Validate JWT structure without verifying signature.
/// Used in anonymous mode where clients generate their own tokens.
pub fn validate_token_anonymous(token: &str) -> Result<Claims> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(ConfluxError::AuthError("Invalid token format".into()));
    }

    // Decode the payload (second part) - JWT uses URL-safe base64 without padding
    let payload_bytes = URL_SAFE_NO_PAD
        .decode(parts[1])
        .map_err(|e| ConfluxError::AuthError(format!("Invalid base64 in token: {e}")))?;

    let claims: Claims = serde_json::from_slice(&payload_bytes)
        .map_err(|e| ConfluxError::AuthError(format!("Invalid claims in token: {e}")))?;

    // Validate timestamps
    let now = Utc::now().timestamp() as usize;
    if claims.exp < now {
        return Err(ConfluxError::AuthError("Token expired".into()));
    }
    if claims.iat > now {
        return Err(ConfluxError::AuthError("Token issued in future".into()));
    }

    Ok(claims)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_and_validate_token() {
        let token = generate_token("testuser");
        let claims = validate_token(&token).expect("should validate");
        assert_eq!(claims.sub, "testuser");
        assert!(!claims.sid.is_empty());
    }

    #[test]
    fn test_validate_token_invalid() {
        let result = validate_token("invalid.token.here");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_token_anonymous_valid() {
        let token = generate_token("anonuser");
        let claims = validate_token_anonymous(&token).expect("should validate");
        assert_eq!(claims.sub, "anonuser");
    }

    #[test]
    fn test_validate_token_anonymous_malformed() {
        let result = validate_token_anonymous("not-a-jwt");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Invalid token format")
        );
    }

    #[test]
    fn test_validate_token_anonymous_invalid_base64() {
        let result = validate_token_anonymous("header.!!!invalid!!!.signature");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid base64"));
    }

    #[test]
    fn test_validate_token_anonymous_expired() {
        // Create an expired token by manually constructing claims
        let expired_claims = Claims {
            sub: "expireduser".to_string(),
            iat: 0,
            exp: 1, // Expired in 1970
            sid: "test-session".to_string(),
        };

        let token = encode(
            &Header::default(),
            &expired_claims,
            &EncodingKey::from_secret(b"any_secret"),
        )
        .unwrap();

        let result = validate_token_anonymous(&token);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("expired"));
    }

    #[test]
    fn test_validate_token_anonymous_future_iat() {
        // Create a token with iat in the future
        let now = Utc::now().timestamp() as usize;
        let future_claims = Claims {
            sub: "futureuser".to_string(),
            iat: now + 3600, // 1 hour in the future
            exp: now + 7200, // 2 hours in the future
            sid: "test-session".to_string(),
        };

        let token = encode(
            &Header::default(),
            &future_claims,
            &EncodingKey::from_secret(b"any_secret"),
        )
        .unwrap();

        let result = validate_token_anonymous(&token);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("future"));
    }

    #[test]
    fn test_validate_token_wrong_secret() {
        // Create a token with a different secret
        let claims = Claims {
            sub: "wrongsecret".to_string(),
            iat: Utc::now().timestamp() as usize,
            exp: (Utc::now() + Duration::hours(1)).timestamp() as usize,
            sid: "test-session".to_string(),
        };

        let token = encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(b"wrong_secret_key"),
        )
        .unwrap();

        // validate_token should reject this because signature doesn't match
        let result = validate_token(&token);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid token"));
    }

    #[test]
    fn test_token_contains_all_claims() {
        let token = generate_token("claimsuser");
        let claims = validate_token(&token).expect("should validate");

        // Verify all claims are present and valid
        assert_eq!(claims.sub, "claimsuser");
        assert!(!claims.sid.is_empty());

        let now = Utc::now().timestamp() as usize;
        assert!(claims.iat <= now);
        assert!(claims.exp > now);
        // Token should expire in ~24 hours
        assert!(claims.exp > now + 23 * 3600);
        assert!(claims.exp <= now + 25 * 3600);
    }

    #[test]
    fn test_each_token_has_unique_session_id() {
        let token1 = generate_token("user1");
        let token2 = generate_token("user1");

        let claims1 = validate_token(&token1).expect("should validate");
        let claims2 = validate_token(&token2).expect("should validate");

        // Same user, but different session IDs
        assert_eq!(claims1.sub, claims2.sub);
        assert_ne!(claims1.sid, claims2.sid);
    }

    #[test]
    fn test_anonymous_validation_accepts_any_signature() {
        // Generate token with server's secret
        let server_token = generate_token("serveruser");

        // Generate token with arbitrary secret
        let claims = Claims {
            sub: "clientuser".to_string(),
            iat: Utc::now().timestamp() as usize,
            exp: (Utc::now() + Duration::hours(1)).timestamp() as usize,
            sid: "client-session".to_string(),
        };
        let client_token = encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(b"client_generated_secret"),
        )
        .unwrap();

        // Both should pass anonymous validation
        let server_claims = validate_token_anonymous(&server_token).expect("should validate");
        let client_claims = validate_token_anonymous(&client_token).expect("should validate");

        assert_eq!(server_claims.sub, "serveruser");
        assert_eq!(client_claims.sub, "clientuser");
    }

    #[test]
    fn test_server_validation_rejects_client_generated_token() {
        // Client generates a token with their own secret
        let claims = Claims {
            sub: "clientuser".to_string(),
            iat: Utc::now().timestamp() as usize,
            exp: (Utc::now() + Duration::hours(1)).timestamp() as usize,
            sid: "client-session".to_string(),
        };
        let client_token = encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(b"client_generated_secret"),
        )
        .unwrap();

        // Server validation should reject it (signature mismatch)
        let result = validate_token(&client_token);
        assert!(result.is_err());

        // But anonymous validation should accept it
        let anon_result = validate_token_anonymous(&client_token);
        assert!(anon_result.is_ok());
    }
}
