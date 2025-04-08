use serde::Deserialize;

use crate::frontend::auth;

#[derive(Deserialize)]
pub struct RegisterPayload {
    pub username: String,
    pub pin: String,
}

#[derive(Deserialize)]
pub struct StoreSecretPayload {
    pub token: AuthToken,
    pub val: String,
}

#[derive(Deserialize)]
pub struct RecoverSecretPayload {
    pub username: String,
    // pub password: String,
    pub token: AuthToken,
}

#[derive(Deserialize, Clone)]
pub struct AuthToken {
    pub valid_until: String,
    pub username: String,
    pub signature: String,
    pub leader_name: String
}

#[derive(Deserialize)]
pub struct GetTokenPayload {
    pub username: String,
    pub pin: String,
}