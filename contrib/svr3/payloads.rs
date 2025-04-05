use serde::Deserialize;

#[derive(Deserialize)]
pub struct RegisterPayload {
    pub username: String,
    pub password: String,
}

#[derive(Deserialize)]
pub struct StoreSecretPayload {
    pub username: String,
    pub password: String,
    pub val: String,
    pub pin: String,
}

#[derive(Deserialize)]
pub struct RecoverSecretPayload {
    pub username: String,
    pub password: String,
    pub pin: String,
}
