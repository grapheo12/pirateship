use serde::Deserialize;

#[derive(Deserialize)]
pub struct RegisterPayload {
    pub username: String,
    pub password: String,
}

#[derive(Deserialize)]
pub struct PubKeyPayload {
    pub username: String,
}
