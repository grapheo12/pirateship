use serde::Deserialize;

#[derive(Deserialize)]
pub struct RegisterPayload {
    pub username: String,
}

#[derive(Deserialize)]
pub struct SendPayload {
    pub sender_account: String,
    pub receiver_account: String,
    pub send_amount: i64,
}
