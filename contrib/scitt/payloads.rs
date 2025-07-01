use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct EntryPayload {
    pub cose_signed_statement: Vec<u8>,
}
