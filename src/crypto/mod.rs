pub mod aes;
// pub mod secp256k1;
// pub mod ed25519;

pub struct KeyStore {
    pub tls_pub_key: String,
    pub tls_priv_key: String,
    pub sign_pub_key: String,
    pub sign_priv_key: String
}