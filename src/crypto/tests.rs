use super::KeyStore;

/// The tests needs a config directory.
/// Create it by running: sh scripts/gen_local_config.sh configs 7 scripts/local_template.json
#[test]
pub fn test_load_key() {
    KeyStore::new(
        &String::from("configs/signing_pub_keys.keylist"),
        &String::from("configs/node1_signing_priv_key.pem"),
    );
}

#[test]
pub fn test_sign_and_verify() {
    let keys = KeyStore::new(
        &String::from("configs/signing_pub_keys.keylist"),
        &String::from("configs/node1_signing_priv_key.pem"),
    );

    let message = String::from("I am become death, the destroyer of worlds!");
    let mut sig = keys.sign(message.as_bytes());
    if !keys.verify(&String::from("node1"), &sig, message.as_bytes()) {
        panic!("I should verify my signature!");
    }

    if keys.verify(&String::from("node2"), &sig, message.as_bytes()) {
        panic!("Node2's key should not verify my signature!");
    }

    sig[0] = !sig[0];

    if keys.verify(&String::from("node1"), &sig, message.as_bytes()) {
        panic!("I should not verify a forged signature!");
    }
}
