// Copyright (c) Shubham Mishra. All rights reserved.
// Licensed under the MIT License.


use sha2::{Digest, Sha256};

pub const DIGEST_LENGTH: usize = 32;

pub fn hash(data: &Vec<u8>) -> Vec<u8> {
    Sha256::new()
        .chain_update(data)
        .finalize()
        .as_slice()
        .to_vec()
    // vec![0u8; DIGEST_LENGTH]
}

pub fn cmp_hash(one: &Vec<u8>, two: &Vec<u8>) -> bool {
    one.as_slice() == two.as_slice()
    // true
}
