use std::{io::{Cursor, Error, ErrorKind}, sync::Arc};
use bytes::Bytes;
use ed25519_dalek::SIGNATURE_LENGTH;
use log::debug;
use rand::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream};
use tokio_rustls::{server, client};

use super::{client::Client, server::Server};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct HandshakeResponse {
    pub name: String,
    pub signature: Bytes
}

impl HandshakeResponse {
    pub(crate) fn serialize_cbor(&self) -> Vec<u8> {
        match serde_cbor::to_vec(self) {
            Ok(b) => b,
            Err(e) => {
                panic!("{}", e);
            }
        }
    }

    pub(crate) fn deserialize_cbor(arr: &Vec<u8>) -> HandshakeResponse {
        let reader = Cursor::new(arr);
        let deser: HandshakeResponse = match serde_cbor::from_reader(reader) {
            Ok(resp) => resp,
            Err(e) => {
                panic!("{}", e);
            },
        };

        deser
    }

}

fn construct_payload(nonce: u32, name: &String) -> Vec<u8> {
    let nonce_enc = nonce.to_be_bytes();
    let mut result = vec![0u8; 0];
    result.extend(nonce_enc.as_slice());
    result.extend(name.as_bytes());
    result
}
/// Authentication handshake
/// Server                              Client
/// -----                               ------
/// Nonce   ---(TLS channel)-------->
///         <-----------------------    <name, Sign(Nonce||name)>
/// if name in keylist && signature verifies against registered pubkey
/// accept connection, or drop.
/// Returns name of peer or error.
pub async fn handshake_server<S>(server: &Arc<Server<S>>, stream: &mut server::TlsStream<TcpStream>) -> Result<String, Error>
    where S: Send + Sync + 'static
{
    let mut rng = rand::rngs::OsRng;
    let nonce: u32 = rng.gen();
    stream.write_u32(nonce).await?;

    // Following usual message structure: Size(u32) | Body
    let sz = stream.read_u32().await? as usize;
    if sz == 0 {
        return Err(Error::new(ErrorKind::InvalidData, "invalid response"));
    }
    let mut buf = vec![0u8; sz];
    stream.read_exact(buf.as_mut()).await?;

    let resp: HandshakeResponse = match serde_cbor::from_slice(buf.as_slice()) {
        Ok(res) => res,
        Err(e) => {
            return Err(Error::new(ErrorKind::InvalidData,
                    format!("invalid response; deserialize error: {}", e.to_string()))); 
        }
    };

    let name = resp.name;
        // String::from(std::str::from_utf8(resp.name).unwrap_or(""));
    if server.key_store.get_pubkey(&name).is_none() {
        return Err(Error::new(ErrorKind::InvalidData, "unknown peer"))
    }

    let payload = construct_payload(nonce, &name);
    let sig: &[u8; SIGNATURE_LENGTH] = resp.signature.as_ref().try_into()
        .unwrap_or(&[0u8; SIGNATURE_LENGTH]);
    // Let's hope a blank signature is never a valid signature.

    if !server.key_store.verify(&name, sig, payload.as_slice()){
        return Err(Error::new(ErrorKind::InvalidData, "invalid signature"))
    }

    Ok(name)
}

pub async fn handshake_client(client: &Arc<Client>, stream: &mut client::TlsStream<TcpStream>) -> Result<(), Error> {
    let nonce = stream.read_u32().await?;
    debug!("Received nonce: {}", nonce);
    let payload = construct_payload(nonce, &client.config.name);
    let signature = client.key_store.sign(&payload.as_slice());
    let signature = Bytes::from(Vec::from(signature));
    let name = client.config.name.clone();
    let resp = HandshakeResponse { name, signature };
    let resp_buf = match serde_cbor::to_vec(&resp) {
        Ok(b) => b,
        Err(_) => {
            return Err(Error::new(ErrorKind::InvalidData, "serialization error"));
        }
    };

    stream.write_u32(resp_buf.len() as u32).await?;
    stream.write_all(resp_buf.as_slice()).await?;


    Ok(())
}