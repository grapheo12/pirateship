use bytes::Bytes;
use ed25519_dalek::SIGNATURE_LENGTH;
use log::debug;
use prost::Message;
use rand::prelude::*;
use std::{
    io::{Error, ErrorKind},
    sync::Arc,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tokio_rustls::{client, server};

use super::{client::Client, proto::auth::ProtoHandshakeResponse, server::Server};

#[derive(Clone, Debug)]
pub(crate) struct HandshakeResponse {
    pub name: String,
    pub signature: Bytes,
}

impl HandshakeResponse {
    pub(crate) fn serialize(&self) -> Vec<u8> {
        let proto = ProtoHandshakeResponse {
            name: self.name.clone(),
            signature: self.signature.to_vec(),
        };
        proto.encode_to_vec()
    }

    pub(crate) fn deserialize(arr: &Vec<u8>) -> Result<HandshakeResponse, Error> {
        let proto = ProtoHandshakeResponse::decode(arr.as_slice());
        let deser = match proto {
            Ok(d) => HandshakeResponse {
                name: d.name,
                signature: Bytes::from(d.signature),
            },
            Err(e) => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("Protobuf error: {}", e),
                ));
            }
        };

        Ok(deser)
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
pub async fn handshake_server<S>(
    server: &Arc<Server<S>>,
    stream: &mut server::TlsStream<TcpStream>,
) -> Result<String, Error>
where
    S: Send + Sync + 'static,
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

    let resp = HandshakeResponse::deserialize(&buf)?;

    let name = resp.name;
    // String::from(std::str::from_utf8(resp.name).unwrap_or(""));
    if server.key_store.get_pubkey(&name).is_none() {
        return Err(Error::new(ErrorKind::InvalidData, "unknown peer"));
    }

    let payload = construct_payload(nonce, &name);
    let sig: &[u8; SIGNATURE_LENGTH] = resp
        .signature
        .as_ref()
        .try_into()
        .unwrap_or(&[0u8; SIGNATURE_LENGTH]);
    // Let's hope a blank signature is never a valid signature.

    if !server.key_store.verify(&name, sig, payload.as_slice()) {
        return Err(Error::new(ErrorKind::InvalidData, "invalid signature"));
    }

    Ok(name)
}

pub async fn handshake_client(
    client: &Arc<Client>,
    stream: &mut client::TlsStream<TcpStream>,
) -> Result<(), Error> {
    let nonce = stream.read_u32().await?;
    debug!("Received nonce: {}", nonce);
    let cfg = client.config.get();
    let payload = construct_payload(nonce, &cfg.net_config.name);
    let signature = client.key_store.sign(&payload.as_slice());
    let signature = Bytes::from(Vec::from(signature));
    let name = cfg.net_config.name.clone();
    let resp = HandshakeResponse { name, signature };
    let resp_buf = resp.serialize();

    stream.write_u32(resp_buf.len() as u32).await?;
    stream.write_all(resp_buf.as_slice()).await?;

    Ok(())
}
