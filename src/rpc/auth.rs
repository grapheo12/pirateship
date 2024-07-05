use std::{io::{Error, ErrorKind}, sync::Arc};
use log::debug;
use rand::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream};
use tokio_rustls::{server, client};

use super::{client::Client, server::Server};

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct HandshakeResponse<'a> {
    pub name: &'a [u8],
    pub signature: &'a [u8]
}

/// Authentication handshake
/// Server                              Client
/// -----                               ------
/// Nonce   ---(TLS channel)-------->
///         <-----------------------    <name, Sign(Nonce||name)>
/// if name in keylist && signature verifies against registered pubkey
/// accept connection, or drop.
/// Returns name of peer or error.
pub async fn handshake_server(_server: &Arc<Server>, stream: &mut server::TlsStream<TcpStream>) -> Result<String, Error> {
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
        Err(_) => {
            return Err(Error::new(ErrorKind::InvalidData, "invalid response; deserialize error")); 
        }
    };

    // TODO: Do crypto verify
    let name = 
        String::from(std::str::from_utf8(resp.name).unwrap_or(""));
    if name == "" {
        return Err(Error::new(ErrorKind::InvalidData, "unknown peer"))
    }

    Ok(name)
}

pub async fn handshake_client(client: &Arc<Client>, stream: &mut client::TlsStream<TcpStream>) -> Result<(), Error> {
    let nonce = stream.read_u32().await?;
    debug!("Received nonce: {}", nonce);
    // TODO: Do crypto signing
    let signature = &[0u8; 64];
    let name = client.config.name.clone();
    let resp = HandshakeResponse { name: name.as_bytes(), signature };
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