use std::{collections::HashMap, fs::File, io::{BufReader, Error, ErrorKind}, path, pin::Pin, sync::{Arc, RwLock}};
use log::{debug, warn};
use rustls::{crypto::aws_lc_rs, pki_types, RootCertStore};
use tokio::{io::AsyncWriteExt, net::TcpStream, sync::Mutex, task::JoinSet};
use tokio_rustls::{client::TlsStream, rustls, TlsConnector};
use crate::{config::Config, crypto::KeyStore};

use super::{auth, MessageRef, PinnedMessage};

#[derive(Clone)]
pub struct PinnedHashMap<K, V>(Pin<Box<Arc<RwLock<HashMap<K, V>>>>>);
impl<K, V> PinnedHashMap<K, V> {
    fn new() -> PinnedHashMap<K, V> {
        PinnedHashMap(Box::pin(Arc::new(RwLock::new(HashMap::new()))))
    }
}

#[derive(Clone)]
pub struct PinnedTlsStream(Pin<Box<Arc<Mutex<TlsStream<TcpStream>>>>>);
impl PinnedTlsStream {
    fn new(stream: TlsStream<TcpStream>) -> PinnedTlsStream {
        PinnedTlsStream(Box::pin(Arc::new(Mutex::new(stream))))
    }

}
pub struct Client {
    pub config: Config,
    pub tls_ca_root_cert: RootCertStore,
    pub sock_map: PinnedHashMap<String, PinnedTlsStream>,
    pub key_store: KeyStore,
    do_auth: bool
}

#[derive(Clone)]
pub struct PinnedClient(Pin<Box<Arc<Client>>>);


enum SendDataType<'a> {
    ByteType(MessageRef<'a>),
    SizeType(u32)
}

impl Client {
    fn load_root_ca_cert(path: &String) -> RootCertStore {
        let cert_path = path::Path::new(path.as_str());
        if !cert_path.exists() {
            panic!("Invalid Certificate Path: {}", path);
        }
        let f = match File::open(cert_path) {
            Ok(_f) => _f,
            Err(e) => {
                panic!("Problem reading cert file: {}", e);
            }
        };

        let mut root_cert_store = rustls::RootCertStore::empty();
        let mut pem = BufReader::new(f);
        for cert in rustls_pemfile::certs(&mut pem){
            root_cert_store.add(match cert {
                Ok(_c) => _c,
                Err(e) => { panic!("Error reading cert: {}", e); }
            }).unwrap();
        }
        root_cert_store
    }
    pub fn new(cfg: &Config, key_store: &KeyStore) -> Client {
        Client {
            config: cfg.clone(),
            tls_ca_root_cert: Client::load_root_ca_cert(&cfg.net_config.tls_root_ca_cert_path),
            sock_map: PinnedHashMap::new(),
            do_auth: true,
            key_store: key_store.to_owned()
        }
    }
    pub fn new_unauthenticated(cfg: &Config) -> Client {
        Client {
            config: cfg.clone(),
            tls_ca_root_cert: Client::load_root_ca_cert(&cfg.net_config.tls_root_ca_cert_path),
            sock_map: PinnedHashMap::new(),
            do_auth: false,
            key_store: KeyStore::empty().to_owned()
        }
    }

    
    pub fn into(self) -> PinnedClient {
        PinnedClient(Box::pin(Arc::new(self)))
    }
}

impl PinnedClient {
    async fn connect(client: &PinnedClient, name: &String) -> Result<PinnedTlsStream, Error>
    {
        let peer = client.0.config.net_config.nodes.get(name).ok_or(ErrorKind::AddrNotAvailable)?;
        // Clones the root cert store. Connect() will be only be called once per node.
        // Or if the connection is dropped and needs to be re-established.
        // So, this should be acceptable.
        let tls_cfg = rustls::ClientConfig::builder_with_provider(aws_lc_rs::default_provider().into())
            .with_safe_default_protocol_versions().unwrap()
            .with_root_certificates(client.0.tls_ca_root_cert.clone())
            .with_no_client_auth();

        let connector = TlsConnector::from(Arc::new(tls_cfg));
        
        let stream = TcpStream::connect(&peer.addr).await?;

        let domain = pki_types::ServerName::try_from(peer.domain.as_str())
            .map_err(|_| Error::new(ErrorKind::InvalidInput, "invalid dnsname"))?
            .to_owned();

        let mut stream = connector.connect(domain, stream).await?;

        if client.0.do_auth {
            auth::handshake_client(&client.0, &mut stream).await?;
        }
        let stream_safe = PinnedTlsStream::new(stream.into());
        client.0.sock_map.0.write().unwrap()
            .insert(name.to_string(), stream_safe.clone());
        Ok(stream_safe)
    }

    async fn get_sock(client: &PinnedClient, name: &String) -> Result<PinnedTlsStream, Error>
    {
        // Is there an open connection?
        let mut sock: Option<PinnedTlsStream> = None;
        {
            let sock_map_reader = client.0.sock_map.0.read().unwrap();
        
            let sock_ = sock_map_reader.get(name);
            if sock_.is_some() {
                sock = Some(sock_.unwrap().clone());
            }
        }

        if sock.is_none() {
            sock = Some(PinnedClient::connect(&client.clone(), name).await?);
        }

        Ok(sock.unwrap())

    }

    
    async fn send_raw<'a>(client: &PinnedClient, name: &String, sock: &PinnedTlsStream, data: SendDataType<'a>) -> Result<(), Error>
    {
        let e = match data {
            SendDataType::ByteType(d) => {
                let mut lsock = sock.0.lock().await;
                lsock.write_all(&d).await
            },
            SendDataType::SizeType(d) => {
                let mut lsock = sock.0.lock().await;
                lsock.write_u32(d).await   // Does this take care of endianness?
            }
        };

        if let Err(e) = e {
            // There is some problem.
            // Reset connection.
            warn!("Problem sending message to {}: {} ... Resetting connection.", name, e);
            let mut lsock = sock.0.lock().await;
            if let Err(e2) = lsock.shutdown().await
            {
                warn!("Problem shutting down socket of {}: {} ... Proceeding anyway", name, e2);
            }
    
            client.0.sock_map.0.write().unwrap()
                .remove(name);
            debug!("Socket removed from sock_map");
            return Err(e);
        }
        Ok(())
    }
    pub async fn send<'b>(client: &PinnedClient, name: &String, data: MessageRef<'b>) -> Result<(), Error> {
        let sock = Self::get_sock(client, name).await?;
        let len = data.len() as u32;

        Self::send_raw(client, name, &sock, SendDataType::SizeType(len)).await?;
        Self::send_raw(client, name, &sock, SendDataType::ByteType(data)).await?;
        Ok(())
    }

    pub async fn reliable_send<'b>(client: &PinnedClient, name: &String, data: MessageRef<'b>) -> Result<(), Error> {
        let mut i = client.0.config.net_config.client_max_retry;
        while i > 0 {
            let done = match Self::send(client, name, data.clone()).await {
                Ok(()) => true,
                Err(_) => {
                    i -= 1;
                    false
                }
            };

            if done {
                return Ok(());
            }
        }

        Err(
            Error::new(ErrorKind::NotConnected, "Could not send within max_retries"))
    }

    /// Sends messages to all peers in `names.`
    /// Returns Ok(n) if >= `min_success` sends could be done.
    /// Else returns Err(n)
    /// Each send will be retried for `rpc_config.client_max_retry` consecutively without delay.
    /// Any clever retry mechanism for the broadcast should be implemented by the caller of this function.
    /// (Such as AIMD.)
    pub async fn broadcast(client: &PinnedClient, names: &Vec<String>, data: &PinnedMessage, min_success: i32) -> Result<i32, i32> {
        let mut handles = JoinSet::new();
        for name in names {
            let _c = client.clone();
            let _n = name.clone();
            let _d = data.clone();
            handles.spawn(async move  {
                let d = _d.as_ref();
                PinnedClient::reliable_send(&_c, &_n, d).await
            });
        }

        let mut successes = 0;

        while let Some(res) = handles.join_next().await {
            match res {
                Ok(_) => { successes += 1; },
                Err(_) => { }
            }
        }

        if successes >= min_success {
            Ok(successes)
        }else{
            Err(successes)
        }

    }
}