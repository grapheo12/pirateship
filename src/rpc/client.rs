use std::{collections::HashMap, fs::File, io::{BufReader, Error, ErrorKind}, path, sync::{Arc, Mutex, RwLock}};
use log::{debug, warn};
use rustls::{crypto::aws_lc_rs, pki_types, RootCertStore};
use tokio::{io::AsyncWriteExt, net::TcpStream};
use tokio_rustls::{client::TlsStream, rustls, TlsConnector};
use crate::config::NetConfig;


pub struct Client {
    pub config: NetConfig,
    pub tls_ca_root_cert: RootCertStore,
    pub sock_map: Arc<RwLock<
        HashMap<String, Arc<Mutex<
            TlsStream<TcpStream>>
            >>
        >>,
}

enum SendDataType<'a> {
    ByteType(&'a [u8]),
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
    pub fn new(net_cfg: &NetConfig) -> Client {
        Client {
            config: net_cfg.clone(),
            tls_ca_root_cert: Client::load_root_ca_cert(&net_cfg.tls_root_ca_cert_path),
            sock_map: Arc::new(RwLock::new(HashMap::new()))
        }
    }

    async fn connect(client: &Arc<Client>, name: &String) -> Result<
        Arc<Mutex<TlsStream<TcpStream>>>, Error>
    {
        let peer = client.config.nodes.get(name).ok_or(ErrorKind::AddrNotAvailable)?;
        // Clones the root cert store. Connect() will be only be called once per node.
        // Or if the connection is dropped and needs to be re-established.
        // So, this should be acceptable.
        let tls_cfg = rustls::ClientConfig::builder_with_provider(aws_lc_rs::default_provider().into())
            .with_safe_default_protocol_versions().unwrap()
            .with_root_certificates(client.tls_ca_root_cert.clone())
            .with_no_client_auth();

        let connector = TlsConnector::from(Arc::new(tls_cfg));
        
        let stream = TcpStream::connect(&peer.addr).await?;

        let domain = pki_types::ServerName::try_from(peer.domain.as_str())
            .map_err(|_| Error::new(ErrorKind::InvalidInput, "invalid dnsname"))?
            .to_owned();

        let stream = connector.connect(domain, stream).await?;

        let stream_safe = Arc::new(Mutex::new(stream.into()));
        client.sock_map.write().unwrap()
            .insert(name.to_string(), stream_safe.clone());
        Ok(stream_safe)
    }

    async fn get_sock(client: &Arc<Client>, name: &String) -> Result<
        Arc<Mutex<TlsStream<TcpStream>>>, Error>
    {
        // Is there an open connection?
        let sock_map_reader = client.sock_map.read().unwrap();
    
        let sock = match sock_map_reader.get(name) {
            Some(s) => {
                let _s = s.clone();
                drop(sock_map_reader);
                _s
            }
            None => {
                drop(sock_map_reader);
                Client::connect(client, name).await?
            }
        };

        Ok(sock)

    }

    
    async fn send_raw<'a>(client: &Arc<Client>, name: &String, sock: &Arc<Mutex<TlsStream<TcpStream>>>, data: SendDataType<'a>) -> Result<(), Error>
    {
        let e = match data {
            SendDataType::ByteType(d) => {
                sock.lock().unwrap()
                    .write_all(d).await
            },
            SendDataType::SizeType(d) => {
                sock.lock().unwrap()
                    .write_u32(d).await   // Does this take care of endianness?
            }
        };

        if let Err(e) = e {
            // There is some problem.
            // Reset connection.
            warn!("Problem sending message to {}: {} ... Resetting connection.", name, e);
            if let Err(e2) = sock.lock().unwrap()
                .shutdown().await
            {
                warn!("Problem shutting down socket of {}: {} ... Proceeding anyway", name, e2);
            }
    
            client.sock_map.write().unwrap()
                .remove(name);
            debug!("Socket removed from sock_map");
            return Err(e);
        }
        Ok(())
    }
    pub async fn send(client: &Arc<Client>, name: &String, data: &[u8]) -> Result<(), Error> {
        let sock = Self::get_sock(client, name).await?;
        let len = data.len() as u32;

        Self::send_raw(client, name, &sock, SendDataType::SizeType(len)).await?;
        Self::send_raw(client, name, &sock, SendDataType::ByteType(data)).await?;
        Ok(())
    }

    pub async fn reliable_send(client: &Arc<Client>, name: &String, data: &[u8]) -> Result<(), Error> {
        let mut i = client.config.client_max_retry;
        while i > 0 {
            let done = match Self::send(client, name, data).await {
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
}